use crate::result::{WSError, WSResult, WsIoErr};
use std::fs;
use std::io::{self, Cursor, Read, Seek, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use walkdir::WalkDir;
use zip::{result::ZipError, write::FileOptions, ZipWriter};

use super::{non_null, SendNonNull}; // 添加这一行以引入PermissionsExt trait 针对下方的.mode()报错   曾俊

pub fn unzip_data_2_path(p: impl AsRef<Path>, data: Vec<u8>) -> WSResult<()> {
    // remove old dir
    let p = p.as_ref();
    if p.exists() {
        fs::remove_dir_all(p).unwrap();
    }
    // create new dir
    fs::create_dir_all(p).unwrap();
    // unzip
    match zip_extract::extract(Cursor::new(data), &p, false) {
        Ok(_) => (),
        Err(e) => {
            return Err(WsIoErr::Zip(e).into());
        }
    }

    Ok(())
}

// pub fn zip_dir_2_data(&self, p: impl AsRef<Path>) -> WSResult<Vec<u8>> {
//     let p = p.as_ref();
//     let mut data = Vec::new();
//     let writer = Cursor::new(&mut data);
//     let mut list = self.list_dir_with_prefix(p, p.to_str().unwrap())?;
//     self.zip_dir(
//         &mut list.iter_mut(),
//         p.to_str().unwrap(),
//         ZipWriter::new(&data),
//         zip::CompressionMethod::Stored,
//     )
//     .map_err(|e| WsIoErr::Zip2(e))?;
//     Ok(data)
// }

fn zip_dir<T>(
    it: &mut dyn Iterator<Item = walkdir::DirEntry>,
    prefix: &Path,
    writer: T,
    method: zip::CompressionMethod,
) -> WSResult<()>
where
    T: Write + Seek,
{
    let mut zip = ZipWriter::new(writer);
    let prefix = Path::new(prefix);
    let mut buffer = Vec::new();
    for entry in it {
        let path = entry.path();
        let name = path.strip_prefix(prefix).unwrap();
        let path_as_string = name.to_str().unwrap().to_owned();

        let options = FileOptions::default()
            .compression_method(method)
            .unix_permissions(
                entry
                    .metadata()
                    .map_err(|e| WSError::from(e))?
                    .permissions()
                    .mode(), // 修改！！！   在文件上方导入了一个PermissionsExt trait     曾俊
            );

        // Write file or directory explicitly
        // Some unzip tools unzip files with directory paths correctly, some do not!
        if path.is_file() {
            tracing::debug!("adding file {path:?} as {name:?} ...");
            zip.start_file(path_as_string, options)
                .map_err(|e| WSError::from(WsIoErr::Zip2(e)))?;
            let mut f = fs::File::open(path).map_err(|e| WSError::from(WsIoErr::Io(e)))?;

            let _ = f
                .read_to_end(&mut buffer)
                .map_err(|e| WSError::from(WsIoErr::Io(e)))?;
            zip.write_all(&buffer)
                .map_err(|e| WSError::from(WsIoErr::Io(e)))?;
            buffer.clear();
        } else if !name.as_os_str().is_empty() {
            // Only if not root! Avoids path spec / warning
            // and mapname conversion failed error on unzip
            tracing::debug!("adding dir {path_as_string:?} as {name:?} ...");
            zip.add_directory(path_as_string, options)
                .map_err(|e| WSError::from(WsIoErr::Zip2(e)))?;
        }
    }
    let _ = zip.finish().map_err(|e| WSError::from(WsIoErr::Zip2(e)))?;
    Ok(())
}

pub fn zip_dir_2_mem(src_dir: &Path, method: zip::CompressionMethod) -> WSResult<Vec<u8>> {
    if !src_dir.is_dir() {
        return Err(WsIoErr::Zip2(ZipError::FileNotFound).into());
    }

    let mut data = Vec::new();
    let walkdir = WalkDir::new(src_dir);
    let it = walkdir.into_iter();

    zip_dir(
        &mut it.filter_map(|e| e.ok()),
        src_dir,
        io::Cursor::new(&mut data),
        method,
    )?;

    Ok(data)
}

pub async fn zip_dir_2_file(
    src_dir: impl AsRef<Path>,
    method: zip::CompressionMethod,
    dst_file: &mut std::fs::File,
) -> WSResult<()> {
    // // if !src_dir.is_dir() {    //泛型参数不会自动解引用    曾俊
    // if !src_dir.as_ref().is_dir() {
    //     return Err(WsIoErr::Zip2(ZipError::FileNotFound).into());
    // }
    let src_dir = src_dir.as_ref().to_path_buf(); // 将 src_dir 转换为 PathBuf

    if !src_dir.is_dir() {
        return Err(WsIoErr::Zip2(ZipError::FileNotFound).into());
    }

    let walkdir = WalkDir::new(src_dir.clone());
    let it = walkdir.into_iter();

    // 使用阻塞线程执行 zip 操作，因为 zip 库不支持异步 IO
    let dst_file_ptr = unsafe { SendNonNull(non_null(dst_file)) };
    tokio::task::spawn_blocking(move || {
        zip_dir(
            &mut it.filter_map(|e| e.ok()),
            // src_dir,          //泛型参数不会自动解引用    曾俊
            src_dir.as_ref(),
            unsafe { dst_file_ptr.as_mut() },
            method,
        )
    })
    .await
    .map_err(|e| {
        WsIoErr::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to execute zip task: {}", e),
        ))
    })??;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::{tempdir, NamedTempFile};

    #[test]
    fn test_zip_and_unzip_single_file() -> WSResult<()> {
        // 创建临时目录
        let src_dir = tempdir()?;
        let src_path = src_dir.path();

        // 创建测试文件
        let test_file_path = src_path.join("test.txt");
        let test_content = b"Hello, World!";
        fs::write(&test_file_path, test_content)?;

        // 创建临时输出文件
        let mut output_file = NamedTempFile::new()?;

        // 执行压缩
        tokio::runtime::Runtime::new()?.block_on(async {
            zip_dir_2_file(
                src_path,
                zip::CompressionMethod::Stored,
                output_file.as_file_mut(),
                // output_file.as_file_mut(),
            )
            .await
        })?;

        // 读取压缩后的数据
        let zip_data = fs::read(output_file.path())?;

        // 创建临时解压目录
        let extract_dir = tempdir()?;

        // 执行解压
        unzip_data_2_path(extract_dir.path(), zip_data)?;

        // 验证解压后的文件内容
        let extracted_content = fs::read(extract_dir.path().join("test.txt"))?;
        assert_eq!(extracted_content, test_content);

        Ok(())
    }

    #[test]
    fn test_zip_and_unzip_directory() -> WSResult<()> {
        // 创建临时目录结构
        let src_dir = tempdir()?;
        let src_path = src_dir.path();

        // 创建子目录和文件
        let sub_dir = src_path.join("subdir");
        fs::create_dir(&sub_dir)?;

        let test_files = vec![
            ("test1.txt", b"Content 1"),
            ("subdir/test2.txt", b"Content 2"),
        ];

        for (path, content) in test_files.iter() {
            let file_path = src_path.join(path);
            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&file_path, content)?;
        }

        // 创建临时输出文件
        let mut output_file = NamedTempFile::new()?;

        // 执行压缩
        tokio::runtime::Runtime::new()?.block_on(async {
            zip_dir_2_file(
                src_path,
                zip::CompressionMethod::Stored,
                output_file.as_file_mut(),
            )
            .await
        })?;

        // 读取压缩后的数据
        let zip_data = fs::read(output_file.path())?;

        // 创建临时解压目录
        let extract_dir = tempdir()?;

        // 执行解压
        unzip_data_2_path(extract_dir.path(), zip_data)?;

        // 验证解压后的文件内容
        for (path, content) in test_files.iter() {
            let extracted_content = fs::read(extract_dir.path().join(path))?;
            assert_eq!(&extracted_content, content);
        }

        Ok(())
    }

    #[test]
    fn test_zip_empty_directory() -> WSResult<()> {
        // 创建空临时目录
        let src_dir = tempdir()?;

        // 创建临时输出文件
        let mut output_file = NamedTempFile::new()?;

        // 执行压缩
        tokio::runtime::Runtime::new()?.block_on(async {
            zip_dir_2_file(
                src_dir.path(),
                zip::CompressionMethod::Stored,
                output_file.as_file_mut(),
            )
            .await
        })?;

        // 读取压缩后的数据
        let zip_data = fs::read(output_file.path())?;

        // 创建临时解压目录
        let extract_dir = tempdir()?;

        // 执行解压
        unzip_data_2_path(extract_dir.path(), zip_data)?;

        // 验证目录是否为空
        let entries = fs::read_dir(extract_dir.path())?;
        assert_eq!(entries.count(), 0);

        Ok(())
    }
}
