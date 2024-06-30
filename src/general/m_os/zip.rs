use super::OperatingSystem;
use crate::result::{ErrCvt, WSError, WSResult, WsIoErr};
use std::{
    fs::{self, File},
    io::{self, Cursor, Read, Seek, Write},
    os::unix::fs::PermissionsExt,
    path::Path,
};
use walkdir::WalkDir;
use zip::{result::ZipError, write::FileOptions, ZipWriter};

impl OperatingSystem {
    pub fn unzip_data_2_path(&self, p: impl AsRef<Path>, data: Vec<u8>) -> WSResult<()> {
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
        let mut zip = zip::ZipWriter::new(writer);
        // let options = FileOptions::default()
        //     .compression_method(method)
        //     .unix_permissions(0o755);

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
                        .mode(),
                );

            // Write file or directory explicitly
            // Some unzip tools unzip files with directory paths correctly, some do not!
            if path.is_file() {
                tracing::debug!("adding file {path:?} as {name:?} ...");
                zip.start_file(path_as_string, options)
                    .map_err(|e| WSError::from(WsIoErr::Zip2(e)))?;
                let mut f = File::open(path).map_err(|e| WSError::from(WsIoErr::Io(e)))?;

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

    pub fn zip_dir_2_data(
        &self,
        src_dir: &Path,
        method: zip::CompressionMethod,
    ) -> WSResult<Vec<u8>> {
        if !Path::new(src_dir).is_dir() {
            return Err(WsIoErr::Zip2(ZipError::FileNotFound).into());
        }

        let mut data = Vec::new();

        let walkdir = WalkDir::new(src_dir);
        let it = walkdir.into_iter();

        Self::zip_dir(
            &mut it.filter_map(|e| e.ok()),
            src_dir,
            io::Cursor::new(&mut data),
            method,
        )?;

        Ok(data)
    }
}
