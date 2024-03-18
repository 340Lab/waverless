use async_trait::async_trait;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::{
    fs::File,
    io::{Read, Seek},
    os::fd::AsRawFd,
    path::PathBuf,
    sync::Arc,
};
use ws_derive::LogicalModule;

use crate::{
    result::{ErrCvt, WSResult},
    sys::{LogicalModule, LogicalModuleNewArgs},
    util::JoinHandleWrapper,
};

#[derive(LogicalModule)]
pub struct Fs {
    fd_files: SkipMap<i32, Arc<Mutex<File>>>,
    pub file_path: PathBuf,
}

#[async_trait]
impl LogicalModule for Fs {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            fd_files: SkipMap::new(),
            file_path: args.nodes_config.file_dir.clone(),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let all = vec![];

        Ok(all)
    }
}

impl Fs {
    pub fn open_file(&self, fname: &str) -> WSResult<i32> {
        let fp = self.file_path.join("files").join(fname);
        tracing::debug!("openning file {:?}", fp);
        let f = File::open(fp).map_err(|e| ErrCvt(e).to_ws_io_err())?;
        let fd = f.as_raw_fd();
        let _ = self.fd_files.insert(fd, Arc::new(Mutex::new(f)));
        Ok(fd)
    }
    pub fn close_file(&self, fd: i32) -> WSResult<()> {
        if let Some(_) = self.fd_files.remove(&fd) {}
        Ok(())
    }
    pub fn read_file_at(&self, fd: i32, offset: i32, buf: &mut [u8]) -> WSResult<usize> {
        if let Some(f) = self.fd_files.get(&fd).map(|v| v.value().clone()) {
            let mut f = f.lock();
            let _ = f
                .seek(std::io::SeekFrom::Start(offset as u64))
                .map_err(|e| ErrCvt(e).to_ws_io_err())?;
            // Read into the buffer
            let bytes_read = f.read(buf).map_err(|e| ErrCvt(e).to_ws_io_err())?;

            Ok(bytes_read)
        } else {
            tracing::error!("function read_file_at: invalid fd {}", fd);

            Ok(0)
        }
    }
}
