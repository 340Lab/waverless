use std::{
    fs::File,
    io::{Read, Seek},
    os::fd::AsRawFd,
    path::PathBuf,
    sync::Arc,
};

use async_trait::async_trait;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use ws_derive::LogicalModule;

use crate::{
    result::{ErrCvt, WSResult},
    sys::{FsView, LogicalModule, LogicalModuleNewArgs},
    util::JoinHandleWrapper,
};

lazy_static::lazy_static! {
    static ref FS: Option<FsView>=None;
}
pub fn fs<'a>() -> &'a Fs {
    let res = &*FS as *const Option<FsView> as *mut Option<FsView>;

    unsafe { (*res).as_ref().unwrap().fs() }
}

#[derive(LogicalModule)]
pub struct Fs {
    fd_files: SkipMap<i32, Arc<Mutex<File>>>,
    file_path: PathBuf,
}

#[async_trait]
impl LogicalModule for Fs {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        unsafe {
            let res = &*FS as *const Option<FsView> as *mut Option<FsView>;
            *res = Some(FsView::new(args.logical_modules_ref.clone()));
        }

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
        let f = File::open(self.file_path.join(fname)).map_err(|e| ErrCvt(e).to_ws_io_err())?;
        let fd = f.as_raw_fd();
        let _ = self.fd_files.insert(fd, Arc::new(Mutex::new(f)));
        Ok(fd)
    }
    pub fn close_file(&self, fd: i32) -> WSResult<()> {
        if let Some(_) = self.fd_files.remove(&fd) {}
        Ok(())
    }
    pub fn read_file_at(&self, fd: i32, offset: i32, buf: &mut [u8]) -> WSResult<usize> {
        let f = self.fd_files.get(&fd).unwrap().value().clone();
        let mut f = f.lock();
        let _ = f
            .seek(std::io::SeekFrom::Start(offset as u64))
            .map_err(|e| ErrCvt(e).to_ws_io_err())?;
        // Read into the buffer
        let bytes_read = f.read(buf).map_err(|e| ErrCvt(e).to_ws_io_err())?;
        Ok(bytes_read)
    }
}
