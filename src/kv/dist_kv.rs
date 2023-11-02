use async_trait::async_trait;

use crate::{result::WSResult, sys::Sys};

use super::KeyRange;

pub struct SetOptions {
    pub consistent: bool,
}

impl SetOptions {
    pub fn new() -> SetOptions {
        SetOptions { consistent: false }
    }
    pub fn set_consistent(mut self, consistent: bool) -> Self {
        self.consistent = consistent;
        self
    }
}

#[async_trait]
pub trait DistKV {
    async fn get<'a>(&'a self, sys: &Sys, key_range: KeyRange<'a>) -> WSResult<Option<Vec<u8>>>;
    async fn set(
        &self,
        sys: &Sys,
        kvs: Vec<(Vec<u8>, Vec<u8>)>,
        opts: SetOptions,
    ) -> WSResult<Option<Vec<(Vec<u8>, Vec<u8>)>>>;
}
