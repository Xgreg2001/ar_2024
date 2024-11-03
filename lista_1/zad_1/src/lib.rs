pub mod client;
pub mod server;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Operation {
    Open,
    Read,
    Write,
    Lseek,
    Chmod,
    Unlink,
    Rename,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Request {
    pub(crate) auth_token: u64,
    pub(crate) sequence_number: u64,
    pub(crate) operation: Operation,
    pub(crate) arguments: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    pub(crate) sequence_number: u64,
    pub(crate) status: u8, // 0 for success, non-zero for error codes
    pub(crate) data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OpenArgs {
    pub(crate) pathname: String,
    pub(crate) mode: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OpenReply {
    pub(crate) file_id: u64,
    pub(crate) result: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReadArgs {
    pub(crate) file_id: u64,
    pub(crate) count: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReadReply {
    pub(crate) data: Vec<u8>,
    pub(crate) bytes_read: usize,
    pub(crate) result: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WriteArgs {
    pub(crate) file_id: u64,
    pub(crate) data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WriteReply {
    pub(crate) bytes_written: usize,
    pub(crate) result: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LseekArgs {
    pub(crate) file_id: u64,
    pub(crate) offset: i64,
    pub(crate) whence: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LseekReply {
    pub(crate) offset: u64,
    pub(crate) result: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChmodArgs {
    pub(crate) pathname: String,
    pub(crate) mode: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UnlinkArgs {
    pub(crate) pathname: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RenameArgs {
    pub(crate) oldpath: String,
    pub(crate) newpath: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SimpleReply {
    pub(crate) result: i32,
}
