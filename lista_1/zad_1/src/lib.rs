pub mod client;
pub mod server;

use std::fmt::Display;

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

impl Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Request {{ auth_token: {}, sequence_number: {}, operation: ",
            self.auth_token, self.sequence_number
        )?;
        match self.operation {
            Operation::Open => {
                let args: OpenArgs = bincode::deserialize(&self.arguments).unwrap();
                write!(f, "Open, arguments: {:?}", args)?;
            }
            Operation::Read => {
                let args: ReadArgs = bincode::deserialize(&self.arguments).unwrap();
                write!(f, "Read, arguments: {:?}", args)?;
            }
            Operation::Write => {
                let args: WriteArgs = bincode::deserialize(&self.arguments).unwrap();
                write!(f, "Write, arguments: {:?}", args)?;
            }
            Operation::Lseek => {
                let args: LseekArgs = bincode::deserialize(&self.arguments).unwrap();
                write!(f, "Lseek, arguments: {:?}", args)?;
            }
            Operation::Chmod => {
                let args: ChmodArgs = bincode::deserialize(&self.arguments).unwrap();
                write!(f, "Chmod, arguments: {:?}", args)?;
            }
            Operation::Unlink => {
                let args: UnlinkArgs = bincode::deserialize(&self.arguments).unwrap();
                write!(f, "Unlink, arguments: {:?}", args)?;
            }
            Operation::Rename => {
                let args: RenameArgs = bincode::deserialize(&self.arguments).unwrap();
                write!(f, "Rename, arguments: {:?}", args)?;
            }
        }

        write!(f, " }}")
    }
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
