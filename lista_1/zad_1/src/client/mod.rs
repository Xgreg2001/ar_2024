use std::io::{Error, ErrorKind};
use std::net::{ToSocketAddrs, UdpSocket};
use std::time::Duration;

use rand::Rng;

use crate::*;

pub struct Client {
    socket: UdpSocket,
    server_addr: String,
    auth_token: u64,
}

impl Client {
    pub fn new(server_addr: &str) -> Result<Self, std::io::Error> {
        let socket = UdpSocket::bind("0.0.0.0:0")?; // Any available port
        socket.set_read_timeout(Some(Duration::from_secs(5)))?;
        let auth_token: u64 = rand::thread_rng().gen();
        Ok(Client {
            socket,
            server_addr: server_addr.to_string(),
            auth_token,
        })
    }

    pub fn send_request(&self, request: &Request) -> Result<Response, std::io::Error> {
        let request_data = bincode::serialize(&request).unwrap();
        self.socket.send_to(
            &request_data,
            &self.server_addr.to_socket_addrs()?.next().unwrap(),
        )?;

        let mut buf = [0u8; 4096];
        match self.socket.recv_from(&mut buf) {
            Ok((n, _)) => {
                let response: Response = bincode::deserialize(&buf[..n]).unwrap();
                if response.sequence_number == request.sequence_number {
                    Ok(response)
                } else {
                    Err(Error::new(ErrorKind::Other, "Sequence number mismatch"))
                }
            }
            Err(e) => Err(e),
        }
    }

    fn send_request_with_retry(&self, request: &Request) -> Result<Response, std::io::Error> {
        if let Ok(response) = self.send_request(request) {
            return Ok(response);
        }
        // Retry once
        if let Ok(response) = self.send_request(request) {
            return Ok(response);
        }
        Err(Error::new(ErrorKind::TimedOut, "Request timed out"))
    }

    pub fn open(&self, pathname: &str, mode: &str) -> Result<RemoteFile, std::io::Error> {
        let seq_num: u64 = rand::thread_rng().gen();
        let args = OpenArgs {
            pathname: pathname.to_string(),
            mode: mode.to_string(),
        };
        let arguments = bincode::serialize(&args).unwrap();
        let request = Request {
            auth_token: self.auth_token,
            sequence_number: seq_num,
            operation: Operation::Open,
            arguments,
        };
        let response = self.send_request_with_retry(&request)?;
        if response.status == 0 {
            let reply: OpenReply = bincode::deserialize(&response.data).unwrap();
            if reply.result == 0 {
                Ok(RemoteFile {
                    client: self,
                    file_id: reply.file_id,
                })
            } else {
                Err(Error::from_raw_os_error(reply.result))
            }
        } else {
            Err(Error::new(ErrorKind::Other, "Operation failed"))
        }
    }

    fn chmod(&self, pathname: &str, mode: u32) -> Result<(), std::io::Error> {
        let seq_num: u64 = rand::thread_rng().gen();
        let args = ChmodArgs {
            pathname: pathname.to_string(),
            mode,
        };
        let arguments = bincode::serialize(&args).unwrap();
        let request = Request {
            auth_token: self.auth_token,
            sequence_number: seq_num,
            operation: Operation::Chmod,
            arguments,
        };
        let response = self.send_request_with_retry(&request)?;
        if response.status == 0 {
            let reply: SimpleReply = bincode::deserialize(&response.data).unwrap();
            if reply.result == 0 {
                Ok(())
            } else {
                Err(Error::from_raw_os_error(reply.result))
            }
        } else {
            Err(Error::new(ErrorKind::Other, "Operation failed"))
        }
    }

    fn unlink(&self, pathname: &str) -> Result<(), std::io::Error> {
        let seq_num: u64 = rand::thread_rng().gen();
        let args = UnlinkArgs {
            pathname: pathname.to_string(),
        };
        let arguments = bincode::serialize(&args).unwrap();
        let request = Request {
            auth_token: self.auth_token,
            sequence_number: seq_num,
            operation: Operation::Unlink,
            arguments,
        };
        let response = self.send_request_with_retry(&request)?;
        if response.status == 0 {
            let reply: SimpleReply = bincode::deserialize(&response.data).unwrap();
            if reply.result == 0 {
                Ok(())
            } else {
                Err(Error::from_raw_os_error(reply.result))
            }
        } else {
            Err(Error::new(ErrorKind::Other, "Operation failed"))
        }
    }

    fn rename(&self, oldpath: &str, newpath: &str) -> Result<(), std::io::Error> {
        let seq_num: u64 = rand::thread_rng().gen();
        let args = RenameArgs {
            oldpath: oldpath.to_string(),
            newpath: newpath.to_string(),
        };
        let arguments = bincode::serialize(&args).unwrap();
        let request = Request {
            auth_token: self.auth_token,
            sequence_number: seq_num,
            operation: Operation::Rename,
            arguments,
        };
        let response = self.send_request_with_retry(&request)?;
        if response.status == 0 {
            let reply: SimpleReply = bincode::deserialize(&response.data).unwrap();
            if reply.result == 0 {
                Ok(())
            } else {
                Err(Error::from_raw_os_error(reply.result))
            }
        } else {
            Err(Error::new(ErrorKind::Other, "Operation failed"))
        }
    }
}

pub struct RemoteFile<'a> {
    client: &'a Client,
    file_id: u64,
}

impl<'a> RemoteFile<'a> {
    pub fn read(&self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        let seq_num: u64 = rand::thread_rng().gen();
        let args = ReadArgs {
            file_id: self.file_id,
            count: buf.len(),
        };
        let arguments = bincode::serialize(&args).unwrap();
        let request = Request {
            auth_token: self.client.auth_token,
            sequence_number: seq_num,
            operation: Operation::Read,
            arguments,
        };
        let response = self.client.send_request_with_retry(&request)?;
        if response.status == 0 {
            let reply: ReadReply = bincode::deserialize(&response.data).unwrap();
            if reply.result == 0 {
                buf[..reply.bytes_read].copy_from_slice(&reply.data);
                Ok(reply.bytes_read)
            } else {
                Err(Error::from_raw_os_error(reply.result))
            }
        } else {
            Err(Error::new(ErrorKind::Other, "Operation failed"))
        }
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize, std::io::Error> {
        let seq_num: u64 = rand::thread_rng().gen();
        let args = WriteArgs {
            file_id: self.file_id,
            data: buf.to_vec(),
        };
        let arguments = bincode::serialize(&args).unwrap();
        let request = Request {
            auth_token: self.client.auth_token,
            sequence_number: seq_num,
            operation: Operation::Write,
            arguments,
        };
        let response = self.client.send_request_with_retry(&request)?;
        if response.status == 0 {
            let reply: WriteReply = bincode::deserialize(&response.data).unwrap();
            if reply.result == 0 {
                Ok(reply.bytes_written)
            } else {
                Err(Error::from_raw_os_error(reply.result))
            }
        } else {
            Err(Error::new(ErrorKind::Other, "Operation failed"))
        }
    }

    pub fn lseek(&self, offset: i64, whence: i32) -> Result<u64, std::io::Error> {
        let seq_num: u64 = rand::thread_rng().gen();
        let args = LseekArgs {
            file_id: self.file_id,
            offset,
            whence,
        };
        let arguments = bincode::serialize(&args).unwrap();
        let request = Request {
            auth_token: self.client.auth_token,
            sequence_number: seq_num,
            operation: Operation::Lseek,
            arguments,
        };
        let response = self.client.send_request_with_retry(&request)?;
        if response.status == 0 {
            let reply: LseekReply = bincode::deserialize(&response.data).unwrap();
            if reply.result == 0 {
                Ok(reply.offset)
            } else {
                Err(Error::from_raw_os_error(reply.result))
            }
        } else {
            Err(Error::new(ErrorKind::Other, "Operation failed"))
        }
    }
}
