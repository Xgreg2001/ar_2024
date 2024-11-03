use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use rand::Rng;

use crate::*;

pub struct Server {
    socket: UdpSocket,
    auth_tokens: Arc<Mutex<HashMap<u64, u64>>>,
    open_files: Arc<Mutex<HashMap<u64, File>>>,
}

impl Server {
    pub fn new(bind_addr: &str) -> Result<Self, std::io::Error> {
        let socket = UdpSocket::bind(bind_addr)?;
        socket.set_nonblocking(true)?;
        Ok(Server {
            socket,
            auth_tokens: Arc::new(Mutex::new(HashMap::new())),
            open_files: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn run(&self) -> Result<(), std::io::Error> {
        let mut buf = [0u8; 4096];
        loop {
            match self.socket.recv_from(&mut buf) {
                Ok((n, src)) => {
                    let request_data = &buf[..n];
                    let request: Request = bincode::deserialize(request_data).unwrap();
                    let response = self.handle_request(request);
                    let response_data = bincode::serialize(&response).unwrap();
                    self.socket.send_to(&response_data, src)?;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(100));
                }
                Err(e) => {
                    eprintln!("Error receiving data: {}", e);
                }
            }
        }
    }

    fn handle_request(&self, request: Request) -> Response {
        let mut auth_tokens = self.auth_tokens.lock().unwrap();
        if !auth_tokens.contains_key(&request.auth_token) {
            auth_tokens.insert(request.auth_token, 0);
        }

        let sequence_number = request.sequence_number;

        let result = match request.operation {
            Operation::Open => self.handle_open(request.arguments),
            Operation::Read => self.handle_read(request.arguments),
            Operation::Write => self.handle_write(request.arguments),
            Operation::Lseek => self.handle_lseek(request.arguments),
            Operation::Chmod => self.handle_chmod(request.arguments),
            Operation::Unlink => self.handle_unlink(request.arguments),
            Operation::Rename => self.handle_rename(request.arguments),
        };

        match result {
            Ok(data) => Response {
                sequence_number,
                status: 0,
                data,
            },
            Err(_) => Response {
                sequence_number,
                status: 1,
                data: Vec::new(),
            },
        }
    }

    fn handle_open(&self, arguments: Vec<u8>) -> Result<Vec<u8>, ()> {
        let args: OpenArgs = bincode::deserialize(&arguments).unwrap();
        let file_result = match args.mode.as_str() {
            "r" => File::open(&args.pathname),
            "w" => File::create(&args.pathname),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid mode",
            )),
        };
        match file_result {
            Ok(file) => {
                let file_id: u64 = rand::thread_rng().gen();
                self.open_files.lock().unwrap().insert(file_id, file);
                let reply = OpenReply { file_id, result: 0 };
                Ok(bincode::serialize(&reply).unwrap())
            }
            Err(e) => {
                let reply = OpenReply {
                    file_id: 0,
                    result: e.raw_os_error().unwrap_or(-1),
                };
                Ok(bincode::serialize(&reply).unwrap())
            }
        }
    }

    fn handle_read(&self, arguments: Vec<u8>) -> Result<Vec<u8>, ()> {
        let args: ReadArgs = bincode::deserialize(&arguments).unwrap();
        let mut open_files = self.open_files.lock().unwrap();
        if let Some(file) = open_files.get_mut(&args.file_id) {
            let mut buffer = vec![0u8; args.count];
            match file.read(&mut buffer) {
                Ok(bytes_read) => {
                    buffer.truncate(bytes_read);
                    let reply = ReadReply {
                        data: buffer,
                        bytes_read,
                        result: 0,
                    };
                    Ok(bincode::serialize(&reply).unwrap())
                }
                Err(e) => {
                    let reply = ReadReply {
                        data: Vec::new(),
                        bytes_read: 0,
                        result: e.raw_os_error().unwrap_or(-1),
                    };
                    Ok(bincode::serialize(&reply).unwrap())
                }
            }
        } else {
            let reply = ReadReply {
                data: Vec::new(),
                bytes_read: 0,
                result: -1, // File not found
            };
            Ok(bincode::serialize(&reply).unwrap())
        }
    }

    fn handle_write(&self, arguments: Vec<u8>) -> Result<Vec<u8>, ()> {
        let args: WriteArgs = bincode::deserialize(&arguments).unwrap();
        let mut open_files = self.open_files.lock().unwrap();
        if let Some(file) = open_files.get_mut(&args.file_id) {
            match file.write(&args.data) {
                Ok(bytes_written) => {
                    let reply = WriteReply {
                        bytes_written,
                        result: 0,
                    };
                    Ok(bincode::serialize(&reply).unwrap())
                }
                Err(e) => {
                    let reply = WriteReply {
                        bytes_written: 0,
                        result: e.raw_os_error().unwrap_or(-1),
                    };
                    Ok(bincode::serialize(&reply).unwrap())
                }
            }
        } else {
            let reply = WriteReply {
                bytes_written: 0,
                result: -1, // File not found
            };
            Ok(bincode::serialize(&reply).unwrap())
        }
    }

    fn handle_lseek(&self, arguments: Vec<u8>) -> Result<Vec<u8>, ()> {
        let args: LseekArgs = bincode::deserialize(&arguments).unwrap();
        let mut open_files = self.open_files.lock().unwrap();
        if let Some(file) = open_files.get_mut(&args.file_id) {
            let whence = match args.whence {
                0 => SeekFrom::Start(args.offset as u64),
                1 => SeekFrom::Current(args.offset),
                2 => SeekFrom::End(args.offset),
                _ => {
                    let reply = LseekReply {
                        offset: 0,
                        result: -1, // Invalid 'whence' parameter
                    };
                    return Ok(bincode::serialize(&reply).unwrap());
                }
            };
            match file.seek(whence) {
                Ok(new_offset) => {
                    let reply = LseekReply {
                        offset: new_offset,
                        result: 0,
                    };
                    Ok(bincode::serialize(&reply).unwrap())
                }
                Err(e) => {
                    let reply = LseekReply {
                        offset: 0,
                        result: e.raw_os_error().unwrap_or(-1),
                    };
                    Ok(bincode::serialize(&reply).unwrap())
                }
            }
        } else {
            let reply = LseekReply {
                offset: 0,
                result: -1, // File not found
            };
            Ok(bincode::serialize(&reply).unwrap())
        }
    }

    fn handle_chmod(&self, arguments: Vec<u8>) -> Result<Vec<u8>, ()> {
        use std::os::unix::fs::PermissionsExt;
        let args: ChmodArgs = bincode::deserialize(&arguments).unwrap();
        match std::fs::set_permissions(&args.pathname, std::fs::Permissions::from_mode(args.mode)) {
            Ok(_) => {
                let reply = SimpleReply { result: 0 };
                Ok(bincode::serialize(&reply).unwrap())
            }
            Err(e) => {
                let reply = SimpleReply {
                    result: e.raw_os_error().unwrap_or(-1),
                };
                Ok(bincode::serialize(&reply).unwrap())
            }
        }
    }

    fn handle_unlink(&self, arguments: Vec<u8>) -> Result<Vec<u8>, ()> {
        let args: UnlinkArgs = bincode::deserialize(&arguments).unwrap();
        match std::fs::remove_file(&args.pathname) {
            Ok(_) => {
                let reply = SimpleReply { result: 0 };
                Ok(bincode::serialize(&reply).unwrap())
            }
            Err(e) => {
                let reply = SimpleReply {
                    result: e.raw_os_error().unwrap_or(-1),
                };
                Ok(bincode::serialize(&reply).unwrap())
            }
        }
    }

    fn handle_rename(&self, arguments: Vec<u8>) -> Result<Vec<u8>, ()> {
        let args: RenameArgs = bincode::deserialize(&arguments).unwrap();
        match std::fs::rename(&args.oldpath, &args.newpath) {
            Ok(_) => {
                let reply = SimpleReply { result: 0 };
                Ok(bincode::serialize(&reply).unwrap())
            }
            Err(e) => {
                let reply = SimpleReply {
                    result: e.raw_os_error().unwrap_or(-1),
                };
                Ok(bincode::serialize(&reply).unwrap())
            }
        }
    }
}
