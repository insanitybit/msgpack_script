use rmpv::ValueRef;
use rmpv::decode::read_value_ref;
use rmp::decode::{read_str_from_slice, DecodeStringError};
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::thread;
use std::time::Duration;
use std::io::{Cursor, Write};
use std::error::Error;
use std::path::PathBuf;
use std::fs::File;
use serdeconv::from_msgpack_slice;

use serde_json;
use serde_json::{Value, to_string_pretty};

use two_lock_queue::{unbounded, Sender, Receiver, RecvTimeoutError};

use completion::*;

pub struct Msgunpacker {
    completion_handler: CompletionHandler
}

impl Msgunpacker {

    pub fn new(completion_handler: CompletionHandler) -> Msgunpacker {
        Msgunpacker {
            completion_handler
        }
    }

    pub fn unpack(&self, path: PathBuf, contents: Vec<u8>) {
        let mut buf = &contents[..];
        let p: Value = from_msgpack_slice(buf).unwrap();

        let js  = to_string_pretty(&p).unwrap();

        let mut new_path = path.file_stem().unwrap().to_str().unwrap();
        let new_path = format!("./{}.json", new_path);

        let mut f = File::create(new_path).unwrap();
        f.write_all(js.as_ref());

        self.completion_handler.success();
    }

    fn route_msg(&mut self, msg: MsgunpackerMessage) {
        match msg {
            MsgunpackerMessage::Unpack {path, contents} => self.unpack(path, contents)
        }
    }
}


enum MsgunpackerMessage {
    Unpack { path: PathBuf, contents: Vec<u8>}
}

#[derive(Clone)]
pub struct MsgunpackerActor {
    sender: Sender<MsgunpackerMessage>,
}

impl MsgunpackerActor {
    pub fn new<F>(new: F, worker_count: usize) -> MsgunpackerActor
        where F: Fn(MsgunpackerActor) -> Msgunpacker,
    {
        let (sender, receiver) = unbounded();

        let msgpack_actor = MsgunpackerActor {
            sender,
        };

        for _ in 0..worker_count {
            let receiver = receiver.clone();
            let mut actor = new(msgpack_actor.clone());
            thread::spawn(
                move || {
                    loop {
                        match receiver.recv_timeout(Duration::from_secs(60)) {
                            Ok(msg) => {
                                actor.route_msg(msg);
                            }
                            Err(RecvTimeoutError::Disconnected) => {
                                break
                            }
                            Err(RecvTimeoutError::Timeout) => {}
                        }
                    }
                });
        }

        msgpack_actor
    }

    pub fn unpack(&self,  path: PathBuf, contents: Vec<u8>) {
        self.sender.send(MsgunpackerMessage::Unpack {
            path,
            contents
        });
    }
}
