use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::thread;
use std::time::Duration;

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

    pub fn unpack(&self, contents: Vec<u8>) {
        let mut de = Deserializer::new(&contents[..]);

        let s: String = match Deserialize::deserialize(&mut de) {
            Ok(s) => s,
            Err(e)  => {
                println!("Failed to deserialize from msgpack: {}", e);
                self.completion_handler.failed();
                return
            }
        };

        println!("{}", s);
        self.completion_handler.success();
    }

    fn route_msg(&mut self, msg: MsgunpackerMessage) {
        match msg {
            MsgunpackerMessage::Unpack {contents} => self.unpack(contents)
        }
    }
}


enum MsgunpackerMessage {
    Unpack {contents: Vec<u8>}
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

    pub fn unpack(&self, contents: Vec<u8>) {
        self.sender.send(MsgunpackerMessage::Unpack {
           contents
        });
    }
}