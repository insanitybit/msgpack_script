use libflate::gzip::Decoder;

use std::fs::File;
use std::io::prelude::*;
use std::io::Cursor;
use std::io;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use two_lock_queue::{unbounded, Sender, Receiver, RecvTimeoutError};

use msgpack::*;
use completion::*;

pub struct Gunzipper {
    unpacker: MsgunpackerActor,
    completion_handler: CompletionHandler
}

impl Gunzipper {
    pub fn new(unpacker: MsgunpackerActor, completion_handler: CompletionHandler) -> Gunzipper {
        Gunzipper {
            unpacker,
            completion_handler
        }
    }

    pub fn gunzip(&mut self, path: PathBuf) {
        let contents = match File::open(&path) {
            Ok(contents) => contents,
            Err(e) => {
                println!("Failed to open file at {} with {}", path.display(), e);
                self.completion_handler.failed();
                return;
            }
        };

        let mut buf = Cursor::new(Vec::with_capacity(100_000));
        let mut decoder = match Decoder::new(contents) {
            Ok(dec) => dec,
            Err(e) => {
                println!("Failed to create decoder for file at {} with {}", path.display(), e);
                self.completion_handler.failed();
                return;
            }
        };

        if let Err(e) = io::copy(&mut decoder, &mut buf) {
            println!("Failed to decode file at {} with {}", path.display(), e);
            self.completion_handler.failed();
            return;
        };

        let contents = buf.into_inner();
        self.unpacker.unpack(contents);
    }

    fn route_msg(&mut self, msg: GunzipperMessage) {
        match msg {
            GunzipperMessage::Gunzip { path } => self.gunzip(path)
        }
    }
}

enum GunzipperMessage {
    Gunzip { path: PathBuf }
}

#[derive(Clone)]
pub struct GunzipperActor {
    sender: Sender<GunzipperMessage>,
}

impl GunzipperActor {
    pub fn new<F>(new: F, worker_count: usize) -> GunzipperActor
        where F: Fn(GunzipperActor) -> Gunzipper,
    {
        let (sender, receiver) = unbounded();

        let gunzip_actor = GunzipperActor {
            sender,
        };

        for _ in 0..worker_count {
            let receiver = receiver.clone();
            let mut actor = new(gunzip_actor.clone());
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

        gunzip_actor
    }

    pub fn gunzip(&self, path: PathBuf) {
        self.sender.send(GunzipperMessage::Gunzip {
            path
        });
    }
}