extern crate ignore;
extern crate rmp_serde as rmps;
extern crate serde_json;
extern crate serde;
extern crate libflate;
extern crate walkdir;
extern crate clap;
extern crate threadpool;
extern crate num_cpus;
extern crate spmc;
extern crate two_lock_queue;

use clap::{Arg, App};
use libflate::gzip::Decoder;
use walkdir::WalkDir;
use threadpool::ThreadPool;
use serde::{Deserialize, Serialize};
use rmps::{Deserializer, Serializer};

use std::time::Duration;
use std::io;
use std::sync::mpsc::channel;
use std::path::Path;
use std::fs::File;
use std::io::prelude::*;
use std::io::Cursor;
use std::io::BufWriter;
use std::sync::mpsc::channel as mpsc_channel;
use std::thread;
use std::iter::repeat;

mod gzip;
mod msgpack;
mod completion;

use gzip::*;
use msgpack::*;
use completion::*;

fn main() {
    let matches = App::new("Msgpack printer")
        .version("1.0")
        .author("Colin O'Brien <cobrien@rapid7.com")
        .about("Will print u some msgpack")
        .arg(
            Arg::with_name("input")
                .short("i")
                .long("input")
                .value_name("INPUT_PATH")
                .required(true)
                .help("Sets a custom config file"),
        )
        .arg(
            Arg::with_name("output")
                .short("o")
                .long("output")
                .value_name("FILE")
                .required(true)
                .help("Sets a custom config file"),
        )
        .arg(Arg::with_name("recurse").short("r").long("recurse").help(
            "Recursively walks the directory, printing out gizip files",
        ))
        .get_matches();


    let input_path = matches.value_of("input").unwrap();
    let output_path = matches.value_of("output").unwrap();

    let input_path = Path::new(&input_path);

    if input_path.is_file() {
        return;
    }

    let completion_handler = CompletionHandler::new();

    let msgunpacker = MsgunpackerActor::new(|_| Msgunpacker::new(completion_handler.clone()), num_cpus::get() / 2);

    let gunzipper = GunzipperActor::new(|_| Gunzipper::new(msgunpacker.clone(), completion_handler.clone()), num_cpus::get());

    let mut count = 0;
    for entry in WalkDir::new(input_path) {
        let gunzipper = gunzipper.clone();
        let entry = entry.expect("Failed to open file");

        if entry.file_type().is_file() {
            if let Some(ext) = entry.path().extension() {
                if ext == "gz" {
                    gunzipper.gunzip(entry.path().to_owned());
                    count += 1;
                }
            }
        }
    }

    completion_handler.start(count);

    let backoff : Vec<_> = (1..250).filter(|i| i % 5 == 0).collect();
    let mut backoff = backoff.into_iter().cycle();

    loop {
        if completion_handler.is_complete() {
            break
        };
        thread::sleep(Duration::from_millis(backoff.next().unwrap()));
    }
}
