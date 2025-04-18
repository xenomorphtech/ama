// Rust implementation in native/rustler_rocksdb/src/lib.rs

use rustler::{Encoder, Env, NifResult, Term};

use rocksdb::{Options, DB};

use std::sync::Mutex;

use lazy_static::lazy_static;

use std::path::Path;

lazy_static! {
    static ref DB_INSTANCE: Mutex<Option<DB>> = Mutex::new(None);
}

#[rustler::nif]

fn init(db_path: String) -> NifResult<bool> {
    let mut db_guard = DB_INSTANCE.lock().unwrap();

    // Create RocksDB options

    let mut options = Options::default();

    options.create_if_missing(true);

    // Open the database

    match DB::open(&options, Path::new(&db_path)) {
        Ok(db) => {
            *db_guard = Some(db);

            Ok(true)
        }

        Err(e) => {
            eprintln!("Failed to open RocksDB: {:?}", e);

            Ok(false)
        }
    }
}

#[rustler::nif]

fn get(key: String) -> NifResult<Option<Vec<u8>>> {
    let db_guard = DB_INSTANCE.lock().unwrap();

    if let Some(db) = db_guard.as_ref() {
        match db.get(key.as_bytes()) {
            Ok(Some(value)) => Ok(Some(value)),

            Ok(None) => Ok(None),

            Err(e) => {
                eprintln!("Error getting value: {:?}", e);

                Ok(None)
            }
        }
    } else {
        eprintln!("Database not initialized");

        Ok(None)
    }
}

#[rustler::nif]

fn put(key: String, value: Vec<u8>) -> NifResult<bool> {
    let db_guard = DB_INSTANCE.lock().unwrap();

    if let Some(db) = db_guard.as_ref() {
        match db.put(key.as_bytes(), value) {
            Ok(_) => Ok(true),

            Err(e) => {
                eprintln!("Error putting value: {:?}", e);

                Ok(false)
            }
        }
    } else {
        eprintln!("Database not initialized");

        Ok(false)
    }
}

rustler::init!("RustlerRocksDB", [init, get, put]);
