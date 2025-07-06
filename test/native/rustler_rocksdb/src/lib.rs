// Rust implementation in native/rustler_rocksdb/src/lib.rs

use rustler::{Encoder, Env, NifResult, Term};

use rocksdb::{Options, DB};

use std::sync::Mutex;

use lazy_static::lazy_static;

use std::path::Path;

lazy_static! {
    static ref DB_INSTANCE: Mutex<Option<DB>> = Mutex::new(None);
}

// Helper to convert Elixir binary to Vec<u8>
fn binary_to_vec(binary: Term) -> NifResult<Vec<u8>> {
    if binary.is_binary() {
        let binary: Binary = binary.decode()?;
        Ok(binary.as_slice().to_vec())
    } else {
        Err(Error::Term(Box::new("Expected binary")))
    }
}

// Helper to convert Vec<u8> to Elixir binary
fn vec_to_binary<'a>(env: Env<'a>, data: Vec<u8>) -> NifResult<Term<'a>> {
    let mut binary = OwnedBinary::new(data.len())
        .ok_or_else(|| Error::Term(Box::new("Failed to allocate binary")))?;
    binary.as_mut_slice().copy_from_slice(&data);
    Ok(binary.release(env).encode(env))
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

// ---------- NIF skeletons with `env` ----------
use rustler::{Env, Term, NifResult, Error};  // ← added Error

// ------------------------ NIF skeletons ------------------------

#[rustler::nif(name = "transaction_get_3")]
fn transaction_get_3(_env: Env, _txn_id: String, _key: String, _opts: Term)
    -> NifResult<Option<Vec<u8>>>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "transaction_get_4")]
fn transaction_get_4(_env: Env, _txn_id: String, _key: String, _opts: Term, _cf: String)
    -> NifResult<Option<Vec<u8>>>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "get_3")]
fn get_3(_env: Env, _key: String, _opts: Term)
    -> NifResult<Option<Vec<u8>>>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "get_4")]
fn get_4(_env: Env, _key: String, _opts: Term, _cf: String)
    -> NifResult<Option<Vec<u8>>>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "iterator_move_2")]
fn iterator_move_2(_env: Env, _iterator: Term, _direction: Term)
    -> NifResult<bool>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "transaction_put_3")]
fn transaction_put_3(_env: Env, _txn_id: String, _key: String, _value: Vec<u8>)
    -> NifResult<bool>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "transaction_put_4")]
fn transaction_put_4(_env: Env, _txn_id: String, _key: String, _value: Vec<u8>, _opts: Term)
    -> NifResult<bool>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "put_4")]
fn put_4(_env: Env, _key: String, _value: Vec<u8>, _opts: Term)
    -> NifResult<bool>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "put_5")]
fn put_5(_env: Env, _key: String, _value: Vec<u8>, _opts: Term, _cf: String)
    -> NifResult<bool>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "transaction_delete_2")]
fn transaction_delete_2(_env: Env, _txn_id: String, _key: String)
    -> NifResult<bool>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "transaction_delete_3")]
fn transaction_delete_3(_env: Env, _txn_id: String, _key: String, _opts: Term)
    -> NifResult<bool>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "delete_3")]
fn delete_3(_env: Env, _key: String, _opts: Term)
    -> NifResult<bool>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "delete_4")]
fn delete_4(_env: Env, _key: String, _opts: Term, _cf: String)
    -> NifResult<bool>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "transaction_iterator_2")]
fn transaction_iterator_2(env: Env, _txn_id: String)
    -> NifResult<Term>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "transaction_iterator_3")]
fn transaction_iterator_3(env: Env, _txn_id: String, _opts: Term)
    -> NifResult<Term>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "iterator_2")]
fn iterator_2(env: Env, _opts: Term)
    -> NifResult<Term>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "iterator_3")]
fn iterator_3(env: Env, _opts: Term, _cf: String)
    -> NifResult<Term>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "flush_3")]
fn flush_3(_env: Env, _opts: Term, _wait: bool)
    -> NifResult<bool>
{
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "compact_range_5")]
fn compact_range_5(_env: Env, _start: Term, _end: Term, _opts: Term, _cf: String, _output_level: i32)
    -> NifResult<bool>
{
    Err(Error::Atom("not_implemented"))
}

// ---------- Rustler exports ----------
rustler::init!("RustlerRocksDB", [
    init,
    get,
    put,
    transaction_get_3,
    transaction_get_4,
    get_3,
    get_4,
    iterator_move_2,
    transaction_put_3,
    transaction_put_4,
    put_4,
    put_5,
    transaction_delete_2,
    transaction_delete_3,
    delete_3,
    delete_4,
    transaction_iterator_2,
    transaction_iterator_3,
    iterator_2,
    iterator_3,
    flush_3,
    compact_range_5
]);


