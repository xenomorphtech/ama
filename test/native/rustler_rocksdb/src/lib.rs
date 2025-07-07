#![allow(unused_variables)]
#![allow(dead_code)]

use rustler::Error;
use rustler::{Encoder, Env, NifResult, Term};

use std::sync::Mutex;

use lazy_static::lazy_static;

use std::path::Path;

use rustler::Binary;
use rustler::OwnedBinary;

use rocksdb::Direction;
use rocksdb::{Options, DB};
use rustler::types::atom;
//use rustler::types::tuple;
use rocksdb::DBIterator;
use rocksdb::IteratorMode;
use rocksdb::ReadOptions;
use rustler::ListIterator;
use rustler::ResourceArc;

lazy_static! {
    static ref DB_INSTANCE: Mutex<Option<DB>> = Mutex::new(None);
}

mod atoms {
    rustler::atoms! {
        // General atoms
        ok,
        error,
        finished,

        // Iterator option atoms
        iterator_mode,
        start,
        end,
        from,

        // Direction atoms
        forward,
        reverse,
        next,
        prev,
        first,
        last,
    }
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

// ------------------------ NIF skeletons ------------------------

#[rustler::nif(name = "transaction_get_3")]
fn transaction_get_3(_txn_id: String, _key: String, _opts: Term) -> NifResult<Option<Vec<u8>>> {
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "transaction_get_4")]
fn transaction_get_4(
    _txn_id: String,
    _key: String,
    _opts: Term,
    _cf: String,
) -> NifResult<Option<Vec<u8>>> {
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "get_3")]
fn get_3(_key: String, _opts: Term) -> NifResult<Option<Vec<u8>>> {
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "get_4")]
fn get_4(_key: String, _opts: Term, _cf: String) -> NifResult<Option<Vec<u8>>> {
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "transaction_put_3")]
fn transaction_put_3(_txn_id: String, _key: String, _value: Vec<u8>) -> NifResult<bool> {
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "transaction_put_4")]
fn transaction_put_4(
    _txn_id: String,
    _key: String,
    _value: Vec<u8>,
    _opts: Term,
) -> NifResult<bool> {
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "put_4")]
fn put_4(_key: String, _value: Vec<u8>, _opts: Term) -> NifResult<bool> {
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "put_5")]
fn put_5(_key: String, _value: Vec<u8>, _opts: Term, _cf: String) -> NifResult<bool> {
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "transaction_delete_2")]
fn transaction_delete_2(_txn_id: String, _key: String) -> NifResult<bool> {
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "transaction_delete_3")]
fn transaction_delete_3(_txn_id: String, _key: String, _opts: Term) -> NifResult<bool> {
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "delete_3")]
fn delete_3(_key: String, _opts: Term) -> NifResult<bool> {
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "delete_4")]
fn delete_4(_key: String, _opts: Term, _cf: String) -> NifResult<bool> {
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "transaction_iterator_2")]
fn transaction_iterator_2(env: Env, _txn_id: String) -> NifResult<Term> {
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "transaction_iterator_3")]
fn transaction_iterator_3<'a>(
    env: Env<'a>,
    _txn_id: String,
    _opts: Term<'a>,
) -> NifResult<Term<'a>> {
    Err(Error::Atom("not_implemented"))
}

pub struct IteratorResource {
    // The iterator has a lifetime dependency on the DB instance.
    // Since the DB is in a lazy_static, it will live for the lifetime of the
    // program. We can use `unsafe` to extend the iterator's lifetime to `'static`.
    // This is safe as long as the DB is not closed while iterators exist.
    iter: Mutex<DBIterator<'static>>,
}

enum ParsedIteratorMode {
    Start,
    End,
    From { key: Vec<u8>, dir: Direction },
}

fn parse_iterator_opts(env: Env, opts: Term) -> NifResult<ParsedIteratorMode> {
    let list: ListIterator = opts.decode()?;

    for item in list {
        let (key_term, value_term) = item.decode::<(Term, Term)>()?;

        if key_term.decode::<atom::Atom>()? == atoms::iterator_mode() {
            // Try to decode the value as a simple atom first (e.g., :start, :end)
            if let Ok(mode_atom) = value_term.decode::<atom::Atom>() {
                if mode_atom == atoms::start() {
                    return Ok(ParsedIteratorMode::Start);
                } else if mode_atom == atoms::end() {
                    return Ok(ParsedIteratorMode::End);
                }
            // Otherwise, try to decode it as a tuple (e.g., {:from, key, :forward})
            } else if let Ok((from_atom, key_term, dir_atom)) =
                value_term.decode::<(atom::Atom, Term, atom::Atom)>()
            {
                // Ensure the tuple starts with the :from atom
                if from_atom == atoms::from() {
                    let key = binary_to_vec(key_term)?;
                    let dir = if dir_atom == atoms::reverse() {
                        Direction::Reverse
                    } else {
                        // Default to forward if not reverse
                        Direction::Forward
                    };
                    return Ok(ParsedIteratorMode::From { key, dir });
                }
            }
        }
    }
    // Default mode if not specified
    Ok(ParsedIteratorMode::Start)
}

#[rustler::nif(name = "iterator")]
fn iterator(env: Env, opts: Term) -> NifResult<ResourceArc<IteratorResource>> {
    let db_guard = DB_INSTANCE.lock().unwrap();
    let db = db_guard.as_ref().ok_or(Error::Atom("db_not_initialized"))?;

    let parsed_mode = parse_iterator_opts(env, opts)?;

    let db_iter = match parsed_mode {
        ParsedIteratorMode::Start => db.iterator(IteratorMode::Start),
        ParsedIteratorMode::End => db.iterator(IteratorMode::End),
        ParsedIteratorMode::From { ref key, dir } => {
            db.iterator(IteratorMode::From(key.as_slice(), dir))
        }
    };

    let static_iter: DBIterator<'static> = unsafe { std::mem::transmute(db_iter) };
    let resource = ResourceArc::new(IteratorResource {
        iter: Mutex::new(static_iter),
    });
    Ok(resource)
}

#[rustler::nif(name = "iterator_2")]
fn iterator_2(env: Env, opts: Term, cf_name: String) -> NifResult<ResourceArc<IteratorResource>> {
    let db_guard = DB_INSTANCE.lock().unwrap();
    let db = db_guard.as_ref().ok_or(Error::Atom("db_not_initialized"))?;
    let cf = db
        .cf_handle(&cf_name)
        .ok_or_else(|| Error::Term(Box::new("Column family not found")))?;
    let parsed_mode = parse_iterator_opts(env, opts)?;
    let read_opts = ReadOptions::default();

    let db_iter = match parsed_mode {
        ParsedIteratorMode::Start => db.iterator_cf_opt(cf, read_opts, IteratorMode::Start),
        ParsedIteratorMode::End => db.iterator_cf_opt(cf, read_opts, IteratorMode::End),
        ParsedIteratorMode::From { ref key, dir } => {
            db.iterator_cf_opt(cf, read_opts, IteratorMode::From(key.as_slice(), dir))
        }
    };

    let static_iter: DBIterator<'static> = unsafe { std::mem::transmute(db_iter) };
    let resource = ResourceArc::new(IteratorResource {
        iter: Mutex::new(static_iter),
    });
    Ok(resource)
}

/// Moves the iterator to the next position.
///
/// This is a specialized and slightly more efficient version of `iterator_move(iter, :next)`.
///
/// Returns `{:ok, {key, value}}` if the iterator is valid after moving,
/// or `:finished` if the iterator has moved past the last element.
#[rustler::nif(name = "iterator_next")]
fn iterator_next<'a>(env: Env<'a>, iter_res: ResourceArc<IteratorResource>) -> NifResult<Term<'a>> {
    let iter = &mut *iter_res.iter.lock().unwrap();

    // Call next() and handle its rich return type directly.
    match iter.next() {
        // Case 1: Successfully got the next key-value pair.
        Some(Ok((key, value))) => {
            // The key and value are Box<[u8]>, which can be encoded directly.
            let mut key_binary = rustler::OwnedBinary::new(key.len()).unwrap();
            key_binary.as_mut_slice().copy_from_slice(&key);

            let mut value_binary = rustler::OwnedBinary::new(value.len()).unwrap();
            value_binary.as_mut_slice().copy_from_slice(&value);

            let ok_atom = atoms::ok().encode(env);
            let key_term = key_binary.release(env).encode(env);
            let value_term = value_binary.release(env).encode(env);

            let data_tuple = rustler::types::tuple::make_tuple(env, &[key_term, value_term]);
            Ok(rustler::types::tuple::make_tuple(
                env,
                &[ok_atom, data_tuple],
            ))
        }
        // Case 2: An error occurred during iteration.
        Some(Err(e)) => Err(Error::Term(Box::new(format!(
            "RocksDB iteration error: {}",
            e
        )))),
        // Case 3: The iterator reached the end.
        None => Ok(atoms::finished().encode(env)),
    }
}

#[rustler::nif(name = "flush_3")]
fn flush_3(_opts: Term, _wait: bool) -> NifResult<bool> {
    Err(Error::Atom("not_implemented"))
}

#[rustler::nif(name = "compact_range_5")]
fn compact_range_5(
    _start: Term,
    _end: Term,
    _opts: Term,
    _cf: String,
    _output_level: i32,
) -> NifResult<bool> {
    Err(Error::Atom("not_implemented"))
}

fn load(env: Env, _: Term) -> bool {
    let _ = rustler::resource!(IteratorResource, env);
    true
}

// ---------- Rustler exports ----------
rustler::init!("Elixir.RustlerRocksDB", load = load);
