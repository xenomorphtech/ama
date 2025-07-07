use rocksdb::{IteratorMode, Options, DB};
use std::env;
use std::fs::File;
use std::io::{self, BufWriter, Write};

/// Dumps the "contractstate" column family from a RocksDB database to a binary file.
/// The binary format for each entry is:
/// - Key length (4 bytes, u32 big-endian)
/// - Key data ([u8])
/// - Value length (4 bytes, u32 big-endian)
/// - Value data ([u8])
fn main() {
    // --- 1. Parse Command-Line Arguments ---
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <path-to-rocksdb> <output-dump-file>", args[0]);
        std::process::exit(1);
    }
    let db_path = &args[1];
    let output_path = &args[2];
    println!("Inspecting database at: {}", db_path);
    println!("Will dump 'contractstate' CF to: {}", output_path);

    // --- 2. List Available Column Families ---
    let cf_names = match DB::list_cf(&Options::default(), db_path) {
        Ok(names) => {
            println!("\nFound {} column families:", names.len());
            for name in &names {
                println!("  - {}", name);
            }
            names
        }
        Err(e) => {
            eprintln!("Failed to list column families: {}.", e);
            eprintln!("Please ensure the path is correct and the database is valid.");
            std::process::exit(1);
        }
    };

    // --- 3. Open the Database with All Column Families ---
    let opts = Options::default();
    match DB::open_cf_for_read_only(&opts, db_path, &cf_names, false) {
        Ok(db) => {
            println!("\nDatabase opened successfully.");

            // --- 4. Find and Dump the "contractstate" Column Family ---
            let target_cf_name = "contractstate";
            if !cf_names.iter().any(|name| name == target_cf_name) {
                eprintln!("\nError: Column family '{}' not found in the database.", target_cf_name);
                std::process::exit(1);
            }

            // We wrap the dumping logic in a function to easily handle I/O errors.
            match dump_cf_to_file(&db, target_cf_name, output_path) {
                Ok(count) => {
                    println!("\nSuccessfully dumped {} key-value pairs from '{}' to '{}'.", count, target_cf_name, output_path);
                }
                Err(e) => {
                    eprintln!("\nAn error occurred while dumping the column family: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to open database with column families: {}", e);
            std::process::exit(1);
        }
    }
}

/// Iterates through a specific column family and writes its contents to a file.
///
/// # Arguments
/// * `db` - An open RocksDB instance.
/// * `cf_name` - The name of the column family to dump.
/// * `output_path` - The path to the file where the dump will be saved.
///
/// # Returns
/// A `Result` containing the number of key-value pairs dumped, or an `io::Error`.
fn dump_cf_to_file(db: &DB, cf_name: &str, output_path: &str) -> io::Result<u64> {
    println!("\n--- Dumping contents of Column Family: '{}' ---", cf_name);

    // Get a handle to the "contractstate" column family.
    // We can unwrap here because we already checked for its existence.
    let cf_handle = db.cf_handle(cf_name).unwrap();

    // Create the output file and wrap it in a BufWriter for efficiency.
    let file = File::create(output_path)?;
    let mut writer = BufWriter::new(file);

    // Create an iterator for the specific column family.
    let iter = db.iterator_cf(&cf_handle, IteratorMode::Start);
    let mut count: u64 = 0;

    for item in iter {
        match item {
            Ok((key, value)) => {
                // --- Serialize and Write Key ---
                // 1. Get key length as a 32-bit unsigned integer.
                let key_len = key.len() as u32;
                // 2. Write the length as 4 bytes in big-endian format.
                writer.write_all(&key_len.to_be_bytes())?;
                // 3. Write the actual key data.
                writer.write_all(&key)?;

                // --- Serialize and Write Value ---
                // 4. Get value length as a 32-bit unsigned integer.
                let value_len = value.len() as u32;
                // 5. Write the length as 4 bytes in big-endian format.
                writer.write_all(&value_len.to_be_bytes())?;
                // 6. Write the actual value data.
                writer.write_all(&value)?;

                count += 1;
            }
            Err(e) => {
                // If the database iterator itself returns an error, we convert it to an io::Error.
                return Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
            }
        }
    }

    // The BufWriter is automatically flushed when it goes out of scope here.
    Ok(count)
}
