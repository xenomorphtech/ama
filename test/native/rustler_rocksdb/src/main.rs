use rocksdb::{DB, IteratorMode, Options};
use std::env;

/// A simple command-line tool to list key names and value sizes from a RocksDB database.
fn main() {
    // --- 1. Parse Command-Line Arguments ---
    // Collect command-line arguments into a vector of strings.
    let args: Vec<String> = env::args().collect();

    // Check if the user provided a path to the database.
    // The first argument (`args[0]`) is the program's own name.
    if args.len() < 2 {
        // Print a usage message to standard error and exit.
        eprintln!("Usage: {} <path-to-rocksdb>", args[0]);
        std::process::exit(1);
    }
    // The second argument (`args[1]`) is the database path.
    let db_path = &args[1];

    println!("Attempting to open database at: {}", db_path);

    // --- 2. Open the Database ---
    // It's good practice to open the database in read-only mode if you don't intend to write.
    // This prevents accidental modification and can be safer.
    let opts = Options::default();
    match DB::open_for_read_only(&opts, db_path, false) {
        Ok(db) => {
            // --- 3. Iterate and Print ---
            println!("Database opened successfully. Listing key -> value size:");
            println!("-------------------------------------------------------");

            // Create an iterator starting from the first key.
            // The `iterator` method returns an iterator that yields `Result<(key, value), Error>`.
            let iter = db.iterator(IteratorMode::Start);

            for item in iter {
                match item {
                    // The item is a Result, which we need to unwrap.
                    Ok((key, value)) => {
                        // `key` and `value` are of type Box<[u8]>.
                        // We use `from_utf8_lossy` to safely convert the key to a string,
                        // even if it contains invalid UTF-8 characters.
                        let key_str = String::from_utf8_lossy(&key);
                        let value_size = value.len();

                        // Print the key (using debug format for safety) and the value's length.
                        println!("Key: {:?} -> Value Size: {} bytes", key_str, value_size);
                    }
                    Err(e) => {
                        eprintln!("Error during iteration: {}", e);
                        // Stop iterating if an error occurs.
                        break;
                    }
                }
            }
        }
        Err(e) => {
            // Handle the case where the database could not be opened.
            eprintln!("Failed to open database at '{}': {}", db_path, e);
            eprintln!("Please ensure the path is correct and the database exists.");
            std::process::exit(1);
        }
    }
}
