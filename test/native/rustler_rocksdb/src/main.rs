use rocksdb::{IteratorMode, Options, DB};
use std::env;

/// A command-line tool to list all column families and their contents in a RocksDB database.
fn main() {
    // --- 1. Parse Command-Line Arguments ---
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <path-to-rocksdb>", args[0]);
        std::process::exit(1);
    }
    let db_path = &args[1];
    println!("Inspecting database at: {}", db_path);

    // --- 2. List Available Column Families ---
    // Before we can open the DB, we must know the names of all column families.
    // `DB::list_cf` inspects the DB files on disk without fully opening the database.
    let cf_names = match DB::list_cf(&Options::default(), db_path) {
        Ok(names) => {
            println!("\nFound {} column families:", names.len());
            // Print each discovered column family name.
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
    // Now we open the database using `open_cf_for_read_only`, providing the list of
    // column family names we just discovered.
    let opts = Options::default();
    match DB::open_cf_for_read_only(&opts, db_path, &cf_names, false) {
        Ok(db) => {
            println!("\nDatabase opened successfully. Listing contents...");

            // --- 4. Iterate Through Each Column Family ---
            for cf_name in &cf_names {
                if cf_name == "default" {
                    continue;
                }
                println!("\n--- Contents of Column Family: '{}' ---", cf_name);

                // To iterate over a specific CF, we first need a "handle" to it.
                // We can unwrap here because we are guaranteed the handle exists,
                // as we successfully opened the DB with this CF name.
                let cf_handle = db.cf_handle(cf_name).unwrap();

                // Create an iterator for this specific column family using `iterator_cf`.
                let iter = db.iterator_cf(&cf_handle, IteratorMode::Start);
                let mut count = 0;

                for item in iter {
                    match item {
                        Ok((key, value)) => {
                            let key_str = String::from_utf8_lossy(&key);
                            let value_size = value.len();
                            println!("  Key: {:?} -> Value Size: {} bytes", key_str, value_size);
                            count += 1;
                        }
                        Err(e) => {
                            eprintln!("  Error during iteration in CF '{}': {}", cf_name, e);
                            break;
                        }
                    }
                }
                if count == 0 {
                    println!("  (This column family is empty)");
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to open database with column families: {}", e);
            std::process::exit(1);
        }
    }
}
