use rocksdb::{IteratorMode, Options, DB};
use std::env;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

const DUMP_DIR: &str = "dump";

/// A command-line tool to dump each column family of a RocksDB database
/// into a separate binary file in a 'dump' subdirectory.
///
/// The binary format for each entry is:
/// - Key length (4 bytes, u32 big-endian)
/// - Key data ([u8])
/// - Value length (4 bytes, u32 big-endian)
/// - Value data ([u8])
fn main() {
    // --- 1. Parse Command-Line Arguments ---
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <path-to-rocksdb>", args[0]);
        std::process::exit(1);
    }
    let db_path = &args[1];
    println!("Inspecting database at: {}", db_path);

    // --- 2. Create Output Directory ---
    match fs::create_dir_all(DUMP_DIR) {
        Ok(_) => println!("Output will be saved in the '{}' directory.", DUMP_DIR),
        Err(e) => {
            eprintln!("Failed to create output directory '{}': {}", DUMP_DIR, e);
            std::process::exit(1);
        }
    }

    // --- 3. List Available Column Families ---
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

    // --- 4. Open the Database with All Column Families ---
    let opts = Options::default();
    match DB::open_cf_for_read_only(&opts, db_path, &cf_names, false) {
        Ok(db) => {
            println!("\nDatabase opened successfully. Starting dump process...");

            // --- 5. Iterate Through and Dump Each Column Family ---
            for cf_name in &cf_names {
                // Construct the output path, e.g., "dump/contractstate.dump"
                let mut output_path = PathBuf::from(DUMP_DIR);
                output_path.push(cf_name);
                output_path.set_extension("dump");

                // Dump the current column family to its dedicated file.
                match dump_cf_to_file(&db, cf_name, &output_path) {
                    Ok(count) => {
                        println!(
                            "  -> Successfully dumped {} key-value pairs from '{}' to '{}'.",
                            count,
                            cf_name,
                            output_path.display()
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "  -> An error occurred while dumping '{}': {}",
                            cf_name, e
                        );
                    }
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
/// This function correctly handles both the "default" CF and other named CFs.
///
/// # Arguments
/// * `db` - An open RocksDB instance.
/// * `cf_name` - The name of the column family to dump.
/// * `output_path` - The path to the file where the dump will be saved.
///
/// # Returns
/// A `Result` containing the number of key-value pairs dumped, or an `io::Error`.
fn dump_cf_to_file(db: &DB, cf_name: &str, output_path: &Path) -> io::Result<u64> {
    // Create the output file and wrap it in a BufWriter for efficiency.
    let file = File::create(output_path)?;
    let mut writer = BufWriter::new(file);

    // Get an iterator for the column family.
    // The method to get an iterator is different for the "default" CF
    // versus other named CFs.
    let iter = if cf_name == "default" {
        db.iterator(IteratorMode::Start) // For the default CF
    } else {
        // For a named CF, we must first get its handle.
        let cf_handle = db.cf_handle(cf_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Column family '{}' handle not found", cf_name),
            )
        })?;
        db.iterator_cf(&cf_handle, IteratorMode::Start)
    };

    let mut count: u64 = 0;

    for item in iter {
        match item {
            Ok((key, value)) => {
                // --- Serialize and Write Key ---
                let key_len = key.len() as u32;
                writer.write_all(&key_len.to_be_bytes())?;
                writer.write_all(&key)?;

                // --- Serialize and Write Value ---
                let value_len = value.len() as u32;
                writer.write_all(&value_len.to_be_bytes())?;
                writer.write_all(&value)?;

                count += 1;
            }
            Err(e) => {
                // If the database iterator returns an error, we convert it to an io::Error.
                return Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
            }
        }
    }

    // The BufWriter is automatically flushed when it goes out of scope.
    Ok(count)
}
