# GrausDb

![Rust](https://img.shields.io/badge/Rust-1.53+-orange)
![License](https://img.shields.io/badge/license-MIT-blue)

GrausDb is a high-performance, thread-safe key-value embedded data store written in Rust. It is designed for simplicity, efficiency, and reliability.
## Features

- **Lock-Free Reads:** GrausDb leverages lock-free data structures for high-performance concurrent reads.
- **Persistence:** Data is persisted to disk for durability.
- **Log-Based Storage:** Key-value pairs are stored in log files.
- **Benchmarks:** Benchmarks included to assess the performance.
## Installation

To use GrausDb in your Rust project, simply add it as a dependency in your `Cargo.toml` file:

```toml
[dependencies]
graus_db = "0.1.0"
```


## Quick start

Here's a quick example of how to use GrausDb in your Rust application:

```rust
let store = GrausDb::open("path")?;

store.set("key".to_owned(), "value".to_owned())?;

let val = store.get("key".to_owned())?;
```

It can also be called from multiple threads:

```rust
let store = GrausDb::open("path")?;

// Calls set method from 8 different threads
for i in 0..8 {
    let store = store.clone();
    thread::spawn(move || {
        store.set(format!("key{}", i), format!("value{}", i)).unwrap();
    });
}
```

## API

GrausDb provides a simple and intuitive API for interacting with the key-value store. Below are some of the key functions and methods exposed by GrausDb, along with usage examples.

### `GrausDb::open`

`open` is used to open a GrausDb instance, creating a new database if it doesn't exist at the specified path.

#### Example:

```rust
use graus_db::{GrausDb, Result};

fn main() -> Result<()> {
    let store = GrausDb::open("my_database")?;
    // Your database is now ready to use.
    Ok(())
}
```

### `set`

The `set` method is used to store a key-value pair in the database.

#### Example:

```rust
use graus_db::{GrausDb, Result};

fn main() -> Result<()> {
    let store = GrausDb::open("my_database")?;
    store.set("key".to_owned(), "value".to_owned())?;
    // Key "key" now has the value "value" in the database.
    Ok(())
}
```

### `get`

The `get` method retrieves the value associated with a given key.


#### Example:

```rust
use graus_db::{GrausDb, Result};

fn main() -> Result<()> {
    let store = GrausDb::open("my_database")?;
    store.set("key".to_owned(), "value".to_owned())?;
    
    if let Some(value) = store.get("key".to_owned())? {
        println!("Value: {}", value); // Outputs: "Value: value"
    } else {
        println!("Key not found");
    }
    Ok(())
}
```

### `remove`

The `remove` method deletes a key and its associated value from the database.


#### Example:

```rust
use graus_db::{GrausDb, Result};

fn main() -> Result<()> {
    let store = GrausDb::open("my_database")?;
    store.set("key".to_owned(), "value".to_owned())?;
    store.remove("key".to_owned())?;
    // Key "key" and its value are now removed from the database.
    Ok(())
}
```


### `update_if`

The `update_if` method updates the value of an existing key atomically, allowing you to provide a custom update function. 

An optional predicate can be passed, the value will only be updated if the predicate is satisfied.

#### Example:

```rust
use graus_db::{GrausDb, Result};

fn main() -> Result<()> {
    let key = ¨key1¨;
    store.set(key.to_owned(), "25".to_owned()).unwrap();

    let update_fn = |value: String| {
        let num = value.parse::<i32>().unwrap();
        (num - 1).to_string()
        };
    let predicate = |value: String| {
        let num = value.parse::<i32>().unwrap();
        num > 0
    };

    let result = store.update_if(
            key.to_owned(),
            update_fn,
            Some(key.to_owned()),
            Some(predicate),
    );
    // Key "key1" now has the value "24" in the database.
    // The function was applied because the predicate was met (25 > 0)
}
```


For more details on how to use GrausDb, please refer to the tests.

## Architecture and Implementation

GrausDb's architecture is built around the principles of log-based storage and lock-free read concurrency:

- Log-Based Storage: GrausDb stores key-value pairs in log files. Log files are named after monotonically increasing generation numbers with a log extension. This design ensures that data is durably persisted to disk.

- Lock-Free Concurrency for Reads: GrausDb uses lock-free data structures to provide high-performance concurrent reads to the data. This enables multiple threads to interact with the database efficiently.

- In-Memory Index: GrausDb maintains an in-memory index that maps keys to their positions in the log. This index allows for fast lookups and efficient data retrieval.

- Compaction: To maintain efficient storage and reduce disk space usage, GrausDb performs compaction when a threshold is reached. Compaction involves rewriting log files, removing stale data, and reclaiming disk space.



## Benchmarks
GrausDb includes built-in benchmarking tools to evaluate its efficiency and to help you make data-driven decisions.

## Future Development
Next features:
- Multithread benchmark
- Range get
- Sync API (journal)
- Internal threadpool + futures

### License
GrausDb is licensed under the MIT License. 

Happy coding with GrausDb!

## Contact

GrausDb is created and maintained by **Ricardo Pallas**.

Website: [https://rpallas92.github.io/](https://rpallas92.github.io/)


