use bytes::Bytes;
use graus_db::{GrausDb, Result};
use tempfile::TempDir;
use walkdir::WalkDir;

// Insert data until total size of the directory decreases.
// Test data correctness after compaction.
#[test]
fn data_is_compacted_when_limit_reached() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;

    let dir_size = || {
        let entries = WalkDir::new(temp_dir.path()).into_iter();
        let len: walkdir::Result<u64> = entries
            .map(|res| {
                res.and_then(|entry| entry.metadata())
                    .map(|metadata| metadata.len())
            })
            .sum();
        len.expect("fail to get directory size")
    };

    let mut current_size = dir_size();
    for iter in 0..10000 {
        for key_id in 0..10 {
            let key = format!("key{}", key_id);
            let value = format!("{}", iter);
            store.set(Bytes::from(key), Bytes::from(value))?;
        }

        let new_size = dir_size();
        if new_size > current_size {
            current_size = new_size;
            continue;
        }
        // Compaction triggered, check content after compaction
        for key_id in 0..10 {
            let key = format!("key{}", key_id);
            assert_eq!(
                store.get(&Bytes::from(key))?,
                Some(Bytes::from(format!("{}", iter)))
            );
        }

        drop(store);
        // reopen and check content
        let store = GrausDb::open(temp_dir.path())?;
        for key_id in 0..10 {
            let key = format!("key{}", key_id);
            assert_eq!(
                store.get(&Bytes::from(key))?,
                Some(Bytes::from(format!("{}", iter)))
            );
        }
        return Ok(());
    }

    panic!("No compaction detected");
}
