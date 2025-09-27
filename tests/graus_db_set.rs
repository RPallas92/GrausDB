use graus_db::{GrausDb, Result};
use tempfile::TempDir;

// Should overwrite existent value
#[test]
fn set_overwrites_value_if_exists() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;

    store.set(b"key1".to_vec(), b"value1")?;
    assert_eq!(store.get(b"key1")?, Some(b"value1".to_vec()));
    store.set(b"key1".to_vec(), b"value2")?;
    assert_eq!(store.get(b"key1")?, Some(b"value2".to_vec()));
    // Open from disk again and check persistent data
    drop(store);
    let store = GrausDb::open(temp_dir.path())?;
    assert_eq!(store.get(b"key1")?, Some(b"value2".to_vec()));
    store.set(b"key1".to_vec(), b"value3")?;
    assert_eq!(store.get(b"key1")?, Some(b"value3".to_vec()));
    Ok(())
}
