use graus_db::{GrausDb, Result};
use tempfile::TempDir;

// Should get previously stored value
#[test]
fn get_returns_value_when_exists() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;

    store.set(b"key1".to_vec(), b"value1".to_vec())?;
    store.set(b"key2".to_vec(), b"value2".to_vec())?;

    assert_eq!(store.get(b"key1")?, Some(b"value1".to_vec()));
    assert_eq!(store.get(b"key2")?, Some(b"value2".to_vec()));
    // Open from disk again and check persistent data
    drop(store);
    let store = GrausDb::open(temp_dir.path())?;
    assert_eq!(store.get(b"key1")?, Some(b"value1".to_vec()));
    assert_eq!(store.get(b"key2")?, Some(b"value2".to_vec()));
    Ok(())
}

// Should get `None` when getting a non-existent key
#[test]
fn get_returns_value_when_not_exists() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;

    store.set(b"key1".to_vec(), b"value1".to_vec())?;
    assert_eq!(store.get(b"key2")?, None);

    // Open from disk again and check persistent data
    drop(store);
    let store = GrausDb::open(temp_dir.path())?;
    assert_eq!(store.get(b"key2")?, None);

    Ok(())
}
