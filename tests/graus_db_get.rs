use graus_db::{GrausDb, Result};
use tempfile::TempDir;

// Should get previously stored value
#[test]
fn get_returns_value_when_exists() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".as_bytes())?;
    store.set("key2".to_owned(), "value2".as_bytes())?;

    assert_eq!(store.get("key1".to_owned())?, Some(b"value1".to_vec()));
    assert_eq!(store.get("key2".to_owned())?, Some(b"value2".to_vec()));

    // Open from disk again and check persistent data
    drop(store);
    let store = GrausDb::open(temp_dir.path())?;
    assert_eq!(store.get("key1".to_owned())?, Some(b"value1".to_vec()));
    assert_eq!(store.get("key2".to_owned())?, Some(b"value2".to_vec()));

    Ok(())
}

// Should get `None` when getting a non-existent key
#[test]
fn get_returns_value_when_not_exists() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;

    store.set("key1".to_owned(), b"value1")?;
    assert_eq!(store.get("key2".to_owned())?, None);

    // Open from disk again and check persistent data
    drop(store);
    let store = GrausDb::open(temp_dir.path())?;
    assert_eq!(store.get("key2".to_owned())?, None);

    Ok(())
}
