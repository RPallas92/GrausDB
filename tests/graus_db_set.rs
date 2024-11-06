use graus_db::{GrausDb, Result};
use tempfile::TempDir;

// Should overwrite existent value
#[test]
fn set_overwrites_value_if_exists() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".as_bytes())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value1".into()));
    store.set("key1".to_owned(), "value2".as_bytes())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value2".into()));

    // Open from disk again and check persistent data
    drop(store);
    let store = GrausDb::open(temp_dir.path())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value2".into()));
    store.set("key1".to_owned(), "value3".as_bytes())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value3".into()));

    Ok(())
}
