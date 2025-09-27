use graus_db::{GrausDb, GrausError, Result};
use tempfile::TempDir;

#[test]
fn remove_removes_key_when_exists() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    store.set(b"key1".to_vec(), b"value1")?;
    assert!(store.remove(b"key1").is_ok());
    assert_eq!(store.get(b"key1")?, None);
    Ok(())
}

#[test]
fn remove_returns_key_not_found_when_not_exists() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    let result = store.remove(b"key1");
    match result {
        Err(GrausError::KeyNotFound) => assert!(true),
        _ => assert!(false),
    }
    Ok(())
}
