use bytes::Bytes;
use graus_db::{GrausDb, Result};
use tempfile::TempDir;

// Should get previously stored value
#[test]
fn get_returns_value_when_exists() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;

    store.set(Bytes::from_static(b"key1"), Bytes::from_static(b"value1"))?;
    store.set(Bytes::from_static(b"key2"), Bytes::from_static(b"value2"))?;

    assert_eq!(
        store.get(&Bytes::from_static(b"key1"))?,
        Some(Bytes::from_static(b"value1"))
    );
    assert_eq!(
        store.get(&Bytes::from_static(b"key2"))?,
        Some(Bytes::from_static(b"value2"))
    );
    // Open from disk again and check persistent data
    drop(store);
    let store = GrausDb::open(temp_dir.path())?;
    assert_eq!(
        store.get(&Bytes::from_static(b"key1"))?,
        Some(Bytes::from_static(b"value1"))
    );
    assert_eq!(
        store.get(&Bytes::from_static(b"key2"))?,
        Some(Bytes::from_static(b"value2"))
    );
    Ok(())
}

// Should get `None` when getting a non-existent key
#[test]
fn get_returns_value_when_not_exists() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;

    store.set(Bytes::from_static(b"key1"), Bytes::from_static(b"value1"))?;
    assert_eq!(store.get(&Bytes::from_static(b"key2"))?, None);

    // Open from disk again and check persistent data
    drop(store);
    let store = GrausDb::open(temp_dir.path())?;
    assert_eq!(store.get(&Bytes::from_static(b"key2"))?, None);

    Ok(())
}
