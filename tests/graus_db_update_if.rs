use graus_db::{GrausDb, GrausError, Result};
use tempfile::TempDir;

#[test]
fn update_if_updates_existing_data_when_predicate_is_satisfied() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    store.set(b"key1".to_vec(), b"value1")?;
    store.set(b"key2".to_vec(), b"value2")?;

    let update_fn = |value: &mut Vec<u8>| {
        for byte in value.iter_mut() {
            // Convert the byte to uppercase if it's a lowercase ASCII character
            if *byte >= b'a' && *byte <= b'z' {
                *byte -= 32; // Convert to uppercase by modifying the byte value
            }
        }
    };
    let predicate = |value: &[u8]| value == b"value2";

    store.update_if(b"key1".to_vec(), update_fn, Some(b"key2"), Some(predicate))?;

    assert_eq!(store.get(b"key1").unwrap(), Some(b"VALUE1".to_vec()));
    Ok(())
}

#[test]
fn update_if_updates_existing_data_when_no_predicate() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    store.set(b"key1".to_vec(), b"value1")?;

    let update_fn = |value: &mut Vec<u8>| {
        for byte in value.iter_mut() {
            // Convert the byte to uppercase if it's a lowercase ASCII character
            if *byte >= b'a' && *byte <= b'z' {
                *byte -= 32; // Convert to uppercase by modifying the byte value
            }
        }
    };

    store.update_if::<_, fn(&[u8]) -> bool>(b"key1".to_vec(), update_fn, None, None)?;
    assert_eq!(store.get(b"key1").unwrap(), Some(b"VALUE1".to_vec()));
    Ok(())
}

#[test]
fn update_if_returns_predicate_error_when_predicate_is_not_satisfied_for_existing_data(
) -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    store.set(b"key1".to_vec(), b"value1")?;
    store.set(b"key2".to_vec(), b"value2")?;

    let update_fn = |value: &mut Vec<u8>| {
        for byte in value.iter_mut() {
            // Convert the byte to uppercase if it's a lowercase ASCII character
            if *byte >= b'a' && *byte <= b'z' {
                *byte -= 32; // Convert to uppercase by modifying the byte value
            }
        }
    };
    let predicate = |value: &[u8]| value == b"value1";

    let result = store.update_if(b"key1".to_vec(), update_fn, Some(b"key2"), Some(predicate));

    match result {
        Err(GrausError::PredicateNotSatisfied) => assert!(true),
        _ => assert!(false),
    }
    assert_eq!(store.get(b"key1").unwrap(), Some(b"value1".to_vec()));
    Ok(())
}

#[test]
fn update_if_returns_key_not_found_error_when_data_not_exists() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;

    let update_fn = |value: &mut Vec<u8>| {
        for byte in value.iter_mut() {
            // Convert the byte to uppercase if it's a lowercase ASCII character
            if *byte >= b'a' && *byte <= b'z' {
                *byte -= 32; // Convert to uppercase by modifying the byte value
            }
        }
    };

    let result = store.update_if::<_, fn(&[u8]) -> bool>(b"key1".to_vec(), update_fn, None, None);

    match result {
        Err(GrausError::KeyNotFound) => assert!(true),
        _ => assert!(false),
    }
    Ok(())
}
