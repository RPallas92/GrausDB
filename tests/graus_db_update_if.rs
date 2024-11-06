use graus_db::{GrausDb, GrausError, Result};
use tempfile::TempDir;

#[test]
fn update_if_updates_existing_data_when_predicate_is_satisfied() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".as_bytes())?;
    store.set("key2".to_owned(), "value2".as_bytes())?;

    let update_fn = |value: &mut [u8]| {
        for byte in value.iter_mut() {
            // Convert the byte to uppercase if it's a lowercase ASCII character
            if *byte >= b'a' && *byte <= b'z' {
                *byte -= 32; // Convert to uppercase by modifying the byte value
            }
        }
    };

    let predicate = |value: &[u8]| value == b"value2";

    store.update_if(
        "key1".to_owned(),
        update_fn,
        Some("key2".to_owned()),
        Some(predicate),
    )?;

    assert_eq!(store.get("key1".to_owned())?, Some("VALUE1".into()));
    Ok(())
}

#[test]
fn update_if_updates_existing_data_when_no_predicate() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".as_bytes())?;

    let update_fn = |value: &mut [u8]| {
        for byte in value.iter_mut() {
            // Convert the byte to uppercase if it's a lowercase ASCII character
            if *byte >= b'a' && *byte <= b'z' {
                *byte -= 32; // Convert to uppercase by modifying the byte value
            }
        }
    };

    store.update_if::<_, _, fn(&[u8]) -> bool>("key1".to_owned(), update_fn, None, None)?;

    assert_eq!(store.get("key1".to_owned())?, Some("VALUE1".into()));
    Ok(())
}

#[test]
fn update_if_returns_predicate_error_when_predicate_is_not_satisfied_for_existing_data(
) -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".as_bytes())?;
    store.set("key2".to_owned(), "value2".as_bytes())?;

    let update_fn = |value: &mut [u8]| {
        for byte in value.iter_mut() {
            // Convert the byte to uppercase if it's a lowercase ASCII character
            if *byte >= b'a' && *byte <= b'z' {
                *byte -= 32; // Convert to uppercase by modifying the byte value
            }
        }
    };
    let predicate = |value: &[u8]| value == b"value1";

    let result = store.update_if(
        "key1".to_owned(),
        update_fn,
        Some("key2".to_owned()),
        Some(predicate),
    );

    match result {
        Err(GrausError::PredicateNotSatisfied) => assert!(true),
        _ => assert!(false),
    }
    assert_eq!(store.get("key1".to_owned())?, Some("value1".into()));
    Ok(())
}

#[test]
fn update_if_returns_key_not_found_error_when_data_not_exists() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;

    let update_fn = |value: &mut [u8]| {
        for byte in value.iter_mut() {
            // Convert the byte to uppercase if it's a lowercase ASCII character
            if *byte >= b'a' && *byte <= b'z' {
                *byte -= 32; // Convert to uppercase by modifying the byte value
            }
        }
    };
    let result =
        store.update_if::<_, _, fn(&[u8]) -> bool>("key1".to_owned(), update_fn, None, None);

    match result {
        Err(GrausError::KeyNotFound) => assert!(true),
        _ => assert!(false),
    }
    Ok(())
}
