use graus_db::{GrausDb, GrausError, Result};
use tempfile::TempDir;

#[test]
fn update_if_updates_existing_data_when_predicate_is_satisfied() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    store.set("key2".to_owned(), "value2".to_owned())?;

    store.update_if(
        "key1".to_owned(),
        |value| value.to_uppercase(),
        Some("key2".to_owned()),
        Some(|value| value == "value2"),
    )?;

    assert_eq!(store.get("key1".to_owned())?, Some("VALUE1".to_owned()));
    Ok(())
}

#[test]
fn update_if_updates_existing_data_when_no_predicate() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;

    store.update_if::<_, fn(String) -> bool>(
        "key1".to_owned(),
        |value| value.to_uppercase(),
        None,
        None,
    )?;

    assert_eq!(store.get("key1".to_owned())?, Some("VALUE1".to_owned()));
    Ok(())
}

#[test]
fn update_if_returns_predicate_error_when_predicate_is_not_satisfied_for_existing_data(
) -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    store.set("key2".to_owned(), "value2".to_owned())?;

    let result = store.update_if(
        "key1".to_owned(),
        |value| value.to_uppercase(),
        Some("key2".to_owned()),
        Some(|value| value == "value1"),
    );

    match result {
        Err(GrausError::PredicateNotSatisfied) => assert!(true),
        _ => assert!(false),
    }
    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    Ok(())
}

#[test]
fn update_if_returns_key_not_found_error_when_data_not_exists() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;

    let result = store.update_if::<_, fn(String) -> bool>(
        "key1".to_owned(),
        |value| value.to_uppercase(),
        None,
        None,
    );

    match result {
        Err(GrausError::KeyNotFound) => assert!(true),
        _ => assert!(false),
    }
    Ok(())
}
