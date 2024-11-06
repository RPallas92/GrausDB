use std::thread;

use graus_db::{GrausDb, Result};
use std::convert::TryInto;
use tempfile::TempDir;

#[test]
fn concurrent_set() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    let mut handles = Vec::new();
    for i in 0..1000 {
        let store = store.clone();
        let handle = thread::spawn(move || {
            store
                .set(format!("key{}", i), format!("value{}", i).as_bytes())
                .unwrap();
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }

    for i in 0..1000 {
        assert_eq!(
            store.get(format!("key{}", i))?,
            Some(format!("value{}", i).into_bytes())
        );
    }

    // Open from disk again and check persistent data
    drop(store);
    let store = GrausDb::open(temp_dir.path())?;
    for i in 0..1000 {
        assert_eq!(
            store.get(format!("key{}", i))?,
            Some(format!("value{}", i).into_bytes())
        );
    }

    Ok(())
}

#[test]
fn concurrent_get() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    for i in 0..100 {
        store
            .set(format!("key{}", i), format!("value{}", i).as_bytes())
            .unwrap();
    }

    let mut handles = Vec::with_capacity(100);
    for thread_id in 0..100 {
        let store = store.clone();
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let key_id = (i + thread_id) % 100;
                assert_eq!(
                    store.get(format!("key{}", key_id)).unwrap(),
                    Some(format!("value{}", key_id).into_bytes())
                );
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }

    // Open from disk again and check persistent data
    drop(store);
    let store = GrausDb::open(temp_dir.path())?;
    let mut handles = Vec::new();
    for thread_id in 0..100 {
        let store = store.clone();
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let key_id = (i + thread_id) % 100;
                assert_eq!(
                    store.get(format!("key{}", key_id)).unwrap(),
                    Some(format!("value{}", key_id).into_bytes())
                );
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }

    Ok(())
}

#[test]
fn concurrent_update_if() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    let key = "key1";
    let initial_value = 1001u64.to_le_bytes();
    store.set(key.to_owned(), &initial_value).unwrap();

    let mut handles = Vec::new();
    for _ in 0..1000 {
        let store = store.clone();
        let update_fn = |value: &mut [u8]| {
            let num = u64::from_le_bytes(value.try_into().expect("incorrect length")) - 1;
            value.copy_from_slice(&num.to_le_bytes()); // Mutate the slice in place
        };
        let handle = thread::spawn(move || {
            let _ =
                store.update_if::<_, _, fn(&[u8]) -> bool>(key.to_owned(), update_fn, None, None);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let expected_value = 1u64.to_le_bytes();
    assert_eq!(store.get(key.to_owned())?, Some(expected_value.into()));

    // Test with predicate
    let value = 25u64.to_le_bytes();
    store.set(key.to_owned(), &value).unwrap();
    let mut handles = Vec::new();
    for _ in 0..1000 {
        let store = store.clone();
        let update_fn = |value: &mut [u8]| {
            let num = u64::from_le_bytes(value.try_into().expect("incorrect length"));
            let incremented_num = num - 1;
            value.copy_from_slice(&incremented_num.to_le_bytes());
        };
        let predicate = |value: &[u8]| {
            let num = u64::from_le_bytes(value.try_into().expect("incorrect length"));
            num > 0
        };
        let handle = thread::spawn(move || {
            let _ = store.update_if(
                key.to_owned(),
                update_fn,
                Some(key.to_owned()),
                Some(predicate),
            );
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }

    let expected_value = 0u64.to_le_bytes();
    assert_eq!(store.get(key.to_owned())?, Some(expected_value.into()));

    // Open from disk again and check persistent data
    drop(store);
    let store = GrausDb::open(temp_dir.path())?;
    assert_eq!(store.get(key.to_owned())?, Some(expected_value.into()));

    Ok(())
}
