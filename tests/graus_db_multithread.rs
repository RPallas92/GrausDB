use std::thread;

use bytes::{Bytes, BytesMut};
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
                .set(
                    Bytes::from(format!("key{}", i)),
                    Bytes::from(format!("value{}", i)),
                )
                .unwrap();
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }

    for i in 0..1000 {
        assert_eq!(
            store.get(&Bytes::from(format!("key{}", i)))?,
            Some(Bytes::from(format!("value{}", i)))
        );
    }

    // Open from disk again and check persistent data
    drop(store);
    let store = GrausDb::open(temp_dir.path())?;
    for i in 0..1000 {
        assert_eq!(
            store.get(&Bytes::from(format!("key{}", i)))?,
            Some(Bytes::from(format!("value{}", i)))
        );
    }

    Ok(())
}

#[test]
fn concurrent_get() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    for i in 0..100 {
        store.set(
            Bytes::from(format!("key{}", i)),
            Bytes::from(format!("value{}", i)),
        )?;
    }

    let mut handles = Vec::with_capacity(100);
    for thread_id in 0..100 {
        let store = store.clone();
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let key_id = (i + thread_id) % 100;
                assert_eq!(
                    store.get(&Bytes::from(format!("key{}", key_id))).unwrap(),
                    Some(Bytes::from(format!("value{}", key_id)))
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
                    store.get(&Bytes::from(format!("key{}", key_id))).unwrap(),
                    Some(Bytes::from(format!("value{}", key_id)))
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
    store.set(Bytes::from(key), Bytes::copy_from_slice(&initial_value))?;

    let mut handles = Vec::new();
    for _ in 0..1000 {
        let store = store.clone();
        let update_fn = |value: &mut BytesMut| {
            let num =
                u64::from_le_bytes(value.as_ref()[..8].try_into().expect("incorrect length")) - 1;
            value.copy_from_slice(&num.to_le_bytes());
        };
        let handle = thread::spawn(move || {
            let _ =
                store.update_if::<_, fn(&Bytes) -> bool>(Bytes::from(key), update_fn, None, None);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let expected_value = 1u64.to_le_bytes();
    assert_eq!(
        store.get(&Bytes::from(key)).unwrap(),
        Some(Bytes::copy_from_slice(&expected_value))
    );

    // Test with predicate
    let value = 25u64.to_le_bytes();
    store.set(Bytes::from(key), Bytes::copy_from_slice(&value))?;

    let mut handles = Vec::new();
    for _ in 0..1000 {
        let store = store.clone();
        let update_fn = |value: &mut BytesMut| {
            let num =
                u64::from_le_bytes(value.as_ref()[..8].try_into().expect("incorrect length")) - 1;
            value.copy_from_slice(&num.to_le_bytes());
        };
        let predicate = |value: &Bytes| {
            let num = u64::from_le_bytes(value[..].try_into().expect("incorrect length"));
            num > 0
        };
        let handle = thread::spawn(move || {
            let _ = store.update_if(
                Bytes::from(key),
                update_fn,
                Some(&Bytes::from(key)),
                Some(predicate),
            );
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }

    let expected_value = 0u64.to_le_bytes();
    assert_eq!(
        store.get(&Bytes::from(key)).unwrap(),
        Some(Bytes::copy_from_slice(&expected_value))
    );

    // Open from disk again and check persistent data
    drop(store);
    let store = GrausDb::open(temp_dir.path())?;
    assert_eq!(
        store.get(&Bytes::from(key)).unwrap(),
        Some(Bytes::copy_from_slice(&expected_value))
    );

    Ok(())
}
