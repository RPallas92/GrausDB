use std::{
    sync::{Arc, Barrier},
    thread,
};

use graus_db::{GrausDb, Result};
use tempfile::TempDir;

#[test]
fn concurrent_set() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    let barrier = Arc::new(Barrier::new(1001));
    for i in 0..1000 {
        let store = store.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            store
                .set(format!("key{}", i), format!("value{}", i))
                .unwrap();
            barrier.wait();
        });
    }
    barrier.wait();

    for i in 0..1000 {
        assert_eq!(store.get(format!("key{}", i))?, Some(format!("value{}", i)));
    }

    // Open from disk again and check persistent data
    drop(store);
    let store = GrausDb::open(temp_dir.path())?;
    for i in 0..1000 {
        assert_eq!(store.get(format!("key{}", i))?, Some(format!("value{}", i)));
    }

    Ok(())
}

#[test]
fn concurrent_get() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = GrausDb::open(temp_dir.path())?;
    for i in 0..100 {
        store
            .set(format!("key{}", i), format!("value{}", i))
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
                    Some(format!("value{}", key_id))
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
                    Some(format!("value{}", key_id))
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
    let barrier = Arc::new(Barrier::new(1001));
    let key = "key1";
    store.set(key.to_owned(), "1001".to_owned()).unwrap();

    for _ in 0..1000 {
        let store = store.clone();
        let barrier = barrier.clone();
        let update_fn = |value: String| {
            let num = value.parse::<i32>().unwrap();
            (num - 1).to_string()
        };
        thread::spawn(move || {
            let _ = store.update_if::<_, fn(String) -> bool>(key.to_owned(), update_fn, None, None);
            barrier.wait();
        });
    }
    barrier.wait();

    assert_eq!(store.get(key.to_owned())?, Some("1".to_owned()));

    // Test with predicate
    store.set(key.to_owned(), "25".to_owned()).unwrap();
    let barrier = Arc::new(Barrier::new(1001));
    for _ in 0..1000 {
        let store = store.clone();
        let barrier = barrier.clone();
        let update_fn = |value: String| {
            let num = value.parse::<i32>().unwrap();
            (num - 1).to_string()
        };
        let predicate = |value: String| {
            let num = value.parse::<i32>().unwrap();
            num > 0
        };
        thread::spawn(move || {
            let _ = store.update_if(
                key.to_owned(),
                update_fn,
                Some(key.to_owned()),
                Some(predicate),
            );
            barrier.wait();
        });
    }
    barrier.wait();

    assert_eq!(store.get(key.to_owned())?, Some("0".to_owned()));

    // Open from disk again and check persistent data
    drop(store);
    let store = GrausDb::open(temp_dir.path())?;
    assert_eq!(store.get(key.to_owned())?, Some("0".to_owned()));

    Ok(())
}
