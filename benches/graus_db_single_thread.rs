use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use graus_db::GrausDb;
use rand::prelude::*;
use std::convert::TryInto;
use tempfile::TempDir;

fn set_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_bench");
    group.bench_function("graus_db_set", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                (GrausDb::open(temp_dir.path()).unwrap(), temp_dir)
            },
            |(store, _temp_dir)| {
                let value = b"value".to_vec();
                for i in 1..(1 << 12) {
                    store
                        .set(format!("key{}", i).into_bytes(), value.clone())
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn update_if_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_bench");
    group.bench_function("graus_db_update_if", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let store = GrausDb::open(temp_dir.path()).unwrap();
                let key = b"key1".to_vec();
                let value: u64 = 3500;
                store
                    .set(key.clone(), value.to_be_bytes().to_vec())
                    .unwrap();
                (store, temp_dir, key)
            },
            |(store, _temp_dir, key)| {
                let update_fn = |value: &mut Vec<u8>| {
                    let num =
                        u64::from_le_bytes(value[..8].try_into().expect("incorrect length")) - 1;
                    value.copy_from_slice(&num.to_le_bytes());
                };
                let predicate = |value: &[u8]| {
                    let num = u64::from_le_bytes(value[..].try_into().expect("incorrect length"));
                    num > 0
                };

                for _ in 1..(1 << 12) {
                    let _ = store.update_if::<_, fn(&[u8]) -> bool>(
                        key.to_owned(),
                        update_fn,
                        Some(key.as_slice()),
                        Some(predicate),
                    );
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_bench");
    for i in &vec![8, 20] {
        group.bench_with_input(format!("graus_db_get_{}", i), i, |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let store = GrausDb::open(temp_dir.path()).unwrap();
            let value = b"value".to_vec();
            for key_i in 1..(1 << i) {
                store
                    .set(format!("key{}", key_i).into_bytes(), value.clone())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 16]);
            b.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(1, 1 << i)).as_bytes())
                    .unwrap();
            })
        });
    }
    group.finish();
}

criterion_group!(benches, set_bench, update_if_bench, get_bench);
criterion_main!(benches);
