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
                for i in 1..(1 << 12) {
                    store.set(format!("key{}", i), b"value").unwrap();
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
                let key = "key1";
                let value: u64 = 3500;
                store.set(key.to_owned(), &value.to_le_bytes()).unwrap();
                (store, temp_dir, key)
            },
            |(store, _temp_dir, key)| {
                let update_fn = |value: &mut [u8]| {
                    let num = u64::from_le_bytes(value.try_into().expect("incorrect length"));
                    let incremented_num = num - 1;
                    value.copy_from_slice(&incremented_num.to_le_bytes());
                };
                let predicate = |value: &[u8]| {
                    let num = u64::from_le_bytes(value.try_into().expect("incorrect length"));
                    num > 0
                };

                for _ in 1..(1 << 12) {
                    let _ = store.update_if(
                        key.to_owned(),
                        update_fn,
                        Some(key.to_owned()),
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
            for key_i in 1..(1 << i) {
                store.set(format!("key{}", key_i), b"value").unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 16]);
            b.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(1, 1 << i)))
                    .unwrap();
            })
        });
    }
    group.finish();
}

criterion_group!(benches, set_bench, update_if_bench, get_bench);
criterion_main!(benches);
