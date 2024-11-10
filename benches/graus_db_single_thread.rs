use bytes::{Bytes, BytesMut};
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
                let value = Bytes::from_static(b"value");
                for i in 1..(1 << 12) {
                    store
                        .set(Bytes::from(format!("key{}", i)), value.clone())
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
                let key = Bytes::from_static(b"key1");
                let value: u64 = 3500;
                store
                    .set(key.clone(), Bytes::copy_from_slice(&value.to_be_bytes()))
                    .unwrap();
                (store, temp_dir, key)
            },
            |(store, _temp_dir, key)| {
                let update_fn = |value: &mut BytesMut| {
                    let num = u64::from_le_bytes(
                        value.as_ref()[..8].try_into().expect("incorrect length"),
                    ) - 1;
                    value.copy_from_slice(&num.to_le_bytes());
                };
                let predicate = |value: &Bytes| {
                    let num = u64::from_le_bytes(value[..].try_into().expect("incorrect length"));
                    num > 0
                };

                for _ in 1..(1 << 12) {
                    let _ = store.update_if(key.to_owned(), update_fn, Some(&key), Some(predicate));
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
            let value = Bytes::from_static(b"value");
            for key_i in 1..(1 << i) {
                store
                    .set(Bytes::from(format!("key{}", key_i)), value.clone())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 16]);
            b.iter(|| {
                store
                    .get(&Bytes::from(format!("key{}", rng.gen_range(1, 1 << i))))
                    .unwrap();
            })
        });
    }
    group.finish();
}

criterion_group!(benches, set_bench, update_if_bench, get_bench);
criterion_main!(benches);
