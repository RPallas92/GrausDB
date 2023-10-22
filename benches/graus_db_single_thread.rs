use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use graus_db::GrausDb;
use rand::prelude::*;
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
                    store.set(format!("key{}", i), "value".to_string()).unwrap();
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
                store.set(key.to_owned(), "3500".to_string()).unwrap();
                (store, temp_dir, key)
            },
            |(store, _temp_dir, key)| {
                let update_fn = |value: String| {
                    let num = value.parse::<i32>().unwrap();
                    (num - 1).to_string()
                };
                let predicate = |value: String| {
                    let num = value.parse::<i32>().unwrap();
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
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
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
