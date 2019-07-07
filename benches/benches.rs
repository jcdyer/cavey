use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use rand::{thread_rng, Rng};
use tempfile::TempDir;

use cavey::{CaveyEngine, CaveyStore, SledStore};

fn length_string(length: usize) -> String {
    let mut rng = thread_rng();
    let mut s = String::with_capacity(length);
    for _ in 0..length {
        s.push(rng.gen_range(0x30, 0x7a).into()); // ASCII characters between 0 and z
    }
    s
}

fn build_kv_pairs(count: usize, maxlen: usize) -> Vec<(String, String)> {
    let mut rng = thread_rng();
    (0..count)
        .map(|_| {
            let key = length_string(rng.gen_range(0, maxlen));
            let value = length_string(rng.gen_range(0, maxlen));
            (key, value)
        })
        .collect()
}

fn write_benchmark(c: &mut Criterion) {
    let inputs = vec!["cavey", "sled"];
    c.bench_function_over_inputs(
        "write",
        |b, &engine| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let engine: Box<dyn CaveyEngine> = match engine {
                        "sled" => Box::new(SledStore::open(&temp_dir).unwrap()),
                        "cavey" => Box::new(CaveyStore::open(&temp_dir).unwrap()),
                        _ => panic!("engine must be one of cavey|sled"),
                    };
                    (temp_dir, engine, build_kv_pairs(100, 100_000))  // Keep temp_dir alive longer...
                },
                |(temp_dir, mut engine, pairs)| {
                    for (key, value) in pairs {
                        let val = engine.put(key, value);
                        if !val.is_ok() {
                            eprintln!("{:?}", val);
                        }
                        assert!(val.is_ok());
                    }
                    drop(temp_dir);
                },
                BatchSize::LargeInput,
            );
        },
        inputs,
    );
}

fn read_benchmark(c: &mut Criterion) {
    let inputs = vec!["sled", "cavey"];
    c.bench_function_over_inputs(
        "read",
        |b, &engine_name| {
            let pairs = build_kv_pairs(10, 100_000);

            let sled_dir = TempDir::new().unwrap();
            let cavey_dir = TempDir::new().unwrap();
            {
                let mut sled_engine: Box<dyn CaveyEngine> = Box::new(SledStore::open(&sled_dir).unwrap());
                let mut cavey_engine: Box<dyn CaveyEngine> = Box::new(CaveyStore::open(&cavey_dir).unwrap());

                for (key, value) in pairs.clone() {
                    sled_engine.put(key, value).unwrap();
                }
                for (key, value) in pairs.clone() {
                    cavey_engine.put(key, value).unwrap();
                }
            }
            b.iter_batched(
                || {
                    let engine: Box<dyn CaveyEngine> = match engine_name {
                        "sled" => Box::new(SledStore::open(&sled_dir).unwrap()),
                        "cavey" => Box::new(CaveyStore::open(&cavey_dir).unwrap()),
                        _ => panic!("engine must be one of cavey|sled"),
                    };
                    (engine, pairs.clone())
                },
                |(mut engine, pairs)| {
                    for (key, value) in pairs {
                        assert_eq!(engine.get(key).unwrap(), Some(value));
                    }
                },
                BatchSize::LargeInput,
            );
        },
        inputs,
    );
}

criterion_group!(benches, write_benchmark, read_benchmark);
criterion_main!(benches);
