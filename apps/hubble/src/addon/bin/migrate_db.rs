use std::{
    sync::{mpsc::sync_channel, Arc},
    time::Instant,
};

use addon::{db::RocksDB, store::PageOptions};
use rocksdb::{DBCompressionType, Options};

const THREADS: usize = 8;
const COUNT_PER_PRINT: usize = 500_000;

fn main() {
    let mut opts = Options::default();
    opts.set_compression_type(DBCompressionType::Lz4);
    opts.create_if_missing(true);
    opts.set_allow_concurrent_memtable_write(true);
    opts.increase_parallelism(4);

    migrate_db(
        "/home/plorio/src/farcaster/hub-monorepo/apps/hubble/.rocks/rocks.hub._default",
        "/home/plorio/src/farcaster/hub-monorepo/apps/hubble/.rocks-lz4/rocks.hub._default",
        opts.clone(),
    );

    migrate_db(
        "/home/plorio/src/farcaster/hub-monorepo/apps/hubble/.rocks/rocks.hub._default/trieDb",
        "/home/plorio/src/farcaster/hub-monorepo/apps/hubble/.rocks-lz4/rocks.hub._default/trieDb",
        opts.clone(),
    );
}

fn migrate_db(source: &str, dest: &str, opts: Options) -> usize {
    println!("Starting");

    let source_db = RocksDB::new(source).unwrap();
    let dest_db = Arc::new(RocksDB::new(dest).unwrap());

    source_db.open().unwrap();
    dest_db.open_with_opt(opts).unwrap();

    let mut threads = vec![];
    let mut senders = vec![];

    for i in 0..THREADS {
        let (item_tx, item_rx) = sync_channel::<(Vec<u8>, Vec<u8>)>(2048);

        senders.push(item_tx);

        let dest_db = dest_db.clone();

        let handle = std::thread::spawn(move || {
            println!("Thread {} started", i);
            while let Ok((key, value)) = item_rx.recv() {
                dest_db.put(&key, &value).unwrap();
            }
            println!("Thread {} closed", i);
        });

        threads.push(handle);
    }

    let mut count = 0;
    let mut last_count_ts = Instant::now();

    let mut first_key = None;
    dest_db
        .for_each_iterator_by_prefix(
            &[],
            &PageOptions {
                reverse: true,
                ..Default::default()
            },
            |key, _| {
                first_key = Some(key.to_vec());
                Ok(true)
            },
        )
        .unwrap();

    source_db
        .for_each_iterator_by_prefix(&[], &PageOptions::default(), |key, value| {
            count += 1;

            senders[count % senders.len()]
                .send((key.to_vec(), value.to_vec()))
                .unwrap();

            if count % COUNT_PER_PRINT == 0 {
                let now = Instant::now();
                let elapsed = now.duration_since(last_count_ts).as_secs_f64();
                let per_second = COUNT_PER_PRINT as f64 / elapsed;
                last_count_ts = now;
                println!("queued {}, {per_second}mps", count);
            }

            Ok(false)
        })
        .unwrap();

    drop(senders);

    println!("DONE reading from database {}", count);

    for handle in threads {
        handle.join().unwrap();
    }

    println!("DONE writing to database {}", count);

    source_db.close().unwrap();
    dest_db.close().unwrap();

    count
}
