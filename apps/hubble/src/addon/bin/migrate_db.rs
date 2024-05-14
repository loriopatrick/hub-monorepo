use std::{
    sync::{mpsc::channel, Arc},
    time::Instant,
};

use addon::{db::RocksDB, store::PageOptions};
use rocksdb::{DBCompressionType, Options};

const THREADS: usize = 16;

fn main() {
    println!("Starting");

    let source_db = RocksDB::new(
        "/home/plorio/src/farcaster/hub-monorepo/apps/hubble/.rocks/rocks.hub._default",
    )
    .unwrap();
    let dest_db = Arc::new(
        RocksDB::new(
            "/home/plorio/src/farcaster/hub-monorepo/apps/hubble/.rocks3/rocks.hub._default",
        )
        .unwrap(),
    );

    source_db.open().unwrap();
    dest_db
        .open_with_opt({
            let mut opt = Options::default();
            opt.create_if_missing(true);
            opt.set_compression_type(DBCompressionType::Lz4);
            opt
        })
        .unwrap();

    let mut threads = vec![];
    let mut senders = vec![];

    for i in 0..THREADS {
        let (item_tx, item_rx) = channel::<(&'static [u8], &'static [u8])>();

        senders.push(item_tx);

        let dest_db = dest_db.clone();

        let handle = std::thread::spawn(move || {
            println!("Thread {} started", i);
            while let Ok((key, value)) = item_rx.recv() {
                dest_db.put(key, value).unwrap();
            }
            println!("Thread {} closed", i);
        });

        threads.push(handle);
    }

    let mut count = 0;
    let mut last_count_ts = Instant::now();

    source_db
        .for_each_iterator_by_prefix(&[], &PageOptions::default(), |key, value| {
            count += 1;

            senders[count % senders.len()]
                .send(unsafe { (std::mem::transmute(key), std::mem::transmute(value)) })
                .unwrap();

            if count % 100_000 == 0 {
                let now = Instant::now();
                let elapsed = now.duration_since(last_count_ts).as_millis();
                let per_second = elapsed as f64 / 100.0;
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
    dest_db.clear().unwrap();
}
