//! High-level tests for channel types.

use async_io::Timer;
use futures::{
    executor::{LocalPool, LocalSpawner},
    future, stream,
    task::LocalSpawnExt,
    FutureExt, SinkExt, Stream, StreamExt,
};
use rand::{thread_rng, Rng};

use std::{future::Future, time::Duration};

use tardigrade::{channel::BroadcastPublisher, task::yield_now};

async fn test_broadcast_without_delays(spawner: LocalSpawner) {
    let broadcast = BroadcastPublisher::<u32>::new(1);
    let rxs = (0..10).map(|threshold| {
        let rx = broadcast.subscribe();
        rx.take_while(move |&x| future::ready(x < threshold))
            .collect::<Vec<_>>()
    });
    let rxs: Vec<_> = rxs.collect();

    let send_items = stream::iter(0..20)
        .map(Ok)
        .forward(broadcast)
        .map(Result::unwrap);
    spawner.spawn_local(send_items).unwrap();

    let rx_elements = future::join_all(rxs).await;
    for (i, elements) in rx_elements.into_iter().enumerate() {
        assert_eq!(elements, (0..i as u32).collect::<Vec<_>>());
    }
}

#[test]
fn broadcast_without_delays() {
    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    pool.run_until(test_broadcast_without_delays(spawner));
}

const ITEMS_COUNT: usize = 10;

fn tested_capacities() -> impl Iterator<Item = usize> {
    1..10
}

async fn collect_with_delays<F: Future<Output = ()>>(
    mut subscriber: impl Stream<Item = u32> + Unpin,
    delay_fn: fn() -> F,
) -> Vec<u32> {
    let mut rng = thread_rng();
    let mut items = Vec::with_capacity(ITEMS_COUNT);
    while let Some(item) = subscriber.next().await {
        items.push(item);
        if rng.gen_range(0.0..1.0) < 0.1 {
            delay_fn().await;
        }
    }
    items
}

async fn test_broadcast_with_delays<F>(capacity: usize, spawner: LocalSpawner, delay_fn: fn() -> F)
where
    F: Future<Output = ()> + 'static,
{
    let mut broadcast = BroadcastPublisher::<u32>::new(capacity);
    let mut rng = thread_rng();
    let (expected_lengths, subscriber_handles): (Vec<_>, Vec<_>) = (0..5)
        .map(|_| {
            let cutoff_idx = if rng.gen() {
                ITEMS_COUNT
            } else {
                rng.gen_range(1..ITEMS_COUNT)
            };
            let subscriber = broadcast.subscribe().take(cutoff_idx);
            let task = collect_with_delays(subscriber, delay_fn);
            let task_handle = spawner.spawn_local_with_handle(task).unwrap();
            (cutoff_idx, task_handle)
        })
        .unzip();
    let outputs = future::join_all(subscriber_handles);

    for item in 0..ITEMS_COUNT as u32 {
        broadcast.send(item).await.unwrap();
        if rng.gen_range(0.0..1.0) < 0.2 {
            delay_fn().await;
        }
    }
    drop(broadcast);

    for (output, expected_len) in outputs.await.into_iter().zip(expected_lengths) {
        assert_eq!(output.len(), expected_len);
        assert!(
            output.iter().copied().eq(0..expected_len as u32),
            "{:?}",
            output
        );
    }
}

#[test]
fn broadcast_with_yields() {
    for capacity in tested_capacities() {
        println!("Testing with capacity {}", capacity);

        let mut pool = LocalPool::new();
        let spawner = pool.spawner();
        pool.spawner()
            .spawn_local(test_broadcast_with_delays(capacity, spawner, yield_now))
            .unwrap();
        pool.run();
    }
}

#[test]
fn broadcast_with_delays() {
    let delay_fn = || async {
        let sleep_duration = thread_rng().gen_range(1..=5);
        Timer::after(Duration::from_millis(sleep_duration)).await;
    };

    for capacity in tested_capacities() {
        println!("Testing with capacity {}", capacity);

        let mut pool = LocalPool::new();
        let spawner = pool.spawner();
        pool.spawner()
            .spawn_local(test_broadcast_with_delays(capacity, spawner, delay_fn))
            .unwrap();
        pool.run();
    }
}
