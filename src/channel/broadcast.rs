use futures::{Sink, Stream};
use slab::Slab;

use std::{
    cell::RefCell,
    collections::VecDeque,
    error, fmt,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

/// Errors that can occur when sending a message over [`BroadcastPublisher`].
#[derive(Debug)]
pub struct BroadcastError<T> {
    item: T,
}

impl<T> BroadcastError<T> {
    /// Extracts the item that was unsuccessfully sent.
    pub fn into_inner(self) -> T {
        self.item
    }
}

impl<T> fmt::Display for BroadcastError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("error publishing value over broadcast channel")
    }
}

impl<T: fmt::Debug> error::Error for BroadcastError<T> {}

/// State of a single `BroadcastSubscriber`.
#[derive(Debug)]
struct SubscriberState {
    next_item_idx: usize,
    next_item_waker: Option<Waker>,
}

impl SubscriberState {
    fn take_waker(&mut self) -> Option<Waker> {
        self.next_item_waker.take()
    }
}

/// State of a broadcast channel.
#[derive(Debug)]
struct BroadcastState<T> {
    is_closed: bool,
    buffered_items: VecDeque<T>,
    capacity: usize,
    next_item_idx: usize,
    capacity_waker: Option<Waker>,
    subscriber_states: Slab<SubscriberState>,
}

impl<T: Clone> BroadcastState<T> {
    fn new(capacity: usize) -> Self {
        Self {
            is_closed: false,
            buffered_items: VecDeque::with_capacity(capacity),
            capacity,
            next_item_idx: 0,
            capacity_waker: None,
            subscriber_states: Slab::with_capacity(1), // pre-allocate for one subscriber
        }
    }

    fn poll_next(&mut self, receiver_key: usize, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let receiver_state = self.subscriber_states.get_mut(receiver_key).unwrap();
        let receiver_offset = self.next_item_idx - receiver_state.next_item_idx;
        if receiver_offset > 0 {
            let item_idx = self.buffered_items.len() - receiver_offset;
            let item = self.buffered_items[item_idx].clone();
            self.advance_subscriber(receiver_key);
            return Poll::Ready(Some(item));
        }

        // At this point, the receiver has seen all buffered items.
        if self.is_closed {
            Poll::Ready(None)
        } else {
            receiver_state.next_item_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.buffered_items.len() < self.capacity {
            Poll::Ready(())
        } else {
            debug_assert_eq!(self.buffered_items.len(), self.capacity);

            let current_waker = self.capacity_waker.as_ref();
            let new_waker = cx.waker();
            if current_waker.map_or(true, |waker| !waker.will_wake(new_waker)) {
                self.capacity_waker = Some(new_waker.clone());
            }
            Poll::Pending
        }
    }

    fn push_item(&mut self, item: T) -> Result<(), BroadcastError<T>> {
        if self.is_closed || self.buffered_items.len() >= self.capacity {
            return Err(BroadcastError { item });
        }

        // TODO: is it sane to drop items in this case?
        if self.subscriber_states.is_empty() {
            // Since there are no subscribers, it's not necessary to buffer the item
            // or increase `next_item_idx`
            return Ok(());
        }

        self.buffered_items.push_back(item);
        self.next_item_idx += 1;
        self.wake_up_to_date_subscribers();
        Ok(())
    }

    fn wake_up_to_date_subscribers(&mut self) {
        for waker in self
            .subscriber_states
            .iter_mut()
            .filter_map(|(_, state)| state.take_waker())
        {
            waker.wake();
        }
    }

    fn advance_subscriber(&mut self, key: usize) {
        let receiver_state = self.subscriber_states.get_mut(key).unwrap();
        let prev_idx = receiver_state.next_item_idx;
        receiver_state.next_item_idx += 1;
        self.maybe_pop_items(prev_idx);
    }

    fn maybe_pop_items(&mut self, prev_idx: usize) {
        let was_at_earliest_idx = prev_idx == self.next_item_idx - self.buffered_items.len();
        if was_at_earliest_idx {
            // Check whether there are other receivers at earliest index
            let new_earliest_idx = self
                .subscriber_states
                .iter()
                .map(|(_, state)| state.next_item_idx)
                .min();

            // If there are no subscribers left, we can drop all buffered items.
            let new_earliest_idx = new_earliest_idx.unwrap_or(self.next_item_idx);
            if new_earliest_idx > prev_idx {
                for _ in prev_idx..new_earliest_idx {
                    self.buffered_items.pop_front();
                }
                if let Some(waker) = self.capacity_waker.take() {
                    waker.wake();
                }
            }
        }
    }

    fn create_subscriber(&mut self, next_item_idx: usize) -> usize {
        self.subscriber_states.insert(SubscriberState {
            next_item_idx,
            next_item_waker: None,
        })
    }

    fn drop_subscriber(&mut self, key: usize) {
        let subscriber_state = self.subscriber_states.remove(key);
        self.maybe_pop_items(subscriber_state.next_item_idx);
    }

    fn drop_publisher(&mut self) {
        self.is_closed = true;
        self.wake_up_to_date_subscribers();
    }
}

/// [`Sink`] that publishes its values to zero or more [`BroadcastSubscriber`]s.
///
/// The subscriber set is dynamic; subscribers can be created using [`Self::subscribe()`],
/// [`Clone`]d, and dropped. Each subscriber receives all values from the sink in the order
/// they are sent to the sink.
///
/// To deal with lagging subscribers, publisher has an internal buffer of items with the capacity
/// specified during [creation](Self::new()). Once a subscriber is sufficiently behind,
/// the publisher will not accept new items awaiting until the offending subscriber catches up
/// or is dropped.
#[derive(Debug)]
pub struct BroadcastPublisher<T: Clone> {
    state: Rc<RefCell<BroadcastState<T>>>,
}

impl<T: Clone> Drop for BroadcastPublisher<T> {
    fn drop(&mut self) {
        self.state.borrow_mut().drop_publisher();
    }
}

impl<T: Clone> BroadcastPublisher<T> {
    /// Creates a new broadcast with the specified `capacity`.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "Broadcast channel capacity must be positive");
        let state = BroadcastState::new(capacity);
        Self {
            state: Rc::new(RefCell::new(state)),
        }
    }

    /// Creates a new subscriber for this broadcast.
    pub fn subscribe(&self) -> BroadcastSubscriber<T> {
        let last_item_idx = self.state.borrow().next_item_idx;
        BroadcastSubscriber {
            key: self.state.borrow_mut().create_subscriber(last_item_idx),
            state: Rc::clone(&self.state),
        }
    }
}

impl<T: Clone> Sink<T> for BroadcastPublisher<T> {
    type Error = BroadcastError<T>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.state.borrow_mut().poll_ready(cx).map(Ok)
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.state.borrow_mut().push_item(item)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(())) // no internal buffering
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.state.borrow_mut().is_closed = true;
        Poll::Ready(Ok(()))
    }
}

/// Subscriber [`Stream`] corresponding to a [`BroadcastPublisher`].
#[derive(Debug)]
pub struct BroadcastSubscriber<T: Clone> {
    state: Rc<RefCell<BroadcastState<T>>>,
    /// Subscriber's key in the `Slab`.
    key: usize,
}

/// Clones the subscriber. The resulting subscriber starts from the same message as this subscriber,
/// and can be polled independently.
impl<T: Clone> Clone for BroadcastSubscriber<T> {
    fn clone(&self) -> Self {
        let key = {
            let mut state = self.state.borrow_mut();
            let next_item_idx = state.subscriber_states[self.key].next_item_idx;
            state.create_subscriber(next_item_idx)
        };

        Self {
            state: Rc::clone(&self.state),
            key,
        }
    }
}

impl<T: Clone> Drop for BroadcastSubscriber<T> {
    fn drop(&mut self) {
        self.state.borrow_mut().drop_subscriber(self.key);
    }
}

impl<T: Clone> Stream for BroadcastSubscriber<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.state.borrow_mut().poll_next(self.key, cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::{FutureExt, SinkExt, StreamExt};

    #[test]
    fn broadcast_state() {
        let mut broadcast = BroadcastPublisher::new(1);
        broadcast.send(0).now_or_never().unwrap().unwrap();
        assert!(broadcast.state.borrow().buffered_items.is_empty());

        let mut rx = broadcast.subscribe();
        assert!(rx.next().now_or_never().is_none());
        // ^ Since the receiver was created after the broadcast, the item should not be received

        broadcast.send(1).now_or_never().unwrap().unwrap();
        let received = rx.next().now_or_never().unwrap();
        assert_eq!(received, Some(1));
        assert!(rx.next().now_or_never().is_none());

        {
            let state = broadcast.state.borrow();
            assert!(state.buffered_items.is_empty());
            assert_eq!(state.next_item_idx, 1);
            assert!(state.capacity_waker.is_none());
            assert_eq!(state.subscriber_states.len(), 1);
            assert_eq!(state.subscriber_states[rx.key].next_item_idx, 1);
        }

        broadcast.send(2).now_or_never().unwrap().unwrap();
        assert!(broadcast.send(-1).now_or_never().is_none()); // channel is at capacity

        let mut other_rx = broadcast.subscribe();
        assert!(other_rx.next().now_or_never().is_none());
        assert!(broadcast.send(-1).now_or_never().is_none()); // channel is still at capacity

        let received = rx.next().now_or_never().unwrap();
        assert_eq!(received, Some(2));
        assert!(rx.next().now_or_never().is_none());
        assert!(other_rx.next().now_or_never().is_none());

        broadcast.send(3).now_or_never().unwrap().unwrap();
        assert!(broadcast.send(-1).now_or_never().is_none()); // channel is at capacity

        let received = rx.next().now_or_never().unwrap();
        assert_eq!(received, Some(3));
        assert!(rx.next().now_or_never().is_none());
        assert!(broadcast.send(-1).now_or_never().is_none());
        // ^ channel is at capacity, since `other_rx` hasn't received a new element

        let received = other_rx.next().now_or_never().unwrap();
        assert_eq!(received, Some(3));

        broadcast.send(4).now_or_never().unwrap().unwrap();
        let received = rx.next().now_or_never().unwrap();
        assert_eq!(received, Some(4));
        drop(other_rx);
        {
            let state = broadcast.state.borrow();
            assert!(state.buffered_items.is_empty());
            assert_eq!(state.subscriber_states.len(), 1);
        }

        broadcast.send(5).now_or_never().unwrap().unwrap();
        let received = rx.next().now_or_never().unwrap();
        assert_eq!(received, Some(5));

        drop(broadcast);
        let received = rx.next().now_or_never().unwrap();
        assert_eq!(received, None);
    }

    // FIXME: test cloning subscribers; receiving elements after sender is dropped / closed
}
