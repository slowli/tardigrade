//! Tests for `Requests`.

use futures::{future, stream, FutureExt, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use tardigrade::{
    channel::{Receiver, Requests, Sender, WithId},
    test::TestWorkflow,
    workflow::{GetInterface, Handle, Init, SpawnWorkflow, TaskHandle, Wasm},
    Data, Json,
};
use tardigrade_shared::interface::Interface;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct Options {
    capacity: usize,
    drop_requests: bool,
    ignore_some_responses: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            capacity: 1,
            drop_requests: false,
            ignore_some_responses: false,
        }
    }
}

#[derive(Debug)]
struct TestedWorkflow;

impl GetInterface for TestedWorkflow {
    const WORKFLOW_NAME: &'static str = "TestedWorkflow";

    fn interface() -> Interface<Self> {
        const SPEC: &[u8] = br#"{
            "v": 0,
            "data": { "strings": {}, "options": {} },
            "in": { "responses": {} },
            "out": { "requests": {} }
        }"#;
        Interface::from_bytes(SPEC).downcast().unwrap()
    }
}

#[tardigrade::handle(for = "TestedWorkflow")]
struct TestHandle<Env> {
    strings: Handle<Data<Vec<String>, Json>, Env>,
    options: Handle<Data<Options, Json>, Env>,
    requests: Handle<Sender<WithId<String>, Json>, Env>,
    responses: Handle<Receiver<WithId<usize>, Json>, Env>,
}

#[tardigrade::init(for = "TestedWorkflow")]
struct TestInit {
    strings: Init<Data<Vec<String>, Json>>,
    options: Init<Data<Options, Json>>,
}

impl SpawnWorkflow for TestedWorkflow {
    fn spawn(handle: TestHandle<Wasm>) -> TaskHandle {
        let strings = handle.strings.into_inner();
        let options = handle.options.into_inner();
        TaskHandle::new(async move {
            let requests = Requests::new(options.capacity, handle.requests, handle.responses);

            if options.drop_requests {
                let expected_responses: Vec<_> = strings.iter().map(String::len).collect();

                let req_futures: Vec<_> = strings
                    .into_iter()
                    .map(|string| requests.request(string))
                    .collect();
                drop(requests);

                if options.ignore_some_responses {
                    // Wait until half of futures is resolved.
                    let len = req_futures.len();
                    stream::iter(req_futures)
                        .buffer_unordered(len)
                        .take(len / 2)
                        .try_for_each(|_| future::ready(Ok(())))
                        .await
                        .unwrap();
                } else {
                    let responses = future::try_join_all(req_futures).await.unwrap();
                    assert_eq!(responses, expected_responses);
                }
            } else {
                for (i, string) in strings.into_iter().enumerate() {
                    let expected_response = string.len();
                    let response_fut = requests.request(string);
                    if options.ignore_some_responses && i % 2 == 0 {
                        assert!(response_fut.now_or_never().is_none()); // drops the response
                    } else {
                        let response = response_fut.await.unwrap();
                        assert_eq!(response, expected_response);
                    }
                }
            }
        })
    }
}

fn test_requests(init: TestInit) {
    println!("Testing with {:?}", init.options);

    let expected_strings = init.strings.clone();
    TestedWorkflow::test(init, |mut handle| async move {
        let mut strings = vec![];
        while let Some(WithId { id, data }) = handle.api.requests.next().await {
            let response = WithId {
                id,
                data: data.len(),
            };
            strings.push(data);
            handle.api.responses.send(response).await;
        }
        assert_eq!(strings, expected_strings);
    });
}

#[test]
fn single_request() {
    test_requests(TestInit {
        strings: vec!["test".to_owned()],
        options: Options::default(),
    });
}

#[test]
fn single_request_with_early_drop() {
    test_requests(TestInit {
        strings: vec!["test".to_owned()],
        options: Options {
            drop_requests: true,
            ..Options::default()
        },
    });
}

#[test]
fn multiple_requests() {
    test_requests(TestInit {
        strings: vec!["test".to_owned(), "foo".to_owned(), "bar".to_owned()],
        options: Options::default(),
    });
}

#[test]
fn multiple_requests_with_early_drop() {
    test_requests(TestInit {
        strings: vec!["test".to_owned(), "foo".to_owned(), "bar".to_owned()],
        options: Options {
            drop_requests: true,
            ..Options::default()
        },
    });
}

#[test]
fn multiple_requests_ignoring_some_responses() {
    test_requests(TestInit {
        strings: vec!["test".to_owned(), "foo".to_owned(), "bar".to_owned()],
        options: Options {
            ignore_some_responses: true,
            ..Options::default()
        },
    });
}

#[test]
fn multiple_requests_with_early_drop_ignoring_some_responses() {
    test_requests(TestInit {
        strings: vec!["test".to_owned(), "foo".to_owned(), "bar".to_owned()],
        options: Options {
            drop_requests: true,
            ignore_some_responses: true,
            ..Options::default()
        },
    });
}

#[test]
fn multiple_requests_with_higher_capacity() {
    for capacity in [2, 3, 5, 7, 10, 15] {
        test_requests(TestInit {
            strings: (0..10).map(|i| i.to_string()).collect(),
            options: Options {
                capacity,
                ..Options::default()
            },
        });
    }
}

#[test]
fn multiple_requests_with_higher_capacity_ignoring_some_responses() {
    for capacity in [2, 3, 5, 7, 10, 15] {
        test_requests(TestInit {
            strings: (0..10).map(|i| i.to_string()).collect(),
            options: Options {
                capacity,
                ignore_some_responses: true,
                ..Options::default()
            },
        });
    }
}