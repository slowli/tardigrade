//! Tests for `Requests`.

use async_trait::async_trait;
use futures::{future, stream, FutureExt, SinkExt, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use std::collections::HashSet;

use tardigrade::{
    channel::{Request, RequestHandles, Response},
    task::TaskResult,
    test::TestInstance,
    workflow::{GetInterface, HandleFormat, SpawnWorkflow, Wasm, WithHandle, WorkflowFn},
    Json,
};

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

#[derive(Debug, Serialize, Deserialize)]
struct TestInit {
    strings: Vec<String>,
    options: Options,
}

#[derive(WithHandle, GetInterface)]
#[tardigrade(auto_interface)]
struct TestedWorkflow<Fmt: HandleFormat = Wasm> {
    #[tardigrade(flatten)]
    str_lengths: RequestHandles<String, usize, Json, Fmt>,
}

impl WorkflowFn for TestedWorkflow {
    type Args = TestInit;
    type Codec = Json;
}

#[async_trait(?Send)]
impl SpawnWorkflow for TestedWorkflow {
    async fn spawn(args: TestInit, handle: Self) -> TaskResult {
        let strings = args.strings;
        let options = args.options;
        let (requests, requests_task) = handle
            .str_lengths
            .process_requests()
            .with_capacity(options.capacity)
            .build();

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
                    .await?;
            } else {
                let responses = future::try_join_all(req_futures).await?;
                assert_eq!(responses, expected_responses);
            }
        } else {
            for (i, string) in strings.into_iter().enumerate() {
                let expected_response = string.len();
                let response_fut = requests.request(string);
                if options.ignore_some_responses && i % 2 == 0 {
                    assert!(response_fut.now_or_never().is_none()); // drops the response
                } else {
                    let response = response_fut.await?;
                    assert_eq!(response, expected_response);
                }
            }
            drop(requests);
        }

        requests_task.await?;
        Ok(())
    }
}

fn test_requests(init: TestInit) {
    println!("Testing with {:?}", init.options);

    let expected_strings = init.strings.clone();
    let options = init.options;
    TestInstance::<TestedWorkflow>::new(init).run(|api| async move {
        let mut str_lengths = api.str_lengths;
        let mut strings = vec![];
        let mut cancelled_ids = HashSet::new();
        while let Some(req) = str_lengths.requests.next().await {
            match req {
                Request::New { id, data, .. } => {
                    let response = Response {
                        id,
                        data: data.len(),
                    };
                    strings.push(data);
                    str_lengths.responses.send(response).await.ok();
                }
                Request::Cancel { id, .. } => {
                    cancelled_ids.insert(id);
                }
            }
        }
        assert_eq!(strings, expected_strings);

        match (options.drop_requests, options.ignore_some_responses) {
            (_, false) => assert!(cancelled_ids.is_empty(), "{cancelled_ids:?}"),
            (false, true) => {
                let len = strings.len() as u64;
                let expected_ids: HashSet<_> = (0..len).filter(|&i| i % 2 == 0).collect();
                assert_eq!(cancelled_ids, expected_ids);
            }
            (true, true) => {
                // We might have received some extra responses, hence the inequality check.
                let max_cancellations = (strings.len() + 1) / 2;
                assert!(
                    cancelled_ids.len() <= max_cancellations,
                    "{cancelled_ids:?}"
                );
            }
        }
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
