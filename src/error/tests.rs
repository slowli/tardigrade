use super::*;

// TODO: test `ErrorContextExt`

#[test]
fn task_error_string_presentation() {
    let mut err = TaskError::from_parts(
        "operation failed".to_owned(),
        ErrorLocation {
            filename: Cow::Borrowed("error.rs"),
            line: 42,
            column: 10,
        },
    );
    assert_eq!(err.to_string(), "error.rs:42:10: operation failed");
    assert_eq!(format!("{err:#}"), "error.rs:42:10: operation failed");

    err.push_context_from_parts(
        "additional context".to_owned(),
        ErrorLocation {
            filename: Cow::Borrowed("lib.rs"),
            line: 100,
            column: 8,
        },
    );
    assert_eq!(
        err.to_string(),
        "lib.rs:100:8: additional context (+1 cause)"
    );
    let expected_message = "lib.rs:100:8: additional context\n\
            Caused by:\n    error.rs:42:10: operation failed\n";
    assert_eq!(format!("{err:#}"), expected_message);
}

#[test]
fn creating_task_error_from_other_error() {
    let err = TaskError::from(SendError::Closed);
    assert!(err.location().filename.ends_with("tests.rs"));

    let err = Err::<(), _>(SendError::Full)
        .context("failed sending trace")
        .unwrap_err();
    assert_error_with_context(&err, "channel is full", "failed sending trace");
    assert!(err.cause().is::<SendError>());

    let channel_name = "trace";
    let err = Err::<(), _>(SendError::Full)
        .with_context(|| format!("failed sending {channel_name}"))
        .unwrap_err();
    assert_error_with_context(&err, "channel is full", "failed sending trace");
    assert!(err.cause().is::<SendError>());
}

fn assert_error_with_context(err: &TaskError, message: &str, ctx_message: &str) {
    assert!(err.location().filename.ends_with("tests.rs"));
    assert_eq!(err.cause().to_string(), message);
    assert_eq!(err.contexts().len(), 1);
    assert!(err.contexts()[0].location().filename.ends_with("tests.rs"));
    assert_eq!(err.contexts()[0].message(), ctx_message);
}

#[test]
fn creating_task_error_from_option() {
    fn get_number() -> Option<u32> {
        None
    }

    let err = get_number().context("failed getting number").unwrap_err();
    assert!(err.location().filename.ends_with("tests.rs"));
    assert_eq!(err.cause().to_string(), "failed getting number");

    let ty = "number";
    let err = get_number()
        .with_context(|| format!("failed getting {ty}"))
        .unwrap_err();
    assert!(err.location().filename.ends_with("tests.rs"));
    assert_eq!(err.cause().to_string(), "failed getting number");
}

#[test]
fn adding_context_to_task_result() {
    fn get_number() -> TaskResult<u32> {
        Err(TaskError::new("no numbers available"))
    }

    let err = get_number().context("failed getting number").unwrap_err();
    assert_error_with_context(&err, "no numbers available", "failed getting number");

    let ty = "number";
    let err = get_number()
        .with_context(|| format!("failed getting {ty}"))
        .unwrap_err();
    assert_error_with_context(&err, "no numbers available", "failed getting number");
}
