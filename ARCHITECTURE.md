# Tardigrade Architecture

*(Read [readme](README.md) first for a high-level intro.)*

## Key abstractions

There are 3 major abstractions involved in workflow management:

- **Engine:** how to instantiate and run workflows
- **Workers:** how to execute tasks that cannot (or shouldn't) be executed
  in the workflow code
- **Storage:** how to persist workflows

Engine and storage are abstracted (to a reasonable degree) in the runtime crate;
see its `engine` and `storage` modules respectively. APIs and implementation
framework for workers is placed in a separate crate. (This is because a worker
can be connected to a runtime using gRPC; thus, bringing the runtime would
bring many unnecessary dependencies such as `wasmtime`.)

Conceptually, the engine uses *message passing* as the foundational
architecture pattern (in particular defining what workflow interfaces look like
and how workflows can interact with the external world), and *WASM* modules / instances
as the implementation tool.

Workers conceptually are similar to message consumers in Kafka. A worker
has a single inbound request channel and a cursor of the last processed message.
The worker records are placed in the same storage as other runtime records,
meaning "exactly once" processing is possible, although it depends on the kind
of connection to the storage used. (E.g., gRPC connections have "at least once" semantics.)
From the workflow perspective, worker implementation is largely opaque; even the cursor-based
request handling and a single inbound request channel are just implementation details.

So far, workers do not utilize an app-level protocol (e.g., [RSocket]) for out-of-the-box
support of different communication patterns, and the only supported pattern is "request-response".
This can be amended in post-PoC releases; for now, embedding such a protocol seems to bring
more open questions than it would solve. (E.g., the flow control logic in RSocket seems to
conceptually clash with `Sink` / `Stream`-based flow control inherent in workflows;
and parts of the protocol like framing are unnecessary given that Tardigrade channels deal
with whole messages rather than bytes.)

The storage conceptually contains a relational model for workflows and communication channels
that should be reasonably easy to map to relational DBs (e.g., Postgres).
Note that while channel messages can be *eventually* sent to a message broker such as Apache Kafka,
initially they must be stored in the same DB as workflow data to achieve transactional workflow updates.
(I.e., the transaction outbox pattern.)

## Workflow modules

Sandboxing workflows as WASM modules brings multiple benefits:

- A workflow can be compiled once and run everywhere. The workflow artifact is well-defined
  and holistic.
- A workflow can be suspended at wait points, its state persisted and then restored
  completely transparently for workflow logic.
- The runtime can (theoretically) customize aspects of workflow execution, e.g., priorities
  of different tasks. It can create and escalate incidents on task failure (perhaps, continuing
  executing other tasks if possible), control rollback of the workflow state etc.
- A workflow could (theoretically) implemented in any WASM-targeting programming language
  and can run / be debugged in any WASM-supporting env (e.g., in a browser).
- Sandboxing means that interfaces for interacting with the external world are well-defined
  (e.g., in terms of security) and can be emulated (e.g., for testing) with reasonable effort.

Further, using Rust is beneficial since it has pull-based / cancellable futures
(vs push-based futures in JS, Java etc.). Task cancellation in particular
is a frequently-used feature in process orchestration (cf. interrupting subprocesses
and boundary events in [BPMN]).

Workflow definition is a WASM module that has well-defined imports / exports.
On the high level, imports define the following functionality:

- Acquiring workflow arguments and channels
- Implementation of [`Stream`] / [`Sink`] interfaces for inbound / outbound channels,
  respectively
- Runtime interface (spawning tasks, creating timers, polling timers, 
  polling tasks for completion)

Exports provide the following functionality:

- Creating the main task
- Polling and dropping tasks
- Creating and waking [`Waker`]s
- Low-level plumbing (e.g., allocating bytes)

Tasks (including the main task) are `Box<dyn Future<Output = ()>>`. Tasks and wakers
are put in a [`Slab`]-like data struct on the client
and are represented on the host side by unique numeric IDs.
This is motivated  by the fact that ID uniqueness is required in some cases,
so using pointers instead of IDs would be unsound.
Dropping tasks and wakers is controlled by the host.

ABI for host ↔ client conversions is (so far) implemented manually. Potentially,
[Canonical ABI] / [`wit-bindgen`] could be used instead, but this seems unwarranted
for now. (Additionally, the async part of `wit-bindgen` is unusable since
it ascribes to a push-based futures model, and workflows need a pull-based / cancellable one.)

In addition to imports / exports, a workflow module also defines a workflow interface
(e.g., names, description and specs for inputs and channels). It is serialized
as JSON in a custom WASM section, similar to how it is done in `wasm-bindgen`.

[BPMN]: https://en.wikipedia.org/wiki/Business_Process_Model_and_Notation
[`Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
[`Sink`]: https://docs.rs/futures/latest/futures/sink/trait.Sink.html
[`Waker`]: https://doc.rust-lang.org/std/task/struct.Waker.html
[`Slab`]: https://docs.rs/slab/latest/slab/struct.Slab.html
[Canonical ABI]: https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md
[`wit-bindgen`]: https://github.com/bytecodealliance/wit-bindgen
[RSocket]: https://rsocket.io/

### Error handling

Errors in tasks and workflows are explicit: the corresponding futures return 
`TaskResult = Result<(), TaskError>`. Internally, `TaskError` is similar to `anyhow::Error`
or similar app-level errors; it aggregates lower-level errors in a uniform format,
and also captures the code location via [`panic::Location`]; this is more lightweight
than capturing the entire backtrace. Like `anyhow::Error`, `TaskError` can capture
additional contexts, though they are lightweight as well (a message and code location).

As with OS processes, the workflow result is determined solely by completion of its main task;
this is more tractable and provides a way to propagate errors. Through some trickery
in the client code, `TaskError`s are propagated within a workflow without losing any info
about the error cause. Such a loss only occurs if an error crosses the client–host boundary
(the original cause is replaced with its `Display`ed message).

Unlike OS processes, panics are not well handled in tasks; a panic in *any* task
that has bubbled up to the task level (i.e., has led to a WASM [`Trap`]) is considered 
to lead to the corrupted workflow state. The `WorkflowManager` can abort such workflows, 
or propagate the error externally. (Other ways to deal with traps could be added in the future.)
The reason for this is that there is no guarantee that a panic unwinds the call stack;
the compilation profile may set `panic = abort`. Thus, the workflow memory
can indeed be corrupted after a panic. Beside broken invariants encapsulated in [`UnwindSafe`],
this corruption may be as simple as non-freed heap allocations, or the broken shadow stack pointer. 

[`panic::Location`]: https://doc.rust-lang.org/std/panic/struct.Location.html
[`Trap`]: https://docs.rs/wasmtime/1/wasmtime/struct.Trap.html
[`UnwindSafe`]: https://doc.rust-lang.org/std/panic/trait.UnwindSafe.html

## Typed workflows

To interact with workflows, Tardigrade uses a concept of a *handle*. A workflow
handle is composed of the handles of its elements, such as channels,
usually using the proc macros from the corresponding crate. Handle type is parameterized
by the *environment*; besides WASM client env, there are environments
for workflow testing and for interacting with a workflow executing in a runtime
(low-level sync and async versions).

Handles allow interacting with a workflow in a type-safe way, but the API
is also flexible enough to allow workflows with the interface not known at compilation time.

## Repository organization

### Crates

- The [`tardigrade`](crates/lib) crate provides client bindings
  (i.e., it is necessary to include it as a dependency for workflow WASM modules).
- The [`tardigrade-derive`](crates/derive) crate provides proc macros
  for client bindings.
- The [`tardigrade-shared`](crates/shared) crate provides some types
  that are used by multiple crates, in particular, the macro crate.
- The [`tardigrade-rt`](crates/rt) crate provides an embeddable runtime
  for workflow modules.
- The [`tardigrade-worker`](crates/worker) crate provides API definitions
  and implementation framework for workers.
- The [`tardigrade-grpc`](crates/grpc) crate provides a gRPC service wrapper
  for the runtime. (Actually, there are multiple services split by the domain;
  e.g., functionality related to channels is extracted into a separate service.)
- The [`tardigrade-cli`](crates/cli) crate provides a gRPC server binary
  with the aforementioned services and some other functionality, such as
  the reflection service and tracing support.

### Other stuff

- [Sample workflow](e2e-tests/pizza) tests all other crates end-to-end.
