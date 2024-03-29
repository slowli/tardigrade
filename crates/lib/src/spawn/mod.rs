//! Spawning and managing child workflows.
//!
//! # Examples
//!
//! ```
//! # use async_trait::async_trait;
//! # use futures::{SinkExt, StreamExt};
//! #
//! # use std::error;
//! #
//! # use tardigrade::{
//! #     channel::{Sender, Receiver},
//! #     task::TaskResult,
//! #     workflow::{GetInterface, InEnv, SpawnWorkflow, WithHandle, Wasm, HandleFormat, WorkflowFn},
//! #     Json,
//! # };
//! // Assume we want to spawn a child workflow defined as follows:
//! #[derive(GetInterface, WithHandle)]
//! #[tardigrade(derive(Debug), auto_interface)]
//! pub struct ChildWorkflow<Fmt: HandleFormat = Wasm> {
//!     pub commands: InEnv<Receiver<String, Json>, Fmt>,
//!     pub events: InEnv<Sender<String, Json>, Fmt>,
//! }
//!
//! impl WorkflowFn for ChildWorkflow {
//!     type Args = ();
//!     type Codec = Json;
//! }
//! # #[async_trait(?Send)]
//! # impl SpawnWorkflow for ChildWorkflow {
//! #     async fn spawn(_args: (), handle: Self) -> TaskResult {
//! #         handle.commands.map(Ok).forward(handle.events).await?;
//! #         Ok(())
//! #     }
//! # }
//!
//! // To spawn a workflow, we should use the following code
//! // in the parent workflow:
//! use tardigrade::spawn::{CreateWorkflow, Workflows};
//! # use tardigrade::test::Runtime;
//!
//! # let mut runtime = Runtime::default();
//! # runtime.workflow_registry_mut().insert::<ChildWorkflow>("child");
//! # runtime.run(async {
//! let builder = Workflows.new_workflow::<ChildWorkflow>("child").await?;
//! let (child_handles, mut self_handles) = builder
//!     .handles(|config| {
//!         // It is possible to customize child workflow initialization via
//!         // `builder.handle()`, but zero config is fine as well.
//!     })
//!     .await;
//! let child = builder.build((), child_handles).await?;
//! // `child` contains handles to the created channels.
//! self_handles.commands.send("ping".to_owned()).await?;
//! # drop(self_handles.commands);
//! let event: String = self_handles.events.next().await.unwrap();
//! # assert_eq!(event, "ping");
//! child.await?;
//! # Ok::<_, Box<dyn error::Error>>(())
//! # }).unwrap();
//! ```

use async_trait::async_trait;
use futures::future::BoxFuture;
use pin_project_lite::pin_project;

use std::{
    borrow::Cow,
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

mod config;
#[cfg(target_arch = "wasm32")]
#[path = "imp_wasm32.rs"]
pub(crate) mod imp;
#[cfg(not(target_arch = "wasm32"))]
#[path = "imp_mock.rs"]
mod imp;

pub use self::config::{ReceiverConfig, SenderConfig, Spawner};
pub use crate::error::HostError;

use self::config::HandlesBuilder;
use crate::{
    channel::{channel, RawReceiver, RawSender},
    handle::{AccessError, AccessErrorKind, HandlePath},
    interface::Interface,
    task::JoinError,
    workflow::{GetInterface, HandleFormat, InEnv, Inverse, Wasm, WithHandle, WorkflowFn},
};

/// Manager of [`Interface`]s that allows obtaining an interface by a string identifier.
#[async_trait]
pub trait ManageInterfaces {
    /// Returns the interface spec of a workflow with the specified ID.
    async fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>>;
}

/// Manager of workflow channels.
#[async_trait]
pub trait CreateChannel {
    /// Format of handles that this manager operates in.
    type Fmt: HandleFormat;

    /// Returns an instance of closed receiver. Since it's closed, aliasing is not a concern.
    fn closed_receiver(&self) -> <Self::Fmt as HandleFormat>::RawReceiver;
    /// Returns an instance of closed sender.
    fn closed_sender(&self) -> <Self::Fmt as HandleFormat>::RawSender;

    /// Creates a new workflow channel.
    async fn new_channel(
        &self,
    ) -> (
        <Self::Fmt as HandleFormat>::RawSender,
        <Self::Fmt as HandleFormat>::RawReceiver,
    );
}

/// Manager of workflows that allows spawning them.
///
/// Depending on the context, the spawned workflow may be a child workflow (if executed
/// in the context of a workflow, via [`Workflows`]), or the top-level workflow
/// (if executed from the host).
#[async_trait]
pub trait CreateWorkflow: ManageInterfaces + CreateChannel {
    /// Handle to an instantiated workflow.
    type Spawned<W: WorkflowFn + WithHandle>;
    /// Error spawning a workflow.
    type Error: 'static + Send + Sync;

    #[doc(hidden)] // implementation detail; should only be used via `WorkflowBuilder`
    fn new_workflow_unchecked<W: WorkflowFn + WithHandle>(
        &self,
        definition_id: &str,
        args: W::Args,
        handles: W::Handle<Self::Fmt>,
    ) -> BoxFuture<'_, Result<Self::Spawned<W>, Self::Error>>;

    /// Initiates creating a new workflow and returns the corresponding builder.
    ///
    /// # Errors
    ///
    /// Returns an error on interface mismatch between `W` and the workflow definition
    /// contained in this manager under `definition_id`.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err)
    )]
    async fn new_workflow<'a, W: WorkflowFn + GetInterface>(
        &'a self,
        definition_id: &'a str,
    ) -> Result<WorkflowBuilder<'a, Self, W>, AccessError>
    where
        Self: Sized,
    {
        let provided_interface = self
            .interface(definition_id)
            .await
            .ok_or(AccessErrorKind::Missing)?;
        W::interface().check_compatibility(&provided_interface)?;

        Ok(WorkflowBuilder {
            manager: self,
            interface: provided_interface,
            definition_id,
            _ty: PhantomData,
        })
    }
}

/// Builder of child workflows.
pub struct WorkflowBuilder<'a, M: CreateWorkflow, W> {
    manager: &'a M,
    interface: Cow<'a, Interface>,
    definition_id: &'a str,
    _ty: PhantomData<W>,
}

impl<M: CreateWorkflow, W> fmt::Debug for WorkflowBuilder<'_, M, W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowBuilder")
            .field("interface", &self.interface)
            .field("definition_id", &self.definition_id)
            .finish()
    }
}

impl<'a, M, W> WorkflowBuilder<'a, M, W>
where
    M: CreateWorkflow,
    M::Spawned<W>: 'a,
    W: WorkflowFn + WithHandle,
{
    /// Creates a new workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow cannot be spawned, e.g., if the provided `args`
    /// are incorrect.
    pub fn build(
        self,
        args: W::Args,
        handles: W::Handle<M::Fmt>,
    ) -> impl Future<Output = Result<M::Spawned<W>, M::Error>> + Send + 'a {
        self.manager
            .new_workflow_unchecked::<W>(self.definition_id, args, handles)
    }
}

impl<'a, M, W> WorkflowBuilder<'a, M, W>
where
    M: CreateWorkflow,
    W: WorkflowFn + WithHandle,
{
    /// Builds the handles pair allowing to configure handles in the process.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "debug",
            skip_all,
            fields(self.definition_id = self.definition_id)
        )
    )]
    #[allow(clippy::missing_panics_doc)] // false positive
    pub async fn handles<F>(&self, config_fn: F) -> HandlesPair<W, M::Fmt>
    where
        F: FnOnce(&InEnv<W, Spawner<M::Fmt>>),
    {
        let (handles_builder, config) = <HandlesBuilder<M::Fmt>>::new::<W>(&self.interface);
        config_fn(&config);
        drop(config);
        let mut container = handles_builder.build(self.manager).await;

        let self_handles = W::take_from_untyped(&mut container, HandlePath::EMPTY).unwrap();
        let mut container = container.for_child();
        let child_handles = W::take_from_untyped(&mut container, HandlePath::EMPTY).unwrap();
        (child_handles, self_handles)
    }
}

/// Pair of handles with a certain interface in a certain format and the inverse format.
pub type HandlesPair<W, Fmt> = (InEnv<W, Fmt>, InEnv<W, Inverse<Fmt>>);

/// Client-side connection to a [workflow manager][`ManageWorkflows`].
#[derive(Debug)]
pub struct Workflows;

#[async_trait]
impl CreateChannel for Workflows {
    type Fmt = Wasm;

    fn closed_receiver(&self) -> RawReceiver {
        RawReceiver::closed()
    }

    fn closed_sender(&self) -> RawSender {
        RawSender::closed()
    }

    async fn new_channel(&self) -> (RawSender, RawReceiver) {
        channel().await
    }
}

impl CreateWorkflow for Workflows {
    type Spawned<W: WorkflowFn + WithHandle> = RemoteWorkflow;
    type Error = HostError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self, args, handles))
    )]
    fn new_workflow_unchecked<W: WorkflowFn + WithHandle>(
        &self,
        definition_id: &str,
        args: W::Args,
        handles: W::Handle<Wasm>,
    ) -> BoxFuture<'_, Result<Self::Spawned<W>, Self::Error>> {
        imp::new_workflow::<W>(definition_id, args, handles)
    }
}

pin_project! {
    /// Handle to a remote workflow (usually, a child workflow previously spawned using
    /// [`Workflows`]).
    ///
    /// The handle can be polled for completion.
    #[derive(Debug)]
    #[repr(transparent)]
    pub struct RemoteWorkflow {
        #[pin]
        inner: imp::RemoteWorkflow,
    }
}

// TODO: allow to abort workflows for symmetry with tasks

impl Future for RemoteWorkflow {
    type Output = Result<(), JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}
