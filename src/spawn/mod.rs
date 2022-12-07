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
//! #     workflow::{GetInterface, InEnv, SpawnWorkflow, TakeHandle, Wasm, WorkflowEnv, WorkflowFn},
//! #     Json,
//! # };
//! // Assume we want to spawn a child workflow defined as follows:
//! #[derive(Debug, GetInterface, TakeHandle)]
//! #[tardigrade(handle = "ChildHandle", auto_interface)]
//! pub struct ChildWorkflow(());
//!
//! #[derive(TakeHandle)]
//! #[tardigrade(derive(Debug))]
//! pub struct ChildHandle<Env: WorkflowEnv> {
//!     pub commands: InEnv<Receiver<String, Json>, Env>,
//!     pub events: InEnv<Sender<String, Json>, Env>,
//! }
//!
//! impl WorkflowFn for ChildWorkflow {
//!     type Args = ();
//!     type Codec = Json;
//! }
//! # #[async_trait(?Send)]
//! # impl SpawnWorkflow for ChildWorkflow {
//! #     async fn spawn(_args: (), handle: ChildHandle<Wasm>) -> TaskResult {
//! #         handle.commands.map(Ok).forward(handle.events).await?;
//! #         Ok(())
//! #     }
//! # }
//!
//! // To spawn a workflow, we should use the following code
//! // in the parent workflow:
//! use tardigrade::spawn::{ManageWorkflowsExt, Workflows, WorkflowHandle};
//! # use tardigrade::test::Runtime;
//!
//! # let mut runtime = Runtime::default();
//! # runtime.workflow_registry_mut().insert::<ChildWorkflow>("child");
//! # runtime.run(async {
//! let builder = Workflows.new_workflow::<ChildWorkflow>("child", ())?;
//! // It is possible to customize child workflow initialization via
//! // `builder.handle()`, but zero config is fine as well.
//! let mut child = builder.build().await?;
//! // `child` contains handles to the created channels.
//! child.api.commands.send("ping".to_owned()).await?;
//! # drop(child.api.commands);
//! let event: String = child.api.events.next().await.unwrap();
//! # assert_eq!(event, "ping");
//! child.workflow.await?;
//! # Ok::<_, Box<dyn error::Error>>(())
//! # }).unwrap();
//! ```

use async_trait::async_trait;
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
    interface::{AccessError, AccessErrorKind, Interface},
    task::JoinError,
    workflow::{
        GetInterface, HandleFormat, InEnv, Inverse, UntypedHandles, Wasm, WithHandle, WorkflowFn,
    },
    Codec,
};

/// Manager of [`Interface`]s that allows obtaining an interface by a string identifier.
pub trait ManageInterfaces {
    /// Returns the interface spec of a workflow with the specified ID.
    fn interface(&self, definition_id: &str) -> Option<Cow<'_, Interface>>;
}

/// Manager of workflow channels.
#[async_trait]
pub trait ManageChannels: ManageInterfaces {
    /// Format of handles that this manager operates in.
    type Fmt: HandleFormat;

    /// Returns an instance of closed receiver. Since it's closed, aliasing is not a concern.
    fn closed_receiver(&self) -> <Self::Fmt as HandleFormat>::RawReceiver;
    /// Returns an instance of closed sender.
    fn closed_sender(&self) -> <Self::Fmt as HandleFormat>::RawSender;

    /// Creates a new workflow channel with the specified parameters.
    async fn create_channel(
        &self,
    ) -> (
        <Self::Fmt as HandleFormat>::RawSender,
        <Self::Fmt as HandleFormat>::RawReceiver,
    );
}

/// Manager of workflows that allows spawning workflows of a certain type.
///
/// This trait is low-level; use [`ManageWorkflowsExt`] for a high-level alternative.
#[async_trait]
pub trait ManageWorkflows: ManageChannels {
    /// Handle to an instantiated workflow.
    type Spawned<W: WorkflowFn + WithHandle>;
    /// Error spawning a workflow.
    type Error: 'static + Send + Sync;

    #[doc(hidden)] // implementation detail; should only be used via `WorkflowBuilder`
    async fn new_workflow_raw(
        &self,
        definition_id: &str,
        args: Vec<u8>,
        handles: UntypedHandles<Self::Fmt>,
    ) -> Result<Self::Spawned<()>, Self::Error>;

    #[doc(hidden)]
    fn downcast<W: WorkflowFn + WithHandle>(&self, spawned: Self::Spawned<()>) -> Self::Spawned<W>;

    /// Initiates creating a new workflow and returns the corresponding builder.
    ///
    /// # Errors
    ///
    /// Returns an error on interface mismatch between `W` and the workflow definition
    /// contained in this manager under `definition_id`.
    fn new_workflow<'a, W: WorkflowFn + GetInterface>(
        &'a self,
        definition_id: &'a str,
    ) -> Result<WorkflowBuilder<'a, Self, W>, AccessError>
    where
        Self: Sized,
    {
        let provided_interface = self
            .interface(definition_id)
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
pub struct WorkflowBuilder<'a, M: ManageWorkflows, W> {
    manager: &'a M,
    interface: Cow<'a, Interface>,
    definition_id: &'a str,
    _ty: PhantomData<W>,
}

impl<M: ManageWorkflows, W> fmt::Debug for WorkflowBuilder<'_, M, W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowBuilder")
            .field("interface", &self.interface)
            .field("definition_id", &self.definition_id)
            .finish()
    }
}

impl<M, W> WorkflowBuilder<'_, M, W>
where
    M: ManageWorkflows,
    W: WorkflowFn + WithHandle,
{
    /// Creates a new workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow cannot be spawned, e.g., if the provided `args`
    /// are incorrect.
    pub async fn build(
        self,
        args: W::Args,
        handles: W::Handle<M::Fmt>,
    ) -> Result<M::Spawned<W>, M::Error> {
        let raw_args = <W::Codec>::encode_value(args);
        let raw_handles = W::into_untyped(handles);
        let spawned = self
            .manager
            .new_workflow_raw(self.definition_id, raw_args, raw_handles)
            .await?;
        Ok(self.manager.downcast(spawned))
    }
}

impl<'a, M, W> WorkflowBuilder<'a, M, W>
where
    M: ManageWorkflows,
    W: WorkflowFn + WithHandle,
{
    /// Builds the handles pair allowing to configure handles in the process.
    pub async fn handles<F>(&self, config_fn: F) -> (InEnv<W, M::Fmt>, InEnv<W, Inverse<M::Fmt>>)
    where
        F: FnOnce(&InEnv<W, Spawner<M::Fmt>>),
    {
        let handles_builder = HandlesBuilder::<W, M::Fmt>::new(&self.interface);
        config_fn(handles_builder.config());
        handles_builder.build(self.manager).await
    }
}

/// Client-side connection to a [workflow manager][`ManageWorkflows`].
#[derive(Debug)]
pub struct Workflows;

#[async_trait]
impl ManageChannels for Workflows {
    type Fmt = Wasm;

    fn closed_receiver(&self) -> RawReceiver {
        RawReceiver::closed()
    }

    fn closed_sender(&self) -> RawSender {
        RawSender::closed()
    }

    async fn create_channel(&self) -> (RawSender, RawReceiver) {
        channel().await
    }
}

#[async_trait]
impl ManageWorkflows for Workflows {
    type Spawned<W: WorkflowFn + WithHandle> = RemoteWorkflow;
    type Error = HostError;

    async fn new_workflow_raw(
        &self,
        definition_id: &str,
        args: Vec<u8>,
        handles: UntypedHandles<Self::Fmt>,
    ) -> Result<Self::Spawned<()>, Self::Error> {
        imp::new_workflow(definition_id, args, handles).await
    }

    fn downcast<W: WorkflowFn + WithHandle>(&self, spawned: Self::Spawned<()>) -> Self::Spawned<W> {
        spawned
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
