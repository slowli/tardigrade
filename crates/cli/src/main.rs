//! gRPC server for Tardigrade runtime.

// Linter settings.
#![warn(missing_debug_implementations, bare_trait_objects)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::must_use_candidate,
    clippy::module_name_repetitions,
    clippy::trait_duplication_in_bounds,
    clippy::doc_markdown // false positive on "gRPC"
)]

use clap::{Parser, ValueEnum};
use futures::FutureExt;
use http::{
    header::{self, HeaderMap, HeaderName, HeaderValue},
    uri::Scheme,
};
use tokio::task;
use tonic::transport::{Error as TransportError, Server};
use tonic_reflection::server as reflection;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use std::{error, future::Future, io, net::SocketAddr, sync::Arc};

use tardigrade_grpc::{
    ChannelsServiceServer, ManagerService, RuntimeServiceServer, TestServiceServer, WithClockType,
    SERVICE_DESCRIPTOR,
};
use tardigrade_rt::{
    engine::Wasmtime,
    manager::WorkflowManager,
    storage::{LocalStorage, Streaming},
    test::MockScheduler,
    Schedule, TokioScheduler,
};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TracingOptions {
    Pretty,
    Plain,
}

impl TracingOptions {
    fn init_tracing(self) {
        match self {
            Self::Pretty => {
                FmtSubscriber::builder()
                    .pretty()
                    .with_writer(io::stderr)
                    .with_env_filter(EnvFilter::from_default_env())
                    .init();
            }
            Self::Plain => {
                FmtSubscriber::builder()
                    .with_writer(io::stderr)
                    .with_env_filter(EnvFilter::from_default_env())
                    .init();
            }
        }
    }
}

type LocalStreamingStorage = Streaming<Arc<LocalStorage>>;
type LocalService<C> = ManagerService<WorkflowManager<Wasmtime, C, LocalStreamingStorage>>;
type LocalTestService = TestServiceServer<LocalService<MockScheduler>>;

#[derive(Debug)]
struct Services<C: Schedule> {
    runtime_service: RuntimeServiceServer<LocalService<C>>,
    test_service: Option<LocalTestService>,
    channels_service: ChannelsServiceServer<LocalService<C>>,
}

impl<C: Schedule> Services<C> {
    fn run_server(self, address: SocketAddr) -> impl Future<Output = Result<(), TransportError>> {
        let reflection = reflection::Builder::configure()
            .register_encoded_file_descriptor_set(SERVICE_DESCRIPTOR)
            .build()
            .unwrap();

        Server::builder()
            .trace_fn(Self::trace_request)
            .add_service(self.runtime_service)
            .add_optional_service(self.test_service)
            .add_service(self.channels_service)
            .add_service(reflection)
            .serve(address)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn trace_request(req: &http::Request<()>) -> tracing::Span {
        tracing::debug_span!(
            "http_request",
            http.method = %req.method(),
            http.flavor = Self::http_version(req.version()),
            http.user_agent = Self::header_str(req.headers(), &header::USER_AGENT),
            http.request_content_length = Self::header_str(req.headers(), &header::CONTENT_LENGTH),
            http.scheme = req.uri().scheme().map(Scheme::as_str),
            http.target = %req.uri().path(),
            http.route = %req.uri().path()
        )
    }

    fn header_str<'a>(headers: &'a HeaderMap<HeaderValue>, name: &HeaderName) -> Option<&'a str> {
        let value = headers.get(name)?;
        match value.to_str() {
            Ok(value) => Some(value),
            Err(err) => {
                tracing::warn!(%err, %name, "failed parsing header");
                None
            }
        }
    }

    fn http_version(version: http::Version) -> Option<&'static str> {
        Some(match version {
            http::Version::HTTP_09 => "0.9",
            http::Version::HTTP_10 => "1.0",
            http::Version::HTTP_11 => "1.1",
            http::Version::HTTP_2 => "2.0",
            _ => return None,
        })
    }
}

/// gRPC server for Tardigrade runtime.
#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Address to bind the gRPC server to.
    address: SocketAddr,

    /// Use a mock scheduler instead of the real one.
    #[arg(long)]
    mock: bool,
    /// Do not drive workflows to completion automatically.
    #[arg(long)]
    no_driver: bool,

    /// Tracing format options.
    #[arg(long, default_value = "pretty")]
    tracing: TracingOptions,
}

impl Cli {
    fn create_services<C: WithClockType>(
        &self,
        clock: C,
        map_service: impl FnOnce(LocalService<C>) -> Option<LocalService<MockScheduler>>,
    ) -> Services<C> {
        let storage = Arc::new(LocalStorage::default());
        let (storage, routing_task) = Streaming::new(storage);
        task::spawn(routing_task);
        let manager = WorkflowManager::builder(Wasmtime::default(), storage)
            .with_clock(clock)
            .build();

        let mut service = if self.no_driver {
            ManagerService::from(manager)
        } else {
            ManagerService::new(manager)
        };
        service.set_clock_type();

        let runtime_service = RuntimeServiceServer::new(service.clone());
        let channels_service = ChannelsServiceServer::new(service.clone());
        let test_service = map_service(service).map(TestServiceServer::new);
        Services {
            runtime_service,
            test_service,
            channels_service,
        }
    }

    async fn run(self) -> Result<(), Box<dyn error::Error>> {
        self.tracing.init_tracing();
        let _entered = tracing::info_span!("run", ?self).entered();

        let server = if self.mock {
            self.create_services(MockScheduler::default(), Some)
                .run_server(self.address)
                .left_future()
        } else {
            self.create_services(TokioScheduler, |_| None)
                .run_server(self.address)
                .right_future()
        };

        tracing::info!(address = %self.address, "server initialized");
        server.await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    Cli::parse().run().await
}
