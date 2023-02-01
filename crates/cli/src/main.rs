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
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::task;
use tonic::transport::{server::Router, Error as TransportError, Server};
use tonic_reflection::server as reflection;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[cfg(unix)]
use std::path::PathBuf;
use std::{error, fmt, future::Future, io, net::SocketAddr, str::FromStr, sync::Arc};

use tardigrade_grpc::{
    ChannelsServiceServer, RuntimeServiceServer, RuntimeWrapper, StorageWrapper, TestServiceServer,
    WithClockType, SERVICE_DESCRIPTOR,
};
use tardigrade_rt::{
    engine::Wasmtime,
    runtime::Runtime,
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
type LocalStorageWrapper = StorageWrapper<LocalStreamingStorage>;
type LocalRuntimeWrapper<C> = RuntimeWrapper<Runtime<Wasmtime, C, LocalStreamingStorage>>;
type LocalTestService = TestServiceServer<LocalRuntimeWrapper<MockScheduler>>;

#[derive(Debug)]
struct Services<C: Schedule> {
    runtime_service: RuntimeServiceServer<LocalRuntimeWrapper<C>>,
    test_service: Option<LocalTestService>,
    channels_service: ChannelsServiceServer<LocalStorageWrapper>,
}

impl<C: Schedule> Services<C> {
    fn run_server(
        self,
        addr: &SocketAddrOrUds,
    ) -> impl Future<Output = Result<(), TransportError>> {
        let reflection = reflection::Builder::configure()
            .register_encoded_file_descriptor_set(SERVICE_DESCRIPTOR)
            .build()
            .unwrap();

        let router = Server::builder()
            .trace_fn(Self::trace_request)
            .add_service(self.runtime_service)
            .add_optional_service(self.test_service)
            .add_service(self.channels_service)
            .add_service(reflection);
        addr.serve(router)
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

#[derive(Debug, Clone)]
enum SocketAddrOrUds {
    Addr(SocketAddr),
    #[cfg(unix)]
    Uds(PathBuf),
}

impl SocketAddrOrUds {
    #[cfg(not(unix))]
    fn serve(&self, router: Router) -> impl Future<Output = Result<(), TransportError>> {
        match self {
            Self::Addr(addr) => router.serve(*addr),
        }
    }

    #[cfg(unix)]
    fn serve(&self, router: Router) -> impl Future<Output = Result<(), TransportError>> {
        match self {
            Self::Addr(addr) => router.serve(*addr).left_future(),
            Self::Uds(path) => {
                let path = path.clone();
                let uds_stream = async_stream::stream! {
                    let uds = match UnixListener::bind(&path) {
                        Ok(uds) => uds,
                        Err(err) => {
                            let message = format!(
                                "cannot bind to Unix domain socket at `{}`: {err}",
                                path.to_string_lossy()
                            );
                            yield Err(io::Error::new(io::ErrorKind::BrokenPipe, message));
                            return;
                        }
                    };

                    loop {
                        yield uds.accept().await.map(|(stream, _)| stream);
                    }
                };
                router.serve_with_incoming(uds_stream).right_future()
            }
        }
    }
}

impl fmt::Display for SocketAddrOrUds {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Addr(addr) => fmt::Display::fmt(addr, formatter),
            #[cfg(unix)]
            Self::Uds(path) => write!(formatter, "sock://{}", path.to_string_lossy()),
        }
    }
}

impl FromStr for SocketAddrOrUds {
    type Err = Box<dyn error::Error + Send + Sync>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        #[cfg(unix)]
        if let Some(path) = s.strip_prefix("sock://") {
            Ok(Self::Uds(PathBuf::from(path)))
        } else {
            let addr = s.parse()?;
            Ok(Self::Addr(addr))
        }

        #[cfg(not(unix))]
        {
            let addr = s.parse()?;
            Ok(Self::Addr(addr))
        }
    }
}

/// gRPC server for Tardigrade runtime.
#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Address to bind the gRPC server to. Can be specified either as a socket address
    /// (e.g., `127.0.0.1:9000`) or, on Unix systems, as a file path to the Unix domain
    /// socket (e.g., `sock:///var/grpc.sock`).
    address: SocketAddrOrUds,

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
        map_service: impl FnOnce(LocalRuntimeWrapper<C>) -> Option<LocalRuntimeWrapper<MockScheduler>>,
    ) -> Services<C> {
        let storage = Arc::new(LocalStorage::default());
        let (storage, routing_task) = Streaming::new(storage);
        task::spawn(routing_task);
        let manager = Runtime::builder(Wasmtime::default(), storage)
            .with_clock(clock)
            .build();

        let mut service = if self.no_driver {
            RuntimeWrapper::from(manager)
        } else {
            RuntimeWrapper::new(manager)
        };
        service.set_clock_type();

        let runtime_service = RuntimeServiceServer::new(service.clone());
        let channels_service = ChannelsServiceServer::new(service.storage_wrapper());
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
                .run_server(&self.address)
                .left_future()
        } else {
            self.create_services(TokioScheduler, |_| None)
                .run_server(&self.address)
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
