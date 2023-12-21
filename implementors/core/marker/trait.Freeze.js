(function() {var implementors = {
"tardigrade":[["impl&lt;T&gt; Freeze for <a class=\"struct\" href=\"tardigrade/channel/struct.BroadcastError.html\" title=\"struct tardigrade::channel::BroadcastError\">BroadcastError</a>&lt;T&gt;<span class=\"where fmt-newline\">where\n    T: Freeze,</span>",1,["tardigrade::channel::broadcast::BroadcastError"]],["impl&lt;T&gt; Freeze for <a class=\"struct\" href=\"tardigrade/channel/struct.BroadcastPublisher.html\" title=\"struct tardigrade::channel::BroadcastPublisher\">BroadcastPublisher</a>&lt;T&gt;",1,["tardigrade::channel::broadcast::BroadcastPublisher"]],["impl&lt;T&gt; Freeze for <a class=\"struct\" href=\"tardigrade/channel/struct.BroadcastSubscriber.html\" title=\"struct tardigrade::channel::BroadcastSubscriber\">BroadcastSubscriber</a>&lt;T&gt;",1,["tardigrade::channel::broadcast::BroadcastSubscriber"]],["impl&lt;Req, Resp&gt; Freeze for <a class=\"struct\" href=\"tardigrade/channel/struct.Requests.html\" title=\"struct tardigrade::channel::Requests\">Requests</a>&lt;Req, Resp&gt;",1,["tardigrade::channel::requests::Requests"]],["impl&lt;'a, Req, Resp, C&gt; Freeze for <a class=\"struct\" href=\"tardigrade/channel/struct.RequestsBuilder.html\" title=\"struct tardigrade::channel::RequestsBuilder\">RequestsBuilder</a>&lt;'a, Req, Resp, C&gt;",1,["tardigrade::channel::requests::RequestsBuilder"]],["impl&lt;Req, Resp, C, Fmt&gt; Freeze for <a class=\"struct\" href=\"tardigrade/channel/struct.RequestHandles.html\" title=\"struct tardigrade::channel::RequestHandles\">RequestHandles</a>&lt;Req, Resp, C, Fmt&gt;<span class=\"where fmt-newline\">where\n    &lt;Fmt as <a class=\"trait\" href=\"tardigrade/workflow/trait.HandleFormat.html\" title=\"trait tardigrade::workflow::HandleFormat\">HandleFormat</a>&gt;::<a class=\"associatedtype\" href=\"tardigrade/workflow/trait.HandleFormat.html#associatedtype.Receiver\" title=\"type tardigrade::workflow::HandleFormat::Receiver\">Receiver</a>&lt;<a class=\"struct\" href=\"tardigrade/channel/struct.Response.html\" title=\"struct tardigrade::channel::Response\">Response</a>&lt;Resp&gt;, C&gt;: Freeze,\n    &lt;Fmt as <a class=\"trait\" href=\"tardigrade/workflow/trait.HandleFormat.html\" title=\"trait tardigrade::workflow::HandleFormat\">HandleFormat</a>&gt;::<a class=\"associatedtype\" href=\"tardigrade/workflow/trait.HandleFormat.html#associatedtype.Sender\" title=\"type tardigrade::workflow::HandleFormat::Sender\">Sender</a>&lt;<a class=\"enum\" href=\"tardigrade/channel/enum.Request.html\" title=\"enum tardigrade::channel::Request\">Request</a>&lt;Req&gt;, C&gt;: Freeze,</span>",1,["tardigrade::channel::requests::RequestHandles"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade/channel/enum.SendError.html\" title=\"enum tardigrade::channel::SendError\">SendError</a>",1,["tardigrade::error::SendError"]],["impl&lt;T, C&gt; Freeze for <a class=\"struct\" href=\"tardigrade/channel/struct.Receiver.html\" title=\"struct tardigrade::channel::Receiver\">Receiver</a>&lt;T, C&gt;",1,["tardigrade::channel::Receiver"]],["impl&lt;T, C&gt; Freeze for <a class=\"struct\" href=\"tardigrade/channel/struct.Sender.html\" title=\"struct tardigrade::channel::Sender\">Sender</a>&lt;T, C&gt;",1,["tardigrade::channel::Sender"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade/task/struct.ErrorLocation.html\" title=\"struct tardigrade::task::ErrorLocation\">ErrorLocation</a>",1,["tardigrade::error::ErrorLocation"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade/task/struct.ErrorContext.html\" title=\"struct tardigrade::task::ErrorContext\">ErrorContext</a>",1,["tardigrade::error::ErrorContext"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade/task/struct.TaskError.html\" title=\"struct tardigrade::task::TaskError\">TaskError</a>",1,["tardigrade::error::TaskError"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade/task/enum.JoinError.html\" title=\"enum tardigrade::task::JoinError\">JoinError</a>",1,["tardigrade::error::JoinError"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade/spawn/struct.HostError.html\" title=\"struct tardigrade::spawn::HostError\">HostError</a>",1,["tardigrade::error::HostError"]],["impl&lt;Fmt&gt; Freeze for <a class=\"struct\" href=\"tardigrade/spawn/struct.Spawner.html\" title=\"struct tardigrade::spawn::Spawner\">Spawner</a>&lt;Fmt&gt;",1,["tardigrade::spawn::config::Spawner"]],["impl&lt;Fmt, T, C&gt; Freeze for <a class=\"struct\" href=\"tardigrade/spawn/struct.ReceiverConfig.html\" title=\"struct tardigrade::spawn::ReceiverConfig\">ReceiverConfig</a>&lt;Fmt, T, C&gt;",1,["tardigrade::spawn::config::ReceiverConfig"]],["impl&lt;Fmt, T, C&gt; Freeze for <a class=\"struct\" href=\"tardigrade/spawn/struct.SenderConfig.html\" title=\"struct tardigrade::spawn::SenderConfig\">SenderConfig</a>&lt;Fmt, T, C&gt;",1,["tardigrade::spawn::config::SenderConfig"]],["impl&lt;'a, M, W&gt; Freeze for <a class=\"struct\" href=\"tardigrade/spawn/struct.WorkflowBuilder.html\" title=\"struct tardigrade::spawn::WorkflowBuilder\">WorkflowBuilder</a>&lt;'a, M, W&gt;",1,["tardigrade::spawn::WorkflowBuilder"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade/spawn/struct.Workflows.html\" title=\"struct tardigrade::spawn::Workflows\">Workflows</a>",1,["tardigrade::spawn::Workflows"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade/spawn/struct.RemoteWorkflow.html\" title=\"struct tardigrade::spawn::RemoteWorkflow\">RemoteWorkflow</a>",1,["tardigrade::spawn::RemoteWorkflow"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade/task/struct.JoinHandle.html\" title=\"struct tardigrade::task::JoinHandle\">JoinHandle</a>",1,["tardigrade::task::JoinHandle"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade/test/struct.MockScheduler.html\" title=\"struct tardigrade::test::MockScheduler\">MockScheduler</a>",1,["tardigrade::test::MockScheduler"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade/test/struct.Timers.html\" title=\"struct tardigrade::test::Timers\">Timers</a>",1,["tardigrade::test::Timers"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade/test/struct.WorkflowRegistry.html\" title=\"struct tardigrade::test::WorkflowRegistry\">WorkflowRegistry</a>",1,["tardigrade::test::WorkflowRegistry"]],["impl !Freeze for <a class=\"struct\" href=\"tardigrade/test/struct.Runtime.html\" title=\"struct tardigrade::test::Runtime\">Runtime</a>",1,["tardigrade::test::Runtime"]],["impl&lt;W&gt; !Freeze for <a class=\"struct\" href=\"tardigrade/test/struct.TestInstance.html\" title=\"struct tardigrade::test::TestInstance\">TestInstance</a>&lt;W&gt;",1,["tardigrade::test::TestInstance"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade/struct.TimerDefinition.html\" title=\"struct tardigrade::TimerDefinition\">TimerDefinition</a>",1,["tardigrade::time::TimerDefinition"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade/struct.Timer.html\" title=\"struct tardigrade::Timer\">Timer</a>",1,["tardigrade::time::Timer"]],["impl&lt;Fmt&gt; Freeze for <a class=\"struct\" href=\"tardigrade/workflow/struct.Inverse.html\" title=\"struct tardigrade::workflow::Inverse\">Inverse</a>&lt;Fmt&gt;",1,["tardigrade::workflow::handle::Inverse"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade/workflow/struct.Wasm.html\" title=\"struct tardigrade::workflow::Wasm\">Wasm</a>",1,["tardigrade::workflow::Wasm"]]],
"tardigrade_grpc":[["impl !Freeze for <a class=\"struct\" href=\"tardigrade_grpc/struct.Client.html\" title=\"struct tardigrade_grpc::Client\">Client</a>",1,["tardigrade_grpc::client::Client"]],["impl&lt;S&gt; Freeze for <a class=\"struct\" href=\"tardigrade_grpc/struct.StorageWrapper.html\" title=\"struct tardigrade_grpc::StorageWrapper\">StorageWrapper</a>&lt;S&gt;<span class=\"where fmt-newline\">where\n    S: Freeze,</span>",1,["tardigrade_grpc::service::storage::StorageWrapper"]],["impl&lt;R&gt; Freeze for <a class=\"struct\" href=\"tardigrade_grpc/struct.RuntimeWrapper.html\" title=\"struct tardigrade_grpc::RuntimeWrapper\">RuntimeWrapper</a>&lt;R&gt;<span class=\"where fmt-newline\">where\n    R: Freeze,</span>",1,["tardigrade_grpc::service::RuntimeWrapper"]],["impl&lt;T&gt; Freeze for <a class=\"struct\" href=\"tardigrade_grpc/struct.RuntimeServiceServer.html\" title=\"struct tardigrade_grpc::RuntimeServiceServer\">RuntimeServiceServer</a>&lt;T&gt;",1,["tardigrade_grpc::proto::runtime_service_server::RuntimeServiceServer"]],["impl&lt;T&gt; Freeze for <a class=\"struct\" href=\"tardigrade_grpc/struct.ChannelsServiceServer.html\" title=\"struct tardigrade_grpc::ChannelsServiceServer\">ChannelsServiceServer</a>&lt;T&gt;",1,["tardigrade_grpc::proto::channels_service_server::ChannelsServiceServer"]],["impl&lt;T&gt; Freeze for <a class=\"struct\" href=\"tardigrade_grpc/struct.TestServiceServer.html\" title=\"struct tardigrade_grpc::TestServiceServer\">TestServiceServer</a>&lt;T&gt;",1,["tardigrade_grpc::proto::test_service_server::TestServiceServer"]]],
"tardigrade_rt":[["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/struct.AsyncIoScheduler.html\" title=\"struct tardigrade_rt::AsyncIoScheduler\">AsyncIoScheduler</a>",1,["tardigrade_rt::backends::async_io::AsyncIoScheduler"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/struct.TokioScheduler.html\" title=\"struct tardigrade_rt::TokioScheduler\">TokioScheduler</a>",1,["tardigrade_rt::backends::tokio::TokioScheduler"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/struct.MockScheduler.html\" title=\"struct tardigrade_rt::MockScheduler\">MockScheduler</a>",1,["tardigrade_rt::backends::MockScheduler"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/struct.ReceiverState.html\" title=\"struct tardigrade_rt::ReceiverState\">ReceiverState</a>",1,["tardigrade_rt::data::channel::ReceiverState"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/struct.SenderState.html\" title=\"struct tardigrade_rt::SenderState\">SenderState</a>",1,["tardigrade_rt::data::channel::SenderState"]],["impl&lt;'a&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/struct.Channels.html\" title=\"struct tardigrade_rt::Channels\">Channels</a>&lt;'a&gt;",1,["tardigrade_rt::data::channel::Channels"]],["impl&lt;'a&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/engine/struct.ReceiverActions.html\" title=\"struct tardigrade_rt::engine::ReceiverActions\">ReceiverActions</a>&lt;'a&gt;",1,["tardigrade_rt::data::channel::ReceiverActions"]],["impl&lt;'a&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/engine/struct.SenderActions.html\" title=\"struct tardigrade_rt::engine::SenderActions\">SenderActions</a>&lt;'a&gt;",1,["tardigrade_rt::data::channel::SenderActions"]],["impl&lt;T&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/engine/struct.WorkflowPoll.html\" title=\"struct tardigrade_rt::engine::WorkflowPoll\">WorkflowPoll</a>&lt;T&gt;<span class=\"where fmt-newline\">where\n    T: Freeze,</span>",1,["tardigrade_rt::data::helpers::WorkflowPoll"]],["impl&lt;'a&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/struct.ChildWorkflow.html\" title=\"struct tardigrade_rt::ChildWorkflow\">ChildWorkflow</a>&lt;'a&gt;",1,["tardigrade_rt::data::spawn::ChildWorkflow"]],["impl&lt;'a&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/engine/struct.ChildActions.html\" title=\"struct tardigrade_rt::engine::ChildActions\">ChildActions</a>&lt;'a&gt;",1,["tardigrade_rt::data::spawn::ChildActions"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/struct.TaskState.html\" title=\"struct tardigrade_rt::TaskState\">TaskState</a>",1,["tardigrade_rt::data::task::TaskState"]],["impl&lt;'a&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/engine/struct.TaskActions.html\" title=\"struct tardigrade_rt::engine::TaskActions\">TaskActions</a>&lt;'a&gt;",1,["tardigrade_rt::data::task::TaskActions"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/struct.TimerState.html\" title=\"struct tardigrade_rt::TimerState\">TimerState</a>",1,["tardigrade_rt::data::time::TimerState"]],["impl&lt;'a&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/engine/struct.TimerActions.html\" title=\"struct tardigrade_rt::engine::TimerActions\">TimerActions</a>&lt;'a&gt;",1,["tardigrade_rt::data::time::TimerActions"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_rt/engine/enum.ReportedErrorKind.html\" title=\"enum tardigrade_rt::engine::ReportedErrorKind\">ReportedErrorKind</a>",1,["tardigrade_rt::data::ReportedErrorKind"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/engine/struct.PersistedWorkflowData.html\" title=\"struct tardigrade_rt::engine::PersistedWorkflowData\">PersistedWorkflowData</a>",1,["tardigrade_rt::data::PersistedWorkflowData"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/engine/struct.WorkflowData.html\" title=\"struct tardigrade_rt::engine::WorkflowData\">WorkflowData</a>",1,["tardigrade_rt::data::WorkflowData"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/engine/struct.WasmtimeInstance.html\" title=\"struct tardigrade_rt::engine::WasmtimeInstance\">WasmtimeInstance</a>",1,["tardigrade_rt::engine::wasmtime::instance::WasmtimeInstance"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/engine/struct.WasmtimeModule.html\" title=\"struct tardigrade_rt::engine::WasmtimeModule\">WasmtimeModule</a>",1,["tardigrade_rt::engine::wasmtime::module::WasmtimeModule"]],["impl !Freeze for <a class=\"struct\" href=\"tardigrade_rt/engine/struct.WasmtimeDefinition.html\" title=\"struct tardigrade_rt::engine::WasmtimeDefinition\">WasmtimeDefinition</a>",1,["tardigrade_rt::engine::wasmtime::module::WasmtimeDefinition"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/engine/struct.Wasmtime.html\" title=\"struct tardigrade_rt::engine::Wasmtime\">Wasmtime</a>",1,["tardigrade_rt::engine::wasmtime::Wasmtime"]],["impl&lt;T, C, S&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/handle/struct.MessageSender.html\" title=\"struct tardigrade_rt::handle::MessageSender\">MessageSender</a>&lt;T, C, S&gt;<span class=\"where fmt-newline\">where\n    S: Freeze,</span>",1,["tardigrade_rt::handle::channel::MessageSender"]],["impl&lt;T, C, S&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/handle/struct.MessageReceiver.html\" title=\"struct tardigrade_rt::handle::MessageReceiver\">MessageReceiver</a>&lt;T, C, S&gt;<span class=\"where fmt-newline\">where\n    S: Freeze,</span>",1,["tardigrade_rt::handle::channel::MessageReceiver"]],["impl&lt;T, C&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/handle/struct.ReceivedMessage.html\" title=\"struct tardigrade_rt::handle::ReceivedMessage\">ReceivedMessage</a>&lt;T, C&gt;",1,["tardigrade_rt::handle::channel::ReceivedMessage"]],["impl&lt;W, S&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/handle/struct.WorkflowHandle.html\" title=\"struct tardigrade_rt::handle::WorkflowHandle\">WorkflowHandle</a>&lt;W, S&gt;<span class=\"where fmt-newline\">where\n    S: Freeze,</span>",1,["tardigrade_rt::handle::workflow::WorkflowHandle"]],["impl&lt;S&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/handle/struct.ErroredWorkflowHandle.html\" title=\"struct tardigrade_rt::handle::ErroredWorkflowHandle\">ErroredWorkflowHandle</a>&lt;S&gt;<span class=\"where fmt-newline\">where\n    S: Freeze,</span>",1,["tardigrade_rt::handle::workflow::ErroredWorkflowHandle"]],["impl&lt;S&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/handle/struct.ErroneousMessage.html\" title=\"struct tardigrade_rt::handle::ErroneousMessage\">ErroneousMessage</a>&lt;S&gt;<span class=\"where fmt-newline\">where\n    S: Freeze,</span>",1,["tardigrade_rt::handle::workflow::ErroneousMessage"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/handle/struct.CompletedWorkflowHandle.html\" title=\"struct tardigrade_rt::handle::CompletedWorkflowHandle\">CompletedWorkflowHandle</a>",1,["tardigrade_rt::handle::workflow::CompletedWorkflowHandle"]],["impl&lt;S&gt; Freeze for <a class=\"enum\" href=\"tardigrade_rt/handle/enum.AnyWorkflowHandle.html\" title=\"enum tardigrade_rt::handle::AnyWorkflowHandle\">AnyWorkflowHandle</a>&lt;S&gt;<span class=\"where fmt-newline\">where\n    S: Freeze,</span>",1,["tardigrade_rt::handle::workflow::AnyWorkflowHandle"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/handle/struct.ConcurrencyError.html\" title=\"struct tardigrade_rt::handle::ConcurrencyError\">ConcurrencyError</a>",1,["tardigrade_rt::storage::helper::ConcurrencyError"]],["impl&lt;'a, S&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/handle/struct.StorageRef.html\" title=\"struct tardigrade_rt::handle::StorageRef\">StorageRef</a>&lt;'a, S&gt;",1,["tardigrade_rt::handle::StorageRef"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_rt/receipt/enum.WakeUpCause.html\" title=\"enum tardigrade_rt::receipt::WakeUpCause\">WakeUpCause</a>",1,["tardigrade_rt::receipt::WakeUpCause"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_rt/receipt/enum.ExecutedFunction.html\" title=\"enum tardigrade_rt::receipt::ExecutedFunction\">ExecutedFunction</a>",1,["tardigrade_rt::receipt::ExecutedFunction"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_rt/receipt/enum.ResourceId.html\" title=\"enum tardigrade_rt::receipt::ResourceId\">ResourceId</a>",1,["tardigrade_rt::receipt::ResourceId"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_rt/receipt/enum.ResourceEventKind.html\" title=\"enum tardigrade_rt::receipt::ResourceEventKind\">ResourceEventKind</a>",1,["tardigrade_rt::receipt::ResourceEventKind"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/receipt/struct.ResourceEvent.html\" title=\"struct tardigrade_rt::receipt::ResourceEvent\">ResourceEvent</a>",1,["tardigrade_rt::receipt::ResourceEvent"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_rt/receipt/enum.ChannelEventKind.html\" title=\"enum tardigrade_rt::receipt::ChannelEventKind\">ChannelEventKind</a>",1,["tardigrade_rt::receipt::ChannelEventKind"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/receipt/struct.ChannelEvent.html\" title=\"struct tardigrade_rt::receipt::ChannelEvent\">ChannelEvent</a>",1,["tardigrade_rt::receipt::ChannelEvent"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_rt/receipt/enum.StubEventKind.html\" title=\"enum tardigrade_rt::receipt::StubEventKind\">StubEventKind</a>",1,["tardigrade_rt::receipt::StubEventKind"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_rt/receipt/enum.StubId.html\" title=\"enum tardigrade_rt::receipt::StubId\">StubId</a>",1,["tardigrade_rt::receipt::StubId"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/receipt/struct.StubEvent.html\" title=\"struct tardigrade_rt::receipt::StubEvent\">StubEvent</a>",1,["tardigrade_rt::receipt::StubEvent"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_rt/receipt/enum.Event.html\" title=\"enum tardigrade_rt::receipt::Event\">Event</a>",1,["tardigrade_rt::receipt::Event"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/receipt/struct.Execution.html\" title=\"struct tardigrade_rt::receipt::Execution\">Execution</a>",1,["tardigrade_rt::receipt::Execution"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/receipt/struct.Receipt.html\" title=\"struct tardigrade_rt::receipt::Receipt\">Receipt</a>",1,["tardigrade_rt::receipt::Receipt"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/receipt/struct.ExecutionError.html\" title=\"struct tardigrade_rt::receipt::ExecutionError\">ExecutionError</a>",1,["tardigrade_rt::receipt::ExecutionError"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/receipt/struct.PanicInfo.html\" title=\"struct tardigrade_rt::receipt::PanicInfo\">PanicInfo</a>",1,["tardigrade_rt::receipt::PanicInfo"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_rt/runtime/enum.Termination.html\" title=\"enum tardigrade_rt::runtime::Termination\">Termination</a>",1,["tardigrade_rt::runtime::driver::Termination"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/runtime/struct.DriveConfig.html\" title=\"struct tardigrade_rt::runtime::DriveConfig\">DriveConfig</a>",1,["tardigrade_rt::runtime::driver::DriveConfig"]],["impl&lt;'a, M, Fmt&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/runtime/struct.RuntimeSpawner.html\" title=\"struct tardigrade_rt::runtime::RuntimeSpawner\">RuntimeSpawner</a>&lt;'a, M, Fmt&gt;",1,["tardigrade_rt::runtime::stubs::RuntimeSpawner"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/runtime/struct.TickResult.html\" title=\"struct tardigrade_rt::runtime::TickResult\">TickResult</a>",1,["tardigrade_rt::runtime::tick::TickResult"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/runtime/struct.WouldBlock.html\" title=\"struct tardigrade_rt::runtime::WouldBlock\">WouldBlock</a>",1,["tardigrade_rt::runtime::tick::WouldBlock"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_rt/runtime/enum.WorkflowTickError.html\" title=\"enum tardigrade_rt::runtime::WorkflowTickError\">WorkflowTickError</a>",1,["tardigrade_rt::runtime::tick::WorkflowTickError"]],["impl&lt;E, C, S&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/runtime/struct.Runtime.html\" title=\"struct tardigrade_rt::runtime::Runtime\">Runtime</a>&lt;E, C, S&gt;<span class=\"where fmt-newline\">where\n    S: Freeze,</span>",1,["tardigrade_rt::runtime::Runtime"]],["impl&lt;E, C, S&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/runtime/struct.RuntimeBuilder.html\" title=\"struct tardigrade_rt::runtime::RuntimeBuilder\">RuntimeBuilder</a>&lt;E, C, S&gt;<span class=\"where fmt-newline\">where\n    C: Freeze,\n    E: Freeze,\n    S: Freeze,</span>",1,["tardigrade_rt::runtime::RuntimeBuilder"]],["impl&lt;'a&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.ModuleRecordMut.html\" title=\"struct tardigrade_rt::storage::ModuleRecordMut\">ModuleRecordMut</a>&lt;'a&gt;",1,["tardigrade_rt::storage::local::ModuleRecordMut"]],["impl&lt;'a&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.LocalStorageSnapshot.html\" title=\"struct tardigrade_rt::storage::LocalStorageSnapshot\">LocalStorageSnapshot</a>&lt;'a&gt;",1,["tardigrade_rt::storage::local::LocalStorageSnapshot"]],["impl !Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.LocalStorage.html\" title=\"struct tardigrade_rt::storage::LocalStorage\">LocalStorage</a>",1,["tardigrade_rt::storage::local::LocalStorage"]],["impl&lt;'a&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.LocalReadonlyTransaction.html\" title=\"struct tardigrade_rt::storage::LocalReadonlyTransaction\">LocalReadonlyTransaction</a>&lt;'a&gt;",1,["tardigrade_rt::storage::local::LocalReadonlyTransaction"]],["impl&lt;'a&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.LocalTransaction.html\" title=\"struct tardigrade_rt::storage::LocalTransaction\">LocalTransaction</a>&lt;'a&gt;",1,["tardigrade_rt::storage::local::LocalTransaction"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.ModuleRecord.html\" title=\"struct tardigrade_rt::storage::ModuleRecord\">ModuleRecord</a>",1,["tardigrade_rt::storage::records::ModuleRecord"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.DefinitionRecord.html\" title=\"struct tardigrade_rt::storage::DefinitionRecord\">DefinitionRecord</a>",1,["tardigrade_rt::storage::records::DefinitionRecord"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_rt/storage/enum.MessageError.html\" title=\"enum tardigrade_rt::storage::MessageError\">MessageError</a>",1,["tardigrade_rt::storage::records::MessageError"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.ChannelRecord.html\" title=\"struct tardigrade_rt::storage::ChannelRecord\">ChannelRecord</a>",1,["tardigrade_rt::storage::records::ChannelRecord"]],["impl&lt;T&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.WorkflowRecord.html\" title=\"struct tardigrade_rt::storage::WorkflowRecord\">WorkflowRecord</a>&lt;T&gt;<span class=\"where fmt-newline\">where\n    T: Freeze,</span>",1,["tardigrade_rt::storage::records::WorkflowRecord"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_rt/storage/enum.WorkflowState.html\" title=\"enum tardigrade_rt::storage::WorkflowState\">WorkflowState</a>",1,["tardigrade_rt::storage::records::WorkflowState"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.ActiveWorkflowState.html\" title=\"struct tardigrade_rt::storage::ActiveWorkflowState\">ActiveWorkflowState</a>",1,["tardigrade_rt::storage::records::ActiveWorkflowState"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.CompletedWorkflowState.html\" title=\"struct tardigrade_rt::storage::CompletedWorkflowState\">CompletedWorkflowState</a>",1,["tardigrade_rt::storage::records::CompletedWorkflowState"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.ErroredWorkflowState.html\" title=\"struct tardigrade_rt::storage::ErroredWorkflowState\">ErroredWorkflowState</a>",1,["tardigrade_rt::storage::records::ErroredWorkflowState"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.ErroneousMessageRef.html\" title=\"struct tardigrade_rt::storage::ErroneousMessageRef\">ErroneousMessageRef</a>",1,["tardigrade_rt::storage::records::ErroneousMessageRef"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_rt/storage/enum.WorkflowSelectionCriteria.html\" title=\"enum tardigrade_rt::storage::WorkflowSelectionCriteria\">WorkflowSelectionCriteria</a>",1,["tardigrade_rt::storage::records::WorkflowSelectionCriteria"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_rt/storage/enum.WorkflowWaker.html\" title=\"enum tardigrade_rt::storage::WorkflowWaker\">WorkflowWaker</a>",1,["tardigrade_rt::storage::records::WorkflowWaker"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.WorkflowWakerRecord.html\" title=\"struct tardigrade_rt::storage::WorkflowWakerRecord\">WorkflowWakerRecord</a>",1,["tardigrade_rt::storage::records::WorkflowWakerRecord"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.MessageEvent.html\" title=\"struct tardigrade_rt::storage::MessageEvent\">MessageEvent</a>",1,["tardigrade_rt::storage::stream::MessageEvent"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_rt/storage/enum.MessageOrEof.html\" title=\"enum tardigrade_rt::storage::MessageOrEof\">MessageOrEof</a>",1,["tardigrade_rt::storage::stream::MessageOrEof"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.CommitStream.html\" title=\"struct tardigrade_rt::storage::CommitStream\">CommitStream</a>",1,["tardigrade_rt::storage::stream::CommitStream"]],["impl&lt;S&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.Streaming.html\" title=\"struct tardigrade_rt::storage::Streaming\">Streaming</a>&lt;S&gt;<span class=\"where fmt-newline\">where\n    S: Freeze,</span>",1,["tardigrade_rt::storage::stream::Streaming"]],["impl&lt;T&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.StreamingTransaction.html\" title=\"struct tardigrade_rt::storage::StreamingTransaction\">StreamingTransaction</a>&lt;T&gt;<span class=\"where fmt-newline\">where\n    T: Freeze,</span>",1,["tardigrade_rt::storage::stream::StreamingTransaction"]],["impl&lt;T&gt; !Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.TransactionAsStorage.html\" title=\"struct tardigrade_rt::storage::TransactionAsStorage\">TransactionAsStorage</a>&lt;T&gt;",1,["tardigrade_rt::storage::transaction::TransactionAsStorage"]],["impl&lt;'a, T&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.TransactionLock.html\" title=\"struct tardigrade_rt::storage::TransactionLock\">TransactionLock</a>&lt;'a, T&gt;",1,["tardigrade_rt::storage::transaction::TransactionLock"]],["impl&lt;'a, T&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.TransactionReadLock.html\" title=\"struct tardigrade_rt::storage::TransactionReadLock\">TransactionReadLock</a>&lt;'a, T&gt;",1,["tardigrade_rt::storage::transaction::TransactionReadLock"]],["impl&lt;T&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.Readonly.html\" title=\"struct tardigrade_rt::storage::Readonly\">Readonly</a>&lt;T&gt;<span class=\"where fmt-newline\">where\n    T: Freeze,</span>",1,["tardigrade_rt::storage::Readonly"]],["impl&lt;S&gt; Freeze for <a class=\"struct\" href=\"tardigrade_rt/storage/struct.InProcessConnection.html\" title=\"struct tardigrade_rt::storage::InProcessConnection\">InProcessConnection</a>&lt;S&gt;<span class=\"where fmt-newline\">where\n    S: Freeze,</span>",1,["tardigrade_rt::storage::InProcessConnection"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/test/struct.WasmOpt.html\" title=\"struct tardigrade_rt::test::WasmOpt\">WasmOpt</a>",1,["tardigrade_rt::test::compiler::WasmOpt"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/test/struct.ModuleCompiler.html\" title=\"struct tardigrade_rt::test::ModuleCompiler\">ModuleCompiler</a>",1,["tardigrade_rt::test::compiler::ModuleCompiler"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_rt/struct.PersistedWorkflow.html\" title=\"struct tardigrade_rt::PersistedWorkflow\">PersistedWorkflow</a>",1,["tardigrade_rt::workflow::persistence::PersistedWorkflow"]]],
"tardigrade_shared":[["impl Freeze for <a class=\"struct\" href=\"tardigrade_shared/struct.Raw.html\" title=\"struct tardigrade_shared::Raw\">Raw</a>",1,["tardigrade_shared::codec::Raw"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_shared/struct.Json.html\" title=\"struct tardigrade_shared::Json\">Json</a>",1,["tardigrade_shared::codec::Json"]],["impl&lt;T, Rx, Sx&gt; Freeze for <a class=\"struct\" href=\"tardigrade_shared/handle/struct.IndexingHandleMap.html\" title=\"struct tardigrade_shared::handle::IndexingHandleMap\">IndexingHandleMap</a>&lt;T, Rx, Sx&gt;<span class=\"where fmt-newline\">where\n    T: Freeze,</span>",1,["tardigrade_shared::helpers::IndexingHandleMap"]],["impl&lt;T&gt; Freeze for <a class=\"struct\" href=\"tardigrade_shared/handle/struct.ReceiverAt.html\" title=\"struct tardigrade_shared::handle::ReceiverAt\">ReceiverAt</a>&lt;T&gt;<span class=\"where fmt-newline\">where\n    T: Freeze,</span>",1,["tardigrade_shared::helpers::ReceiverAt"]],["impl&lt;T&gt; Freeze for <a class=\"struct\" href=\"tardigrade_shared/handle/struct.SenderAt.html\" title=\"struct tardigrade_shared::handle::SenderAt\">SenderAt</a>&lt;T&gt;<span class=\"where fmt-newline\">where\n    T: Freeze,</span>",1,["tardigrade_shared::helpers::SenderAt"]],["impl&lt;'a&gt; Freeze for <a class=\"struct\" href=\"tardigrade_shared/handle/struct.HandlePath.html\" title=\"struct tardigrade_shared::handle::HandlePath\">HandlePath</a>&lt;'a&gt;",1,["tardigrade_shared::path::HandlePath"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_shared/handle/struct.HandlePathBuf.html\" title=\"struct tardigrade_shared::handle::HandlePathBuf\">HandlePathBuf</a>",1,["tardigrade_shared::path::HandlePathBuf"]],["impl&lt;Rx, Sx&gt; Freeze for <a class=\"enum\" href=\"tardigrade_shared/handle/enum.Handle.html\" title=\"enum tardigrade_shared::handle::Handle\">Handle</a>&lt;Rx, Sx&gt;<span class=\"where fmt-newline\">where\n    Rx: Freeze,\n    Sx: Freeze,</span>",1,["tardigrade_shared::handle::Handle"]],["impl Freeze for <a class=\"enum\" href=\"tardigrade_shared/handle/enum.AccessErrorKind.html\" title=\"enum tardigrade_shared::handle::AccessErrorKind\">AccessErrorKind</a>",1,["tardigrade_shared::handle::AccessErrorKind"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_shared/handle/struct.HandleLocation.html\" title=\"struct tardigrade_shared::handle::HandleLocation\">HandleLocation</a>",1,["tardigrade_shared::handle::HandleLocation"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_shared/handle/struct.AccessError.html\" title=\"struct tardigrade_shared::handle::AccessError\">AccessError</a>",1,["tardigrade_shared::handle::AccessError"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_shared/interface/struct.ReceiverSpec.html\" title=\"struct tardigrade_shared::interface::ReceiverSpec\">ReceiverSpec</a>",1,["tardigrade_shared::interface::ReceiverSpec"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_shared/interface/struct.SenderSpec.html\" title=\"struct tardigrade_shared::interface::SenderSpec\">SenderSpec</a>",1,["tardigrade_shared::interface::SenderSpec"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_shared/interface/struct.ArgsSpec.html\" title=\"struct tardigrade_shared::interface::ArgsSpec\">ArgsSpec</a>",1,["tardigrade_shared::interface::ArgsSpec"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_shared/interface/struct.Interface.html\" title=\"struct tardigrade_shared::interface::Interface\">Interface</a>",1,["tardigrade_shared::interface::Interface"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_shared/interface/struct.InterfaceBuilder.html\" title=\"struct tardigrade_shared::interface::InterfaceBuilder\">InterfaceBuilder</a>",1,["tardigrade_shared::interface::InterfaceBuilder"]],["impl&lt;T&gt; Freeze for <a class=\"enum\" href=\"tardigrade_shared/enum.Request.html\" title=\"enum tardigrade_shared::Request\">Request</a>&lt;T&gt;<span class=\"where fmt-newline\">where\n    T: Freeze,</span>",1,["tardigrade_shared::types::Request"]],["impl&lt;T&gt; Freeze for <a class=\"struct\" href=\"tardigrade_shared/struct.Response.html\" title=\"struct tardigrade_shared::Response\">Response</a>&lt;T&gt;<span class=\"where fmt-newline\">where\n    T: Freeze,</span>",1,["tardigrade_shared::types::Response"]]],
"tardigrade_worker":[["impl Freeze for <a class=\"struct\" href=\"tardigrade_worker/struct.WorkerRecord.html\" title=\"struct tardigrade_worker::WorkerRecord\">WorkerRecord</a>",1,["tardigrade_worker::connection::WorkerRecord"]],["impl&lt;T: ?<a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Sized.html\" title=\"trait core::marker::Sized\">Sized</a>&gt; Freeze for <a class=\"struct\" href=\"tardigrade_worker/struct.Request.html\" title=\"struct tardigrade_worker::Request\">Request</a>&lt;T&gt;<span class=\"where fmt-newline\">where\n    &lt;T as <a class=\"trait\" href=\"tardigrade_worker/trait.WorkerInterface.html\" title=\"trait tardigrade_worker::WorkerInterface\">WorkerInterface</a>&gt;::<a class=\"associatedtype\" href=\"tardigrade_worker/trait.WorkerInterface.html#associatedtype.Request\" title=\"type tardigrade_worker::WorkerInterface::Request\">Request</a>: Freeze,</span>",1,["tardigrade_worker::Request"]],["impl Freeze for <a class=\"struct\" href=\"tardigrade_worker/struct.Cancellation.html\" title=\"struct tardigrade_worker::Cancellation\">Cancellation</a>",1,["tardigrade_worker::Cancellation"]],["impl&lt;T: ?<a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Sized.html\" title=\"trait core::marker::Sized\">Sized</a>&gt; Freeze for <a class=\"struct\" href=\"tardigrade_worker/struct.ResponseSender.html\" title=\"struct tardigrade_worker::ResponseSender\">ResponseSender</a>&lt;T&gt;",1,["tardigrade_worker::ResponseSender"]],["impl&lt;F, Req, C&gt; Freeze for <a class=\"struct\" href=\"tardigrade_worker/struct.FnHandler.html\" title=\"struct tardigrade_worker::FnHandler\">FnHandler</a>&lt;F, Req, C&gt;<span class=\"where fmt-newline\">where\n    F: Freeze,</span>",1,["tardigrade_worker::FnHandler"]],["impl&lt;H, C&gt; Freeze for <a class=\"struct\" href=\"tardigrade_worker/struct.Worker.html\" title=\"struct tardigrade_worker::Worker\">Worker</a>&lt;H, C&gt;<span class=\"where fmt-newline\">where\n    C: Freeze,\n    H: Freeze,</span>",1,["tardigrade_worker::Worker"]]]
};if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()