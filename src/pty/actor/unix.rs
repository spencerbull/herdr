use std::{
    collections::VecDeque,
    io::{Read, Write},
    os::fd::{AsRawFd, OwnedFd, RawFd},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc as std_mpsc, Arc, Mutex,
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use tokio::sync::mpsc::{self, error::TryRecvError as DataTryRecvError};
use tracing::{debug, warn};

use crate::pty::fd;

use super::{GuardedWriteError, GuardedWriteValidator, OutputBoundaryRequest};

// Actor handle methods must call wake_actor() after queuing work. The idle
// timeout is only a fallback for missed wakes; PTY and wake readiness drive
// normal responsiveness.
const ACTOR_IDLE_POLL_MS: i32 = 1000;
const ACTOR_COMMAND_BUFFER: usize = 1024;
const HANDOFF_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
const GUARDED_WRITE_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActorState {
    Running,
    Quiesced,
    Released,
}

pub(crate) struct PtyReadResult {
    pub terminal_responses: Vec<Bytes>,
}

impl PtyReadResult {
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self {
            terminal_responses: Vec::new(),
        }
    }
}

type ReadCallback = Box<dyn FnMut(&[u8]) -> PtyReadResult + Send + 'static>;
type ReaderExitCallback = Box<dyn FnOnce() + Send + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PtyResize {
    rows: u16,
    cols: u16,
    cell_width_px: u32,
    cell_height_px: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PtyResizeRequest {
    resize: PtyResize,
    terminal_responses: Vec<Bytes>,
}

#[derive(Default)]
struct SharedPtyControls {
    resize: Option<PtyResizeRequest>,
    nudge: Option<PtyResize>,
    terminal_responses: Vec<Bytes>,
}

pub(crate) struct PtyIoActorConfig {
    pub pane_id: u32,
    pub master_fd: OwnedFd,
    pub initially_quiesced: bool,
    pub on_read: ReadCallback,
    pub on_reader_exit: Option<ReaderExitCallback>,
}

enum PtyIoDataCommand {
    WriteUserInput(Bytes),
    GuardedWrite {
        bytes: Bytes,
        validate: GuardedWriteValidator,
        deadline: Instant,
        cancelled: Arc<AtomicBool>,
        reply: std_mpsc::Sender<Result<(), GuardedWriteError>>,
    },
}

enum PtyIoControlCommand {
    BeginHandoff(std_mpsc::Sender<std::io::Result<()>>),
    DrainOutputBoundary {
        deadline: Instant,
        reply: std_mpsc::Sender<Result<(), GuardedWriteError>>,
    },
    DuplicateForHandoff(std_mpsc::Sender<std::io::Result<RawFd>>),
    ForegroundProcessGroup(std_mpsc::Sender<Option<u32>>),
    RollbackHandoff(std_mpsc::Sender<std::io::Result<()>>),
    ReleaseAfterCommit(std_mpsc::Sender<std::io::Result<()>>),
    Shutdown,
}

#[derive(Clone)]
pub(crate) struct PtyIoActorHandle {
    data_tx: mpsc::Sender<PtyIoDataCommand>,
    control_tx: std_mpsc::Sender<PtyIoControlCommand>,
    wake: fd::WakeWriter,
    user_writes: Arc<Mutex<UserWriteGate>>,
    controls: Arc<Mutex<SharedPtyControls>>,
    response_order: Arc<Mutex<()>>,
}

#[derive(Debug)]
struct UserWriteGate {
    accepting: bool,
    queued_user_writes: usize,
    guarded_write_pending: bool,
}

impl PtyIoActorHandle {
    pub(crate) async fn write_user_input(
        &self,
        bytes: Bytes,
    ) -> Result<(), mpsc::error::SendError<Bytes>> {
        {
            let user_writes = self
                .user_writes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !user_writes.accepting || user_writes.guarded_write_pending {
                return Err(mpsc::error::SendError(bytes));
            }
        }

        let permit = match self.data_tx.reserve().await {
            Ok(permit) => permit,
            Err(_) => return Err(mpsc::error::SendError(bytes)),
        };

        let mut user_writes = self
            .user_writes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !user_writes.accepting || user_writes.guarded_write_pending {
            return Err(mpsc::error::SendError(bytes));
        }
        user_writes.queued_user_writes = user_writes.queued_user_writes.saturating_add(1);
        permit.send(PtyIoDataCommand::WriteUserInput(bytes));
        self.wake_actor();
        Ok(())
    }

    pub(crate) fn try_write_user_input(
        &self,
        bytes: Bytes,
    ) -> Result<(), mpsc::error::TrySendError<Bytes>> {
        let mut user_writes = self
            .user_writes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !user_writes.accepting || user_writes.guarded_write_pending {
            return Err(mpsc::error::TrySendError::Closed(bytes));
        }
        user_writes.queued_user_writes = user_writes.queued_user_writes.saturating_add(1);
        match self
            .data_tx
            .try_send(PtyIoDataCommand::WriteUserInput(bytes))
        {
            Ok(()) => {
                self.wake_actor();
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(PtyIoDataCommand::WriteUserInput(bytes))) => {
                user_writes.queued_user_writes = user_writes.queued_user_writes.saturating_sub(1);
                Err(mpsc::error::TrySendError::Full(bytes))
            }
            Err(mpsc::error::TrySendError::Closed(PtyIoDataCommand::WriteUserInput(bytes))) => {
                user_writes.queued_user_writes = user_writes.queued_user_writes.saturating_sub(1);
                Err(mpsc::error::TrySendError::Closed(bytes))
            }
            Err(_) => unreachable!("try_write_user_input only queues ordinary input"),
        }
    }

    pub(crate) fn write_guarded(
        &self,
        bytes: Bytes,
        validate: GuardedWriteValidator,
    ) -> Result<(), GuardedWriteError> {
        if bytes.len() != 1 {
            return Err(GuardedWriteError::Io(
                "guarded writes must contain exactly one byte".to_string(),
            ));
        }
        let (reply_tx, reply_rx) = std_mpsc::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        {
            let mut user_writes = self
                .user_writes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !user_writes.accepting {
                return Err(GuardedWriteError::Closed);
            }
            if user_writes.queued_user_writes != 0 || user_writes.guarded_write_pending {
                return Err(GuardedWriteError::Busy);
            }
            user_writes.guarded_write_pending = true;
            match self.data_tx.try_send(PtyIoDataCommand::GuardedWrite {
                bytes,
                validate,
                deadline: Instant::now() + GUARDED_WRITE_TIMEOUT,
                cancelled: Arc::clone(&cancelled),
                reply: reply_tx,
            }) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    user_writes.guarded_write_pending = false;
                    return Err(GuardedWriteError::Busy);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    user_writes.guarded_write_pending = false;
                    return Err(GuardedWriteError::Closed);
                }
            }
            self.wake_actor();
        }
        match reply_rx.recv_timeout(GUARDED_WRITE_TIMEOUT) {
            Ok(result) => result,
            Err(std_mpsc::RecvTimeoutError::Disconnected) => Err(GuardedWriteError::Closed),
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                cancelled.store(true, Ordering::Release);
                self.wake_actor();
                reply_rx.recv().unwrap_or(Err(GuardedWriteError::Closed))
            }
        }
    }

    pub(crate) const fn guarded_writes_supported(&self) -> bool {
        true
    }

    pub(crate) fn request_output_boundary(
        &self,
    ) -> Result<OutputBoundaryRequest, GuardedWriteError> {
        let (reply_tx, reply_rx) = std_mpsc::channel();
        self.control_tx
            .send(PtyIoControlCommand::DrainOutputBoundary {
                deadline: Instant::now() + GUARDED_WRITE_TIMEOUT,
                reply: reply_tx,
            })
            .map_err(|_| GuardedWriteError::Closed)?;
        self.wake_actor();
        Ok(OutputBoundaryRequest::new(reply_rx))
    }

    pub(crate) fn write_terminal_response(&self, response: impl FnOnce() -> Option<Bytes>) {
        let _order = self
            .response_order
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(bytes) = response() else {
            return;
        };
        if !bytes.is_empty() {
            self.controls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .terminal_responses
                .push(bytes);
            self.wake_actor();
        }
    }

    pub(crate) fn resize(
        &self,
        rows: u16,
        cols: u16,
        cell_width_px: u32,
        cell_height_px: u32,
        terminal_responses: Vec<Bytes>,
    ) {
        {
            let mut controls = self
                .controls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            controls.resize = Some(PtyResizeRequest {
                resize: PtyResize {
                    rows,
                    cols,
                    cell_width_px,
                    cell_height_px,
                },
                terminal_responses,
            });
        }
        self.wake_actor();
    }

    pub(crate) fn nudge_child_redraw_after_handoff(
        &self,
        rows: u16,
        cols: u16,
        cell_width_px: u32,
        cell_height_px: u32,
    ) {
        {
            let mut controls = self
                .controls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            controls.nudge = Some(PtyResize {
                rows,
                cols,
                cell_width_px,
                cell_height_px,
            });
        }
        self.wake_actor();
    }

    pub(crate) fn begin_handoff(&self, timeout: Duration) -> std::io::Result<()> {
        let (reply_tx, reply_rx) = std_mpsc::channel();
        {
            let mut user_writes = self
                .user_writes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            user_writes.accepting = false;
            if self
                .control_tx
                .send(PtyIoControlCommand::BeginHandoff(reply_tx))
                .is_err()
            {
                user_writes.accepting = true;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "pty actor closed",
                ));
            }
            self.wake_actor();
        }
        match reply_rx.recv_timeout(timeout) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => {
                let _ = self.rollback_handoff();
                Err(err)
            }
            Err(_) => {
                let _ = self.rollback_handoff();
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timed out waiting for PTY actor to quiesce",
                ))
            }
        }
    }

    pub(crate) fn duplicate_for_handoff(&self) -> std::io::Result<RawFd> {
        let (reply_tx, reply_rx) = std_mpsc::channel();
        self.control_tx
            .send(PtyIoControlCommand::DuplicateForHandoff(reply_tx))
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pty actor closed"))?;
        self.wake_actor();
        reply_rx.recv_timeout(Duration::from_secs(1)).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "timed out waiting for PTY handoff duplicate",
            )
        })?
    }

    pub(crate) fn foreground_process_group_id(&self) -> Option<u32> {
        let (reply_tx, reply_rx) = std_mpsc::channel();
        self.control_tx
            .send(PtyIoControlCommand::ForegroundProcessGroup(reply_tx))
            .ok()?;
        self.wake_actor();
        reply_rx.recv_timeout(Duration::from_secs(1)).ok()?
    }

    pub(crate) fn rollback_handoff(&self) -> std::io::Result<()> {
        let (reply_tx, reply_rx) = std_mpsc::channel();
        self.control_tx
            .send(PtyIoControlCommand::RollbackHandoff(reply_tx))
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pty actor closed"))?;
        self.wake_actor();
        let result = reply_rx.recv_timeout(Duration::from_secs(1)).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "timed out waiting for PTY handoff rollback",
            )
        })?;
        if result.is_ok() {
            let mut user_writes = self
                .user_writes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            user_writes.accepting = true;
        }
        result
    }

    pub(crate) fn release_after_commit(&self) -> std::io::Result<()> {
        {
            let mut user_writes = self
                .user_writes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            user_writes.accepting = false;
        }
        let (reply_tx, reply_rx) = std_mpsc::channel();
        self.control_tx
            .send(PtyIoControlCommand::ReleaseAfterCommit(reply_tx))
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pty actor closed"))?;
        self.wake_actor();
        reply_rx.recv_timeout(Duration::from_secs(1)).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "timed out waiting for PTY actor release",
            )
        })?
    }

    pub(crate) fn shutdown(&self) {
        {
            let mut user_writes = self
                .user_writes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            user_writes.accepting = false;
        }
        if self.control_tx.send(PtyIoControlCommand::Shutdown).is_ok() {
            self.wake_actor();
        }
    }

    fn wake_actor(&self) {
        if let Err(err) = self.wake.wake() {
            debug!(err = %err, "failed to wake PTY actor");
        }
    }
}

pub(crate) struct PtyIoActor;

impl PtyIoActor {
    pub(crate) fn spawn(config: PtyIoActorConfig) -> std::io::Result<PtyIoActorHandle> {
        Self::spawn_inner(config, None)
    }

    fn spawn_inner(
        config: PtyIoActorConfig,
        poll_observer: Option<std_mpsc::Sender<()>>,
    ) -> std::io::Result<PtyIoActorHandle> {
        fd::set_cloexec(config.master_fd.as_raw_fd())?;
        fd::set_nonblocking(config.master_fd.as_raw_fd())?;

        let (data_tx, data_rx) = mpsc::channel(ACTOR_COMMAND_BUFFER);
        let (control_tx, control_rx) = std_mpsc::channel();
        let wake_pipe = fd::create_wake_pipe()?;
        let user_writes = Arc::new(Mutex::new(UserWriteGate {
            accepting: !config.initially_quiesced,
            queued_user_writes: 0,
            guarded_write_pending: false,
        }));
        let controls = Arc::new(Mutex::new(SharedPtyControls::default()));
        let response_order = Arc::new(Mutex::new(()));
        let handle = PtyIoActorHandle {
            data_tx,
            control_tx,
            wake: wake_pipe.writer,
            user_writes: Arc::clone(&user_writes),
            controls: Arc::clone(&controls),
            response_order: Arc::clone(&response_order),
        };

        let mut runner = PtyIoActorRunner {
            pane_id: config.pane_id,
            file: std::fs::File::from(config.master_fd),
            data_rx,
            control_rx,
            state: if config.initially_quiesced {
                ActorState::Quiesced
            } else {
                ActorState::Running
            },
            pending_writes: VecDeque::new(),
            current_write_offset: 0,
            wake_read_fd: wake_pipe.read_fd,
            controls,
            response_order,
            user_writes,
            on_read: config.on_read,
            on_reader_exit: config.on_reader_exit,
            poll_observer,
        };
        std::thread::Builder::new()
            .name(format!("herdr-pty-{}", config.pane_id))
            .spawn(move || runner.run())
            .map_err(|err| std::io::Error::other(err.to_string()))?;

        Ok(handle)
    }

    #[cfg(test)]
    fn spawn_with_poll_observer(
        config: PtyIoActorConfig,
        poll_observer: std_mpsc::Sender<()>,
    ) -> std::io::Result<PtyIoActorHandle> {
        Self::spawn_inner(config, Some(poll_observer))
    }
}

enum PendingWrite {
    UserInput(Bytes),
    TerminalResponse(Bytes),
    Guarded {
        bytes: Bytes,
        validate: GuardedWriteValidator,
        deadline: Instant,
        cancelled: Arc<AtomicBool>,
        reply: std_mpsc::Sender<Result<(), GuardedWriteError>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GuardedReadResult {
    NoData,
    DataRead,
    Closed,
}

impl PendingWrite {
    fn bytes(&self) -> &Bytes {
        match self {
            Self::UserInput(bytes)
            | Self::TerminalResponse(bytes)
            | Self::Guarded { bytes, .. } => bytes,
        }
    }
}

struct PtyIoActorRunner {
    pane_id: u32,
    file: std::fs::File,
    data_rx: mpsc::Receiver<PtyIoDataCommand>,
    control_rx: std_mpsc::Receiver<PtyIoControlCommand>,
    state: ActorState,
    pending_writes: VecDeque<PendingWrite>,
    current_write_offset: usize,
    wake_read_fd: OwnedFd,
    controls: Arc<Mutex<SharedPtyControls>>,
    response_order: Arc<Mutex<()>>,
    user_writes: Arc<Mutex<UserWriteGate>>,
    on_read: ReadCallback,
    on_reader_exit: Option<ReaderExitCallback>,
    poll_observer: Option<std_mpsc::Sender<()>>,
}

impl PtyIoActorRunner {
    fn enqueue_terminal_response(&mut self, bytes: Bytes) {
        if !bytes.is_empty() {
            self.pending_writes
                .push_back(PendingWrite::TerminalResponse(bytes));
        }
    }

    fn run(&mut self) {
        let mut should_exit = false;
        while !should_exit {
            should_exit = self.drain_commands();
            if should_exit || self.state == ActorState::Released {
                break;
            }

            self.apply_pending_controls();

            if !self.pending_writes.is_empty() {
                self.flush_pending_writes_once();
            }

            if let Some(poll_observer) = &self.poll_observer {
                let _ = poll_observer.send(());
            }

            match fd::poll_pty_and_wake(
                self.file.as_raw_fd(),
                self.wake_read_fd.as_raw_fd(),
                self.state == ActorState::Running,
                !self.pending_writes.is_empty(),
                ACTOR_IDLE_POLL_MS,
            ) {
                Ok(readiness) => {
                    if readiness.wake_ready {
                        if let Err(err) = fd::drain_wake_fd(self.wake_read_fd.as_raw_fd()) {
                            debug!(pane = self.pane_id, err = %err, "PTY actor wake drain failed");
                            break;
                        }
                        continue;
                    }
                    if self.state == ActorState::Running
                        && readiness.pty_read_ready
                        && !self.read_once()
                    {
                        break;
                    }
                    if readiness.pty_write_ready && !self.pending_writes.is_empty() {
                        self.flush_pending_writes_once();
                    }
                }
                Err(err) => {
                    debug!(pane = self.pane_id, err = %err, "PTY actor poll failed");
                    break;
                }
            }
        }

        self.finish_actor_exit();
        debug!(pane = self.pane_id, "PTY actor exiting");
    }

    fn finish_actor_exit(&mut self) {
        {
            let mut gate = self
                .user_writes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            gate.accepting = false;
            self.data_rx.close();
        }
        self.fail_pending_writes(GuardedWriteError::Closed);
        while let Ok(command) = self.data_rx.try_recv() {
            match command {
                PtyIoDataCommand::WriteUserInput(_) => self.finish_user_write(),
                PtyIoDataCommand::GuardedWrite { reply, .. } => {
                    self.finish_guarded_write();
                    let _ = reply.send(Err(GuardedWriteError::Closed));
                }
            }
        }
        self.close_user_write_gate();
        if let Some(on_reader_exit) = self.on_reader_exit.take() {
            on_reader_exit();
        }
    }

    fn drain_commands(&mut self) -> bool {
        if self.drain_control_commands() {
            return true;
        }
        self.drain_data_commands()
    }

    fn drain_control_commands(&mut self) -> bool {
        let mut should_exit = false;
        loop {
            match self.control_rx.try_recv() {
                Ok(command) => {
                    if self.handle_control_command(command) {
                        should_exit = true;
                        break;
                    }
                }
                Err(std_mpsc::TryRecvError::Empty) => break,
                Err(std_mpsc::TryRecvError::Disconnected) => {
                    should_exit = true;
                    break;
                }
            }
        }
        should_exit
    }

    fn drain_data_commands(&mut self) -> bool {
        let mut should_exit = false;
        loop {
            match self.data_rx.try_recv() {
                Ok(command) => {
                    if self.handle_data_command(command) {
                        should_exit = true;
                        break;
                    }
                }
                Err(DataTryRecvError::Empty) => break,
                Err(DataTryRecvError::Disconnected) => {
                    should_exit = true;
                    break;
                }
            }
        }
        should_exit
    }

    fn handle_data_command(&mut self, command: PtyIoDataCommand) -> bool {
        match command {
            PtyIoDataCommand::WriteUserInput(bytes) => {
                if self.state == ActorState::Running {
                    if bytes.is_empty() {
                        self.finish_user_write();
                    } else {
                        self.pending_writes
                            .push_back(PendingWrite::UserInput(bytes));
                    }
                } else {
                    self.finish_user_write();
                }
            }
            PtyIoDataCommand::GuardedWrite {
                bytes,
                validate,
                deadline,
                cancelled,
                reply,
            } => {
                if self.state != ActorState::Running {
                    self.finish_guarded_write();
                    let _ = reply.send(Err(GuardedWriteError::Closed));
                } else if !self.pending_writes.is_empty() {
                    self.finish_guarded_write();
                    let _ = reply.send(Err(GuardedWriteError::Busy));
                } else {
                    self.pending_writes.push_back(PendingWrite::Guarded {
                        bytes,
                        validate,
                        deadline,
                        cancelled,
                        reply,
                    });
                }
            }
        }
        false
    }

    fn handle_control_command(&mut self, command: PtyIoControlCommand) -> bool {
        match command {
            PtyIoControlCommand::BeginHandoff(reply) => {
                let result = self.begin_handoff();
                let _ = reply.send(result);
            }
            PtyIoControlCommand::DrainOutputBoundary { deadline, reply } => {
                let result = if self.state == ActorState::Running {
                    self.drain_output_boundary(deadline)
                } else {
                    Err(GuardedWriteError::Busy)
                };
                let closed = result == Err(GuardedWriteError::Closed);
                let _ = reply.send(result);
                if closed {
                    return true;
                }
            }
            PtyIoControlCommand::DuplicateForHandoff(reply) => {
                let result = if self.state == ActorState::Quiesced {
                    fd::duplicate_cloexec_fd(self.file.as_raw_fd())
                } else {
                    Err(std::io::Error::other(
                        "PTY actor must be quiesced before handoff duplication",
                    ))
                };
                let _ = reply.send(result);
            }
            PtyIoControlCommand::ForegroundProcessGroup(reply) => {
                let result =
                    crate::platform::foreground_process_group_id_for_tty_fd(self.file.as_raw_fd());
                let _ = reply.send(result);
            }
            PtyIoControlCommand::RollbackHandoff(reply) => {
                let result = if self.state == ActorState::Released {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "PTY actor was released before handoff rollback",
                    ))
                } else {
                    self.state = ActorState::Running;
                    Ok(())
                };
                let _ = reply.send(result);
            }
            PtyIoControlCommand::ReleaseAfterCommit(reply) => {
                self.state = ActorState::Released;
                self.fail_pending_writes(GuardedWriteError::Closed);
                let _ = reply.send(Ok(()));
                return true;
            }
            PtyIoControlCommand::Shutdown => return true,
        }
        false
    }

    fn begin_handoff(&mut self) -> std::io::Result<()> {
        self.drain_pre_quiesce_commands();
        self.apply_pending_controls();
        if self.state == ActorState::Released {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "PTY actor was released before handoff quiesce",
            ));
        }
        let deadline = Instant::now() + HANDOFF_DRAIN_TIMEOUT;
        self.flush_pending_writes_once();
        while !self.pending_writes.is_empty() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timed out draining PTY writes before handoff",
                ));
            }
            let timeout_ms = remaining.as_millis().min(i32::MAX as u128) as i32;
            let readiness = fd::poll_pty_and_wake(
                self.file.as_raw_fd(),
                self.wake_read_fd.as_raw_fd(),
                true,
                true,
                timeout_ms,
            )?;
            if readiness.wake_ready {
                fd::drain_wake_fd(self.wake_read_fd.as_raw_fd())?;
            }
            if readiness.pty_read_ready && !self.read_once() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "PTY closed while draining writes before handoff",
                ));
            }
            if readiness.pty_write_ready {
                self.flush_pending_writes_once();
            }
        }
        self.state = ActorState::Quiesced;
        Ok(())
    }

    fn drain_pre_quiesce_commands(&mut self) {
        while let Ok(command) = self.data_rx.try_recv() {
            self.handle_data_command(command);
        }
    }

    fn apply_pending_controls(&mut self) {
        let (resize, nudge, terminal_responses) = {
            let mut controls = self
                .controls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (
                controls.resize.take(),
                controls.nudge.take(),
                std::mem::take(&mut controls.terminal_responses),
            )
        };
        if self.state == ActorState::Released {
            return;
        }
        if let Some(request) = resize {
            self.resize(request.resize);
            self.enqueue_terminal_responses(request.terminal_responses);
        }
        if let Some(nudge) = nudge {
            self.nudge(nudge);
        }
        self.enqueue_terminal_responses(terminal_responses);
    }

    fn read_once(&mut self) -> bool {
        let mut buf = [0u8; 8192];
        match self.file.read(&mut buf) {
            Ok(0) => false,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => true,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => true,
            Err(err) => {
                debug!(pane = self.pane_id, err = %err, "PTY actor read failed");
                false
            }
            Ok(n) => {
                self.process_read_bytes(&buf[..n]);
                true
            }
        }
    }

    fn read_pending_output_before_guarded_write(&mut self) -> GuardedReadResult {
        let mut buf = [0u8; 8192];
        loop {
            match self.file.read(&mut buf) {
                Ok(0) => return GuardedReadResult::Closed,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    return GuardedReadResult::NoData;
                }
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(err) => {
                    debug!(
                        pane = self.pane_id,
                        err = %err,
                        "PTY actor pre-guard read failed"
                    );
                    return GuardedReadResult::Closed;
                }
                Ok(n) => {
                    self.process_read_bytes(&buf[..n]);
                    return GuardedReadResult::DataRead;
                }
            }
        }
    }

    fn drain_output_boundary(&mut self, deadline: Instant) -> Result<(), GuardedWriteError> {
        let mut buf = [0u8; 8192];
        loop {
            if Instant::now() >= deadline {
                return Err(GuardedWriteError::TimedOut);
            }
            match self.file.read(&mut buf) {
                Ok(0) => return Err(GuardedWriteError::Closed),
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(GuardedWriteError::Io(err.to_string())),
                Ok(n) => self.process_read_bytes(&buf[..n]),
            }
        }
    }

    fn process_read_bytes(&mut self, bytes: &[u8]) {
        let response_order = Arc::clone(&self.response_order);
        let _order = response_order
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let result = (self.on_read)(bytes);
        self.controls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .terminal_responses
            .extend(result.terminal_responses);
        drop(_order);
        let terminal_responses = std::mem::take(
            &mut self
                .controls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .terminal_responses,
        );
        self.enqueue_terminal_responses(terminal_responses);
    }

    fn enqueue_terminal_responses(&mut self, terminal_responses: Vec<Bytes>) {
        if self.state == ActorState::Released {
            return;
        }
        for bytes in terminal_responses {
            self.enqueue_terminal_response(bytes);
        }
    }

    fn flush_pending_writes_once(&mut self) {
        while !self.pending_writes.is_empty() {
            if self.current_write_offset == 0
                && matches!(
                    self.pending_writes.front(),
                    Some(PendingWrite::Guarded { .. })
                )
            {
                match self.read_pending_output_before_guarded_write() {
                    GuardedReadResult::NoData => {}
                    GuardedReadResult::DataRead => {
                        let pending = self
                            .pending_writes
                            .pop_front()
                            .expect("guarded write remains queued");
                        self.complete_pending_write(
                            pending,
                            Err(GuardedWriteError::ValidationFailed),
                        );
                        continue;
                    }
                    GuardedReadResult::Closed => {
                        self.fail_pending_writes(GuardedWriteError::Closed);
                        return;
                    }
                }
            }

            if self.current_write_offset == 0 {
                let guarded_failure = {
                    let pending = self
                        .pending_writes
                        .front()
                        .expect("pending write remains queued");
                    match pending {
                        PendingWrite::Guarded {
                            deadline,
                            cancelled,
                            ..
                        } if cancelled.load(Ordering::Acquire) || Instant::now() >= *deadline => {
                            Some(GuardedWriteError::TimedOut)
                        }
                        PendingWrite::Guarded {
                            validate,
                            deadline,
                            cancelled,
                            ..
                        } => {
                            let valid =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    validate()
                                }))
                                .unwrap_or(false);
                            if !valid {
                                Some(GuardedWriteError::ValidationFailed)
                            } else if cancelled.load(Ordering::Acquire)
                                || Instant::now() >= *deadline
                            {
                                Some(GuardedWriteError::TimedOut)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                };
                if let Some(error) = guarded_failure {
                    let pending = self
                        .pending_writes
                        .pop_front()
                        .expect("guarded write remains queued");
                    self.complete_pending_write(pending, Err(error));
                    continue;
                }

                if matches!(
                    self.pending_writes.front(),
                    Some(PendingWrite::Guarded { .. })
                ) {
                    match self.read_pending_output_before_guarded_write() {
                        GuardedReadResult::NoData => {}
                        GuardedReadResult::DataRead => {
                            let pending = self
                                .pending_writes
                                .pop_front()
                                .expect("guarded write remains queued");
                            self.complete_pending_write(
                                pending,
                                Err(GuardedWriteError::ValidationFailed),
                            );
                            continue;
                        }
                        GuardedReadResult::Closed => {
                            self.fail_pending_writes(GuardedWriteError::Closed);
                            return;
                        }
                    }
                }
            }

            let pending = self
                .pending_writes
                .front()
                .expect("pending write remains queued");
            let chunk = &pending.bytes()[self.current_write_offset..];
            match self.file.write(chunk) {
                Ok(0) => {
                    warn!(pane = self.pane_id, "PTY actor write returned zero bytes");
                    if matches!(pending, PendingWrite::Guarded { .. }) {
                        let pending = self
                            .pending_writes
                            .pop_front()
                            .expect("guarded write remains queued");
                        self.current_write_offset = 0;
                        self.complete_pending_write(
                            pending,
                            Err(GuardedWriteError::Io(
                                std::io::ErrorKind::WriteZero.to_string(),
                            )),
                        );
                    }
                    return;
                }
                Ok(written) => {
                    self.current_write_offset += written;
                    if self.current_write_offset >= pending.bytes().len() {
                        let pending = self
                            .pending_writes
                            .pop_front()
                            .expect("completed write remains queued");
                        self.current_write_offset = 0;
                        self.complete_pending_write(pending, Ok(()));
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => return,
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => return,
                Err(err) => {
                    warn!(pane = self.pane_id, err = %err, "PTY actor write failed");
                    self.fail_pending_writes(GuardedWriteError::Io(err.to_string()));
                    self.current_write_offset = 0;
                    return;
                }
            }
        }
        let _ = self.file.flush();
    }

    fn complete_pending_write(
        &mut self,
        pending: PendingWrite,
        result: Result<(), GuardedWriteError>,
    ) {
        match pending {
            PendingWrite::UserInput(_) => self.finish_user_write(),
            PendingWrite::TerminalResponse(_) => {}
            PendingWrite::Guarded { reply, .. } => {
                self.finish_guarded_write();
                let _ = reply.send(result);
            }
        }
    }

    fn fail_pending_writes(&mut self, error: GuardedWriteError) {
        while let Some(pending) = self.pending_writes.pop_front() {
            self.complete_pending_write(pending, Err(error.clone()));
        }
        self.current_write_offset = 0;
    }

    fn finish_user_write(&self) {
        let mut gate = self
            .user_writes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        gate.queued_user_writes = gate.queued_user_writes.saturating_sub(1);
    }

    fn finish_guarded_write(&self) {
        let mut gate = self
            .user_writes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        gate.guarded_write_pending = false;
    }

    fn close_user_write_gate(&self) {
        let mut gate = self
            .user_writes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        gate.accepting = false;
        gate.queued_user_writes = 0;
        gate.guarded_write_pending = false;
    }

    fn resize(&self, resize: PtyResize) {
        self.log_resize_result(fd::resize_pty_fd(
            self.file.as_raw_fd(),
            resize.rows,
            resize.cols,
            resize.cell_width_px,
            resize.cell_height_px,
        ));
    }

    fn nudge(&mut self, resize: PtyResize) {
        if self.state == ActorState::Released {
            return;
        }
        let nudge = if resize.rows > 2 {
            (
                resize.rows - 1,
                resize.cols,
                resize.cell_width_px,
                resize.cell_height_px,
            )
        } else {
            (
                resize.rows,
                resize.cols.saturating_sub(1).max(4),
                resize.cell_width_px,
                resize.cell_height_px,
            )
        };
        if nudge
            == (
                resize.rows,
                resize.cols,
                resize.cell_width_px,
                resize.cell_height_px,
            )
        {
            return;
        }
        self.log_resize_result(fd::resize_pty_fd(
            self.file.as_raw_fd(),
            nudge.0,
            nudge.1,
            nudge.2,
            nudge.3,
        ));
        std::thread::sleep(Duration::from_millis(30));
        self.log_resize_result(fd::resize_pty_fd(
            self.file.as_raw_fd(),
            resize.rows,
            resize.cols,
            resize.cell_width_px,
            resize.cell_height_px,
        ));
    }

    fn log_resize_result(&self, result: std::io::Result<()>) {
        if let Err(err) = result {
            debug!(pane = self.pane_id, err = %err, "PTY resize failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        os::fd::{AsRawFd, FromRawFd, IntoRawFd},
        os::unix::net::UnixStream,
        sync::atomic::{AtomicBool, Ordering},
    };

    fn test_wake_pair() -> (fd::WakeWriter, OwnedFd) {
        let pipe = fd::create_wake_pipe().expect("wake pipe");
        (pipe.writer, pipe.read_fd)
    }

    fn test_user_write_gate() -> Arc<Mutex<UserWriteGate>> {
        Arc::new(Mutex::new(UserWriteGate {
            accepting: true,
            queued_user_writes: 0,
            guarded_write_pending: false,
        }))
    }

    fn wait_output_boundary(request: &OutputBoundaryRequest) -> Result<(), GuardedWriteError> {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            match request.poll()? {
                Some(()) => return Ok(()),
                None if Instant::now() < deadline => std::thread::yield_now(),
                None => return Err(GuardedWriteError::TimedOut),
            }
        }
    }

    fn actor_with_socket_pair(
        initially_quiesced: bool,
    ) -> (PtyIoActorHandle, UnixStream, std_mpsc::Receiver<Bytes>) {
        actor_with_socket_pair_and_poll_observer(initially_quiesced, None)
    }

    fn actor_with_socket_pair_and_poll_observer(
        initially_quiesced: bool,
        poll_observer: Option<std_mpsc::Sender<()>>,
    ) -> (PtyIoActorHandle, UnixStream, std_mpsc::Receiver<Bytes>) {
        let (actor_socket, peer) = UnixStream::pair().expect("socket pair");
        actor_socket
            .set_nonblocking(true)
            .expect("actor socket nonblocking");
        peer.set_read_timeout(Some(Duration::from_secs(1)))
            .expect("peer timeout");
        let owned = unsafe { OwnedFd::from_raw_fd(actor_socket.into_raw_fd()) };
        let (read_tx, read_rx) = std_mpsc::channel();
        let config = PtyIoActorConfig {
            pane_id: 1,
            master_fd: owned,
            initially_quiesced,
            on_read: Box::new(move |bytes| {
                read_tx
                    .send(Bytes::copy_from_slice(bytes))
                    .expect("read callback receiver alive");
                PtyReadResult::empty()
            }),
            on_reader_exit: None,
        };
        let handle = if let Some(poll_observer) = poll_observer {
            PtyIoActor::spawn_with_poll_observer(config, poll_observer)
        } else {
            PtyIoActor::spawn(config)
        }
        .expect("actor spawn");
        (handle, peer, read_rx)
    }

    fn actor_runner_for_unit_test() -> (PtyIoActorRunner, UnixStream) {
        let (actor_socket, peer) = UnixStream::pair().expect("socket pair");
        actor_socket
            .set_nonblocking(true)
            .expect("actor socket nonblocking");
        let owned = unsafe { OwnedFd::from_raw_fd(actor_socket.into_raw_fd()) };
        let (_data_tx, data_rx) = mpsc::channel(ACTOR_COMMAND_BUFFER);
        let (_control_tx, control_rx) = std_mpsc::channel();
        let wake_pipe = fd::create_wake_pipe().expect("wake pipe");
        let runner = PtyIoActorRunner {
            pane_id: 1,
            file: std::fs::File::from(owned),
            data_rx,
            control_rx,
            state: ActorState::Running,
            pending_writes: VecDeque::new(),
            current_write_offset: 0,
            wake_read_fd: wake_pipe.read_fd,
            controls: Arc::new(Mutex::new(SharedPtyControls::default())),
            response_order: Arc::new(Mutex::new(())),
            user_writes: test_user_write_gate(),
            on_read: Box::new(|_| PtyReadResult::empty()),
            on_reader_exit: None,
            poll_observer: None,
        };
        (runner, peer)
    }

    fn backpressured_actor() -> (PtyIoActorHandle, UnixStream, usize) {
        let (mut actor_socket, peer) = UnixStream::pair().expect("socket pair");
        actor_socket
            .set_nonblocking(true)
            .expect("actor socket nonblocking");
        peer.set_read_timeout(Some(Duration::from_secs(1)))
            .expect("peer timeout");
        let fill = [0xAA; 8192];
        let mut prefilled = 0;
        loop {
            match actor_socket.write(&fill) {
                Ok(written) => prefilled += written,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(err) => panic!("failed to fill actor write buffer: {err}"),
            }
        }
        assert!(prefilled > 0);
        let owned = unsafe { OwnedFd::from_raw_fd(actor_socket.into_raw_fd()) };
        let handle = PtyIoActor::spawn(PtyIoActorConfig {
            pane_id: 1,
            master_fd: owned,
            initially_quiesced: false,
            on_read: Box::new(|_| PtyReadResult::empty()),
            on_reader_exit: None,
        })
        .expect("actor spawn");
        (handle, peer, prefilled)
    }

    #[test]
    fn actor_ignores_empty_user_input_write() {
        let (mut runner, _peer) = actor_runner_for_unit_test();

        assert!(!runner.handle_data_command(PtyIoDataCommand::WriteUserInput(Bytes::new())));

        assert!(runner.pending_writes.is_empty());
    }

    #[test]
    fn actor_writes_user_input_to_owned_fd() {
        let (handle, mut peer, _read_rx) = actor_with_socket_pair(false);

        handle
            .try_write_user_input(Bytes::from_static(b"hello"))
            .expect("write command accepted");

        let mut buf = [0u8; 5];
        peer.read_exact(&mut buf).expect("peer receives write");
        assert_eq!(&buf, b"hello");
        handle.shutdown();
    }

    #[test]
    fn output_boundary_drains_pending_pty_output_before_acknowledging() {
        let (handle, mut peer, read_rx) = actor_with_socket_pair(false);
        peer.write_all(b"old process footer")
            .expect("old process output reaches actor fd");

        let request = handle
            .request_output_boundary()
            .expect("output boundary request accepted");
        wait_output_boundary(&request).expect("output boundary succeeds");

        assert_eq!(
            read_rx.try_recv().expect("output callback completed"),
            Bytes::from_static(b"old process footer")
        );
        handle.shutdown();
    }

    #[test]
    fn guarded_write_rejects_an_earlier_queued_user_write_without_validating() {
        let (data_tx, mut data_rx) = mpsc::channel(2);
        let (control_tx, _control_rx) = std_mpsc::channel();
        let (wake, _wake_read_fd) = test_wake_pair();
        let handle = PtyIoActorHandle {
            data_tx,
            control_tx,
            wake,
            user_writes: test_user_write_gate(),
            controls: Arc::new(Mutex::new(SharedPtyControls::default())),
            response_order: Arc::new(Mutex::new(())),
        };
        handle
            .try_write_user_input(Bytes::from_static(b"earlier"))
            .expect("ordinary input queued");
        let validations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let validation_count = Arc::clone(&validations);

        let result = handle.write_guarded(
            Bytes::from_static(b"\x1b"),
            Arc::new(move || {
                validation_count.fetch_add(1, Ordering::AcqRel);
                true
            }),
        );

        assert_eq!(result, Err(GuardedWriteError::Busy));
        assert_eq!(validations.load(Ordering::Acquire), 0);
        assert!(matches!(
            data_rx.try_recv(),
            Ok(PtyIoDataCommand::WriteUserInput(bytes))
                if bytes == Bytes::from_static(b"earlier")
        ));
        assert!(matches!(data_rx.try_recv(), Err(DataTryRecvError::Empty)));
    }

    #[test]
    fn guarded_write_observes_pending_pty_output_before_validating() {
        let (mut runner, mut peer) = actor_runner_for_unit_test();
        let output_observed = Arc::new(AtomicBool::new(false));
        let observed_by_callback = Arc::clone(&output_observed);
        runner.on_read = Box::new(move |_| {
            observed_by_callback.store(true, Ordering::Release);
            PtyReadResult::empty()
        });
        peer.write_all(b"pending redraw")
            .expect("pending PTY output reaches actor fd");

        let validations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let validation_count = Arc::clone(&validations);
        let (reply_tx, reply_rx) = std_mpsc::channel();
        runner.handle_data_command(PtyIoDataCommand::GuardedWrite {
            bytes: Bytes::from_static(b"\x1b"),
            validate: Arc::new(move || {
                validation_count.fetch_add(1, Ordering::AcqRel);
                true
            }),
            deadline: Instant::now() + GUARDED_WRITE_TIMEOUT,
            cancelled: Arc::new(AtomicBool::new(false)),
            reply: reply_tx,
        });

        runner.flush_pending_writes_once();

        assert!(output_observed.load(Ordering::Acquire));
        assert_eq!(validations.load(Ordering::Acquire), 0);
        assert_eq!(
            reply_rx.recv().expect("guarded result"),
            Err(GuardedWriteError::ValidationFailed)
        );
        peer.set_nonblocking(true).expect("peer nonblocking");
        let mut action = [0; 1];
        let err = peer
            .read(&mut action)
            .expect_err("stale guarded byte must not be written");
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    }

    #[test]
    fn guarded_write_rechecks_for_pty_output_after_validating() {
        let (mut runner, mut peer) = actor_runner_for_unit_test();
        let output_observed = Arc::new(AtomicBool::new(false));
        let observed_by_callback = Arc::clone(&output_observed);
        runner.on_read = Box::new(move |_| {
            observed_by_callback.store(true, Ordering::Release);
            PtyReadResult::empty()
        });
        let output_peer = Arc::new(Mutex::new(peer.try_clone().expect("clone output peer")));
        let (reply_tx, reply_rx) = std_mpsc::channel();
        runner.handle_data_command(PtyIoDataCommand::GuardedWrite {
            bytes: Bytes::from_static(b"\x1b"),
            validate: Arc::new(move || {
                output_peer
                    .lock()
                    .expect("output peer lock")
                    .write_all(b"redraw during validation")
                    .expect("redraw reaches actor fd");
                true
            }),
            deadline: Instant::now() + GUARDED_WRITE_TIMEOUT,
            cancelled: Arc::new(AtomicBool::new(false)),
            reply: reply_tx,
        });

        runner.flush_pending_writes_once();

        assert!(output_observed.load(Ordering::Acquire));
        assert_eq!(
            reply_rx.recv().expect("guarded result"),
            Err(GuardedWriteError::ValidationFailed)
        );
        peer.set_nonblocking(true).expect("peer nonblocking");
        let mut action = [0; 1];
        let err = peer
            .read(&mut action)
            .expect_err("guarded byte must not follow a validation-time redraw");
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    }

    #[test]
    fn guarded_write_validates_at_writable_boundary_and_acks_after_write() {
        let (handle, mut peer, prefilled) = backpressured_actor();
        let validations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let validation_count = Arc::clone(&validations);
        let guarded_handle = handle.clone();
        let guarded = std::thread::spawn(move || {
            guarded_handle.write_guarded(
                Bytes::from_static(b"\x1b"),
                Arc::new(move || {
                    validation_count.fetch_add(1, Ordering::AcqRel);
                    true
                }),
            )
        });

        std::thread::sleep(Duration::from_millis(50));
        assert!(
            !guarded.is_finished(),
            "guarded action must not acknowledge a backpressured write"
        );
        assert!(
            handle
                .try_write_user_input(Bytes::from_static(b"later"))
                .is_err(),
            "ordinary input cannot interpose while a guarded write is pending"
        );

        let mut prefill = vec![0; prefilled];
        peer.read_exact(&mut prefill)
            .expect("peer drains prefilled actor buffer");
        assert!(prefill.iter().all(|byte| *byte == 0xAA));
        guarded
            .join()
            .expect("guarded writer joins")
            .expect("guarded write succeeds after PTY becomes writable");
        let mut action = [0; 1];
        peer.read_exact(&mut action)
            .expect("peer receives guarded byte before success acknowledgment");
        assert_eq!(action, [0x1b]);
        assert!(validations.load(Ordering::Acquire) >= 2);
        handle.shutdown();
    }

    #[test]
    fn guarded_write_timeout_cancels_before_later_writability() {
        let (handle, mut peer, prefilled) = backpressured_actor();
        let guarded_handle = handle.clone();
        let guarded = std::thread::spawn(move || {
            guarded_handle.write_guarded(Bytes::from_static(b"\x1b"), Arc::new(|| true))
        });

        assert_eq!(
            guarded.join().expect("guarded writer joins"),
            Err(GuardedWriteError::TimedOut)
        );

        let mut prefill = vec![0; prefilled];
        peer.read_exact(&mut prefill)
            .expect("peer drains prefilled actor buffer");
        peer.set_read_timeout(Some(Duration::from_millis(100)))
            .expect("short peer timeout");
        let mut action = [0; 1];
        let err = peer
            .read_exact(&mut action)
            .expect_err("cancelled guarded byte must never be written later");
        assert!(matches!(
            err.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        ));
        handle.shutdown();
    }

    #[test]
    fn actor_exit_acks_queued_guard_before_blocking_reader_exit_callback() {
        let (actor_socket, _peer) = UnixStream::pair().expect("socket pair");
        actor_socket
            .set_nonblocking(true)
            .expect("actor socket nonblocking");
        let (data_tx, data_rx) = mpsc::channel(ACTOR_COMMAND_BUFFER);
        let outstanding_permit = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime")
            .block_on(data_tx.clone().reserve_owned())
            .expect("reserve data-queue capacity");
        let (control_tx, control_rx) = std_mpsc::channel();
        let wake_pipe = fd::create_wake_pipe().expect("wake pipe");
        let user_writes = test_user_write_gate();
        let controls = Arc::new(Mutex::new(SharedPtyControls::default()));
        let response_order = Arc::new(Mutex::new(()));
        let handle = PtyIoActorHandle {
            data_tx,
            control_tx,
            wake: wake_pipe.writer,
            user_writes: Arc::clone(&user_writes),
            controls: Arc::clone(&controls),
            response_order: Arc::clone(&response_order),
        };
        let (exit_started_tx, exit_started_rx) = std_mpsc::channel();
        let (exit_release_tx, exit_release_rx) = std_mpsc::channel();
        let mut runner = PtyIoActorRunner {
            pane_id: 1,
            file: std::fs::File::from(unsafe { OwnedFd::from_raw_fd(actor_socket.into_raw_fd()) }),
            data_rx,
            control_rx,
            state: ActorState::Running,
            pending_writes: VecDeque::new(),
            current_write_offset: 0,
            wake_read_fd: wake_pipe.read_fd,
            controls,
            response_order,
            user_writes: Arc::clone(&user_writes),
            on_read: Box::new(|_| PtyReadResult::empty()),
            on_reader_exit: Some(Box::new(move || {
                exit_started_tx
                    .send(())
                    .expect("exit callback observer alive");
                exit_release_rx
                    .recv()
                    .expect("exit callback release arrives");
            })),
            poll_observer: None,
        };
        let (guarded_result_tx, guarded_result_rx) = std_mpsc::channel();
        let guarded = std::thread::spawn(move || {
            let result = handle.write_guarded(Bytes::from_static(b"\x1b"), Arc::new(|| true));
            guarded_result_tx
                .send(result)
                .expect("guarded result observer alive");
        });

        let queued_deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if user_writes
                .lock()
                .expect("user write gate")
                .guarded_write_pending
            {
                break;
            }
            assert!(
                Instant::now() < queued_deadline,
                "guarded command should enter the data queue"
            );
            std::thread::yield_now();
        }
        std::thread::sleep(GUARDED_WRITE_TIMEOUT + Duration::from_millis(100));
        assert!(
            guarded_result_rx.try_recv().is_err(),
            "timed-out caller must wait for the actor's definitive acknowledgment"
        );

        let actor_exit = std::thread::spawn(move || runner.finish_actor_exit());
        exit_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("reader-exit callback starts");
        assert_eq!(
            guarded_result_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("queued guarded write is acknowledged before callback returns"),
            Err(GuardedWriteError::Closed)
        );
        {
            let gate = user_writes.lock().expect("user write gate");
            assert!(!gate.accepting);
            assert_eq!(gate.queued_user_writes, 0);
            assert!(!gate.guarded_write_pending);
        }

        exit_release_tx
            .send(())
            .expect("release reader-exit callback");
        actor_exit.join().expect("actor exit joins");
        guarded.join().expect("guarded caller joins");
        drop(outstanding_permit);
    }

    #[test]
    fn guarded_write_contains_validator_panics_and_keeps_actor_usable() {
        let (handle, mut peer, _read_rx) = actor_with_socket_pair(false);

        assert_eq!(
            handle.write_guarded(
                Bytes::from_static(b"\x1b"),
                Arc::new(|| panic!("validator panic"))
            ),
            Err(GuardedWriteError::ValidationFailed)
        );
        handle
            .try_write_user_input(Bytes::from_static(b"after"))
            .expect("ordinary writer remains usable");
        let mut after = [0; 5];
        peer.read_exact(&mut after)
            .expect("peer receives later ordinary input");
        assert_eq!(&after, b"after");
        handle.shutdown();
    }

    #[test]
    fn actor_wakes_idle_poll_for_user_input() {
        let (poll_tx, poll_rx) = std_mpsc::channel();
        let (handle, mut peer, _read_rx) =
            actor_with_socket_pair_and_poll_observer(false, Some(poll_tx));
        peer.set_read_timeout(Some(Duration::from_millis(500)))
            .expect("peer timeout");
        poll_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("actor entered idle poll");

        let start = Instant::now();
        handle
            .try_write_user_input(Bytes::from_static(b"x"))
            .expect("write command accepted");

        let mut buf = [0u8; 1];
        peer.read_exact(&mut buf)
            .expect("peer receives write without waiting for actor poll timeout");
        assert_eq!(&buf, b"x");
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "actor write should be driven by wake fd, not the idle poll timeout"
        );
        handle.shutdown();
    }

    #[test]
    fn actor_reads_output_while_input_is_backpressured() {
        let (mut actor_socket, mut peer) = UnixStream::pair().expect("socket pair");
        actor_socket
            .set_nonblocking(true)
            .expect("actor socket nonblocking");
        peer.set_read_timeout(Some(Duration::from_secs(1)))
            .expect("peer timeout");

        let fill = [0xAA; 8192];
        let mut prefilled = 0;
        loop {
            match actor_socket.write(&fill) {
                Ok(written) => prefilled += written,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(err) => panic!("failed to fill actor write buffer: {err}"),
            }
        }
        assert!(prefilled > 0, "actor write buffer should accept some bytes");

        let owned = unsafe { OwnedFd::from_raw_fd(actor_socket.into_raw_fd()) };
        let (read_tx, read_rx) = std_mpsc::channel();
        let handle = PtyIoActor::spawn(PtyIoActorConfig {
            pane_id: 1,
            master_fd: owned,
            initially_quiesced: false,
            on_read: Box::new(move |bytes| {
                read_tx
                    .send(Bytes::copy_from_slice(bytes))
                    .expect("read callback receiver alive");
                PtyReadResult::empty()
            }),
            on_reader_exit: None,
        })
        .expect("actor spawn");

        let marker = Bytes::from_static(b"queued-input");
        handle
            .try_write_user_input(marker.clone())
            .expect("write command accepted");

        const OUTPUT_LEN: usize = 128 * 1024;
        let mut peer_writer = peer.try_clone().expect("clone peer writer");
        let output_writer = std::thread::spawn(move || {
            peer_writer
                .write_all(&vec![0xBB; OUTPUT_LEN])
                .expect("peer writes sustained output");
        });
        let deadline = Instant::now() + Duration::from_millis(500);
        let mut output_len = 0;
        while output_len < OUTPUT_LEN {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "actor did not keep reading blocked peer output"
            );
            let output = read_rx
                .recv_timeout(remaining)
                .expect("actor keeps reading while input remains blocked");
            assert!(output.iter().all(|byte| *byte == 0xBB));
            output_len += output.len();
        }
        assert_eq!(output_len, OUTPUT_LEN);
        output_writer.join().expect("output writer joins");

        let mut received_input = vec![0; prefilled + marker.len()];
        peer.read_exact(&mut received_input)
            .expect("peer receives prefill and queued input");
        assert!(received_input[..prefilled].iter().all(|byte| *byte == 0xAA));
        assert_eq!(&received_input[prefilled..], marker.as_ref());
        handle.shutdown();
    }

    #[test]
    fn actor_wakes_idle_poll_for_handoff_control() {
        let (poll_tx, poll_rx) = std_mpsc::channel();
        let (handle, _peer, _read_rx) =
            actor_with_socket_pair_and_poll_observer(false, Some(poll_tx));
        poll_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("actor entered idle poll");

        let start = Instant::now();
        let handoff_handle = handle.clone();
        let handoff =
            std::thread::spawn(move || handoff_handle.begin_handoff(Duration::from_secs(1)));

        handoff
            .join()
            .expect("handoff thread joins")
            .expect("handoff control should wake idle actor");
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "handoff control should be driven by wake fd, not the idle poll timeout"
        );
        handle.shutdown();
    }

    #[test]
    fn poll_ignores_pty_hup_without_pty_interest() {
        let (actor_socket, peer) = UnixStream::pair().expect("socket pair");
        actor_socket
            .set_nonblocking(true)
            .expect("actor socket nonblocking");
        drop(peer);
        let wake_pipe = fd::create_wake_pipe().expect("wake pipe");

        let readiness = fd::poll_pty_and_wake(
            actor_socket.as_raw_fd(),
            wake_pipe.read_fd.as_raw_fd(),
            false,
            false,
            10,
        )
        .expect("poll succeeds");

        assert!(!readiness.pty_read_ready);
        assert!(!readiness.pty_write_ready);
        assert!(!readiness.wake_ready);
    }

    #[test]
    fn actor_delivers_fd_reads_to_callback() {
        let (handle, mut peer, read_rx) = actor_with_socket_pair(false);

        peer.write_all(b"from-peer").expect("peer write");

        let read = read_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("actor read callback");
        assert_eq!(read, Bytes::from_static(b"from-peer"));
        handle.shutdown();
    }

    #[test]
    fn begin_handoff_stops_reads_and_rejects_user_writes_until_rollback() {
        let (handle, mut peer, read_rx) = actor_with_socket_pair(false);

        handle
            .begin_handoff(Duration::from_secs(1))
            .expect("handoff quiesced");
        assert!(handle
            .try_write_user_input(Bytes::from_static(b"blocked"))
            .is_err());

        peer.write_all(b"held").expect("peer write during quiesce");
        assert!(
            read_rx.recv_timeout(Duration::from_millis(150)).is_err(),
            "actor must not read while quiesced"
        );

        handle.rollback_handoff().expect("rollback resumes actor");
        let read = read_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("actor reads held bytes after rollback");
        assert_eq!(read, Bytes::from_static(b"held"));

        handle
            .try_write_user_input(Bytes::from_static(b"after"))
            .expect("write accepted after rollback");
        let mut buf = [0u8; 5];
        peer.read_exact(&mut buf).expect("peer receives after");
        assert_eq!(&buf, b"after");
        handle.shutdown();
    }

    #[test]
    fn output_boundary_does_not_read_while_actor_is_quiesced() {
        let (handle, mut peer, read_rx) = actor_with_socket_pair(false);
        handle
            .begin_handoff(Duration::from_secs(1))
            .expect("handoff quiesced");
        peer.write_all(b"held during handoff")
            .expect("peer write during quiesce");

        let request = handle
            .request_output_boundary()
            .expect("output boundary request accepted");
        assert_eq!(wait_output_boundary(&request), Err(GuardedWriteError::Busy));
        assert!(
            read_rx.recv_timeout(Duration::from_millis(150)).is_err(),
            "output boundary must preserve quiesced unread bytes"
        );

        handle.rollback_handoff().expect("rollback resumes actor");
        assert_eq!(
            read_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("held bytes read after rollback"),
            Bytes::from_static(b"held during handoff")
        );
        handle.shutdown();
    }

    #[test]
    fn duplicate_for_handoff_requires_quiesced_actor() {
        let (handle, mut peer, read_rx) = actor_with_socket_pair(false);

        assert!(handle.duplicate_for_handoff().is_err());
        handle
            .begin_handoff(Duration::from_secs(1))
            .expect("handoff quiesced");
        let duplicate = handle
            .duplicate_for_handoff()
            .expect("handoff duplicate created");
        assert!(duplicate >= 0);
        unsafe {
            libc::close(duplicate);
        }
        handle.rollback_handoff().expect("rollback resumes actor");

        peer.write_all(b"still-live").expect("peer write");
        let read = read_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("actor still reads after duplicate closes");
        assert_eq!(read, Bytes::from_static(b"still-live"));
        handle.shutdown();
    }

    #[test]
    fn resize_and_nudge_keep_latest_request_when_command_queue_is_full() {
        let (data_tx, _data_rx) = mpsc::channel(1);
        let (control_tx, _control_rx) = std_mpsc::channel();
        data_tx
            .try_send(PtyIoDataCommand::WriteUserInput(Bytes::from_static(
                b"fill",
            )))
            .expect("fill command queue");
        let controls = Arc::new(Mutex::new(SharedPtyControls::default()));
        let (wake, _wake_read_fd) = test_wake_pair();
        let handle = PtyIoActorHandle {
            data_tx,
            control_tx,
            wake,
            user_writes: test_user_write_gate(),
            controls: Arc::clone(&controls),
            response_order: Arc::new(Mutex::new(())),
        };

        handle.resize(20, 80, 8, 16, vec![Bytes::from_static(b"old")]);
        handle.resize(40, 120, 9, 18, vec![Bytes::from_static(b"new")]);
        handle.nudge_child_redraw_after_handoff(41, 121, 10, 20);
        handle.write_terminal_response(|| Some(Bytes::from_static(b"response")));

        let controls = controls.lock().expect("controls lock");
        assert_eq!(
            controls.resize,
            Some(PtyResizeRequest {
                resize: PtyResize {
                    rows: 40,
                    cols: 120,
                    cell_width_px: 9,
                    cell_height_px: 18,
                },
                terminal_responses: vec![Bytes::from_static(b"new")],
            })
        );
        assert_eq!(
            controls.nudge,
            Some(PtyResize {
                rows: 41,
                cols: 121,
                cell_width_px: 10,
                cell_height_px: 20,
            })
        );
        assert_eq!(
            controls.terminal_responses,
            vec![Bytes::from_static(b"response")]
        );
    }

    #[test]
    fn appearance_transition_report_precedes_query_of_new_scheme() {
        let (actor_socket, mut peer) = UnixStream::pair().expect("socket pair");
        actor_socket
            .set_nonblocking(true)
            .expect("actor socket nonblocking");
        let owned = unsafe { OwnedFd::from_raw_fd(actor_socket.into_raw_fd()) };
        let (data_tx, data_rx) = mpsc::channel(ACTOR_COMMAND_BUFFER);
        let (control_tx, control_rx) = std_mpsc::channel();
        let wake_pipe = fd::create_wake_pipe().expect("wake pipe");
        let controls = Arc::new(Mutex::new(SharedPtyControls::default()));
        let response_order = Arc::new(Mutex::new(()));
        let light = Arc::new(AtomicBool::new(false));
        let query_light = Arc::clone(&light);
        let runner = PtyIoActorRunner {
            pane_id: 1,
            file: std::fs::File::from(owned),
            data_rx,
            control_rx,
            state: ActorState::Running,
            pending_writes: VecDeque::new(),
            current_write_offset: 0,
            wake_read_fd: wake_pipe.read_fd,
            controls: Arc::clone(&controls),
            response_order: Arc::clone(&response_order),
            user_writes: test_user_write_gate(),
            on_read: Box::new(move |_| PtyReadResult {
                terminal_responses: vec![if query_light.load(Ordering::Acquire) {
                    Bytes::from_static(b"query-light")
                } else {
                    Bytes::from_static(b"query-dark")
                }],
            }),
            on_reader_exit: None,
            poll_observer: None,
        };
        let handle = PtyIoActorHandle {
            data_tx,
            control_tx,
            wake: wake_pipe.writer,
            user_writes: test_user_write_gate(),
            controls,
            response_order,
        };
        let (changed_tx, changed_rx) = std_mpsc::channel();
        let (continue_tx, continue_rx) = std_mpsc::channel();

        let appearance = std::thread::spawn(move || {
            handle.write_terminal_response(|| {
                light.store(true, Ordering::Release);
                changed_tx.send(()).expect("notify appearance change");
                continue_rx.recv().expect("continue appearance report");
                Some(Bytes::from_static(b"live-light"))
            });
        });
        changed_rx.recv().expect("appearance changed");
        peer.write_all(b"query").expect("write query");
        let reader = std::thread::spawn(move || {
            let mut runner = runner;
            assert!(runner.read_once());
            runner
        });
        continue_tx.send(()).expect("release appearance report");
        appearance.join().expect("appearance thread joins");
        let runner = reader.join().expect("reader thread joins");

        let pending_bytes = runner
            .pending_writes
            .iter()
            .map(PendingWrite::bytes)
            .cloned()
            .collect::<VecDeque<_>>();
        assert_eq!(
            pending_bytes,
            VecDeque::from([
                Bytes::from_static(b"live-light"),
                Bytes::from_static(b"query-light"),
            ])
        );
    }

    #[test]
    fn resize_writes_terminal_responses_after_applying_resize() {
        let (handle, mut peer, _read_rx) = actor_with_socket_pair(false);
        let response = Bytes::from_static(b"\x1B[48;40;100;720;900t");

        handle.resize(40, 100, 9, 18, vec![response.clone()]);

        let mut buf = vec![0; response.len()];
        peer.read_exact(&mut buf)
            .expect("peer receives resize response");
        assert_eq!(Bytes::from(buf), response);
        handle.shutdown();
    }

    #[tokio::test]
    async fn async_user_input_waits_for_queue_capacity() {
        let (data_tx, mut data_rx) = mpsc::channel(1);
        let (control_tx, _control_rx) = std_mpsc::channel();
        data_tx
            .try_send(PtyIoDataCommand::WriteUserInput(Bytes::from_static(
                b"fill",
            )))
            .expect("fill data queue");
        let (wake, _wake_read_fd) = test_wake_pair();
        let handle = PtyIoActorHandle {
            data_tx,
            control_tx,
            wake,
            user_writes: test_user_write_gate(),
            controls: Arc::new(Mutex::new(SharedPtyControls::default())),
            response_order: Arc::new(Mutex::new(())),
        };

        let write = tokio::spawn(async move {
            handle
                .write_user_input(Bytes::from_static(b"wait-for-capacity"))
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !write.is_finished(),
            "async input should wait for queue capacity"
        );

        assert!(matches!(
            data_rx.recv().await,
            Some(PtyIoDataCommand::WriteUserInput(_))
        ));
        write
            .await
            .expect("write task joins")
            .expect("write succeeds after capacity opens");
        match data_rx.recv().await {
            Some(PtyIoDataCommand::WriteUserInput(bytes)) => {
                assert_eq!(bytes, Bytes::from_static(b"wait-for-capacity"));
            }
            _ => panic!("expected queued user input"),
        }
    }

    #[tokio::test]
    async fn async_user_input_waiting_for_capacity_is_rejected_after_handoff_begins() {
        let (data_tx, mut data_rx) = mpsc::channel(1);
        let (control_tx, control_rx) = std_mpsc::channel();
        data_tx
            .try_send(PtyIoDataCommand::WriteUserInput(Bytes::from_static(
                b"fill",
            )))
            .expect("fill data queue");
        let (wake, _wake_read_fd) = test_wake_pair();
        let handle = PtyIoActorHandle {
            data_tx,
            control_tx,
            wake,
            user_writes: test_user_write_gate(),
            controls: Arc::new(Mutex::new(SharedPtyControls::default())),
            response_order: Arc::new(Mutex::new(())),
        };
        let write_handle = handle.clone();
        let write = tokio::spawn(async move {
            write_handle
                .write_user_input(Bytes::from_static(b"after-handoff-start"))
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let handoff = std::thread::spawn(move || handle.begin_handoff(Duration::from_secs(1)));
        match control_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("handoff control command")
        {
            PtyIoControlCommand::BeginHandoff(reply) => {
                reply.send(Ok(())).expect("handoff waiter alive");
            }
            _ => panic!("expected begin handoff command"),
        }
        handoff
            .join()
            .expect("handoff thread joins")
            .expect("handoff succeeds");
        assert!(matches!(
            data_rx.recv().await,
            Some(PtyIoDataCommand::WriteUserInput(_))
        ));

        let err = write.await.expect("write task joins").expect_err(
            "write waiting for capacity must be rejected after handoff closes the input gate",
        );
        assert_eq!(err.0, Bytes::from_static(b"after-handoff-start"));
        match tokio::time::timeout(Duration::from_millis(50), data_rx.recv()).await {
            Err(_) | Ok(None) => {}
            Ok(Some(_)) => panic!("rejected write must not be queued"),
        }
    }

    #[test]
    fn handoff_control_is_not_blocked_by_full_data_queue() {
        let (data_tx, _data_rx) = mpsc::channel(1);
        let (control_tx, control_rx) = std_mpsc::channel();
        data_tx
            .try_send(PtyIoDataCommand::WriteUserInput(Bytes::from_static(
                b"fill",
            )))
            .expect("fill data queue");
        let (wake, _wake_read_fd) = test_wake_pair();
        let handle = PtyIoActorHandle {
            data_tx,
            control_tx,
            wake,
            user_writes: test_user_write_gate(),
            controls: Arc::new(Mutex::new(SharedPtyControls::default())),
            response_order: Arc::new(Mutex::new(())),
        };

        let handoff = std::thread::spawn(move || handle.begin_handoff(Duration::from_secs(1)));
        match control_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("handoff control command")
        {
            PtyIoControlCommand::BeginHandoff(reply) => {
                reply.send(Ok(())).expect("handoff waiter alive");
            }
            _ => panic!("expected begin handoff command"),
        }

        handoff
            .join()
            .expect("handoff thread joins")
            .expect("handoff succeeds despite full data queue");
    }

    #[test]
    fn begin_handoff_drains_user_writes_already_in_command_queue() {
        let (actor_socket, mut peer) = UnixStream::pair().expect("socket pair");
        actor_socket
            .set_nonblocking(true)
            .expect("actor socket nonblocking");
        peer.set_read_timeout(Some(Duration::from_secs(1)))
            .expect("peer timeout");
        let (data_tx, data_rx) = mpsc::channel(ACTOR_COMMAND_BUFFER);
        let (_control_tx, control_rx) = std_mpsc::channel();
        data_tx
            .try_send(PtyIoDataCommand::WriteUserInput(Bytes::from_static(
                b"queued-before-ack",
            )))
            .expect("queued write");
        let mut runner = PtyIoActorRunner {
            pane_id: 1,
            file: std::fs::File::from(unsafe { OwnedFd::from_raw_fd(actor_socket.into_raw_fd()) }),
            data_rx,
            control_rx,
            state: ActorState::Running,
            pending_writes: VecDeque::new(),
            current_write_offset: 0,
            wake_read_fd: fd::create_wake_pipe().expect("wake pipe").read_fd,
            controls: Arc::new(Mutex::new(SharedPtyControls::default())),
            response_order: Arc::new(Mutex::new(())),
            user_writes: test_user_write_gate(),
            on_read: Box::new(|_| PtyReadResult::empty()),
            on_reader_exit: None,
            poll_observer: None,
        };

        runner.begin_handoff().expect("handoff drains queued write");

        let mut buf = [0u8; 17];
        peer.read_exact(&mut buf)
            .expect("queued write reaches peer before quiesce ack");
        assert_eq!(&buf, b"queued-before-ack");
        assert_eq!(runner.state, ActorState::Quiesced);
    }

    #[test]
    fn release_after_commit_prevents_further_io() {
        let (handle, mut peer, read_rx) = actor_with_socket_pair(false);

        handle.release_after_commit().expect("actor released");
        assert!(handle
            .try_write_user_input(Bytes::from_static(b"blocked"))
            .is_err());

        let _ = peer.write_all(b"ignored");
        assert!(read_rx.recv_timeout(Duration::from_millis(150)).is_err());
    }
}
