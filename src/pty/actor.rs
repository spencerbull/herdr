use std::sync::{mpsc as std_mpsc, Arc};

pub(crate) type GuardedWriteValidator = Arc<dyn Fn() -> bool + Send + Sync + 'static>;

pub(crate) struct OutputBoundaryRequest {
    reply: std_mpsc::Receiver<Result<(), GuardedWriteError>>,
}

impl OutputBoundaryRequest {
    #[cfg(any(unix, test))]
    pub(crate) fn new(reply: std_mpsc::Receiver<Result<(), GuardedWriteError>>) -> Self {
        Self { reply }
    }

    pub(crate) fn poll(&self) -> Result<Option<()>, GuardedWriteError> {
        match self.reply.try_recv() {
            Ok(Ok(())) => Ok(Some(())),
            Ok(Err(error)) => Err(error),
            Err(std_mpsc::TryRecvError::Empty) => Ok(None),
            Err(std_mpsc::TryRecvError::Disconnected) => Err(GuardedWriteError::Closed),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    windows,
    allow(dead_code),
    // Windows withholds guarded capabilities because ConPTY writes cannot be
    // cancelled safely; the Unix actor constructs the detailed failure modes.
)]
pub(crate) enum GuardedWriteError {
    Busy,
    ValidationFailed,
    TimedOut,
    Closed,
    Io(String),
}

impl std::fmt::Display for GuardedWriteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => formatter.write_str("PTY input queue is not empty"),
            Self::ValidationFailed => formatter.write_str("guarded PTY write validation failed"),
            Self::TimedOut => formatter.write_str("guarded PTY write timed out"),
            Self::Closed => formatter.write_str("PTY actor is closed"),
            Self::Io(message) => write!(formatter, "PTY write failed: {message}"),
        }
    }
}

#[cfg(unix)]
mod unix;

#[cfg(unix)]
pub(crate) use unix::*;

#[cfg(windows)]
mod windows {
    use std::io::{Read, Write};
    use std::sync::{mpsc as std_mpsc, Arc, Mutex};
    use std::time::Duration;

    use bytes::Bytes;
    use portable_pty::{MasterPty, PtySize};
    use tokio::sync::mpsc;
    use tracing::{debug, warn};

    use super::{GuardedWriteError, GuardedWriteValidator, OutputBoundaryRequest};

    pub(crate) struct PtyReadResult {
        pub terminal_responses: Vec<Bytes>,
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

    struct PtyResizeRequest {
        resize: PtyResize,
        terminal_responses: Vec<Bytes>,
    }

    pub(crate) struct PtyIoActorConfig {
        pub pane_id: u32,
        pub master: Box<dyn MasterPty + Send>,
        pub initially_quiesced: bool,
        pub on_read: ReadCallback,
        pub on_reader_exit: Option<ReaderExitCallback>,
    }

    enum PtyIoControlCommand {
        Resize(PtyResizeRequest),
        Shutdown,
    }

    enum PtyIoDataCommand {
        WriteUserInput(Bytes),
    }

    enum PtyIoWriteCommand {
        UserInput(Bytes),
        TerminalResponse(Bytes),
    }

    #[derive(Debug)]
    struct UserWriteGate {
        accepting: bool,
        queued_user_writes: usize,
    }

    #[derive(Clone)]
    pub(crate) struct PtyIoActorHandle {
        data_tx: mpsc::Sender<PtyIoDataCommand>,
        control_tx: std_mpsc::Sender<PtyIoControlCommand>,
        write_tx: std_mpsc::Sender<PtyIoWriteCommand>,
        response_order: Arc<Mutex<()>>,
        user_writes: Arc<Mutex<UserWriteGate>>,
    }

    impl PtyIoActorHandle {
        pub(crate) async fn write_user_input(
            &self,
            bytes: Bytes,
        ) -> Result<(), mpsc::error::SendError<Bytes>> {
            {
                let gate = self
                    .user_writes
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if !gate.accepting {
                    return Err(mpsc::error::SendError(bytes));
                }
            }

            let permit = match self.data_tx.reserve().await {
                Ok(permit) => permit,
                Err(_) => return Err(mpsc::error::SendError(bytes)),
            };
            let mut gate = self
                .user_writes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !gate.accepting {
                return Err(mpsc::error::SendError(bytes));
            }
            gate.queued_user_writes = gate.queued_user_writes.saturating_add(1);
            permit.send(PtyIoDataCommand::WriteUserInput(bytes));
            Ok(())
        }

        pub(crate) fn try_write_user_input(
            &self,
            bytes: Bytes,
        ) -> Result<(), mpsc::error::TrySendError<Bytes>> {
            let mut gate = self
                .user_writes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !gate.accepting {
                return Err(mpsc::error::TrySendError::Closed(bytes));
            }
            gate.queued_user_writes = gate.queued_user_writes.saturating_add(1);
            match self
                .data_tx
                .try_send(PtyIoDataCommand::WriteUserInput(bytes))
            {
                Ok(()) => Ok(()),
                Err(mpsc::error::TrySendError::Full(PtyIoDataCommand::WriteUserInput(bytes))) => {
                    gate.queued_user_writes = gate.queued_user_writes.saturating_sub(1);
                    Err(mpsc::error::TrySendError::Full(bytes))
                }
                Err(mpsc::error::TrySendError::Closed(PtyIoDataCommand::WriteUserInput(bytes))) => {
                    gate.queued_user_writes = gate.queued_user_writes.saturating_sub(1);
                    Err(mpsc::error::TrySendError::Closed(bytes))
                }
            }
        }

        pub(crate) fn write_guarded(
            &self,
            bytes: Bytes,
            validate: GuardedWriteValidator,
        ) -> Result<(), GuardedWriteError> {
            let _ = (bytes, validate);
            Err(GuardedWriteError::Io(
                "guarded PTY writes are unavailable on Windows because the synchronous ConPTY writer cannot be cancelled safely"
                    .to_string(),
            ))
        }

        pub(crate) const fn guarded_writes_supported(&self) -> bool {
            false
        }

        pub(crate) fn request_output_boundary(
            &self,
        ) -> Result<OutputBoundaryRequest, GuardedWriteError> {
            Err(GuardedWriteError::Io(
                "guarded PTY output barriers are unavailable on Windows".to_string(),
            ))
        }

        pub(crate) fn write_terminal_response(&self, response: impl FnOnce() -> Option<Bytes>) {
            let _order = self
                .response_order
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(bytes) = response().filter(|bytes| !bytes.is_empty()) {
                let _ = self
                    .write_tx
                    .send(PtyIoWriteCommand::TerminalResponse(bytes));
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
            let _ = self
                .control_tx
                .send(PtyIoControlCommand::Resize(PtyResizeRequest {
                    resize: PtyResize {
                        rows,
                        cols,
                        cell_width_px,
                        cell_height_px,
                    },
                    terminal_responses,
                }));
        }

        pub(crate) fn shutdown(&self) {
            if let Ok(mut gate) = self.user_writes.lock() {
                gate.accepting = false;
            }
            let _ = self.control_tx.send(PtyIoControlCommand::Shutdown);
        }
    }

    pub(crate) struct PtyIoActor;

    impl PtyIoActor {
        pub(crate) fn spawn(config: PtyIoActorConfig) -> std::io::Result<PtyIoActorHandle> {
            let PtyIoActorConfig {
                pane_id,
                master,
                initially_quiesced,
                mut on_read,
                on_reader_exit,
            } = config;

            let mut reader = master
                .try_clone_reader()
                .map_err(|err| std::io::Error::other(err.to_string()))?;
            let mut writer = master
                .take_writer()
                .map_err(|err| std::io::Error::other(err.to_string()))?;
            let (data_tx, mut data_rx) = mpsc::channel::<PtyIoDataCommand>(1024);
            let (control_tx, control_rx) = std_mpsc::channel::<PtyIoControlCommand>();
            let (write_tx, write_rx) = std_mpsc::channel::<PtyIoWriteCommand>();
            let response_order = Arc::new(Mutex::new(()));
            let user_writes = Arc::new(Mutex::new(UserWriteGate {
                accepting: !initially_quiesced,
                queued_user_writes: 0,
            }));

            {
                let user_writes = Arc::clone(&user_writes);
                std::thread::spawn(move || {
                    for command in write_rx {
                        let (bytes, is_user_write) = match command {
                            PtyIoWriteCommand::UserInput(bytes) => (bytes, true),
                            PtyIoWriteCommand::TerminalResponse(bytes) => (bytes, false),
                        };
                        let result = writer.write_all(&bytes).and_then(|()| writer.flush());
                        if is_user_write {
                            finish_user_write(&user_writes);
                        }
                        if result.is_err() {
                            break;
                        }
                    }
                    close_user_write_gate(&user_writes);
                    debug!(pane_id, "windows pty writer thread exiting");
                });
            }

            {
                let write_tx = write_tx.clone();
                let user_writes = Arc::clone(&user_writes);
                std::thread::spawn(move || {
                    while let Some(command) = data_rx.blocking_recv() {
                        let PtyIoDataCommand::WriteUserInput(bytes) = command;
                        if write_tx.send(PtyIoWriteCommand::UserInput(bytes)).is_err() {
                            finish_user_write(&user_writes);
                            break;
                        }
                    }
                    debug!(pane_id, "windows pty input thread exiting");
                });
            }

            {
                let write_tx = write_tx.clone();
                let response_order = Arc::clone(&response_order);
                std::thread::spawn(move || {
                    let mut buf = [0u8; 8192];
                    loop {
                        match reader.read(&mut buf) {
                            Ok(0) => break,
                            Ok(n) => {
                                let _order = response_order
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                                let result = on_read(&buf[..n]);
                                if result.terminal_responses.into_iter().any(|response| {
                                    write_tx
                                        .send(PtyIoWriteCommand::TerminalResponse(response))
                                        .is_err()
                                }) {
                                    break;
                                }
                            }
                            Err(err) => {
                                debug!(pane_id, err = %err, "windows pty reader failed");
                                break;
                            }
                        }
                    }
                    if let Some(on_reader_exit) = on_reader_exit {
                        on_reader_exit();
                    }
                    debug!(pane_id, "windows pty reader thread exiting");
                });
            }

            {
                let write_tx = write_tx.clone();
                std::thread::spawn(move || {
                    for command in control_rx {
                        match command {
                            PtyIoControlCommand::Resize(request) => {
                                let size = request.resize;
                                if let Err(err) = master.resize(PtySize {
                                    rows: size.rows,
                                    cols: size.cols,
                                    pixel_width: size.cell_width_px.min(u16::MAX as u32) as u16,
                                    pixel_height: size.cell_height_px.min(u16::MAX as u32) as u16,
                                }) {
                                    warn!(pane_id, err = %err, "windows pty resize failed");
                                }
                                if request.terminal_responses.into_iter().any(|response| {
                                    write_tx
                                        .send(PtyIoWriteCommand::TerminalResponse(response))
                                        .is_err()
                                }) {
                                    break;
                                }
                            }
                            PtyIoControlCommand::Shutdown => break,
                        }
                    }
                    debug!(pane_id, "windows pty control thread exiting");
                });
            }

            Ok(PtyIoActorHandle {
                data_tx,
                control_tx,
                write_tx,
                response_order,
                user_writes,
            })
        }
    }

    fn finish_user_write(user_writes: &Arc<Mutex<UserWriteGate>>) {
        let mut gate = user_writes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        gate.queued_user_writes = gate.queued_user_writes.saturating_sub(1);
    }

    fn close_user_write_gate(user_writes: &Arc<Mutex<UserWriteGate>>) {
        let mut gate = user_writes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        gate.accepting = false;
        gate.queued_user_writes = 0;
    }

    #[allow(dead_code)]
    fn _assert_duration_send(_: Duration) {}
}

#[cfg(windows)]
pub(crate) use windows::*;
