use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::{
    api::schema::{
        AgentActionCapability, AgentActionEvidence, AgentActionKind, AgentStatus, ErrorBody,
        PaneInfo,
    },
    detect::{manifest::DetectionInput, Agent},
    terminal::TerminalState,
};

use super::App;

const ACTION_CAPABILITY_TTL: Duration = Duration::from_secs(30);
const SNAPSHOT_ATTEMPTS: usize = 3;

pub(crate) struct AgentActionRegistry {
    epoch: [u8; 32],
    inner: Mutex<AgentActionRegistryInner>,
}

#[derive(Default)]
struct AgentActionRegistryInner {
    active: HashMap<String, StoredAgentActionCapability>,
    slots: HashMap<AgentActionSlot, AgentActionSlotState>,
    process_evidence: HashMap<String, AgentActionProcessEvidenceState>,
    boundary_probes: HashMap<String, AgentActionBoundaryProbeState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AgentActionSlot {
    terminal_id: String,
    action: AgentActionKind,
}

#[derive(Debug, Clone)]
struct AgentActionSlotState {
    binding_digest: [u8; 32],
    capability_id: String,
    consumed: bool,
}

#[derive(Debug, Clone, Copy)]
struct AgentActionProcessEvidenceState {
    process_instance: super::agents::AgentProcessInstance,
    pty_output_baseline: u64,
    freshness: AgentActionEvidenceFreshness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentActionEvidenceFreshness {
    AwaitingAbsence,
    AwaitingReturn,
    Ready,
}

enum AgentActionBoundaryProbeState {
    InFlight {
        process_instance: super::agents::AgentProcessInstance,
        request: crate::pty::actor::OutputBoundaryRequest,
    },
    AwaitingQuietOutput {
        process_instance: super::agents::AgentProcessInstance,
        observed_output_seq: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentActionBoundaryProbeStatus {
    Start,
    Pending,
    Ready,
}

#[derive(Clone)]
struct StoredAgentActionCapability {
    public: AgentActionCapability,
    binding: AgentActionBinding,
    binding_digest: [u8; 32],
    expires_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AgentActionBinding {
    action: AgentActionKind,
    terminal_id: String,
    workspace_id: String,
    tab_id: String,
    pane_id: String,
    agent: String,
    expected_agent: Agent,
    agent_status: AgentStatus,
    state_change_seq: u64,
    revision: u64,
    detection_content_seq: u64,
    screen_fingerprint: [u8; 32],
    process_instance: super::agents::AgentProcessInstance,
    evidence: AgentActionEvidenceBinding,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AgentActionEvidenceBinding {
    public: AgentActionEvidence,
    priority: i32,
    region: String,
}

struct DetectionSnapshot {
    screen: String,
    osc_title: String,
    osc_progress: String,
    content_seq: u64,
    pty_output_seq: u64,
    fingerprint: [u8; 32],
}

impl AgentActionRegistry {
    pub(crate) fn new() -> Self {
        let mut epoch = [0_u8; 32];
        fill_secret(&mut epoch);
        Self {
            epoch,
            inner: Mutex::new(AgentActionRegistryInner::default()),
        }
    }

    fn issue(&self, binding: AgentActionBinding) -> Option<AgentActionCapability> {
        let now = Instant::now();
        let binding_digest = binding.digest();
        let slot = AgentActionSlot {
            terminal_id: binding.terminal_id.clone(),
            action: binding.action,
        };
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner.remove_expired(now);

        if let Some(existing_slot) = inner.slots.get(&slot).cloned() {
            if existing_slot.binding_digest == binding_digest {
                if existing_slot.consumed {
                    return None;
                }
                if let Some(existing) = inner.active.get(&existing_slot.capability_id) {
                    return Some(existing.public.clone());
                }
            }
            inner.active.remove(&existing_slot.capability_id);
            inner.slots.remove(&slot);
        }

        let capability_id = loop {
            let mut nonce = [0_u8; 32];
            fill_secret(&mut nonce);
            let mut id_hasher = Sha256::new();
            hash_field(&mut id_hasher, b"herdr-agent-action-capability-v1");
            hash_field(&mut id_hasher, &self.epoch);
            hash_field(&mut id_hasher, &nonce);
            hash_field(&mut id_hasher, &binding_digest);
            let candidate = format!("act_{:x}", id_hasher.finalize());
            if !inner.active.contains_key(&candidate) {
                break candidate;
            }
        };
        let expires_at_unix_ms = SystemTime::now()
            .checked_add(ACTION_CAPABILITY_TTL)
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
            .unwrap_or_default();
        let public = binding.public(capability_id.clone(), expires_at_unix_ms);
        let stored = StoredAgentActionCapability {
            public: public.clone(),
            binding,
            binding_digest,
            expires_at: now + ACTION_CAPABILITY_TTL,
        };
        inner.active.insert(capability_id.clone(), stored);
        inner.slots.insert(
            slot,
            AgentActionSlotState {
                binding_digest,
                capability_id,
                consumed: false,
            },
        );
        Some(public)
    }

    fn take(&self, capability_id: &str) -> Option<StoredAgentActionCapability> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let stored = inner.active.remove(capability_id)?;
        let slot = AgentActionSlot {
            terminal_id: stored.binding.terminal_id.clone(),
            action: stored.binding.action,
        };
        if let Some(slot_state) = inner.slots.get_mut(&slot) {
            if slot_state.capability_id == capability_id {
                slot_state.consumed = true;
            }
        }
        if Instant::now() >= stored.expires_at {
            inner.slots.remove(&slot);
            return None;
        }
        Some(stored)
    }

    fn process_boundary_required(
        &self,
        terminal_id: &str,
        process_instance: super::agents::AgentProcessInstance,
    ) -> bool {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner
            .process_evidence
            .get(terminal_id)
            .is_none_or(|state| state.process_instance != process_instance)
    }

    fn poll_process_boundary_probe(
        &self,
        terminal_id: &str,
        process_instance: super::agents::AgentProcessInstance,
        pty_output_seq: u64,
    ) -> AgentActionBoundaryProbeStatus {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(probe) = inner.boundary_probes.remove(terminal_id) else {
            return AgentActionBoundaryProbeStatus::Start;
        };

        match probe {
            AgentActionBoundaryProbeState::InFlight {
                process_instance: probe_process,
                request,
            } if probe_process == process_instance => match request.poll() {
                Ok(Some(())) => AgentActionBoundaryProbeStatus::Ready,
                Ok(None) => {
                    inner.boundary_probes.insert(
                        terminal_id.to_string(),
                        AgentActionBoundaryProbeState::InFlight {
                            process_instance,
                            request,
                        },
                    );
                    AgentActionBoundaryProbeStatus::Pending
                }
                Err(_) => {
                    inner.boundary_probes.insert(
                        terminal_id.to_string(),
                        AgentActionBoundaryProbeState::AwaitingQuietOutput {
                            process_instance,
                            observed_output_seq: pty_output_seq,
                        },
                    );
                    AgentActionBoundaryProbeStatus::Pending
                }
            },
            AgentActionBoundaryProbeState::AwaitingQuietOutput {
                process_instance: probe_process,
                observed_output_seq,
            } if probe_process == process_instance && observed_output_seq != pty_output_seq => {
                inner.boundary_probes.insert(
                    terminal_id.to_string(),
                    AgentActionBoundaryProbeState::AwaitingQuietOutput {
                        process_instance,
                        observed_output_seq: pty_output_seq,
                    },
                );
                AgentActionBoundaryProbeStatus::Pending
            }
            AgentActionBoundaryProbeState::AwaitingQuietOutput {
                process_instance: probe_process,
                observed_output_seq,
            } if probe_process == process_instance && observed_output_seq == pty_output_seq => {
                AgentActionBoundaryProbeStatus::Start
            }
            _ => AgentActionBoundaryProbeStatus::Start,
        }
    }

    fn start_process_boundary_probe(
        &self,
        terminal_id: &str,
        process_instance: super::agents::AgentProcessInstance,
        request: crate::pty::actor::OutputBoundaryRequest,
    ) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner.boundary_probes.insert(
            terminal_id.to_string(),
            AgentActionBoundaryProbeState::InFlight {
                process_instance,
                request,
            },
        );
    }

    fn defer_process_boundary_probe(
        &self,
        terminal_id: &str,
        process_instance: super::agents::AgentProcessInstance,
        pty_output_seq: u64,
    ) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner.boundary_probes.insert(
            terminal_id.to_string(),
            AgentActionBoundaryProbeState::AwaitingQuietOutput {
                process_instance,
                observed_output_seq: pty_output_seq,
            },
        );
    }

    fn record_process_boundary(
        &self,
        terminal_id: &str,
        process_instance: super::agents::AgentProcessInstance,
        pty_output_seq: u64,
        trusted_evidence_present: bool,
    ) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner.boundary_probes.remove(terminal_id);
        inner.process_evidence.insert(
            terminal_id.to_string(),
            AgentActionProcessEvidenceState {
                process_instance,
                pty_output_baseline: pty_output_seq,
                freshness: if trusted_evidence_present {
                    AgentActionEvidenceFreshness::AwaitingAbsence
                } else {
                    AgentActionEvidenceFreshness::AwaitingReturn
                },
            },
        );
    }

    fn process_evidence_is_fresh(
        &self,
        terminal_id: &str,
        process_instance: super::agents::AgentProcessInstance,
        pty_output_seq: u64,
        trusted_evidence_present: bool,
    ) -> bool {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(state) = inner.process_evidence.get_mut(terminal_id) else {
            return false;
        };
        if state.process_instance != process_instance {
            return false;
        }

        match state.freshness {
            AgentActionEvidenceFreshness::AwaitingAbsence => {
                if !trusted_evidence_present && state.pty_output_baseline != pty_output_seq {
                    state.freshness = AgentActionEvidenceFreshness::AwaitingReturn;
                }
                state.pty_output_baseline = pty_output_seq;
                false
            }
            AgentActionEvidenceFreshness::AwaitingReturn => {
                let evidence_advanced = state.pty_output_baseline != pty_output_seq;
                state.pty_output_baseline = pty_output_seq;
                if trusted_evidence_present && evidence_advanced {
                    state.freshness = AgentActionEvidenceFreshness::Ready;
                    true
                } else {
                    false
                }
            }
            AgentActionEvidenceFreshness::Ready => {
                state.pty_output_baseline = pty_output_seq;
                trusted_evidence_present
            }
        }
    }

    pub(crate) fn remove_terminal(&self, terminal_id: &str) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner
            .active
            .retain(|_, stored| stored.binding.terminal_id != terminal_id);
        inner
            .slots
            .retain(|slot, _| slot.terminal_id != terminal_id);
        inner.process_evidence.remove(terminal_id);
        inner.boundary_probes.remove(terminal_id);
    }

    #[cfg(test)]
    pub(crate) fn expire_for_test(&self, capability_id: &str) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(stored) = inner.active.get_mut(capability_id) {
            stored.expires_at = Instant::now() - Duration::from_millis(1);
        }
    }

    #[cfg(test)]
    pub(crate) fn entry_counts_for_test(&self) -> (usize, usize) {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (inner.active.len(), inner.slots.len())
    }

    #[cfg(test)]
    pub(crate) fn process_evidence_count_for_test(&self) -> usize {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner.process_evidence.len()
    }
}

impl AgentActionRegistryInner {
    fn remove_expired(&mut self, now: Instant) {
        let expired: Vec<_> = self
            .active
            .iter()
            .filter(|(_, stored)| now >= stored.expires_at)
            .map(|(id, stored)| {
                (
                    id.clone(),
                    AgentActionSlot {
                        terminal_id: stored.binding.terminal_id.clone(),
                        action: stored.binding.action,
                    },
                )
            })
            .collect();
        for (id, slot) in expired {
            self.active.remove(&id);
            if self
                .slots
                .get(&slot)
                .is_some_and(|state| state.capability_id == id)
            {
                self.slots.remove(&slot);
            }
        }
    }
}

impl AgentActionBinding {
    fn digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hash_field(&mut hasher, b"herdr-agent-action-binding-v1");
        hash_field(&mut hasher, action_label(self.action).as_bytes());
        hash_field(&mut hasher, self.terminal_id.as_bytes());
        hash_field(&mut hasher, self.workspace_id.as_bytes());
        hash_field(&mut hasher, self.tab_id.as_bytes());
        hash_field(&mut hasher, self.pane_id.as_bytes());
        hash_field(&mut hasher, self.agent.as_bytes());
        hash_field(
            &mut hasher,
            crate::detect::agent_label(self.expected_agent).as_bytes(),
        );
        hash_field(&mut hasher, status_label(self.agent_status).as_bytes());
        hash_field(&mut hasher, &self.state_change_seq.to_be_bytes());
        hash_field(&mut hasher, &self.revision.to_be_bytes());
        hash_field(&mut hasher, &self.detection_content_seq.to_be_bytes());
        hash_field(&mut hasher, &self.screen_fingerprint);
        hash_field(&mut hasher, &self.process_instance.pid.to_be_bytes());
        hash_field(
            &mut hasher,
            &self.process_instance.start_identity.to_be_bytes(),
        );
        hash_field(
            &mut hasher,
            &self
                .process_instance
                .process_group_id
                .unwrap_or_default()
                .to_be_bytes(),
        );
        hash_field(&mut hasher, self.evidence.public.manifest_source.as_bytes());
        hash_field(
            &mut hasher,
            self.evidence.public.manifest_version.as_bytes(),
        );
        hash_field(&mut hasher, self.evidence.public.rule_id.as_bytes());
        hash_field(&mut hasher, &self.evidence.priority.to_be_bytes());
        hash_field(&mut hasher, self.evidence.region.as_bytes());
        hasher.finalize().into()
    }

    fn public(&self, capability_id: String, expires_at_unix_ms: u64) -> AgentActionCapability {
        AgentActionCapability {
            capability_id,
            action: self.action,
            terminal_id: self.terminal_id.clone(),
            workspace_id: self.workspace_id.clone(),
            tab_id: self.tab_id.clone(),
            pane_id: self.pane_id.clone(),
            agent: self.agent.clone(),
            agent_status: self.agent_status,
            state_change_seq: self.state_change_seq,
            revision: self.revision,
            expires_at_unix_ms,
            evidence: self.evidence.public.clone(),
        }
    }
}

impl App {
    pub(super) fn agent_action_capabilities(
        &self,
        ws_idx: usize,
        pane_id: crate::layout::PaneId,
        terminal: &TerminalState,
        pane: &PaneInfo,
    ) -> Vec<AgentActionCapability> {
        self.current_agent_action_binding(ws_idx, pane_id, terminal, pane)
            .and_then(|binding| self.agent_action_registry.issue(binding))
            .into_iter()
            .collect()
    }

    pub(super) fn perform_agent_action(
        &self,
        capability_id: &str,
    ) -> Result<AgentActionCapability, ErrorBody> {
        let stored = self
            .agent_action_registry
            .take(capability_id)
            .ok_or_else(|| ErrorBody {
                code: "agent_action_capability_not_found".into(),
                message: "agent action capability is unknown, expired, consumed, or from another server session"
                    .into(),
            })?;

        let resolved = self
            .resolve_agent_target(&stored.binding.pane_id)
            .map_err(|_| stale_capability_error())?;
        let current_pane_id = self
            .public_pane_id(resolved.ws_idx, resolved.pane_id)
            .ok_or_else(stale_capability_error)?;
        let current_workspace_id = self.public_workspace_id(resolved.ws_idx);
        let current_tab_id = self
            .public_tab_id(resolved.ws_idx, resolved.tab_idx)
            .ok_or_else(stale_capability_error)?;
        if current_pane_id != stored.binding.pane_id
            || current_workspace_id != stored.binding.workspace_id
            || current_tab_id != stored.binding.tab_id
            || resolved.terminal_id != stored.binding.terminal_id
        {
            return Err(stale_capability_error());
        }

        let terminal_id = self
            .state
            .workspaces
            .get(resolved.ws_idx)
            .and_then(|workspace| workspace.terminal_id(resolved.pane_id))
            .ok_or_else(stale_capability_error)?;
        let terminal = self
            .state
            .terminals
            .get(terminal_id)
            .ok_or_else(stale_capability_error)?;
        let pane = self
            .pane_info(resolved.ws_idx, resolved.pane_id)
            .ok_or_else(stale_capability_error)?;
        let current = self
            .current_agent_action_binding(resolved.ws_idx, resolved.pane_id, terminal, &pane)
            .ok_or_else(stale_capability_error)?;
        if current != stored.binding || current.digest() != stored.binding_digest {
            return Err(stale_capability_error());
        }

        let runtime = self
            .lookup_runtime_sender(resolved.ws_idx, resolved.pane_id)
            .ok_or_else(stale_capability_error)?;
        let writer_snapshot =
            capture_detection_snapshot(runtime).ok_or_else(stale_capability_error)?;
        if writer_snapshot.content_seq != stored.binding.detection_content_seq
            || writer_snapshot.fingerprint != stored.binding.screen_fingerprint
        {
            return Err(stale_capability_error());
        }
        let bytes = guarded_action_bytes(stored.binding.action);

        let detection_validator = runtime.guarded_detection_validator(
            stored.binding.expected_agent,
            writer_snapshot.content_seq,
            writer_snapshot.screen,
            writer_snapshot.osc_title,
            writer_snapshot.osc_progress,
        );
        let process_validator = super::agents::runtime_agent_process_validator(
            runtime,
            stored.binding.expected_agent,
            stored.binding.process_instance,
        );
        let expires_at = stored.expires_at;
        let writer_validator: crate::pty::actor::GuardedWriteValidator =
            std::sync::Arc::new(move || {
                Instant::now() < expires_at && process_validator() && detection_validator()
            });

        runtime
            .write_guarded(bytes, writer_validator)
            .map_err(|err| match err {
                crate::pty::actor::GuardedWriteError::ValidationFailed => stale_capability_error(),
                _ => ErrorBody {
                    code: "agent_action_failed".into(),
                    message: format!(
                        "guarded agent action could not be written and will not be retried: {err}"
                    ),
                },
            })?;
        Ok(stored.public)
    }

    fn current_agent_action_binding(
        &self,
        ws_idx: usize,
        pane_id: crate::layout::PaneId,
        terminal: &TerminalState,
        pane: &PaneInfo,
    ) -> Option<AgentActionBinding> {
        if terminal.full_lifecycle_hook_authority_active()
            || terminal.managed_agent_launch_pending()
        {
            return None;
        }
        if pane.agent_status != AgentStatus::Working {
            return None;
        }
        let expected_agent = terminal.effective_known_agent()?;
        let runtime = self.lookup_runtime_sender(ws_idx, pane_id)?;
        if !runtime.guarded_writes_supported() {
            return None;
        }
        let process_instance =
            super::agents::runtime_agent_process_instance(runtime, expected_agent)?;
        if self
            .agent_action_registry
            .process_boundary_required(&pane.terminal_id, process_instance)
        {
            let pre_boundary_snapshot = capture_detection_snapshot(runtime)?;
            let mut probe_status = self.agent_action_registry.poll_process_boundary_probe(
                &pane.terminal_id,
                process_instance,
                pre_boundary_snapshot.pty_output_seq,
            );
            if probe_status == AgentActionBoundaryProbeStatus::Start {
                match runtime.request_output_boundary() {
                    Ok(request) => {
                        self.agent_action_registry.start_process_boundary_probe(
                            &pane.terminal_id,
                            process_instance,
                            request,
                        );
                        probe_status = self.agent_action_registry.poll_process_boundary_probe(
                            &pane.terminal_id,
                            process_instance,
                            pre_boundary_snapshot.pty_output_seq,
                        );
                    }
                    Err(_) => {
                        self.agent_action_registry.defer_process_boundary_probe(
                            &pane.terminal_id,
                            process_instance,
                            pre_boundary_snapshot.pty_output_seq,
                        );
                        return None;
                    }
                }
            }
            if probe_status != AgentActionBoundaryProbeStatus::Ready {
                return None;
            }
            if super::agents::runtime_agent_process_instance(runtime, expected_agent)
                != Some(process_instance)
            {
                return None;
            }
            let boundary_snapshot = capture_detection_snapshot(runtime)?;
            let boundary_evidence = crate::detect::manifest::trusted_interrupt_evidence(
                expected_agent,
                DetectionInput {
                    screen: &boundary_snapshot.screen,
                    osc_title: &boundary_snapshot.osc_title,
                    osc_progress: &boundary_snapshot.osc_progress,
                },
            );
            self.agent_action_registry.record_process_boundary(
                &pane.terminal_id,
                process_instance,
                boundary_snapshot.pty_output_seq,
                boundary_evidence.is_some(),
            );
            return None;
        }
        let snapshot = capture_detection_snapshot(runtime)?;
        let evidence = crate::detect::manifest::trusted_interrupt_evidence(
            expected_agent,
            DetectionInput {
                screen: &snapshot.screen,
                osc_title: &snapshot.osc_title,
                osc_progress: &snapshot.osc_progress,
            },
        );
        if !self.agent_action_registry.process_evidence_is_fresh(
            &pane.terminal_id,
            process_instance,
            snapshot.pty_output_seq,
            evidence.is_some(),
        ) {
            return None;
        }
        let evidence = evidence?;
        // Approval remains intentionally unavailable until a current agent UI
        // has an action-specific prompt rule verified from a live screen.
        Some(AgentActionBinding {
            action: AgentActionKind::Interrupt,
            terminal_id: pane.terminal_id.clone(),
            workspace_id: pane.workspace_id.clone(),
            tab_id: pane.tab_id.clone(),
            pane_id: pane.pane_id.clone(),
            agent: crate::detect::agent_label(expected_agent).to_string(),
            expected_agent,
            agent_status: pane.agent_status,
            state_change_seq: terminal.last_agent_state_change_seq.unwrap_or(0),
            revision: pane.revision,
            detection_content_seq: snapshot.content_seq,
            screen_fingerprint: snapshot.fingerprint,
            process_instance,
            evidence: AgentActionEvidenceBinding {
                public: AgentActionEvidence {
                    manifest_source: "bundled".into(),
                    manifest_version: evidence.manifest_version,
                    rule_id: evidence.rule_id,
                },
                priority: evidence.priority,
                region: evidence.region,
            },
        })
    }
}

fn capture_detection_snapshot(
    runtime: &crate::terminal::TerminalRuntime,
) -> Option<DetectionSnapshot> {
    for _ in 0..SNAPSHOT_ATTEMPTS {
        let content_before = runtime.detection_content_seq();
        let output_before = runtime.pty_output_seq();
        let screen = runtime.detection_text();
        let osc_title = runtime.agent_osc_title();
        let osc_progress = runtime.agent_osc_progress();
        let output_after = runtime.pty_output_seq();
        let content_after = runtime.detection_content_seq();
        if content_before == content_after && output_before == output_after {
            let fingerprint = snapshot_fingerprint(&screen, &osc_title, &osc_progress);
            return Some(DetectionSnapshot {
                screen,
                osc_title,
                osc_progress,
                content_seq: content_after,
                pty_output_seq: output_after,
                fingerprint,
            });
        }
    }
    None
}

fn snapshot_fingerprint(screen: &str, osc_title: &str, osc_progress: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, b"herdr-agent-action-screen-v1");
    hash_field(&mut hasher, screen.as_bytes());
    hash_field(&mut hasher, osc_title.as_bytes());
    hash_field(&mut hasher, osc_progress.as_bytes());
    hasher.finalize().into()
}

fn hash_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn fill_secret(bytes: &mut [u8]) {
    if let Err(err) = getrandom::fill(bytes) {
        panic!("operating system random source unavailable for agent action capability: {err}");
    }
}

fn action_label(action: AgentActionKind) -> &'static str {
    match action {
        AgentActionKind::Approve => "approve",
        AgentActionKind::Interrupt => "interrupt",
    }
}

fn guarded_action_bytes(action: AgentActionKind) -> Bytes {
    match action {
        AgentActionKind::Approve => Bytes::from_static(b"\r"),
        AgentActionKind::Interrupt => Bytes::from_static(b"\x1b"),
    }
}

fn status_label(status: AgentStatus) -> &'static str {
    match status {
        AgentStatus::Idle => "idle",
        AgentStatus::Working => "working",
        AgentStatus::Blocked => "blocked",
        AgentStatus::Done => "done",
        AgentStatus::Unknown => "unknown",
    }
}

fn stale_capability_error() -> ErrorBody {
    ErrorBody {
        code: "agent_action_capability_stale".into(),
        message: "agent action capability no longer matches the live agent, pane, state, or screen"
            .into(),
    }
}

#[cfg(test)]
mod tests {
    use super::{guarded_action_bytes, AgentActionKind, AgentActionRegistry};

    #[test]
    fn guarded_action_bytes_are_single_byte_and_terminal_protocol_independent() {
        assert_eq!(
            guarded_action_bytes(AgentActionKind::Approve),
            bytes::Bytes::from_static(b"\r")
        );
        assert_eq!(
            guarded_action_bytes(AgentActionKind::Interrupt),
            bytes::Bytes::from_static(b"\x1b")
        );
    }

    #[test]
    fn resize_only_screen_changes_cannot_advance_process_evidence_freshness() {
        let registry = AgentActionRegistry::new();
        let process = super::super::agents::AgentProcessInstance {
            pid: 7,
            start_identity: 11,
            process_group_id: Some(7),
        };
        registry.record_process_boundary("terminal", process, 20, true);

        // Resize/reflow can hide and restore matched cells, but it does not
        // advance the PTY-output generation.
        assert!(!registry.process_evidence_is_fresh("terminal", process, 20, false));
        assert!(!registry.process_evidence_is_fresh("terminal", process, 20, true));

        // Real PTY output may establish absence. A resize-only return still
        // cannot make the inherited evidence fresh.
        assert!(!registry.process_evidence_is_fresh("terminal", process, 21, false));
        assert!(!registry.process_evidence_is_fresh("terminal", process, 21, true));
        assert!(registry.process_evidence_is_fresh("terminal", process, 22, true));
    }
}
