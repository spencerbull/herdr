use std::time::Duration;

use bytes::Bytes;

use crate::api::schema::{
    AgentPerformActionParams, AgentPromptParams, AgentRenameParams, AgentSendKeysParams,
    AgentStartParams, AgentTarget, PaneReadResult, ResponseResult,
};
use crate::app::App;

use super::responses::{encode_error, encode_error_body, encode_success};

const AGENT_PROMPT_SUBMIT_DELAY: Duration = Duration::from_millis(300);

impl App {
    pub(super) fn handle_agent_list(&mut self, id: String) -> String {
        encode_success(
            id,
            ResponseResult::AgentList {
                agents: self.collect_agent_infos(),
            },
        )
    }

    pub(super) fn handle_agent_get(&mut self, id: String, target: AgentTarget) -> String {
        self.reconcile_managed_agent_target(&target.target);
        let agent = match self.agent_info_for_target(&target.target) {
            Ok(agent) => agent,
            Err(err) => return encode_error_body(id, self.agent_target_error_body(err)),
        };

        encode_success(id, ResponseResult::AgentInfo { agent })
    }

    pub(super) fn handle_agent_focus(&mut self, id: String, target: AgentTarget) -> String {
        let agent = match self.focus_agent_target(&target.target) {
            Ok(agent) => agent,
            Err(err) => return encode_error_body(id, self.agent_target_error_body(err)),
        };

        encode_success(id, ResponseResult::AgentInfo { agent })
    }

    pub(super) fn handle_agent_rename(&mut self, id: String, params: AgentRenameParams) -> String {
        let agent = match self.rename_agent_target(&params.target, params.name) {
            Ok(agent) => agent,
            Err(err) => return encode_error_body(id, self.agent_rename_error_body(err)),
        };

        encode_success(id, ResponseResult::AgentInfo { agent })
    }

    pub(super) fn handle_agent_start(&mut self, id: String, params: AgentStartParams) -> String {
        let (agent, argv) = match self.start_agent(params) {
            Ok(started) => started,
            Err(err) => return encode_error_body(id, self.agent_start_error_body(err)),
        };

        encode_success(id, ResponseResult::AgentStarted { agent, argv })
    }

    pub(super) fn handle_agent_prompt(&mut self, id: String, params: AgentPromptParams) -> String {
        if params.text.is_empty() {
            return encode_error(id, "empty_agent_prompt", "agent prompt must not be empty");
        }
        let resolved = match self.resolve_agent_target(&params.target) {
            Ok(resolved) => resolved,
            Err(err) => return encode_error_body(id, self.agent_target_error_body(err)),
        };
        let Some(terminal_id) = self
            .state
            .workspaces
            .get(resolved.ws_idx)
            .and_then(|workspace| workspace.terminal_id(resolved.pane_id))
            .cloned()
        else {
            return agent_not_found(id, &params.target);
        };
        let Some(terminal) = self.state.terminals.get(&terminal_id) else {
            return agent_not_found(id, &params.target);
        };
        if terminal.state == crate::detect::AgentState::Blocked {
            return encode_error(
                id,
                "agent_blocked",
                format!(
                    "agent {} is blocked and requires interactive input",
                    params.target
                ),
            );
        }
        let Some(expected_agent) = terminal.effective_known_agent() else {
            return agent_not_ready(id, &params.target);
        };
        if terminal.managed_agent_launch_pending() {
            return agent_not_ready(id, &params.target);
        }
        let Some(runtime) = self.lookup_runtime_sender(resolved.ws_idx, resolved.pane_id) else {
            return agent_not_found(id, &params.target);
        };
        if !super::super::agents::runtime_hosts_agent(runtime, expected_agent) {
            return encode_error(
                id,
                "agent_not_ready",
                format!(
                    "agent {} is no longer the pane foreground process",
                    params.target
                ),
            );
        }
        if expected_agent == crate::detect::Agent::GithubCopilot {
            // Copilot ignores synthetic Enter after focus loss until it receives focus gained.
            let focus = match crate::ghostty::encode_focus(crate::ghostty::FocusEvent::Gained) {
                Ok(focus) => focus,
                Err(err) => return encode_error(id, "agent_prompt_failed", err.to_string()),
            };
            if let Err(err) = runtime.try_send_bytes(Bytes::from(focus)) {
                return encode_error(id, "agent_prompt_failed", err.to_string());
            }
        }
        let (text, enter) =
            crate::app::api_helpers::encode_api_submission_parts(runtime, &params.text);
        if let Err(err) = runtime.try_send_bytes(Bytes::from(text)) {
            return encode_error(id, "agent_prompt_failed", err.to_string());
        }
        runtime.send_bytes_after(Bytes::from(enter), AGENT_PROMPT_SUBMIT_DELAY);
        let Some(agent) = self.agent_info(resolved.ws_idx, resolved.pane_id) else {
            return agent_not_found(id, &params.target);
        };
        encode_success(id, ResponseResult::AgentPrompted { agent })
    }

    pub(super) fn handle_agent_read(
        &mut self,
        id: String,
        params: crate::api::schema::AgentReadParams,
    ) -> String {
        let resolved = match self.resolve_agent_target(&params.target) {
            Ok(resolved) => resolved,
            Err(err) => return encode_error_body(id, self.agent_target_error_body(err)),
        };
        let Some((pane, workspace_id)) = self.lookup_runtime(resolved.ws_idx, resolved.pane_id)
        else {
            return agent_not_found(id, &params.target);
        };
        let snapshot = crate::app::api_helpers::read_terminal_snapshot(
            pane,
            params.source,
            params.format,
            params.lines,
        );

        encode_success(
            id,
            ResponseResult::PaneRead {
                read: PaneReadResult {
                    pane_id: self
                        .public_pane_id(resolved.ws_idx, resolved.pane_id)
                        .unwrap_or_else(|| params.target.clone()),
                    workspace_id,
                    tab_id: self
                        .public_tab_id(resolved.ws_idx, resolved.tab_idx)
                        .unwrap(),
                    source: params.source,
                    format: params.format,
                    text: snapshot.text,
                    revision: 0,
                    truncated: snapshot.truncated,
                },
            },
        )
    }

    pub(super) fn handle_agent_explain(&mut self, id: String, target: AgentTarget) -> String {
        let resolved = match self.resolve_agent_target(&target.target) {
            Ok(resolved) => resolved,
            Err(err) => return encode_error_body(id, self.agent_target_error_body(err)),
        };
        let Some((pane, _workspace_id)) = self.lookup_runtime(resolved.ws_idx, resolved.pane_id)
        else {
            return agent_not_found(id, &target.target);
        };
        let Some(terminal_id) = self
            .state
            .workspaces
            .get(resolved.ws_idx)
            .and_then(|workspace| workspace.terminal_id(resolved.pane_id))
        else {
            return agent_not_found(id, &target.target);
        };
        let Some(terminal) = self.state.terminals.get(terminal_id) else {
            return agent_not_found(id, &target.target);
        };
        if terminal.full_lifecycle_hook_authority_active() {
            let explain = serde_json::json!({
                "agent": terminal.effective_agent_label().unwrap_or("unknown"),
                "state": crate::detect::manifest::agent_state_label(terminal.state),
                "manifest_source": null,
                "manifest_version": null,
                "cached_remote_version": null,
                "local_override_shadowing_remote": false,
                "remote_update_status": null,
                "remote_update_error": null,
                "matched_rule": null,
                "visible_idle": false,
                "visible_blocker": false,
                "visible_working": false,
                "screen_detection_skipped": true,
                "screen_detection_skip_reason": "full_lifecycle_hook_authority",
                "skip_state_update": false,
                "skipped_update_reason": null,
                "fallback_reason": null,
                "warning": null,
                "evaluated_rules": [],
            });
            return encode_success(id, ResponseResult::AgentExplain { explain });
        }
        let Some(agent) = terminal.effective_known_agent().or(terminal.detected_agent) else {
            return encode_error(
                id,
                "agent_explain_unavailable",
                format!(
                    "agent target {} does not have a detected agent label",
                    target.target
                ),
            );
        };

        let screen = pane.detection_text();
        let osc_title = pane.agent_osc_title();
        let osc_progress = pane.agent_osc_progress();
        let explain = crate::detect::manifest::explain_with_input(
            agent,
            crate::detect::manifest::DetectionInput {
                screen: &screen,
                osc_title: &osc_title,
                osc_progress: &osc_progress,
            },
        );
        let value = crate::detect::manifest::explain_to_json_value(&explain);

        encode_success(id, ResponseResult::AgentExplain { explain: value })
    }

    pub(super) fn handle_agent_send_keys(
        &mut self,
        id: String,
        params: AgentSendKeysParams,
    ) -> String {
        let resolved = match self.resolve_agent_target(&params.target) {
            Ok(resolved) => resolved,
            Err(err) => return encode_error_body(id, self.agent_target_error_body(err)),
        };
        let Some(terminal_id) = self
            .state
            .workspaces
            .get(resolved.ws_idx)
            .and_then(|workspace| workspace.terminal_id(resolved.pane_id))
        else {
            return agent_not_found(id, &params.target);
        };
        let Some(expected_agent) = self
            .state
            .terminals
            .get(terminal_id)
            .and_then(|terminal| terminal.effective_known_agent())
        else {
            return agent_not_ready(id, &params.target);
        };
        let Some(runtime) = self.lookup_runtime_sender(resolved.ws_idx, resolved.pane_id) else {
            return agent_not_found(id, &params.target);
        };
        if !super::super::agents::runtime_hosts_agent(runtime, expected_agent) {
            return agent_not_ready(id, &params.target);
        }
        let encoded = match super::super::api_helpers::encode_api_keys(runtime, &params.keys) {
            Ok(encoded) => encoded,
            Err(key) => {
                return encode_error(id, "invalid_key", format!("unsupported key {key}"));
            }
        };
        let bytes: Vec<u8> = encoded.into_iter().flatten().collect();
        if let Err(err) = runtime.try_send_bytes(Bytes::from(bytes)) {
            return encode_error(id, "agent_send_keys_failed", err.to_string());
        }

        encode_success(id, ResponseResult::Ok {})
    }

    pub(super) fn handle_agent_perform_action(
        &mut self,
        id: String,
        params: AgentPerformActionParams,
    ) -> String {
        let capability = match self.perform_agent_action(&params.capability_id) {
            Ok(capability) => capability,
            Err(error) => return encode_error_body(id, error),
        };
        encode_success(
            id,
            ResponseResult::AgentActionPerformed {
                action: capability.action,
                terminal_id: capability.terminal_id,
                pane_id: capability.pane_id,
            },
        )
    }
}

fn agent_not_ready(id: String, target: &str) -> String {
    encode_error(
        id,
        "agent_not_ready",
        format!("agent {target} is not an active named agent"),
    )
}

fn agent_not_found(id: String, target: &str) -> String {
    encode_error(
        id,
        "agent_not_found",
        format!("agent target {target} not found"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::schema::{
            AgentActionCapability, AgentActionKind, AgentStatus, ErrorResponse, SuccessResponse,
        },
        app::Mode,
        config::Config,
        detect::{Agent, AgentState},
        workspace::Workspace,
    };

    const CODEX_INTERRUPT_SCREEN: &[u8] =
        b"\xe2\x80\xa2 Working (4s \xe2\x80\xa2 esc to interrupt)\n";

    fn app_with_agent() -> App {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &Config::default(),
            true,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        app.state.workspaces = vec![Workspace::test_new("agent")];
        app.state.ensure_test_terminals();
        app.state.active = Some(0);
        app.state.selected = 0;
        app.state.mode = Mode::Terminal;
        app
    }

    fn app_with_codex_action_screen(
        state: AgentState,
        screen: &[u8],
    ) -> (
        App,
        crate::layout::PaneId,
        tokio::sync::mpsc::Receiver<Bytes>,
    ) {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_agent_name("reviewer".into());
        terminal.set_detected_state(Some(Agent::Codex), state);
        terminal.last_agent_state_change_seq = Some(7);
        let (runtime, rx) =
            crate::terminal::TerminalRuntime::test_with_channel_and_scrollback_bytes(
                100, 24, 0, screen, 4,
            );
        app.state.insert_test_runtime(pane_id, runtime);
        (app, pane_id, rx)
    }

    fn only_action(
        app: &App,
        pane_id: crate::layout::PaneId,
        action: AgentActionKind,
    ) -> AgentActionCapability {
        let info = app.agent_info(0, pane_id).unwrap();
        assert_eq!(info.actions.len(), 1);
        assert_eq!(info.actions[0].action, action);
        info.actions[0].clone()
    }

    #[tokio::test]
    async fn blocked_prompts_do_not_expose_approval_actions() {
        for screen in [
            b"continue? [y/n]\n".as_slice(),
            CODEX_INTERRUPT_SCREEN,
            b"Allow command?\n\
$ cargo test\n\
\xe2\x80\xba 1. Yes, proceed (y)\n\
Press enter to confirm or esc to cancel\n"
                .as_slice(),
            b"Would you like to run the following command?\n\
$ cargo test\n\
\xe2\x80\xba Yes, just this once\n\
No, and tell Codex what to do differently\n"
                .as_slice(),
        ] {
            let (app, pane_id, _rx) = app_with_codex_action_screen(AgentState::Blocked, screen);
            assert!(app.agent_info(0, pane_id).unwrap().actions.is_empty());
        }

        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0]
            .terminal_id(pane_id)
            .unwrap()
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_hook_authority(
            "herdr:omp".into(),
            "omp".into(),
            AgentState::Working,
            None,
            Some(1),
        );
        assert!(terminal.full_lifecycle_hook_authority_active());
        let (runtime, _rx) =
            crate::terminal::TerminalRuntime::test_with_channel_and_scrollback_bytes(
                100,
                24,
                0,
                CODEX_INTERRUPT_SCREEN,
                4,
            );
        app.state.insert_test_runtime(pane_id, runtime);
        assert!(app.agent_info(0, pane_id).unwrap().actions.is_empty());
    }

    #[tokio::test]
    async fn guarded_interrupt_requires_visible_chrome_sends_escape_once_and_rejects_replay() {
        let (mut app, pane_id, mut rx) =
            app_with_codex_action_screen(AgentState::Working, CODEX_INTERRUPT_SCREEN);
        let capability = only_action(&app, pane_id, AgentActionKind::Interrupt);
        assert_eq!(capability.evidence.rule_id, "screen_working_fallback");

        let response = app.handle_agent_perform_action(
            "interrupt".into(),
            AgentPerformActionParams {
                capability_id: capability.capability_id.clone(),
            },
        );
        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        assert!(matches!(
            success.result,
            ResponseResult::AgentActionPerformed {
                action: AgentActionKind::Interrupt,
                ..
            }
        ));
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"\x1b"));
        assert!(rx.try_recv().is_err());
        assert!(app.agent_info(0, pane_id).unwrap().actions.is_empty());

        let replay = app.handle_agent_perform_action(
            "interrupt-replay".into(),
            AgentPerformActionParams {
                capability_id: capability.capability_id,
            },
        );
        let error: ErrorResponse = serde_json::from_str(&replay).unwrap();
        assert_eq!(error.error.code, "agent_action_capability_not_found");
        assert!(rx.try_recv().is_err());

        let (app, pane_id, _rx) =
            app_with_codex_action_screen(AgentState::Working, b"still working\n");
        assert!(app.agent_info(0, pane_id).unwrap().actions.is_empty());
    }

    #[tokio::test]
    async fn same_kind_process_replacement_rejects_bound_interrupt() {
        let (mut app, pane_id, mut rx) =
            app_with_codex_action_screen(AgentState::Working, CODEX_INTERRUPT_SCREEN);
        let capability = only_action(&app, pane_id, AgentActionKind::Interrupt);
        app.lookup_runtime_sender(0, pane_id)
            .unwrap()
            .test_replace_agent_process_instance();

        let response = app.handle_agent_perform_action(
            "same-kind-replacement".into(),
            AgentPerformActionParams {
                capability_id: capability.capability_id,
            },
        );
        let error: ErrorResponse = serde_json::from_str(&response).unwrap();
        assert_eq!(error.error.code, "agent_action_capability_stale");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn changed_screen_state_sequence_revision_and_pane_identity_reject_old_actions() {
        let (mut app, pane_id, mut rx) =
            app_with_codex_action_screen(AgentState::Working, CODEX_INTERRUPT_SCREEN);
        let capability = only_action(&app, pane_id, AgentActionKind::Interrupt);
        app.lookup_runtime_sender(0, pane_id)
            .unwrap()
            .test_process_pty_bytes(b"\nchanged");
        let response = app.handle_agent_perform_action(
            "screen-changed".into(),
            AgentPerformActionParams {
                capability_id: capability.capability_id,
            },
        );
        let error: ErrorResponse = serde_json::from_str(&response).unwrap();
        assert_eq!(error.error.code, "agent_action_capability_stale");
        assert!(rx.try_recv().is_err());

        let (mut app, pane_id, mut rx) =
            app_with_codex_action_screen(AgentState::Working, CODEX_INTERRUPT_SCREEN);
        let capability = only_action(&app, pane_id, AgentActionKind::Interrupt);
        let terminal_id = app.state.workspaces[0]
            .terminal_id(pane_id)
            .unwrap()
            .clone();
        app.state
            .terminals
            .get_mut(&terminal_id)
            .unwrap()
            .last_agent_state_change_seq = Some(8);
        let response = app.handle_agent_perform_action(
            "sequence-changed".into(),
            AgentPerformActionParams {
                capability_id: capability.capability_id,
            },
        );
        let error: ErrorResponse = serde_json::from_str(&response).unwrap();
        assert_eq!(error.error.code, "agent_action_capability_stale");
        assert!(rx.try_recv().is_err());

        let (mut app, pane_id, mut rx) =
            app_with_codex_action_screen(AgentState::Working, CODEX_INTERRUPT_SCREEN);
        let capability = only_action(&app, pane_id, AgentActionKind::Interrupt);
        let terminal_id = app.state.workspaces[0]
            .terminal_id(pane_id)
            .unwrap()
            .clone();
        app.state.terminals.get_mut(&terminal_id).unwrap().revision += 1;
        let response = app.handle_agent_perform_action(
            "revision-changed".into(),
            AgentPerformActionParams {
                capability_id: capability.capability_id,
            },
        );
        let error: ErrorResponse = serde_json::from_str(&response).unwrap();
        assert_eq!(error.error.code, "agent_action_capability_stale");
        assert!(rx.try_recv().is_err());

        let (mut app, pane_id, mut rx) =
            app_with_codex_action_screen(AgentState::Working, CODEX_INTERRUPT_SCREEN);
        let capability = only_action(&app, pane_id, AgentActionKind::Interrupt);
        app.state.workspaces[0]
            .public_pane_numbers
            .insert(pane_id, 2);
        let response = app.handle_agent_perform_action(
            "pane-moved".into(),
            AgentPerformActionParams {
                capability_id: capability.capability_id,
            },
        );
        let error: ErrorResponse = serde_json::from_str(&response).unwrap();
        assert_eq!(error.error.code, "agent_action_capability_stale");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn unavailable_capabilities_and_failed_send_are_not_retried() {
        let (mut app, pane_id, mut rx) =
            app_with_codex_action_screen(AgentState::Working, CODEX_INTERRUPT_SCREEN);
        let capability = only_action(&app, pane_id, AgentActionKind::Interrupt);
        app.agent_action_registry
            .expire_for_test(&capability.capability_id);
        let response = app.handle_agent_perform_action(
            "expired".into(),
            AgentPerformActionParams {
                capability_id: capability.capability_id,
            },
        );
        let error: ErrorResponse = serde_json::from_str(&response).unwrap();
        assert_eq!(error.error.code, "agent_action_capability_not_found");
        assert!(rx.try_recv().is_err());

        let (mut app, pane_id, mut rx) =
            app_with_codex_action_screen(AgentState::Working, CODEX_INTERRUPT_SCREEN);
        let capability = only_action(&app, pane_id, AgentActionKind::Interrupt);
        app.agent_action_registry = crate::app::agent_actions::AgentActionRegistry::new();
        let response = app.handle_agent_perform_action(
            "old-epoch".into(),
            AgentPerformActionParams {
                capability_id: capability.capability_id,
            },
        );
        let error: ErrorResponse = serde_json::from_str(&response).unwrap();
        assert_eq!(error.error.code, "agent_action_capability_not_found");
        assert!(rx.try_recv().is_err());

        let (mut app, pane_id, rx) =
            app_with_codex_action_screen(AgentState::Working, CODEX_INTERRUPT_SCREEN);
        let capability = only_action(&app, pane_id, AgentActionKind::Interrupt);
        drop(rx);
        let response = app.handle_agent_perform_action(
            "closed-channel".into(),
            AgentPerformActionParams {
                capability_id: capability.capability_id.clone(),
            },
        );
        let error: ErrorResponse = serde_json::from_str(&response).unwrap();
        assert_eq!(error.error.code, "agent_action_failed");
        let replay = app.handle_agent_perform_action(
            "closed-channel-replay".into(),
            AgentPerformActionParams {
                capability_id: capability.capability_id,
            },
        );
        let error: ErrorResponse = serde_json::from_str(&replay).unwrap();
        assert_eq!(error.error.code, "agent_action_capability_not_found");
        assert!(app.agent_info(0, pane_id).unwrap().actions.is_empty());
    }

    #[tokio::test]
    async fn expired_capabilities_are_not_found_before_or_after_listing_cleanup() {
        let (mut app, pane_id, mut rx) =
            app_with_codex_action_screen(AgentState::Working, CODEX_INTERRUPT_SCREEN);
        let capability = only_action(&app, pane_id, AgentActionKind::Interrupt);
        app.agent_action_registry
            .expire_for_test(&capability.capability_id);
        assert_eq!(app.agent_action_registry.entry_counts_for_test(), (1, 1));
        assert_eq!(app.agent_info(0, pane_id).unwrap().actions.len(), 1);
        let response = app.handle_agent_perform_action(
            "expired-after-list".into(),
            AgentPerformActionParams {
                capability_id: capability.capability_id,
            },
        );
        let error: ErrorResponse = serde_json::from_str(&response).unwrap();
        assert_eq!(error.error.code, "agent_action_capability_not_found");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn consumed_tombstone_is_reclaimed_when_terminal_disappears() {
        let (mut app, pane_id, mut rx) =
            app_with_codex_action_screen(AgentState::Working, CODEX_INTERRUPT_SCREEN);
        let terminal_id = app.state.workspaces[0]
            .terminal_id(pane_id)
            .unwrap()
            .clone();
        let capability = only_action(&app, pane_id, AgentActionKind::Interrupt);
        let response = app.handle_agent_perform_action(
            "consume-before-close".into(),
            AgentPerformActionParams {
                capability_id: capability.capability_id,
            },
        );
        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        assert!(matches!(
            success.result,
            ResponseResult::AgentActionPerformed { .. }
        ));
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"\x1b"));
        assert_eq!(app.agent_action_registry.entry_counts_for_test(), (0, 1));

        app.state.terminals.remove(&terminal_id);
        app.state.terminal_runtime_shutdowns.push(terminal_id);
        app.shutdown_detached_terminal_runtimes();

        assert_eq!(app.agent_action_registry.entry_counts_for_test(), (0, 0));
    }

    #[tokio::test]
    async fn agent_prompt_accepts_pane_ids_and_working_agents_atomically() {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_agent_name("reviewer".into());
        terminal.set_detected_state(Some(Agent::OpenCode), AgentState::Working);
        let (runtime, mut rx) =
            crate::terminal::TerminalRuntime::test_with_channel_and_scrollback_bytes(
                80, 24, 0, b"", 1,
            );
        runtime.test_process_pty_bytes(b"\x1b[?2004h");
        app.state.insert_test_runtime(pane_id, runtime);

        let public_pane_id = app.public_pane_id(0, pane_id).unwrap();
        let bracketed_started = std::time::Instant::now();
        let response = app.handle_agent_prompt(
            "req".into(),
            AgentPromptParams {
                target: public_pane_id,
                text: "A != B".into(),
                wait: None,
            },
        );
        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        let ResponseResult::AgentPrompted { agent, .. } = success.result else {
            panic!("expected prompted response");
        };
        assert_eq!(agent.name.as_deref(), Some("reviewer"));
        assert_eq!(
            rx.try_recv().unwrap(),
            Bytes::from_static(b"\x1b[200~A != B\x1b[201~")
        );
        assert!(rx.try_recv().is_err());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from_static(b"\r")
        );
        assert!(bracketed_started.elapsed() >= AGENT_PROMPT_SUBMIT_DELAY);

        app.lookup_runtime_sender(0, pane_id)
            .unwrap()
            .test_process_pty_bytes(b"\x1b[?2004l");
        let raw_started = std::time::Instant::now();
        let raw = app.handle_agent_prompt(
            "req-raw".into(),
            AgentPromptParams {
                target: "reviewer".into(),
                text: "A != B".into(),
                wait: None,
            },
        );
        let raw: SuccessResponse = serde_json::from_str(&raw).unwrap();
        assert!(matches!(raw.result, ResponseResult::AgentPrompted { .. }));
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"A != B"));
        assert!(rx.try_recv().is_err());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from_static(b"\r")
        );
        assert!(raw_started.elapsed() >= AGENT_PROMPT_SUBMIT_DELAY);

        let rejected = app.handle_agent_prompt(
            "req-label".into(),
            AgentPromptParams {
                target: "opencode".into(),
                text: "wrong target".into(),
                wait: None,
            },
        );
        let error: crate::api::schema::ErrorResponse = serde_json::from_str(&rejected).unwrap();
        assert_eq!(error.error.code, "agent_not_found");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn agent_prompt_rejects_blocked_agent_without_writing() {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_agent_name("reviewer".into());
        terminal.set_detected_state(Some(Agent::GithubCopilot), AgentState::Blocked);
        let (runtime, mut rx) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
        app.state.insert_test_runtime(pane_id, runtime);

        let response = app.handle_agent_prompt(
            "req".into(),
            AgentPromptParams {
                target: "reviewer".into(),
                text: "unrelated prompt".into(),
                wait: None,
            },
        );

        let error: crate::api::schema::ErrorResponse = serde_json::from_str(&response).unwrap();
        assert_eq!(error.error.code, "agent_blocked");
        assert!(
            tokio::time::timeout(
                AGENT_PROMPT_SUBMIT_DELAY + Duration::from_millis(100),
                rx.recv()
            )
            .await
            .is_err(),
            "blocked prompt wrote or scheduled terminal input"
        );
    }

    #[tokio::test]
    async fn agent_prompt_focuses_copilot_before_submitting() {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_agent_name("reviewer".into());
        terminal.set_detected_state(Some(Agent::GithubCopilot), AgentState::Idle);
        let (runtime, mut rx) =
            crate::terminal::TerminalRuntime::test_with_channel_and_scrollback_bytes(
                80, 24, 0, b"", 3,
            );
        runtime.test_process_pty_bytes(b"\x1b[?2004h");
        app.state.insert_test_runtime(pane_id, runtime);

        let response = app.handle_agent_prompt(
            "req".into(),
            AgentPromptParams {
                target: "reviewer".into(),
                text: "A != B".into(),
                wait: None,
            },
        );
        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        assert!(matches!(
            success.result,
            ResponseResult::AgentPrompted { .. }
        ));
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"\x1b[I"));
        assert_eq!(
            rx.try_recv().unwrap(),
            Bytes::from_static(b"\x1b[200~A != B\x1b[201~")
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from_static(b"\r")
        );
    }

    #[tokio::test]
    async fn agent_send_keys_validates_every_key_before_writing() {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_agent_name("reviewer".into());
        terminal.set_detected_state(Some(Agent::Pi), AgentState::Idle);
        let (runtime, mut rx) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
        app.state.insert_test_runtime(pane_id, runtime);

        let rejected = app.handle_agent_send_keys(
            "req-invalid".into(),
            AgentSendKeysParams {
                target: "reviewer".into(),
                keys: vec!["enter".into(), "not-a-key".into()],
            },
        );
        let error: crate::api::schema::ErrorResponse = serde_json::from_str(&rejected).unwrap();
        assert_eq!(error.error.code, "invalid_key");
        assert!(rx.try_recv().is_err());

        let sent = app.handle_agent_send_keys(
            "req-valid".into(),
            AgentSendKeysParams {
                target: "reviewer".into(),
                keys: vec!["up".into(), "enter".into()],
            },
        );
        let success: SuccessResponse = serde_json::from_str(&sent).unwrap();
        assert!(matches!(success.result, ResponseResult::Ok {}));
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"\x1b[A\r"));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn agent_prompt_rejects_managed_agent_while_startup_is_pending() {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        let now = std::time::Instant::now();
        terminal.begin_managed_agent(
            "reviewer".into(),
            Agent::OpenCode,
            now,
            std::time::Duration::from_secs(3),
            std::time::Duration::from_secs(10),
        );
        terminal.set_detected_state(Some(Agent::OpenCode), AgentState::Idle);
        let (runtime, mut rx) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
        app.state.insert_test_runtime(pane_id, runtime);

        let response = app.handle_agent_prompt(
            "req-pending".into(),
            AgentPromptParams {
                target: "reviewer".into(),
                text: "A != B".into(),
                wait: None,
            },
        );
        let error: crate::api::schema::ErrorResponse = serde_json::from_str(&response).unwrap();
        assert_eq!(error.error.code, "agent_not_ready");
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn agent_focus_marks_already_focused_done_agent_seen() {
        let mut app = app_with_agent();
        app.state.outer_terminal_focus = Some(false);

        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        app.state
            .terminals
            .get_mut(&terminal_id)
            .unwrap()
            .set_detected_state(Some(Agent::Pi), AgentState::Idle);
        app.state.workspaces[0].tabs[0]
            .panes
            .get_mut(&pane_id)
            .unwrap()
            .seen = false;
        app.state.workspaces[0].tabs[0].layout.focus_pane(pane_id);

        let response = app.handle_agent_focus(
            "req".into(),
            AgentTarget {
                target: app.public_pane_id(0, pane_id).unwrap(),
            },
        );

        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        let ResponseResult::AgentInfo { agent } = success.result else {
            panic!("expected agent info response");
        };
        assert_eq!(agent.agent_status, AgentStatus::Idle);
    }

    #[test]
    fn agent_rename_does_not_replace_the_pane_label() {
        let mut app = app_with_agent();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_manual_label("shell-pane".into());
        terminal.set_detected_state(Some(Agent::Pi), AgentState::Idle);
        let target = app.public_pane_id(0, pane_id).unwrap();

        for name in [Some("reviewer".to_string()), None] {
            let response = app.handle_agent_rename(
                "req".into(),
                AgentRenameParams {
                    target: target.clone(),
                    name,
                },
            );
            let success: SuccessResponse = serde_json::from_str(&response).unwrap();
            assert!(matches!(success.result, ResponseResult::AgentInfo { .. }));
            assert_eq!(
                app.state.terminals[&terminal_id].manual_label.as_deref(),
                Some("shell-pane")
            );
        }
    }
}
