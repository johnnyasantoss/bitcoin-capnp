use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use bitcoin_ipc::{BitcoinIpc, BlockCreateOptions, BlockWaitOptions, MonitorClient, TipChange};
use crossterm::event::{Event as CEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use futures::StreamExt;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Paragraph, Wrap};
use ratatui::Frame;
use ratatui::Terminal;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const MAX_MESSAGES: usize = 100;

enum Event {
    Key(KeyEvent),
    Render,
    Connected(MonitorClient),
    ConnectionFailed(String),
    TipChange(TipChange),
    ActionResult(String),
    ActionError(String),
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum InputMode {
    Normal,
    Version,
    Timestamp,
    Nonce,
    Coinbase,
}

#[derive(Clone, Debug, PartialEq)]
enum ConnectionState {
    Connecting,
    Connected,
    Failed(String),
}

struct AppState {
    connection_state: ConnectionState,
    monitor: Option<MonitorClient>,
    is_test_chain: Option<bool>,
    is_ibd: Option<bool>,
    tip_height: Option<u64>,
    tip_hash: Option<Vec<u8>>,
    tip_changed_at: Option<Instant>,
    block_header: Option<Vec<u8>>,
    block_size: Option<usize>,
    tx_fees: Option<Vec<i64>>,
    tx_sigops: Option<Vec<i64>>,
    coinbase_commitment: Option<Vec<u8>>,
    witness_commitment_index: Option<i32>,
    merkle_path_count: Option<usize>,
    input_mode: InputMode,
    version_buf: String,
    timestamp_buf: String,
    nonce_buf: String,
    coinbase_buf: String,
    messages: Vec<String>,
    is_waiting: bool,
    wait_started_at: Option<Instant>,
    socket_path: String,
}

impl AppState {
    fn new(path: &str) -> Self {
        Self {
            connection_state: ConnectionState::Connecting,
            monitor: None,
            is_test_chain: None,
            is_ibd: None,
            tip_height: None,
            tip_hash: None,
            tip_changed_at: None,
            block_header: None,
            block_size: None,
            tx_fees: None,
            tx_sigops: None,
            coinbase_commitment: None,
            witness_commitment_index: None,
            merkle_path_count: None,
            input_mode: InputMode::Normal,
            version_buf: String::new(),
            timestamp_buf: String::new(),
            nonce_buf: String::new(),
            coinbase_buf: String::new(),
            messages: Vec::new(),
            is_waiting: false,
            wait_started_at: None,
            socket_path: path.to_string(),
        }
    }

    fn push_message(&mut self, msg: String) {
        self.messages.push(msg);
        if self.messages.len() > MAX_MESSAGES {
            self.messages.remove(0);
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <bitcoin_unix_socket_path>", args[0]);
        std::process::exit(1);
    }

    let socket = args[1].clone();
    let cancel = CancellationToken::new();

    tokio::spawn({
        let c = cancel.clone();
        async move {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to listen for Ctrl+C signal");
            c.cancel();
        }
    });

    run_app(Path::new(&socket), cancel).await;
}

async fn run_app(path: &Path, cancel: CancellationToken) {
    let mut terminal = init_terminal().expect("terminal init");
    let _guard = TerminalGuard;

    terminal
        .draw(|f| {
            let area = f.area();
            let text = Line::from(Span::styled(
                format!(" bitcoin-ipc TUI  ({}x{}) ", area.width, area.height),
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ));
            f.render_widget(Paragraph::new(text), Rect::new(0, 0, area.width, 1));
        })
        .expect("test draw failed");

    let ipc = BitcoinIpc::new(path);
    let mut state = AppState::new(path.to_str().unwrap_or("unknown"));
    state.push_message("Connecting to Bitcoin Core...".into());

    let (tui_tx, mut tui_rx) = mpsc::unbounded_channel::<Event>();
    spawn_keyboard_task(cancel.clone(), tui_tx.clone());
    spawn_render_task(cancel.clone(), tui_tx.clone());

    let _ = terminal.draw(|f| draw(f, &state));

    // Spawn connection as a background task so the TUI stays responsive.
    let ipc_clone = ipc.clone();
    let tui_tx_clone = tui_tx.clone();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        let timeout = CancellationToken::new();
        let timeout_c = timeout.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            timeout_c.cancel();
        });

        let result = tokio::select! {
            _ = cancel_clone.cancelled() => return,
            _ = timeout.cancelled() => {
                Err("Connection timed out after 10s".to_string())
            }
            r = ipc_clone.mining.start_monitoring(1, 1) => {
                r.map_err(|e| format!("Connection failed: {e}"))
            }
        };

        match result {
            Ok(monitor) => {
                let _ = tui_tx_clone.send(Event::Connected(monitor));
            }
            Err(e) => {
                let _ = tui_tx_clone.send(Event::ConnectionFailed(e));
            }
        }
    });

    main_loop(
        &mut terminal,
        &mut tui_rx,
        &ipc,
        &mut state,
        &cancel,
        &tui_tx,
    )
    .await;
}

async fn fetch_template(state: &mut AppState) {
    if let Some(ref monitor) = state.monitor {
        state.block_header = monitor.get_block_header().await.ok();
        state.block_size = monitor.get_block().await.ok().map(|b| b.len());
        state.tx_fees = monitor.get_tx_fees().await.ok();
        state.tx_sigops = monitor.get_tx_sigops().await.ok();
        state.coinbase_commitment = monitor.get_coinbase_commitment().await.ok();
        state.witness_commitment_index = monitor.get_witness_commitment_index().await.ok();
        state.merkle_path_count = monitor
            .get_coinbase_merkle_path()
            .await
            .ok()
            .map(|p| p.len());
    }
}

async fn main_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    tui_rx: &mut mpsc::UnboundedReceiver<Event>,
    ipc: &BitcoinIpc,
    state: &mut AppState,
    cancel: &CancellationToken,
    tui_tx: &mpsc::UnboundedSender<Event>,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            event = tui_rx.recv() => {
                match event {
                    Some(Event::Key(key)) => {
                        handle_key(key, state, ipc, cancel, tui_tx).await;
                    }
                    Some(Event::Render) => {
                        let _ = terminal.draw(|f| draw(f, state));
                    }
                    Some(Event::Connected(monitor)) => {
                        state.connection_state = ConnectionState::Connected;
                        state.monitor = Some(monitor.clone());
                        state.push_message("Connected".into());

                        // Fetch node info and template in background.
                        let ipc_info = ipc.clone();
                        let tx = tui_tx.clone();
                        let c = cancel.clone();
                        tokio::spawn(async move {
                            if c.is_cancelled() { return; }
                            let mut msgs: Vec<String> = Vec::new();
                            if let Ok(v) = ipc_info.mining.is_test_chain().await {
                                msgs.push(format!("is_test_chain: {v}"));
                            }
                            if let Ok(v) = ipc_info.mining.is_initial_block_download().await {
                                msgs.push(format!("is_ibd: {v}"));
                            }
                            if let Ok(Some(tip)) = ipc_info.mining.get_tip().await {
                                msgs.push(format!("tip: {} ({} bytes)", tip.height, tip.hash.len()));
                            }
                            for msg in msgs {
                                let _ = tx.send(Event::ActionResult(msg));
                            }
                        });

                        // Subscribe to tip changes.
                        let mut tip_rx = monitor.subscribe_tip_changes();
                        let tui_tip_tx = tui_tx.clone();
                        let cancel_tip = cancel.clone();
                        tokio::spawn(async move {
                            loop {
                                tokio::select! {
                                    _ = cancel_tip.cancelled() => break,
                                    result = tip_rx.recv() => {
                                        match result {
                                            Ok(tip) => { let _ = tui_tip_tx.send(Event::TipChange(tip)); }
                                            Err(_) => break,
                                        }
                                    }
                                }
                            }
                        });

                        // Fetch initial template.
                        fetch_template(state).await;
                    }
                    Some(Event::ConnectionFailed(e)) => {
                        state.connection_state = ConnectionState::Failed(e.clone());
                        state.push_message(format!("Connection failed: {e}"));
                    }
                    Some(Event::TipChange(tip)) => {
                        state.tip_height = Some(tip.height);
                        state.tip_hash = Some(tip.hash);
                        state.tip_changed_at = Some(Instant::now());
                    }
                    Some(Event::ActionResult(msg)) => {
                        state.is_waiting = false;
                        if msg.contains("tip:") {
                            // Parse node info messages to update state fields.
                            // Simpler: refetch template on any action result that looks like a template update.
                        }
                        state.push_message(msg);
                    }
                    Some(Event::ActionError(e)) => {
                        state.is_waiting = false;
                        state.push_message(format!("error: {e}"));
                    }
                    None => break,
                }
            }
        }
    }
}

fn is_quit_key(key: &KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char('q'))
        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
        || key.code == KeyCode::Esc
}

async fn handle_key(
    key: KeyEvent,
    state: &mut AppState,
    ipc: &BitcoinIpc,
    cancel: &CancellationToken,
    tui_tx: &mpsc::UnboundedSender<Event>,
) {
    if is_quit_key(&key) {
        cancel.cancel();
        return;
    }

    if state.input_mode != InputMode::Normal {
        handle_input_mode(key, state, ipc, cancel, tui_tx).await;
        return;
    }

    if state.connection_state != ConnectionState::Connected {
        return;
    }

    match key.code {
        KeyCode::Char('r') => {
            state.push_message("Refreshing template...".into());
            let ipc_c = ipc.clone();
            let tx = tui_tx.clone();
            let c = cancel.clone();
            tokio::spawn(async move {
                let timeout = CancellationToken::new();
                let timeout_c = timeout.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(15)).await;
                    timeout_c.cancel();
                });
                let opts = BlockCreateOptions {
                    use_mempool: true,
                    block_reserved_weight: 0,
                    coinbase_output_max_additional_sigops: 0,
                };
                let result = tokio::select! {
                    _ = c.cancelled() => return,
                    _ = timeout.cancelled() => Err("refresh timed out".to_string()),
                    r = ipc_c.mining.create_new_block(&opts) => {
                        r.map_err(|e| format!("refresh error: {e}"))
                    }
                };
                match result {
                    Ok(()) => {
                        let _ = tx.send(Event::ActionResult("Template refreshed".to_string()));
                    }
                    Err(e) => {
                        let _ = tx.send(Event::ActionError(e));
                    }
                }
            });
        }
        KeyCode::Char('e') => {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let ipc_c = ipc.clone();
            let tx = tui_tx.clone();
            let c = cancel.clone();
            tokio::spawn(async move {
                let msg = format!("ping @ {ts}");
                let timeout = CancellationToken::new();
                let timeout_c = timeout.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    timeout_c.cancel();
                });
                let result = tokio::select! {
                    _ = c.cancelled() => return,
                    _ = timeout.cancelled() => Err("echo timed out".to_string()),
                    r = ipc_c.echo.echo(&msg, timeout.clone()) => {
                        r.map_err(|e| format!("echo error: {e}"))
                    }
                };
                match result {
                    Ok(reply) => {
                        let _ = tx.send(Event::ActionResult(format!("echo: {reply}")));
                    }
                    Err(e) => {
                        let _ = tx.send(Event::ActionError(e));
                    }
                }
            });
        }
        KeyCode::Char('w') => {
            state.is_waiting = true;
            state.wait_started_at = Some(Instant::now());
            state.push_message("Waiting for next template...".into());
            let mon = match &state.monitor {
                Some(m) => m.clone(),
                None => return,
            };
            let tx = tui_tx.clone();
            let c = cancel.clone();
            tokio::spawn(async move {
                let timeout = CancellationToken::new();
                let timeout_c = timeout.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(125)).await;
                    timeout_c.cancel();
                });
                let opts = BlockWaitOptions {
                    timeout: 120.0,
                    fee_threshold: 0,
                };
                let result = tokio::select! {
                    _ = c.cancelled() => return,
                    _ = timeout.cancelled() => Err("wait_next timed out".to_string()),
                    r = mon.wait_next(Some(&opts)) => {
                        r.map_err(|e| format!("wait_next error: {e}"))
                    }
                };
                match result {
                    Ok(()) => {
                        let _ = tx.send(Event::ActionResult(
                            "Template updated via wait_next".to_string(),
                        ));
                    }
                    Err(e) => {
                        let _ = tx.send(Event::ActionError(e));
                    }
                }
            });
        }
        KeyCode::Char('s') => {
            state.input_mode = InputMode::Version;
            state.version_buf.clear();
            state.timestamp_buf.clear();
            state.nonce_buf.clear();
            state.coinbase_buf.clear();
            state.push_message("Enter solution: version (dec)".into());
        }
        KeyCode::Char('d') => {
            let ipc_c = ipc.clone();
            let tx = tui_tx.clone();
            let c = cancel.clone();
            tokio::spawn(async move {
                let timeout = CancellationToken::new();
                let timeout_c = timeout.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    timeout_c.cancel();
                });
                let result = tokio::select! {
                    _ = c.cancelled() => return,
                    _ = timeout.cancelled() => Err("destroy timed out".to_string()),
                    r = ipc_c.echo.destroy() => {
                        r.map_err(|e| format!("destroy error: {e}"))
                    }
                };
                match result {
                    Ok(()) => {
                        let _ = tx.send(Event::ActionResult("Echo destroyed".to_string()));
                    }
                    Err(e) => {
                        let _ = tx.send(Event::ActionError(e));
                    }
                }
            });
        }
        _ => {}
    }
}

async fn handle_input_mode(
    key: KeyEvent,
    state: &mut AppState,
    ipc: &BitcoinIpc,
    cancel: &CancellationToken,
    tui_tx: &mpsc::UnboundedSender<Event>,
) {
    match key.code {
        KeyCode::Esc => {
            state.input_mode = InputMode::Normal;
            state.push_message("Input cancelled".into());
        }
        KeyCode::Enter => {
            advance_input(state, ipc, cancel, tui_tx).await;
        }
        KeyCode::Backspace => {
            let buf = current_buffer_mut(state);
            buf.pop();
        }
        KeyCode::Char(c) => {
            if state.input_mode == InputMode::Coinbase {
                if c.is_ascii_hexdigit() {
                    current_buffer_mut(state).push(c);
                }
            } else if c.is_ascii_digit() {
                current_buffer_mut(state).push(c);
            }
        }
        _ => {}
    }
}

fn current_buffer_mut(state: &mut AppState) -> &mut String {
    match state.input_mode {
        InputMode::Version => &mut state.version_buf,
        InputMode::Timestamp => &mut state.timestamp_buf,
        InputMode::Nonce => &mut state.nonce_buf,
        InputMode::Coinbase => &mut state.coinbase_buf,
        InputMode::Normal => unreachable!(),
    }
}

async fn advance_input(
    state: &mut AppState,
    _ipc: &BitcoinIpc,
    _cancel: &CancellationToken,
    _tui_tx: &mpsc::UnboundedSender<Event>,
) {
    match state.input_mode {
        InputMode::Version => {
            state.input_mode = InputMode::Timestamp;
            state.push_message("Enter timestamp (dec)".into());
        }
        InputMode::Timestamp => {
            state.input_mode = InputMode::Nonce;
            state.push_message("Enter nonce (dec)".into());
        }
        InputMode::Nonce => {
            state.input_mode = InputMode::Coinbase;
            state.push_message("Enter coinbase (hex, no 0x)".into());
        }
        InputMode::Coinbase => {
            let version = state.version_buf.parse::<u32>().unwrap_or(0);
            let timestamp = state.timestamp_buf.parse::<u32>().unwrap_or(0);
            let nonce = state.nonce_buf.parse::<u32>().unwrap_or(0);
            let coinbase = decode_hex(&state.coinbase_buf).unwrap_or_default();

            state.input_mode = InputMode::Normal;
            state.push_message(format!(
                "Submitting: version={version} ts={timestamp} nonce={nonce} coinbase={}",
                state.coinbase_buf
            ));
            if let Some(ref monitor) = state.monitor {
                match monitor
                    .submit_solution(version, timestamp, nonce, &coinbase)
                    .await
                {
                    Ok(accepted) => {
                        state.push_message(format!("Solution submitted — accepted: {accepted}"));
                    }
                    Err(e) => state.push_message(format!("submit error: {e}")),
                }
            }
        }
        InputMode::Normal => unreachable!(),
    }
}

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 || s.is_empty() {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

fn spawn_keyboard_task(cancel: CancellationToken, tx: mpsc::UnboundedSender<Event>) {
    tokio::spawn(async move {
        let mut reader = crossterm::event::EventStream::new();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                event = reader.next() => {
                    match event {
                        Some(Ok(CEvent::Key(key))) if key.kind == KeyEventKind::Press => {
                            let _ = tx.send(Event::Key(key));
                        }
                        _ => {}
                    }
                }
            }
        }
    });
}

fn spawn_render_task(cancel: CancellationToken, tx: mpsc::UnboundedSender<Event>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(33));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {
                    let _ = tx.send(Event::Render);
                }
            }
        }
    });
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }
}

fn init_terminal() -> io::Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    Terminal::new(CrosstermBackend::new(stdout))
}

fn draw(f: &mut Frame, state: &AppState) {
    let [title_area, mid_area, template_area, bottom_area, msg_area, legend_area, status_area] =
        Layout::vertical([
            Constraint::Length(1),
            Constraint::Length(5),
            Constraint::Min(0),
            Constraint::Length(5),
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .areas(f.area());

    let [node_area, tip_area] =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(mid_area);

    let [summary_area, actions_area] =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(bottom_area);

    render_title(f, title_area, state);
    render_node_info(f, node_area, state);
    render_chain_tip(f, tip_area, state);
    render_template(f, template_area, state);
    render_summary(f, summary_area, state);
    render_actions(f, actions_area, state);
    render_messages(f, msg_area, state);
    render_legend(f, legend_area);
    render_status(f, status_area, state);
}

fn render_title(f: &mut Frame, area: Rect, state: &AppState) {
    let text = Line::from(vec![
        Span::styled(
            " Bitcoin IPC Monitor ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("— {}", state.socket_path),
            Style::default().fg(Color::White),
        ),
        Span::raw("  "),
        Span::styled(
            "[q]uit",
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::DIM),
        ),
    ]);
    f.render_widget(Paragraph::new(text), area);
}

fn render_node_info(f: &mut Frame, area: Rect, state: &AppState) {
    let block = Block::bordered().title("NODE");
    let test_chain = match state.is_test_chain {
        Some(true) => Span::styled("yes", Style::default().fg(Color::Green)),
        Some(false) => Span::styled("no", Style::default().fg(Color::Red)),
        None => Span::styled("—", Style::default().fg(Color::DarkGray)),
    };
    let ibd = match state.is_ibd {
        Some(true) => Span::styled("yes", Style::default().fg(Color::Red)),
        Some(false) => Span::styled("no", Style::default().fg(Color::Green)),
        None => Span::styled("—", Style::default().fg(Color::DarkGray)),
    };
    let text = Text::from(vec![
        Line::from(vec![Span::raw("  test chain: "), test_chain]),
        Line::from(vec![Span::raw("  IBD:        "), ibd]),
    ]);
    f.render_widget(Paragraph::new(text).block(block), area);
}

fn render_chain_tip(f: &mut Frame, area: Rect, state: &AppState) {
    let block = Block::bordered().title("TIP");
    let height = match state.tip_height {
        Some(h) => Span::styled(format!("{h}"), Style::default().fg(Color::White)),
        None => Span::styled("—", Style::default().fg(Color::DarkGray)),
    };
    let hash = match &state.tip_hash {
        Some(h) if h.len() >= 8 => {
            let short: String = h[..4]
                .iter()
                .chain(h.iter().rev().take(4).rev())
                .map(|b| format!("{b:02x}"))
                .collect();
            Span::styled(
                format!("{}...{}", &short[..8], &short[short.len() - 8..]),
                Style::default().fg(Color::White),
            )
        }
        Some(h) => Span::styled(format!("{:02x?}", h), Style::default().fg(Color::White)),
        None => Span::styled("—", Style::default().fg(Color::DarkGray)),
    };
    let changed = match state.tip_changed_at {
        Some(t) => {
            let secs = t.elapsed().as_secs();
            if secs < 60 {
                Span::styled(format!("{secs}s ago"), Style::default().fg(Color::Green))
            } else {
                Span::styled(format!("{secs}s ago"), Style::default().fg(Color::Yellow))
            }
        }
        None => Span::styled("never", Style::default().fg(Color::DarkGray)),
    };
    let text = Text::from(vec![
        Line::from(vec![Span::raw("  height: "), height]),
        Line::from(vec![Span::raw("  hash:   "), hash]),
        Line::from(vec![Span::raw("  since:  "), changed]),
    ]);
    f.render_widget(Paragraph::new(text).block(block), area);
}

fn render_template(f: &mut Frame, area: Rect, state: &AppState) {
    let block = Block::bordered().title("TEMPLATE");

    let inner = if let Some(header) = &state.block_header {
        let mut text = render_hexdump(header);
        text.lines.push(Line::from(Span::raw("")));

        if let Some(commitment) = &state.coinbase_commitment {
            let hex: String = commitment.iter().map(|b| format!("{b:02x}")).collect();
            text.lines.push(Line::from(vec![
                Span::styled("coinbase commitment: ", Style::default().fg(Color::Cyan)),
                Span::styled(hex, Style::default().fg(Color::White)),
            ]));
        }
        let ws = state
            .witness_commitment_index
            .map(|i| format!("{i}"))
            .unwrap_or_else(|| "—".into());
        text.lines.push(Line::from(vec![
            Span::styled("witness idx:        ", Style::default().fg(Color::Cyan)),
            Span::styled(ws, Style::default().fg(Color::White)),
        ]));
        let mp = state
            .merkle_path_count
            .map(|c| format!("{c}"))
            .unwrap_or_else(|| "—".into());
        text.lines.push(Line::from(vec![
            Span::styled("merkle paths:       ", Style::default().fg(Color::Cyan)),
            Span::styled(mp, Style::default().fg(Color::White)),
        ]));

        Paragraph::new(text).block(block).wrap(Wrap { trim: false })
    } else {
        Paragraph::new(Line::from(Span::styled(
            " No template — press r to refresh",
            Style::default().fg(Color::DarkGray),
        )))
        .block(block)
    };

    f.render_widget(inner, area);
}

fn render_summary(f: &mut Frame, area: Rect, state: &AppState) {
    let block = Block::bordered().title("SUMMARY");

    let tx_count = state
        .tx_fees
        .as_ref()
        .map(|f| f.len())
        .map(|n| Span::styled(format!("{n}"), Style::default().fg(Color::White)))
        .unwrap_or_else(|| Span::styled("—", Style::default().fg(Color::DarkGray)));

    let fees = state
        .tx_fees
        .as_ref()
        .map(|f| {
            let min = f.iter().min().unwrap_or(&0);
            let max = f.iter().max().unwrap_or(&0);
            Span::styled(format!("{min} .. {max}"), Style::default().fg(Color::White))
        })
        .unwrap_or_else(|| Span::styled("—", Style::default().fg(Color::DarkGray)));

    let sigops = state
        .tx_sigops
        .as_ref()
        .map(|s| {
            let min = s.iter().min().unwrap_or(&0);
            let max = s.iter().max().unwrap_or(&0);
            Span::styled(format!("{min} .. {max}"), Style::default().fg(Color::White))
        })
        .unwrap_or_else(|| Span::styled("—", Style::default().fg(Color::DarkGray)));

    let block_size = state
        .block_size
        .map(|s| {
            if s > 1_000_000 {
                format!("{:.1} MB", s as f64 / 1_000_000.0)
            } else if s > 1_000 {
                format!("{:.1} KB", s as f64 / 1_000.0)
            } else {
                format!("{s} B")
            }
        })
        .map(|s| Span::styled(s, Style::default().fg(Color::White)))
        .unwrap_or_else(|| Span::styled("—", Style::default().fg(Color::DarkGray)));

    let text = Text::from(vec![
        Line::from(vec![Span::raw("  txs:    "), tx_count]),
        Line::from(vec![Span::raw("  fees:   "), fees]),
        Line::from(vec![Span::raw("  sigops: "), sigops]),
        Line::from(vec![Span::raw("  size:   "), block_size]),
    ]);
    f.render_widget(Paragraph::new(text).block(block), area);
}

fn render_actions(f: &mut Frame, area: Rect, state: &AppState) {
    let block = Block::bordered().title("ACTIONS");
    let wait_suffix = if state.is_waiting {
        let spinner = wait_char(state.wait_started_at);
        format!("  [{spinner}]")
    } else {
        String::new()
    };

    let text = match state.connection_state {
        ConnectionState::Connecting => Text::from(vec![Line::from(Span::styled(
            "  Waiting for connection...",
            Style::default().fg(Color::DarkGray),
        ))]),
        ConnectionState::Failed(_) => Text::from(vec![Line::from(Span::styled(
            "  Connection failed — q to quit",
            Style::default().fg(Color::DarkGray),
        ))]),
        ConnectionState::Connected => Text::from(vec![
            Line::from(vec![key_binding("r"), Span::raw("refresh template")]),
            Line::from(vec![key_binding("e"), Span::raw("echo ping")]),
            Line::from(vec![
                key_binding("w"),
                Span::raw(format!("wait next template{wait_suffix}")),
            ]),
            Line::from(vec![key_binding("s"), Span::raw("submit solution")]),
            Line::from(vec![key_binding("d"), Span::raw("destroy echo")]),
            Line::from(vec![key_binding("q"), Span::raw("quit")]),
        ]),
    };
    f.render_widget(Paragraph::new(text).block(block), area);
}

fn key_binding(k: &str) -> Span<'static> {
    Span::styled(
        format!(" {k}  "),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )
}

fn wait_char(started: Option<Instant>) -> char {
    let elapsed = started.map(|t| t.elapsed().as_millis()).unwrap_or(0);
    const SPINNERS: &[char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
    let idx = (elapsed / 80) % SPINNERS.len() as u128;
    SPINNERS[idx as usize]
}

fn render_messages(f: &mut Frame, area: Rect, state: &AppState) {
    let block = Block::bordered().title("MESSAGES");
    let lines: Vec<Line> = state
        .messages
        .iter()
        .rev()
        .take(area.height as usize - 2)
        .rev()
        .map(|m| {
            if m.starts_with("echo:") {
                Line::from(Span::styled(m.clone(), Style::default().fg(Color::Green)))
            } else if m.contains("error") {
                Line::from(Span::styled(m.clone(), Style::default().fg(Color::Red)))
            } else if m.contains("accepted") {
                Line::from(Span::styled(m.clone(), Style::default().fg(Color::Green)))
            } else if m.contains("rejected") {
                Line::from(Span::styled(m.clone(), Style::default().fg(Color::Red)))
            } else {
                Line::from(Span::raw(m.clone()))
            }
        })
        .collect();
    f.render_widget(
        Paragraph::new(Text::from(lines))
            .block(block)
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn render_legend(f: &mut Frame, area: Rect) {
    let text = Line::from(vec![
        Span::styled(" OFFSET=cyan ", Style::default().fg(Color::Cyan)),
        Span::styled("HEX=white ", Style::default().fg(Color::White)),
        Span::styled("NUL=dark_gray ", Style::default().fg(Color::DarkGray)),
        Span::styled("ASC=green ", Style::default().fg(Color::Green)),
        Span::styled("NON-PRINT=dark_gray", Style::default().fg(Color::DarkGray)),
    ]);
    f.render_widget(Paragraph::new(text), area);
}

fn render_status(f: &mut Frame, area: Rect, state: &AppState) {
    match &state.connection_state {
        ConnectionState::Connecting => {
            let text = Line::from(vec![
                Span::styled(" Connecting... ", Style::default().fg(Color::Yellow)),
                hint("q", "quit"),
            ]);
            f.render_widget(Paragraph::new(text), area);
        }
        ConnectionState::Failed(ref msg) => {
            let text = Line::from(vec![
                Span::styled(
                    format!(" Connection failed: {msg} "),
                    Style::default().fg(Color::Red),
                ),
                hint("q", "quit"),
            ]);
            f.render_widget(Paragraph::new(text), area);
        }
        ConnectionState::Connected => {
            if state.input_mode != InputMode::Normal {
                let (label, buf) = match state.input_mode {
                    InputMode::Version => ("version", &state.version_buf),
                    InputMode::Timestamp => ("timestamp", &state.timestamp_buf),
                    InputMode::Nonce => ("nonce", &state.nonce_buf),
                    InputMode::Coinbase => ("coinbase", &state.coinbase_buf),
                    InputMode::Normal => unreachable!(),
                };
                let text = Line::from(vec![
                    Span::styled(format!(" {label}: "), Style::default().fg(Color::Yellow)),
                    Span::styled(buf.clone(), Style::default().fg(Color::White)),
                    Span::styled(
                        " ▏Esc=cancel Enter=confirm",
                        Style::default()
                            .fg(Color::DarkGray)
                            .add_modifier(Modifier::DIM),
                    ),
                ]);
                f.render_widget(Paragraph::new(text), area);
            } else {
                let text = Line::from(vec![
                    hint("r", "refresh"),
                    hint("e", "echo"),
                    hint("w", "wait"),
                    hint("s", "submit"),
                    hint("d", "destroy"),
                    hint("q", "quit"),
                ]);
                f.render_widget(Paragraph::new(text), area);
            }
        }
    }
}

fn hint(key: &str, action: &str) -> Span<'static> {
    Span::styled(
        format!(" {key}={action}"),
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::DIM),
    )
}

fn render_hexdump(data: &[u8]) -> Text<'static> {
    let mut lines: Vec<Line<'static>> = Vec::new();

    let header = Line::from(vec![
        Span::styled("  OFFSET  ", Style::default().fg(Color::Cyan)),
        Span::styled("│ ", Style::default().add_modifier(Modifier::DIM)),
        Span::styled("00 01 02 03 04 05 06 07", Style::default().fg(Color::White)),
        Span::styled("  │ ", Style::default().add_modifier(Modifier::DIM)),
        Span::styled("ASCII", Style::default().fg(Color::Green)),
    ]);
    lines.push(header);
    lines.push(Line::from(Span::styled(
        "──────────┼─────────────────────┼──────",
        Style::default().add_modifier(Modifier::DIM),
    )));

    for (row, chunk) in data.chunks(8).enumerate() {
        let mut spans: Vec<Span<'static>> = Vec::new();

        spans.push(Span::styled(
            format!("  {:#06x}  ", row * 8),
            Style::default().fg(Color::Cyan),
        ));

        spans.push(Span::styled(
            "│ ",
            Style::default().add_modifier(Modifier::DIM),
        ));

        for (i, byte) in chunk.iter().enumerate() {
            if i > 0 {
                spans.push(Span::raw(" "));
            }
            let style = if *byte == 0 {
                Style::default().fg(Color::DarkGray)
            } else {
                Style::default().fg(Color::White)
            };
            spans.push(Span::styled(format!("{byte:02x}"), style));
        }

        let remaining = 8 - chunk.len();
        if remaining > 0 {
            for _ in 0..remaining {
                spans.push(Span::raw("   "));
            }
        }

        spans.push(Span::styled(
            " │ ",
            Style::default().add_modifier(Modifier::DIM),
        ));

        for byte in chunk {
            let (ch, style) = if byte.is_ascii_graphic() || *byte == b' ' {
                (*byte as char, Style::default().fg(Color::Green))
            } else {
                ('.', Style::default().fg(Color::DarkGray))
            };
            spans.push(Span::styled(ch.to_string(), style));
        }

        if remaining > 0 {
            for _ in 0..remaining {
                spans.push(Span::raw(" "));
            }
        }

        lines.push(Line::from(spans));
    }

    Text::from(lines)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    #[test]
    fn test_draw_empty_state_no_panic() {
        let state = AppState::new("/test/sock");
        let backend = TestBackend::new(80, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| draw(f, &state)).unwrap();
    }

    #[test]
    fn test_draw_populated_state_no_panic() {
        let mut state = AppState::new("/test/sock");
        state.is_test_chain = Some(true);
        state.is_ibd = Some(false);
        state.tip_height = Some(842103);
        state.tip_hash = Some(vec![0xa3, 0xf2, 0xe1, 0xd0, 0x00, 0x00, 0x00, 0x01]);
        state.tip_changed_at = Some(Instant::now());
        state.block_header = Some(vec![0u8; 80]);
        state.block_size = Some(1_234_567);
        state.tx_fees = Some(vec![100, 500, 50_000]);
        state.tx_sigops = Some(vec![10, 50, 200]);
        state.coinbase_commitment = Some(vec![0xa3, 0xf2, 0xe1, 0xd0]);
        state.witness_commitment_index = Some(1);
        state.merkle_path_count = Some(3);
        state.push_message("Test message".into());
        state.push_message("echo: pong".into());
        state.push_message("error: something".into());

        let backend = TestBackend::new(80, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| draw(f, &state)).unwrap();
    }

    #[test]
    fn test_draw_very_small_terminal_no_panic() {
        let state = AppState::new("/test/sock");
        let backend = TestBackend::new(40, 10);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| draw(f, &state)).unwrap();
    }

    #[test]
    fn test_draw_input_mode_version_no_panic() {
        let mut state = AppState::new("/test/sock");
        state.input_mode = InputMode::Version;
        state.version_buf = "123".into();

        let backend = TestBackend::new(80, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| draw(f, &state)).unwrap();
    }

    #[test]
    fn test_draw_all_input_modes() {
        for mode in &[
            InputMode::Version,
            InputMode::Timestamp,
            InputMode::Nonce,
            InputMode::Coinbase,
        ] {
            let mut state = AppState::new("/test/sock");
            state.input_mode = *mode;
            state.version_buf = "1".into();
            state.timestamp_buf = "2".into();
            state.nonce_buf = "3".into();
            state.coinbase_buf = "aabb".into();

            let backend = TestBackend::new(80, 30);
            let mut terminal = Terminal::new(backend).unwrap();
            terminal.draw(|f| draw(f, &state)).unwrap();
        }
    }

    #[test]
    fn test_draw_waiting_spinner() {
        let mut state = AppState::new("/test/sock");
        state.is_waiting = true;
        state.wait_started_at = Some(Instant::now());

        let backend = TestBackend::new(80, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| draw(f, &state)).unwrap();
    }

    #[test]
    fn test_decode_hex() {
        assert_eq!(decode_hex(""), None);
        assert_eq!(decode_hex("a"), None);
        assert_eq!(decode_hex("a3f2"), Some(vec![0xa3, 0xf2]));
        assert_eq!(decode_hex("0001ff"), Some(vec![0x00, 0x01, 0xff]));
        assert_eq!(decode_hex("deadbeef"), Some(vec![0xde, 0xad, 0xbe, 0xef]));
        assert_eq!(decode_hex("xyz"), None);
        assert_eq!(decode_hex("gg"), None);
    }

    #[test]
    fn test_render_hexdump_empty() {
        let text = render_hexdump(&[]);
        assert_eq!(text.lines.len(), 2);
        assert!(!text.lines[0].spans.is_empty());
    }

    #[test]
    fn test_render_hexdump_header() {
        let data = vec![0xa3, 0xf2, 0xe1, 0xd0, 0x00, 0x00, 0x00, 0x01];
        let text = render_hexdump(&data);
        let header_str: String = text.lines[0]
            .spans
            .iter()
            .map(|s| s.content.as_ref())
            .collect();
        assert!(header_str.contains("OFFSET"));
        assert!(header_str.contains("ASCII"));
    }

    #[test]
    fn test_render_hexdump_data_row() {
        let data = vec![0xa3, 0xf2, 0xe1, 0xd0, 0x00, 0x00, 0x00, 0x01];
        let text = render_hexdump(&data);
        let row_2_str: String = text.lines[2]
            .spans
            .iter()
            .map(|s| s.content.as_ref())
            .collect();
        assert!(row_2_str.contains("0x0000"));
        assert!(row_2_str.contains("a3"));
        assert!(row_2_str.contains("f2"));
    }

    #[test]
    fn test_render_hexdump_all_colors() {
        let data: Vec<u8> = (0x00..=0x07).collect();
        let text = render_hexdump(&data);
        assert!(text.lines.len() >= 3);
    }

    #[test]
    fn test_render_hexdump_multiple_rows() {
        let data: Vec<u8> = (0x00..0x18).collect();
        let text = render_hexdump(&data);
        let body: Vec<&Line> = text.lines[2..].iter().collect();
        assert_eq!(body.len(), 3);
    }

    #[test]
    fn test_render_hexdump_incomplete_row() {
        let data = vec![0x01, 0x02, 0x03];
        let text = render_hexdump(&data);
        let body: Vec<&Line> = text.lines[2..].iter().collect();
        assert_eq!(body.len(), 1);
    }

    #[test]
    fn test_render_node_info_all_variants() {
        for (test_chain, ibd) in [
            (Some(true), Some(true)),
            (Some(true), Some(false)),
            (Some(false), Some(true)),
            (Some(false), Some(false)),
            (None, None),
        ] {
            let mut state = AppState::new("/test");
            state.is_test_chain = test_chain;
            state.is_ibd = ibd;

            let backend = TestBackend::new(80, 30);
            let mut terminal = Terminal::new(backend).unwrap();
            terminal.draw(|f| draw(f, &state)).unwrap();
        }
    }

    #[test]
    fn test_render_template_no_header() {
        let state = AppState::new("/test");
        let backend = TestBackend::new(80, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| draw(f, &state)).unwrap();
    }

    #[test]
    fn test_current_buffer_mut() {
        let mut state = AppState::new("/test");
        state.input_mode = InputMode::Version;
        current_buffer_mut(&mut state).push_str("123");
        assert_eq!(state.version_buf, "123");

        state.input_mode = InputMode::Timestamp;
        current_buffer_mut(&mut state).push_str("456");
        assert_eq!(state.timestamp_buf, "456");

        state.input_mode = InputMode::Nonce;
        current_buffer_mut(&mut state).push_str("789");
        assert_eq!(state.nonce_buf, "789");

        state.input_mode = InputMode::Coinbase;
        current_buffer_mut(&mut state).push_str("aabb");
        assert_eq!(state.coinbase_buf, "aabb");
    }

    #[test]
    #[should_panic(expected = "internal error: entered unreachable code")]
    fn test_current_buffer_mut_normal_panics() {
        let mut state = AppState::new("/test");
        state.input_mode = InputMode::Normal;
        current_buffer_mut(&mut state);
    }

    #[test]
    fn test_push_message_truncates() {
        let mut state = AppState::new("/test");
        for i in 0..MAX_MESSAGES + 10 {
            state.push_message(format!("msg {i}"));
        }
        assert_eq!(state.messages.len(), MAX_MESSAGES);
        assert_eq!(state.messages[0], format!("msg {}", 10));
    }

    #[test]
    fn test_app_state_new_values() {
        let state = AppState::new("/custom/sock");
        assert_eq!(state.socket_path, "/custom/sock");
        assert_eq!(state.input_mode, InputMode::Normal);
        assert!(state.is_test_chain.is_none());
        assert!(state.is_ibd.is_none());
        assert!(state.tip_height.is_none());
        assert!(state.messages.is_empty());
        assert!(!state.is_waiting);
    }

    #[test]
    fn test_draw_connecting_state_no_panic() {
        let mut state = AppState::new("/test/sock");
        state.connection_state = ConnectionState::Connecting;
        let backend = TestBackend::new(80, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| draw(f, &state)).unwrap();
    }

    #[test]
    fn test_draw_failed_state_no_panic() {
        let mut state = AppState::new("/test/sock");
        state.connection_state = ConnectionState::Failed("socket not found".to_string());
        let backend = TestBackend::new(80, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| draw(f, &state)).unwrap();
    }

    #[test]
    fn test_draw_connected_state_no_panic() {
        let mut state = AppState::new("/test/sock");
        state.connection_state = ConnectionState::Connected;
        state.is_test_chain = Some(false);
        state.is_ibd = Some(false);
        state.tip_height = Some(842103);
        state.tip_hash = Some(vec![0xa3, 0xf2, 0xe1, 0xd0, 0x00, 0x00, 0x00, 0x01]);
        state.tip_changed_at = Some(Instant::now());
        state.block_header = Some(vec![0u8; 80]);
        let backend = TestBackend::new(80, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| draw(f, &state)).unwrap();
    }

    #[test]
    fn test_app_state_default_connection_state() {
        let state = AppState::new("/test");
        assert_eq!(state.connection_state, ConnectionState::Connecting);
        assert!(state.monitor.is_none());
    }
}
