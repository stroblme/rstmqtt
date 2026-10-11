// SPDX-License-Identifier: MIT OR Apache-2.0

//! Topic tree, message details, publish form, connection controls and status bar.
//! eframe persists settings and window geometry.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use eframe::egui;
use egui::text::LayoutJob;
use egui::{
    Color32, FontFamily, FontId, Key, Modifiers, Rect, RichText, Sense, Stroke, TextEdit,
    TextFormat, Ui, pos2, vec2,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::mqtt::{Profile, Session, Shared, State, Subscribed, Version, check_filter};
use crate::payload::{self, Change, Seg, View};
use crate::tree::{Node, Props, ROOT, Tree};

/// Flat dark palette shared with rsttherm and rstptz, plus state and diff colours.
mod pal {
    use eframe::egui::Color32;

    pub const BG: Color32 = Color32::from_rgb(0x16, 0x18, 0x1D);
    pub const SURFACE: Color32 = Color32::from_rgb(0x1E, 0x21, 0x28);
    pub const HOVER: Color32 = Color32::from_rgb(0x27, 0x2B, 0x33);
    pub const ACTIVE: Color32 = Color32::from_rgb(0x31, 0x37, 0x42);
    pub const BORDER: Color32 = Color32::from_rgb(0x2E, 0x33, 0x3C);
    pub const TEXT: Color32 = Color32::from_rgb(0xE4, 0xE7, 0xEC);
    pub const MUTED: Color32 = Color32::from_rgb(0x8B, 0x91, 0x9E);
    pub const ACCENT: Color32 = Color32::from_rgb(0x4C, 0x8D, 0xFF);
    /// Connected state and added diff lines.
    pub const GOOD: Color32 = Color32::from_rgb(0x4C, 0xC3, 0x8A);
    pub const WARN: Color32 = Color32::from_rgb(0xF5, 0xA5, 0x24);
    /// Errors and removed diff lines.
    pub const BAD: Color32 = Color32::from_rgb(0xFF, 0x6B, 0x6B);
}

const GAP: f32 = 8.0;
const PAD: f32 = 14.0;
/// Tree row height, tighter than buttons for scanning.
const ROW: f32 = 26.0;
/// Indentation per tree level.
const INDENT: f32 = 16.0;
/// Initial width of the resizable details panel.
const DETAILS: f32 = 480.0;
const MODAL: f32 = 380.0;
/// Maximum payload editor height before scrolling.
const PAYLOAD: f32 = 160.0;
/// Modal text field width.
const FIELD: f32 = 220.0;
/// Topic change highlight duration.
const FLASH: Duration = Duration::from_secs(1);
/// Status note duration.
const NOTE: Duration = Duration::from_secs(3);
/// Rendered text byte limit until "Show all", to bound layout and paint cost.
const LIMIT: usize = 64 * 1024;
/// Character limit for topic row previews.
const PREVIEW: usize = 120;
/// Publish history length.
const RECENT: usize = 20;
/// Publish history size limit in bytes of text.
const RECENT_BYTES: usize = 64 * 1024;
/// Topic row rebuild interval while only messages change.
// ponytail: each rebuild scans the whole tree; with 100k+ topics under steady
// traffic, update the rows incrementally from the inserted topics instead.
const REBUILD: Duration = Duration::from_millis(250);
/// Messages kept per topic.
const HISTORY: usize = 100;
/// Message rate history length in seconds.
const PULSE: usize = 60;
/// Text field padding to match button height.
const INPUT: egui::Margin = egui::Margin::symmetric(8, 6);
/// Button icons from egui's bundled emoji font.
const COPY: &str = "🗐";
const EDIT: &str = "✏";
const PLUG: &str = "🔌";
/// eframe's app id, which also names the settings directory.
const APP: &str = "rstmqtt";

#[derive(Serialize, Deserialize)]
#[serde(default)]
struct Settings {
    profile: Profile,
    /// Filters the last persistent session may still hold.
    subscribed: Option<Subscribed>,
    view: View,
    /// Published messages, newest first.
    sent: Vec<Draft>,
    /// Whether `sent` is saved; otherwise it is kept in memory only.
    remember: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            profile: Profile::default(),
            subscribed: None,
            view: View::Auto,
            sent: Vec::new(),
            remember: true,
        }
    }
}

/// Publish form data and publish history entry.
#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
struct Draft {
    topic: String,
    payload: String,
    qos: u8,
    retain: bool,
    /// MQTT v5 only, as is `user`.
    content_type: String,
    user: Vec<(String, String)>,
}

impl Draft {
    /// Bytes of text, counted against [`RECENT_BYTES`].
    fn bytes(&self) -> usize {
        let user: usize = self.user.iter().map(|(k, v)| k.len() + v.len()).sum();
        self.topic.len() + self.payload.len() + self.content_type.len() + user
    }
}

/// Adds `d` to the front of the publish history, dropping its duplicate and the
/// oldest entries beyond [`RECENT`] or [`RECENT_BYTES`]. A draft over the byte
/// limit by itself is not added.
fn record(sent: &mut Vec<Draft>, d: Draft) {
    sent.retain(|s| *s != d);
    if d.bytes() > RECENT_BYTES {
        return;
    }
    sent.insert(0, d);
    sent.truncate(RECENT);
    let mut total = 0;
    let keep = sent
        .iter()
        .take_while(|s| {
            total += s.bytes();
            total <= RECENT_BYTES
        })
        .count();
    sent.truncate(keep);
}

/// Connection modal state, committed only on connecting.
struct Editing {
    profile: Profile,
    /// Copy of the password, sent only with this profile.
    password: String,
    /// Whether the password was cleared for a changed broker or username.
    cleared: bool,
    /// Why connecting was refused.
    error: Option<String>,
}

impl Editing {
    /// Clears the password the first time the broker or username differs from
    /// `saved`, so one broker's password reaches another only if entered again.
    fn forget(&mut self, saved: &Profile) {
        fn id(p: &Profile) -> (&str, u16, &str, bool) {
            (&p.host, p.port, &p.username, p.tls)
        }
        if !self.cleared && id(&self.profile) != id(saved) {
            self.password.clear();
            self.cleared = true;
        }
    }
}

/// Topic rows kept between frames, since building them scans the tree.
#[derive(Default)]
struct Rows {
    list: Vec<(usize, usize)>,
    /// Filter and tree change count the list was built for, and when.
    filter: String,
    changes: u64,
    built: Option<Instant>,
    /// Forces a rebuild, as after expanding, collapsing or removing topics, or a
    /// new tree, since the list holds node ids.
    stale: bool,
}

impl Rows {
    /// Rebuilds the list when stale or the filter changed, or at most every
    /// [`REBUILD`] while only the tree changed. Returns whether a tree change is
    /// still to show.
    fn update(&mut self, tree: &Tree, filter: &str) -> bool {
        let changed = tree.changes() != self.changes;
        let due = self.built.is_none_or(|t| t.elapsed() >= REBUILD);
        if self.stale || filter != self.filter || (changed && due) {
            self.list = tree.rows(filter);
            self.filter = filter.to_owned();
            self.changes = tree.changes();
            self.built = Some(Instant::now());
            self.stale = false;
            return false;
        }
        changed
    }
}

/// Display data of the shown message, kept while its key holds.
struct Rendered {
    /// Node, message number, view, diff and "Show all" it is for.
    key: (usize, u64, View, bool, bool),
    /// The view used, which resolves `Auto`.
    view: View,
    /// Rendered text, or diff lines; empty with `json`.
    parts: Vec<(String, Color32)>,
    /// Parsed JSON object or array for the tree, outside diffs.
    json: Option<Value>,
    /// What the display limit left out.
    cut: Option<String>,
}

impl Rendered {
    /// Renders message `i` of `node`, within [`LIMIT`] unless "Show all".
    /// Diffs only messages shown in full.
    fn new(node: &Node, i: usize, key: (usize, u64, View, bool, bool)) -> Self {
        let (_, _, view, diff, all) = key;
        let max = if all { usize::MAX } else { LIMIT };
        let payload = &node.history[i].payload;
        let view = payload::resolve(payload, view, max);
        let old = (diff && i > 0).then(|| &node.history[i - 1].payload);
        let json = (old.is_none() && view == View::Json)
            .then(|| serde_json::from_slice::<Value>(payload).ok())
            .flatten()
            .filter(|v| v.is_object() || v.is_array());
        if json.is_some() {
            return Self {
                key,
                view,
                parts: Vec::new(),
                json,
                cut: None,
            };
        }
        let (text, shown) = payload::render_prefix(payload, view, max);
        let mut cut = Vec::new();
        if shown < payload.len() {
            cut.push(format!(
                "Showing {} of {}",
                size(shown),
                size(payload.len())
            ));
        }
        let parts = match old.map(|old| (old.len(), payload::render_prefix(old, view, max))) {
            Some((len, (old, old_shown))) if old_shown == len && shown == payload.len() => {
                payload::diff(&old, &text)
                    .into_iter()
                    .enumerate()
                    .map(|(k, (change, line))| {
                        let (sign, colour) = match change {
                            Change::Same => (' ', pal::TEXT),
                            Change::Added => ('+', pal::GOOD),
                            Change::Removed => ('-', pal::BAD),
                        };
                        let newline = if k > 0 { "\n" } else { "" };
                        (format!("{newline}{sign} {line}"), colour)
                    })
                    .collect()
            }
            Some(_) => {
                cut.push("Too long to diff".into());
                vec![(text, pal::TEXT)]
            }
            None => vec![(text, pal::TEXT)],
        };
        Self {
            key,
            view,
            parts,
            json: None,
            cut: (!cut.is_empty()).then(|| cut.join(" · ")),
        }
    }
}

/// Runs the GUI until the window closes.
pub fn run() -> eframe::Result {
    // The settings hold the publish history, so only the user may read them.
    // Errors leave eframe's defaults.
    #[cfg(unix)]
    if let Some(dir) = eframe::storage_dir(APP) {
        use std::os::unix::fs::PermissionsExt;
        let private = std::fs::Permissions::from_mode(0o700);
        let _ =
            std::fs::create_dir_all(&dir).and_then(|()| std::fs::set_permissions(&dir, private));
    }
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1200.0, 760.0])
            .with_min_inner_size([720.0, 480.0]),
        ..Default::default()
    };
    eframe::run_native(
        APP,
        options,
        Box::new(|cc| {
            theme(&cc.egui_ctx);
            Ok(Box::new(App::new(cc)))
        }),
    )
}

/// Applies Inter fonts and the flat dark palette used by rsttherm.
fn theme(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();
    fonts.font_data.insert(
        "inter".into(),
        egui::FontData::from_static(include_bytes!("../assets/Inter-Regular.ttf")).into(),
    );
    fonts.font_data.insert(
        "inter-semibold".into(),
        egui::FontData::from_static(include_bytes!("../assets/Inter-SemiBold.ttf")).into(),
    );
    // Inserted at the front rather than replacing the list, so egui's own fonts
    // stay as fallbacks for anything Inter does not cover.
    fonts
        .families
        .entry(FontFamily::Proportional)
        .or_default()
        .insert(0, "inter".into());
    fonts
        .families
        .insert(semibold(), vec!["inter-semibold".into()]);
    ctx.set_fonts(fonts);

    ctx.all_styles_mut(|style| {
        use egui::TextStyle;

        style.spacing.item_spacing = vec2(GAP, GAP);
        style.spacing.button_padding = vec2(12.0, 7.0);
        style.spacing.interact_size.y = 30.0;

        style.text_styles = [
            (TextStyle::Small, FontId::proportional(12.0)),
            (TextStyle::Body, FontId::proportional(14.0)),
            (TextStyle::Button, FontId::proportional(14.0)),
            (TextStyle::Monospace, FontId::monospace(13.0)),
            (TextStyle::Heading, FontId::proportional(17.0)),
        ]
        .into();

        let visuals = &mut style.visuals;
        visuals.panel_fill = pal::BG;
        visuals.extreme_bg_color = pal::BG;
        visuals.window_fill = pal::SURFACE;
        visuals.window_stroke = Stroke::new(1.0, pal::BORDER);
        visuals.selection.bg_fill = pal::ACCENT;
        visuals.selection.stroke = Stroke::new(1.0, pal::TEXT);

        for (widget, fill, text) in [
            (
                &mut visuals.widgets.noninteractive,
                pal::SURFACE,
                pal::MUTED,
            ),
            (&mut visuals.widgets.inactive, pal::SURFACE, pal::TEXT),
            (&mut visuals.widgets.hovered, pal::HOVER, pal::TEXT),
            (&mut visuals.widgets.active, pal::ACTIVE, pal::TEXT),
            (&mut visuals.widgets.open, pal::HOVER, pal::TEXT),
        ] {
            widget.corner_radius = egui::CornerRadius::same(6);
            widget.bg_fill = fill;
            widget.weak_bg_fill = fill;
            widget.bg_stroke = Stroke::new(1.0, pal::BORDER);
            widget.fg_stroke = Stroke::new(1.0, text);
            widget.expansion = 0.0;
        }
    });
}

/// Returns the semibold font family registered by `theme`.
fn semibold() -> FontFamily {
    FontFamily::Name("semibold".into())
}

struct App {
    s: Settings,
    /// Displayed session data, retained after disconnecting.
    shared: Arc<Mutex<Shared>>,
    session: Option<Session>,
    /// Password kept in memory only.
    password: String,
    /// Pending connect (`true`) or disconnect, deferred until the frame releases
    /// `shared` because ending a session locks it.
    pending: Option<bool>,
    editing: Option<Editing>,
    /// Subtree path and retained topics awaiting confirmation to clear.
    confirm: Option<(String, Vec<String>)>,
    filter: String,
    rows: Rows,
    selected: Option<usize>,
    /// Topic of `selected`, which the selection follows to another node.
    selected_path: String,
    /// Displayed message number, stable as new messages arrive; `None` follows
    /// the latest.
    entry: Option<u64>,
    diff: bool,
    show_all: bool,
    rendered: Option<Rendered>,
    /// Scrolls the selection into view after keyboard navigation.
    scroll: bool,
    draft: Draft,
    /// Error shown below the publish form.
    publish_error: Option<String>,
    /// Message rates sampled from the root counter about once per second, newest last.
    pulse: VecDeque<f32>,
    counted: u64,
    sampled: Instant,
    note: String,
    noted: Instant,
}

impl App {
    fn new(cc: &eframe::CreationContext) -> Self {
        let s: Settings = cc
            .storage
            .and_then(|storage| eframe::get_value(storage, eframe::APP_KEY))
            .unwrap_or_default();
        Self {
            shared: Arc::new(Mutex::new(Shared {
                tree: Tree::new(HISTORY),
                state: State::Disconnected,
                notes: Vec::new(),
                subscribed: None,
            })),
            s,
            session: None,
            password: String::new(),
            pending: None,
            editing: None,
            confirm: None,
            filter: String::new(),
            rows: Rows::default(),
            selected: None,
            selected_path: String::new(),
            entry: None,
            diff: false,
            show_all: false,
            rendered: None,
            scroll: false,
            draft: Draft::default(),
            publish_error: None,
            pulse: VecDeque::from(vec![0.0; PULSE]),
            counted: 0,
            sampled: Instant::now(),
            note: String::new(),
            noted: Instant::now(),
        }
    }

    fn connect(&mut self, ctx: &egui::Context) {
        // Ends the old connection before the new one, which may reuse its client
        // id.
        self.session = None;
        let session = match Session::start(
            &self.s.profile,
            Some(&self.password),
            self.s.subscribed.as_ref(),
            HISTORY,
            ctx.clone(),
        ) {
            Ok(session) => session,
            Err(e) => {
                self.notify(e);
                return;
            }
        };
        self.shared = session.shared.clone();
        self.session = Some(session);
        self.selected = None;
        // Node ids of the old tree.
        self.rows.stale = true;
        self.rendered = None;
        self.counted = 0;
    }

    fn notify(&mut self, note: impl Into<String>) {
        self.note = note.into();
        self.noted = Instant::now();
    }

    fn modal_open(&self) -> bool {
        self.editing.is_some() || self.confirm.is_some()
    }

    /// Selects `id` and updates the publish topic when the selection changes.
    fn select(&mut self, tree: &Tree, id: usize) {
        if self.selected != Some(id) {
            self.selected = Some(id);
            self.selected_path = tree.path(id);
            self.entry = None;
            self.show_all = false;
            self.draft.topic = self.selected_path.clone();
        }
    }

    /// Follows the selected topic to its node, which a removal can change, or
    /// deselects it once gone. Freed nodes are reused for other topics.
    fn follow(&mut self, tree: &Tree) {
        if self.selected.is_some() {
            let id = tree.find(&self.selected_path);
            if id != self.selected {
                self.entry = None;
                self.show_all = false;
            }
            self.selected = id;
        }
    }

    fn sample(&mut self, tree: &Tree) {
        let elapsed = self.sampled.elapsed().as_secs_f32();
        if elapsed >= 1.0 {
            let total = tree.node(ROOT).subtree_messages;
            // Clears lower the count; that is no negative rate.
            self.pulse.pop_front();
            self.pulse
                .push_back(total.saturating_sub(self.counted) as f32 / elapsed);
            self.counted = total;
            self.sampled = Instant::now();
        }
    }

    /// Handles `Event::Copy`: Ctrl+C copies the displayed value; Ctrl+Shift+C the topic.
    /// Skips focused text fields, selected label text and open modals.
    fn shortcuts(&mut self, ctx: &egui::Context, tree: &Tree) {
        let Some(shift) = ctx.input(|i| {
            i.events
                .contains(&egui::Event::Copy)
                .then_some(i.modifiers.shift)
        }) else {
            return;
        };
        let selecting = ctx
            .with_plugin(|s: &mut egui::text_selection::LabelSelectionState| s.has_selection())
            .unwrap_or(false);
        let Some(id) = self.selected else { return };
        if ctx.text_edit_focused() || selecting || self.modal_open() {
            return;
        }
        if shift {
            ctx.copy_text(tree.path(id));
            self.notify("Copied topic");
        } else if let Some(i) = shown(tree.node(id), self.entry) {
            ctx.copy_text(payload::render(
                &tree.node(id).history[i].payload,
                self.s.view,
            ));
            self.notify("Copied value");
        }
    }

    fn top_bar(&mut self, ui: &mut Ui, state: &State) {
        ui.horizontal(|ui| {
            let p = &self.s.profile;
            let host = RichText::new(format!("{}:{}", p.host, p.port));
            ui.label(host.family(semibold()).color(pal::TEXT));
            if p.tls {
                ui.label(muted("TLS"));
            }
            ui.label(muted(match p.version {
                Version::V311 => "MQTT 3.1.1",
                Version::V5 => "MQTT 5",
            }));
            if ui.button(EDIT).on_hover_text("Edit connection").clicked() {
                self.editing = Some(Editing {
                    profile: self.s.profile.clone(),
                    password: self.password.clone(),
                    cleared: false,
                    error: None,
                });
            }
            let (text, colour) = state_label(state);
            let action = match self.session {
                Some(_) => "disconnect",
                None => "connect",
            };
            let mut hover = format!("{text}, click to {action}");
            if let State::Reconnecting(error) | State::Failed(error) = state {
                hover = format!("{hover}\n{error}");
            }
            let plug = ui.button(RichText::new(PLUG).color(colour));
            if plug.on_hover_text(hover).clicked() {
                self.pending = Some(self.session.is_none());
            }
        });
    }

    fn status_bar(&self, ui: &mut Ui, tree: &Tree, state: &State) {
        let root = tree.node(ROOT);
        let text = format!(
            "{} · {} · {} · {:.0} msg/s · {}",
            state_label(state).0,
            count(root.subtree_messages, "message"),
            count(root.subtree_topics.into(), "topic"),
            self.pulse.back().copied().unwrap_or_default(),
            size(tree.bytes()),
        );
        ui.horizontal(|ui| {
            ui.label(RichText::new(text).small().color(pal::MUTED));
            if tree.dropped() > 0 {
                let dropped = format!("· {} dropped", tree.dropped());
                ui.label(RichText::new(dropped).small().color(pal::MUTED))
                    .on_hover_text(
                        "messages not kept: beyond 100,000 topic levels, or too large for \
                         the 256 MiB memory limit even after evicting older messages",
                    );
            }
            if self.noted.elapsed() < NOTE {
                ui.label(RichText::new(&self.note).small());
            }
            ui.with_layout(right_to_left(), |ui| self.pulse_line(ui));
        });
    }

    /// Draws the last `PULSE` seconds of message rate, scaled to the peak.
    fn pulse_line(&self, ui: &mut Ui) {
        let (rect, _) = ui.allocate_exact_size(vec2(160.0, 14.0), Sense::hover());
        let peak = self.pulse.iter().copied().fold(1.0, f32::max);
        let step = rect.width() / (PULSE - 1) as f32;
        let points = (self.pulse.iter().enumerate())
            .map(|(i, r)| {
                pos2(
                    rect.left() + i as f32 * step,
                    rect.bottom() - r / peak * rect.height(),
                )
            })
            .collect();
        ui.painter().line(points, Stroke::new(1.5, pal::ACCENT));
    }

    /// Draws the filter and topic rows; returns whether any highlight is fading.
    fn topics(&mut self, ui: &mut Ui, tree: &mut Tree, connected: bool) -> bool {
        let filter = ui.add(
            TextEdit::singleline(&mut self.filter)
                .margin(INPUT)
                .hint_text("Filter topics and values (Ctrl+F)")
                .desired_width(f32::INFINITY),
        );
        if ui.input_mut(|i| i.consume_key(Modifiers::COMMAND, Key::F)) {
            filter.request_focus();
        }
        // Taken for the frame, so `keys` and the row closures can borrow `self`.
        let mut rows = std::mem::take(&mut self.rows);
        if rows.update(tree, &self.filter) {
            ui.ctx().request_repaint_after(REBUILD);
        }
        if self.keys(ui, tree, &rows.list) {
            rows.stale = true;
            rows.update(tree, &self.filter);
        }
        let opened = !self.filter.is_empty();
        let read = &*tree;
        let mut toggle = None;
        let mut remove = None;
        let mut fading = false;
        let body = egui::TextStyle::Body.resolve(ui.style());
        ui.spacing_mut().item_spacing.y = 0.0;
        egui::ScrollArea::vertical().auto_shrink(false).show_rows(
            ui,
            ROW,
            rows.list.len(),
            |ui, range| {
                // Where row 0 would be, drawn or not.
                let top = ui.max_rect().top() - range.start as f32 * ROW;
                for &(id, depth) in &rows.list[range] {
                    let node = read.node(id);
                    let (rect, row) =
                        ui.allocate_exact_size(vec2(ui.available_width(), ROW), Sense::click());
                    let branch = !node.children.is_empty();
                    let open = node.expanded || opened;
                    // A closed branch flashes for what changed inside it.
                    let changed = if branch && !open {
                        node.subtree_updated
                    } else {
                        node.updated
                    };
                    let flash = 1.0 - changed.elapsed().as_secs_f32() / FLASH.as_secs_f32();

                    let painter = ui.painter_at(rect);
                    if self.selected == Some(id) {
                        painter.rect_filled(rect, 4.0, pal::ACTIVE);
                    } else if row.hovered() {
                        painter.rect_filled(rect, 4.0, pal::HOVER);
                    }
                    if flash > 0.0 {
                        fading = true;
                        painter.rect_filled(rect, 4.0, pal::ACCENT.gamma_multiply(0.4 * flash));
                    }
                    let x = rect.left() + GAP + depth as f32 * INDENT;
                    if branch {
                        let hit = Rect::from_min_size(pos2(x, rect.top()), vec2(INDENT, ROW));
                        let arrow = ui.interact(hit, row.id.with("arrow"), Sense::click());
                        let mut icon = arrow.clone();
                        icon.rect = Rect::from_center_size(hit.center(), vec2(10.0, 10.0));
                        egui::collapsing_header::paint_default_icon(
                            ui,
                            if open { 1.0 } else { 0.0 },
                            &icon,
                        );
                        if arrow.clicked() {
                            toggle = Some(id);
                        }
                    }

                    let mut parts = vec![if node.name.is_empty() {
                        ("(empty)".to_string(), pal::MUTED)
                    } else {
                        (node.name.clone(), pal::TEXT)
                    }];
                    if let Some(msg) = node.history.back() {
                        let value = payload::preview(&msg.payload, PREVIEW);
                        parts.push((format!(": {value}"), pal::MUTED));
                    }
                    if branch {
                        parts.push((
                            format!(
                                "  {} · {}",
                                count(node.subtree_topics.into(), "topic"),
                                count(node.subtree_messages, "message")
                            ),
                            pal::MUTED,
                        ));
                    }
                    let galley = painter.layout_job(job(&body, parts));
                    let at = pos2(x + INDENT, rect.center().y - galley.size().y / 2.0);
                    painter.galley(at, galley, pal::TEXT);

                    if row.clicked() {
                        self.select(read, id);
                    }
                    if row.double_clicked() {
                        toggle = Some(id);
                    }
                    row.context_menu(|ui| {
                        if self.row_menu(ui, read, id, connected) {
                            remove = Some(id);
                        }
                    });
                }
                if std::mem::take(&mut self.scroll)
                    && let Some(i) = self
                        .selected
                        .and_then(|s| rows.list.iter().position(|&(id, _)| id == s))
                {
                    let y = top + i as f32 * ROW;
                    ui.scroll_to_rect(
                        Rect::from_x_y_ranges(ui.max_rect().x_range(), y..=y + ROW),
                        None,
                    );
                }
            },
        );
        if let Some(id) = toggle {
            let node = tree.node_mut(id);
            node.expanded = !node.expanded;
            rows.stale = true;
        }
        if let Some(id) = remove {
            tree.remove(id);
            rows.stale = true;
            self.rendered = None;
            self.follow(tree);
        }
        self.rows = rows;
        fading
    }

    /// Up/Down select rows; Left/Right collapse/expand; Enter toggles expansion.
    /// Left on a closed node selects its parent. Returns whether expansion changed.
    fn keys(&mut self, ui: &Ui, tree: &mut Tree, rows: &[(usize, usize)]) -> bool {
        if ui.ctx().text_edit_focused() || self.modal_open() || rows.is_empty() {
            return false;
        }
        let [up, down, left, right, enter] = [
            Key::ArrowUp,
            Key::ArrowDown,
            Key::ArrowLeft,
            Key::ArrowRight,
            Key::Enter,
        ]
        .map(|key| ui.input_mut(|i| i.consume_key(Modifiers::NONE, key)));
        if up || down {
            let at = self
                .selected
                .and_then(|s| rows.iter().position(|&(id, _)| id == s));
            let i = match at {
                Some(i) if up => i.saturating_sub(1),
                Some(i) => (i + 1).min(rows.len() - 1),
                None => 0,
            };
            self.select(tree, rows[i].0);
            self.scroll = true;
            return false;
        }
        let Some(id) = self.selected else {
            return false;
        };
        let node = tree.node(id);
        let open = node.expanded && !node.children.is_empty();
        if left && !open {
            if node.parent != ROOT {
                self.select(tree, node.parent);
                self.scroll = true;
            }
            return false;
        }
        if (left && open) || (right && !open) || enter {
            let node = tree.node_mut(id);
            node.expanded = !node.expanded;
            return true;
        }
        false
    }

    /// Returns whether "Remove from view" was clicked.
    fn row_menu(&mut self, ui: &mut Ui, tree: &Tree, id: usize, connected: bool) -> bool {
        let node = tree.node(id);
        let topic = tree.path(id);
        if ui.button("Copy topic").clicked() {
            ui.ctx().copy_text(topic.clone());
            self.notify("Copied topic");
        }
        let latest = node.history.back();
        if ui
            .add_enabled(latest.is_some(), egui::Button::new("Copy value"))
            .clicked()
            && let Some(msg) = latest
        {
            ui.ctx()
                .copy_text(payload::render(&msg.payload, self.s.view));
            self.notify("Copied value");
        }
        if ui.button("Publish here").clicked() {
            self.draft.topic = topic.clone();
        }
        ui.separator();
        if ui
            .add_enabled(connected, egui::Button::new("Clear retained"))
            .clicked()
        {
            self.clear(&topic);
        }
        if ui
            .add_enabled(
                connected && !node.children.is_empty(),
                egui::Button::new("Clear retained below…"),
            )
            .clicked()
        {
            let topics = tree
                .subtree(id)
                .into_iter()
                .filter(|&i| tree.node(i).retained)
                .map(|i| tree.path(i))
                .collect();
            self.confirm = Some((topic, topics));
        }
        ui.separator();
        ui.button("Remove from view")
            .on_hover_text("local only; topics come back with their next message")
            .clicked()
    }

    /// Queues an empty retained message to clear `topic` via broker delivery.
    fn clear(&mut self, topic: &str) {
        let Some(session) = &self.session else { return };
        let note = match session.publish(topic, Vec::new(), 0, true, None) {
            Ok(()) => format!("Queued clear of {topic}"),
            Err(e) => format!("Clear of {topic} not queued: {e}"),
        };
        self.notify(note);
    }

    /// Displays the selected topic's path, value and history.
    fn inspect(&mut self, ui: &mut Ui, tree: &Tree) {
        let Some(id) = self.selected else {
            ui.label(muted("Select a topic to see its messages."));
            return;
        };
        self.breadcrumb(ui, tree, id);
        let node = tree.node(id);
        let Some(i) = shown(node, self.entry) else {
            // A level that only has topics below it.
            ui.label(muted(format!(
                "{} · {} below",
                count(node.subtree_topics.into(), "topic"),
                count(node.subtree_messages, "message")
            )));
            return;
        };
        let msg = &node.history[i];

        ui.add_space(GAP);
        ui.horizontal(|ui| {
            for view in View::ALL {
                ui.selectable_value(&mut self.s.view, view, view.label());
            }
            ui.with_layout(right_to_left(), |ui| {
                if ui.button(COPY).on_hover_text("Copy value").clicked() {
                    ui.ctx()
                        .copy_text(payload::render(&msg.payload, self.s.view));
                    self.notify("Copied value");
                }
            });
        });
        let number = node.received - (node.history.len() - 1 - i) as u64;
        let key = (id, number, self.s.view, self.diff, self.show_all);
        let rendered = match self.rendered.take() {
            Some(r) if r.key == key => r,
            _ => Rendered::new(node, i, key),
        };
        egui::Frame::new()
            .fill(pal::SURFACE)
            .corner_radius(6)
            .inner_margin(GAP)
            .show(ui, |ui| {
                ui.set_width(ui.available_width());
                if let Some(json) = &rendered.json {
                    self.json_tree(ui, json, &mut Vec::new());
                } else if rendered.parts.iter().all(|(t, _)| t.is_empty()) {
                    // An empty payload, or a diff of two.
                    ui.label(muted("(empty)"));
                } else {
                    // Hex lines are a fixed layout, so they scroll rather than wrap.
                    self.text_block(ui, &rendered.parts, rendered.view != View::Hex);
                }
                if let Some(cut) = &rendered.cut {
                    self.show_all_row(ui, cut);
                }
            });
        self.rendered = Some(rendered);

        let mut meta = vec![format!("QoS {}", msg.qos)];
        if msg.retain {
            meta.push("retained".into());
        }
        meta.push(msg.time.format("%H:%M:%S%.3f").to_string());
        meta.push(size(msg.payload.len()));
        ui.label(muted(meta.join(" · ")));
        if let Some(props) = msg.props.as_deref().filter(|p| **p != Props::default()) {
            egui::CollapsingHeader::new("v5 properties").show(ui, |ui| properties(ui, props));
        }

        ui.add_space(GAP);
        ui.horizontal(|ui| {
            heading(ui, "History");
            ui.with_layout(right_to_left(), |ui| {
                ui.checkbox(&mut self.diff, "Diff with previous")
            });
        });
        // ponytail: every entry is drawn; with a cap in the thousands, move this
        // into `show_rows`.
        let latest = node.history.len() - 1;
        for (k, m) in node.history.iter().enumerate().rev() {
            let text = format!(
                "{}  {:>9}  {}",
                m.time.format("%H:%M:%S%.3f"),
                size(m.payload.len()),
                payload::preview(&m.payload, PREVIEW)
            );
            let entry = egui::Button::selectable(k == i, RichText::new(text).monospace());
            if ui.add(entry.truncate()).clicked() {
                self.entry = (k != latest).then(|| node.received - (latest - k) as u64);
                self.show_all = false;
            }
        }
    }

    /// Displays topic levels as clickable node selections.
    fn breadcrumb(&mut self, ui: &mut Ui, tree: &Tree, id: usize) {
        let mut levels = Vec::new();
        let mut at = id;
        while at != ROOT {
            levels.push(at);
            at = tree.node(at).parent;
        }
        levels.reverse();
        right_row(ui, |ui| {
            if ui.button(COPY).on_hover_text("Copy topic").clicked() {
                ui.ctx().copy_text(tree.path(id));
                self.notify("Copied topic");
            }
            let wrapped = egui::Layout::left_to_right(egui::Align::Center).with_main_wrap(true);
            ui.with_layout(wrapped, |ui| {
                ui.spacing_mut().item_spacing.x = 2.0;
                for (k, &level) in levels.iter().enumerate() {
                    if k > 0 {
                        ui.label(muted("/"));
                    }
                    let name = &tree.node(level).name;
                    let mut text = if name.is_empty() {
                        muted("(empty)").italics()
                    } else {
                        RichText::new(name)
                    };
                    if level == id {
                        text = text.family(semibold());
                    }
                    if ui
                        .add(egui::Label::new(text).sense(Sense::click()))
                        .clicked()
                    {
                        self.select(tree, level);
                    }
                }
            });
        });
    }

    /// Displays selectable monospace text, capped at [`LIMIT`] bytes until "Show all".
    fn text_block(&mut self, ui: &mut Ui, parts: &[(String, Color32)], wrap: bool) {
        let font = egui::TextStyle::Monospace.resolve(ui.style());
        let total: usize = parts.iter().map(|(t, _)| t.len()).sum();
        let mut job = LayoutJob::default();
        for (text, colour) in parts {
            let room = LIMIT.saturating_sub(job.text.len());
            let cut = !self.show_all && text.len() > room;
            let text = if cut {
                &text[..text.floor_char_boundary(room)]
            } else {
                text
            };
            job.append(text, 0.0, TextFormat::simple(font.clone(), *colour));
            if cut {
                break;
            }
        }
        let shown = job.text.len();
        let label = egui::Label::new(job).selectable(true);
        if wrap {
            ui.add(label.wrap());
        } else {
            egui::ScrollArea::horizontal()
                .id_salt("value")
                .show(ui, |ui| ui.add(label.extend()));
        }
        if shown < total {
            self.show_all_row(ui, &format!("Showing {} of {}", size(shown), size(total)));
        }
    }

    /// Displays what the display limit left out, with a button to lift it.
    fn show_all_row(&mut self, ui: &mut Ui, cut: &str) {
        ui.horizontal(|ui| {
            ui.label(muted(cut));
            if ui.button("Show all").clicked() {
                self.show_all = true;
            }
        });
    }

    /// Displays a collapsible JSON tree; right-click to copy paths or values.
    fn json_tree<'a>(&mut self, ui: &mut Ui, value: &'a Value, path: &mut Vec<Seg<'a>>) {
        let members: Vec<(String, Seg<'a>, &'a Value)> = match value {
            Value::Object(map) => map
                .iter()
                .map(|(k, v)| (k.clone(), Seg::Key(k), v))
                .collect(),
            Value::Array(items) => items
                .iter()
                .enumerate()
                .map(|(i, v)| (format!("[{i}]"), Seg::Index(i), v))
                .collect(),
            _ => return,
        };
        let body = egui::TextStyle::Body.resolve(ui.style());
        for (name, seg, value) in members {
            path.push(seg);
            let summary = match value {
                Value::Object(map) => Some(count(map.len() as u64, "key")),
                Value::Array(items) => Some(count(items.len() as u64, "item")),
                _ => None,
            };
            let response = match summary {
                Some(summary) => {
                    let label = job(
                        &body,
                        vec![(name, pal::TEXT), (format!("  {summary}"), pal::MUTED)],
                    );
                    // The first two levels start open.
                    egui::CollapsingHeader::new(label)
                        .id_salt(payload::json_path(path))
                        .default_open(path.len() < 3)
                        .show(ui, |ui| self.json_tree(ui, value, path))
                        .header_response
                }
                None => {
                    let label = job(
                        &body,
                        vec![
                            (name, pal::TEXT),
                            (": ".into(), pal::MUTED),
                            (value.to_string(), pal::ACCENT),
                        ],
                    );
                    ui.add(egui::Label::new(label).sense(Sense::click()))
                }
            };
            response.context_menu(|ui| {
                if ui.button("Copy path").clicked() {
                    ui.ctx().copy_text(payload::json_path(path));
                    self.notify("Copied path");
                }
                if ui.button("Copy value").clicked() {
                    ui.ctx().copy_text(match value {
                        Value::String(s) => s.clone(),
                        v => v.to_string(),
                    });
                    self.notify("Copied value");
                }
            });
            path.pop();
        }
    }

    fn publish(&mut self, ui: &mut Ui, connected: bool) {
        let header = RichText::new("Publish")
            .family(semibold())
            .color(pal::MUTED);
        egui::CollapsingHeader::new(header)
            .default_open(true)
            .show(ui, |ui| {
                let d = &mut self.draft;
                ui.add(
                    TextEdit::singleline(&mut d.topic)
                        .margin(INPUT)
                        .hint_text("Topic")
                        .desired_width(f32::INFINITY),
                );
                // Scrolls rather than growing the form off the panel.
                let payload = egui::ScrollArea::vertical()
                    .id_salt("payload")
                    .max_height(PAYLOAD)
                    .show(ui, |ui| {
                        ui.add(
                            TextEdit::multiline(&mut d.payload)
                                .margin(INPUT)
                                .hint_text("Payload")
                                .code_editor()
                                .desired_rows(4)
                                .desired_width(f32::INFINITY),
                        )
                    })
                    .inner;
                if payload.changed() {
                    self.publish_error = None;
                }
                if self.s.profile.version == Version::V5 {
                    field(
                        ui,
                        "Content type",
                        TextEdit::singleline(&mut d.content_type)
                            .margin(INPUT)
                            .desired_width(FIELD),
                    );
                    let mut remove = None;
                    for (i, (key, value)) in d.user.iter_mut().enumerate() {
                        ui.horizontal(|ui| {
                            let width = (ui.available_width() - 30.0 - 2.0 * GAP) / 2.0;
                            ui.add(
                                TextEdit::singleline(key)
                                    .margin(INPUT)
                                    .hint_text("Key")
                                    .desired_width(width),
                            );
                            ui.add(
                                TextEdit::singleline(value)
                                    .margin(INPUT)
                                    .hint_text("Value")
                                    .desired_width(width),
                            );
                            if ui.button("−").clicked() {
                                remove = Some(i);
                            }
                        });
                    }
                    if let Some(i) = remove {
                        d.user.remove(i);
                    }
                    if ui.button("Add user property").clicked() {
                        d.user.push(Default::default());
                    }
                }
                let mut send = false;
                ui.horizontal(|ui| {
                    egui::ComboBox::from_id_salt("recent")
                        .selected_text("Recent")
                        .width(80.0)
                        .show_ui(ui, |ui| {
                            for sent in &self.s.sent {
                                let text = format!(
                                    "{}  {}",
                                    sent.topic,
                                    payload::preview(sent.payload.as_bytes(), 40)
                                );
                                if ui.selectable_label(false, text).clicked() {
                                    *d = sent.clone();
                                }
                            }
                            if !self.s.sent.is_empty() {
                                ui.separator();
                            }
                            ui.checkbox(&mut self.s.remember, "Remember publish history")
                                .on_hover_text(
                                    "saved with the settings; otherwise kept until the app closes",
                                );
                            let clear = egui::Button::new("Clear history");
                            if ui.add_enabled(!self.s.sent.is_empty(), clear).clicked() {
                                self.s.sent.clear();
                            }
                        });
                    if ui.button("Format JSON").clicked() {
                        match serde_json::from_str::<Value>(&d.payload)
                            .and_then(|v| serde_json::to_string_pretty(&v))
                        {
                            Ok(pretty) => d.payload = pretty,
                            Err(e) => self.publish_error = Some(format!("Not JSON: {e}")),
                        }
                    }
                    qos(ui, "publish qos", &mut d.qos);
                    ui.checkbox(&mut d.retain, "Retain");
                    ui.with_layout(right_to_left(), |ui| {
                        send = ui
                            .add_enabled(connected, egui::Button::new("Send"))
                            .on_disabled_hover_text("not connected")
                            .clicked();
                    });
                });
                if let Some(error) = &self.publish_error {
                    ui.label(RichText::new(error).small().color(pal::BAD));
                }
                if send {
                    self.send();
                }
            });
    }

    fn send(&mut self) {
        let Some(session) = &self.session else { return };
        let d = self.draft.clone();
        // Ignored on v3.1.1.
        let props = Props {
            content_type: Some(d.content_type.clone()).filter(|c| !c.is_empty()),
            user: d.user.clone(),
            ..Props::default()
        };
        let sent = session.publish(
            &d.topic,
            d.payload.clone().into_bytes(),
            d.qos,
            d.retain,
            Some(&props),
        );
        match sent {
            Ok(()) => {
                self.publish_error = None;
                self.notify(format!("Queued to {}", d.topic));
                record(&mut self.s.sent, d);
            }
            Err(e) => self.publish_error = Some(e),
        }
    }

    fn connection_modal(&mut self, ctx: &egui::Context) {
        let Some(e) = &mut self.editing else { return };
        e.forget(&self.s.profile);
        let Editing {
            profile: p,
            password,
            error,
            ..
        } = e;
        let modal = modal(ctx, "connection").show(ctx, |ui| {
            ui.set_width(MODAL);
            heading(ui, "Connection");
            field(
                ui,
                "Host",
                TextEdit::singleline(&mut p.host)
                    .margin(INPUT)
                    .desired_width(FIELD),
            );
            field(ui, "Port", egui::DragValue::new(&mut p.port));
            ui.checkbox(&mut p.tls, "TLS")
                .on_hover_text("verifies the broker's certificate and host name");
            if p.tls {
                field(
                    ui,
                    "CA file",
                    TextEdit::singleline(&mut p.ca_file)
                        .margin(INPUT)
                        .hint_text("system roots")
                        .desired_width(FIELD),
                )
                .on_hover_text("PEM file of the CA certificates to trust");
            }
            ui.horizontal(|ui| {
                ui.label("MQTT version");
                ui.with_layout(right_to_left(), |ui| {
                    ui.selectable_value(&mut p.version, Version::V5, "5");
                    ui.selectable_value(&mut p.version, Version::V311, "3.1.1");
                });
            });
            field(
                ui,
                "Client id",
                TextEdit::singleline(&mut p.client_id)
                    .margin(INPUT)
                    .desired_width(FIELD),
            )
            .on_hover_text("a broker drops the older of two clients with the same id");
            field(
                ui,
                "Username",
                TextEdit::singleline(&mut p.username)
                    .margin(INPUT)
                    .desired_width(FIELD),
            );
            field(
                ui,
                "Password",
                TextEdit::singleline(password)
                    .margin(INPUT)
                    .password(true)
                    .desired_width(FIELD),
            )
            .on_hover_text(
                "kept until the app closes, never saved; cleared on changing the broker \
                 or username",
            );
            field(
                ui,
                "Keep alive",
                egui::DragValue::new(&mut p.keep_alive).suffix(" s"),
            );
            ui.checkbox(&mut p.clean, "Clean session").on_hover_text(
                "unchecked, the broker keeps the session (subscriptions and queued QoS 1 \
                 and 2 messages) for this client id across disconnects; on MQTT 5 it never \
                 expires unless the broker caps it. An empty client id always uses a clean \
                 session.",
            );

            ui.add_space(GAP);
            heading(ui, "Subscriptions");
            let mut remove = None;
            for (i, (filter, level)) in p.subscriptions.iter_mut().enumerate() {
                ui.horizontal(|ui| {
                    let width = ui.available_width() - 110.0;
                    let invalid = check_filter(filter).err();
                    let edit = ui.add(
                        TextEdit::singleline(filter)
                            .margin(INPUT)
                            .text_color_opt(invalid.as_ref().map(|_| pal::BAD))
                            .desired_width(width),
                    );
                    if let Some(invalid) = invalid {
                        edit.on_hover_text(invalid);
                    }
                    qos(ui, ("subscription", i), level);
                    if ui.button("−").clicked() {
                        remove = Some(i);
                    }
                });
            }
            if let Some(i) = remove {
                p.subscriptions.remove(i);
            }
            if ui.button("Add subscription").clicked() {
                p.subscriptions.push((String::new(), 0));
            }
            if let Some(error) = error {
                ui.label(RichText::new(error.as_str()).small().color(pal::BAD));
            }

            ui.add_space(GAP);
            right_row(ui, |ui| {
                let connect = ui.button("Connect").clicked();
                let cancel = ui.button("Cancel").clicked();
                (connect, cancel)
            })
        });
        let (connect, cancel) = modal.inner;
        if connect && let Some(e) = self.editing.take() {
            match e.profile.check(&e.password) {
                Ok(()) => {
                    self.s.profile = e.profile;
                    self.password = e.password;
                    self.pending = Some(true);
                }
                Err(error) => {
                    self.editing = Some(Editing {
                        error: Some(error),
                        ..e
                    })
                }
            }
        } else if cancel || modal.should_close() {
            self.editing = None;
        }
    }

    fn confirm_modal(&mut self, ctx: &egui::Context) {
        let Some((topic, topics)) = &self.confirm else {
            return;
        };
        let modal = modal(ctx, "clear").show(ctx, |ui| {
            ui.set_width(MODAL);
            heading(ui, "Clear retained");
            ui.label(format!(
                "Publish an empty retained message to {} at and below {topic}?",
                count(topics.len() as u64, "topic")
            ));
            ui.label(muted(
                "Covers topics that delivered a retained message to this client. On \
                 MQTT 3.1.1, a retained message published after subscribing arrives \
                 without the flag and is not included. Retained messages outside the \
                 subscriptions stay on the broker.",
            ));
            ui.add_space(GAP);
            right_row(ui, |ui| {
                let clear = ui
                    .add_enabled(!topics.is_empty(), egui::Button::new("Clear"))
                    .clicked();
                let cancel = ui.button("Cancel").clicked();
                (clear, cancel)
            })
        });
        let (clear, cancel) = modal.inner;
        if clear
            && let Some((_, topics)) = self.confirm.take()
            && let Some(session) = &self.session
        {
            session.clear(topics);
        } else if cancel || modal.should_close() {
            self.confirm = None;
        }
    }
}

/// Displays set MQTT v5 properties, one per row.
fn properties(ui: &mut Ui, p: &Props) {
    egui::Grid::new("props")
        .num_columns(2)
        .spacing(vec2(PAD, GAP))
        .show(ui, |ui| {
            let mut row = |name: &str, value: String| {
                ui.label(muted(name));
                ui.label(RichText::new(value).color(pal::TEXT));
                ui.end_row();
            };
            if let Some(c) = &p.content_type {
                row("Content type", c.clone());
            }
            if let Some(t) = &p.response_topic {
                row("Response topic", t.clone());
            }
            if let Some(c) = &p.correlation {
                row("Correlation data", payload::preview(c, PREVIEW));
            }
            if let Some(e) = p.expiry {
                row("Expiry", format!("{e} s"));
            }
            if let Some(utf8) = p.utf8 {
                row(
                    "Payload format",
                    if utf8 { "UTF-8" } else { "bytes" }.into(),
                );
            }
            for (key, value) in &p.user {
                row(key, value.clone());
            }
        });
}

/// Returns the selected history index, falling back to the latest message.
/// Returns `None` when history is empty.
fn shown(node: &Node, entry: Option<u64>) -> Option<usize> {
    let latest = node.history.len().checked_sub(1)?;
    // Message number `n` sits `received - n` entries before the latest, unless
    // it has been evicted.
    Some(
        entry
            .and_then(|n| (latest as u64 + n).checked_sub(node.received))
            .map_or(latest, |i| i as usize),
    )
}

fn state_label(state: &State) -> (&'static str, Color32) {
    match state {
        State::Connecting => ("connecting", pal::WARN),
        State::Connected => ("connected", pal::GOOD),
        State::Reconnecting(_) => ("reconnecting", pal::WARN),
        State::Failed(_) => ("failed", pal::BAD),
        State::Disconnected => ("disconnected", pal::MUTED),
    }
}

/// Builds a text layout with `font` and per-part colours.
fn job(font: &FontId, parts: Vec<(String, Color32)>) -> LayoutJob {
    let mut job = LayoutJob::default();
    for (text, colour) in parts {
        job.append(&text, 0.0, TextFormat::simple(font.clone(), colour));
    }
    job
}

fn muted(text: impl Into<String>) -> RichText {
    RichText::new(text).color(pal::MUTED)
}

fn count(n: u64, what: &str) -> String {
    format!("{n} {what}{}", if n == 1 { "" } else { "s" })
}

fn size(bytes: usize) -> String {
    match bytes {
        0..1024 => format!("{bytes} B"),
        1024..1_048_576 => format!("{:.1} KiB", bytes as f32 / 1024.0),
        _ => format!("{:.1} MiB", bytes as f32 / 1_048_576.0),
    }
}

impl eframe::App for App {
    fn ui(&mut self, ui: &mut Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        let shared = self.shared.clone();
        // Held for the whole frame, so the session task waits that long.
        let mut guard = shared.lock().unwrap();
        let Shared {
            tree,
            state,
            notes,
            subscribed,
        } = &mut *guard;
        self.follow(tree);
        if !notes.is_empty() {
            self.notify(notes.join(" · "));
            notes.clear();
        }
        if subscribed.is_some() {
            self.s.subscribed = subscribed.take();
        }
        self.sample(tree);
        self.shortcuts(&ctx, tree);
        let connected = *state == State::Connected;

        let frame = egui::Frame::new().fill(pal::BG).inner_margin(PAD);
        egui::Panel::top("connection")
            .frame(frame)
            .show(ui, |ui| self.top_bar(ui, state));
        egui::Panel::bottom("status")
            .frame(frame.inner_margin(egui::Margin::symmetric(PAD as i8, GAP as i8)))
            .show(ui, |ui| self.status_bar(ui, tree, state));
        egui::Panel::right("details")
            .resizable(true)
            .default_size(DETAILS)
            .frame(frame)
            .show(ui, |ui| {
                egui::Panel::bottom("publish")
                    .frame(egui::Frame::new().inner_margin(egui::Margin::symmetric(0, GAP as i8)))
                    .show(ui, |ui| self.publish(ui, connected));
                egui::CentralPanel::default()
                    .frame(egui::Frame::new())
                    .show(ui, |ui| {
                        // Takes its own room rather than covering the
                        // right-aligned buttons.
                        ui.spacing_mut().scroll = egui::style::ScrollStyle::solid();
                        egui::ScrollArea::vertical()
                            .auto_shrink(false)
                            .show(ui, |ui| self.inspect(ui, tree));
                    });
            });
        let fading = egui::CentralPanel::default()
            .frame(frame)
            .show(ui, |ui| self.topics(ui, tree, connected))
            .inner;
        self.connection_modal(&ctx);
        self.confirm_modal(&ctx);
        drop(guard);

        match self.pending.take() {
            Some(true) => self.connect(&ctx),
            Some(false) => {
                if let Some(session) = self.session.take() {
                    session.disconnect();
                }
            }
            None => {}
        }
        // The tree is drawn last, so what a click or key changed there reaches
        // the other panels in the next frame; that one should not wait.
        let acted = ctx.input(|i| {
            i.pointer.any_click()
                || i.events
                    .iter()
                    .any(|e| matches!(e, egui::Event::Key { pressed: true, .. }))
        });
        if fading || acted {
            ctx.request_repaint();
        } else {
            // For the rate and the note.
            ctx.request_repaint_after(Duration::from_secs(1));
        }
    }

    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        let unsaved = (!self.s.remember).then(|| std::mem::take(&mut self.s.sent));
        eframe::set_value(storage, eframe::APP_KEY, &self.s);
        if let Some(sent) = unsaved {
            self.s.sent = sent;
        }
    }
}

fn heading(ui: &mut Ui, text: &str) {
    ui.label(RichText::new(text).family(semibold()).color(pal::MUTED));
}

/// Displays a label and a right-aligned widget.
fn field(ui: &mut Ui, label: &str, widget: impl egui::Widget) -> egui::Response {
    ui.horizontal(|ui| {
        ui.label(label);
        ui.with_layout(right_to_left(), |ui| ui.add(widget)).inner
    })
    .inner
}

/// Creates a modal with panel padding.
fn modal(ctx: &egui::Context, id: &str) -> egui::Modal {
    egui::Modal::new(egui::Id::new(id))
        .frame(egui::Frame::popup(&ctx.global_style()).inner_margin(PAD))
}

fn right_to_left() -> egui::Layout {
    egui::Layout::right_to_left(egui::Align::Center)
}

/// Lays out a single row from the right, avoiding full-height vertical centering.
fn right_row<R>(ui: &mut Ui, add: impl FnOnce(&mut Ui) -> R) -> R {
    ui.horizontal(|ui| ui.with_layout(right_to_left(), add).inner)
        .inner
}

fn qos(ui: &mut Ui, id: impl std::hash::Hash + std::fmt::Debug, value: &mut u8) {
    egui::ComboBox::from_id_salt(id)
        .selected_text(format!("QoS {value}"))
        .width(64.0)
        .show_ui(ui, |ui| {
            for level in 0..=2 {
                ui.selectable_value(value, level, format!("QoS {level}"));
            }
        });
}

#[cfg(test)]
mod tests {
    use chrono::Local;

    use super::*;
    use crate::tree::Message;

    fn msg(payload: &str) -> Message {
        Message {
            payload: payload.into(),
            qos: 0,
            retain: false,
            time: Local::now(),
            props: None,
        }
    }

    #[test]
    fn shown_entry_stays_put_and_falls_back_to_the_latest() {
        let mut tree = Tree::new(3);
        let a = tree.insert("a", msg("1")).unwrap();
        tree.insert("a", msg("2"));
        assert_eq!(shown(tree.node(a), None), Some(1));
        assert_eq!(shown(tree.node(a), Some(1)), Some(0));
        // Two more push message 1 out and message 2 to the front.
        tree.insert("a", msg("3"));
        tree.insert("a", msg("4"));
        assert_eq!(shown(tree.node(a), Some(2)), Some(0));
        assert_eq!(shown(tree.node(a), Some(1)), Some(2));
        assert_eq!(shown(tree.node(ROOT), None), None);
    }

    #[test]
    fn rows_rebuild_at_once_for_filter_or_stale_and_throttled_for_traffic() {
        let mut tree = Tree::new(3);
        let a = tree.insert("a", msg("1")).unwrap();
        let mut rows = Rows::default();
        assert!(!rows.update(&tree, ""));
        assert_eq!(rows.list, [(a, 0)]);
        let b = tree.insert("b", msg("2")).unwrap();
        assert!(rows.update(&tree, ""), "waits during traffic");
        assert_eq!(rows.list, [(a, 0)]);
        assert!(!rows.update(&tree, "b"));
        assert_eq!(rows.list, [(b, 0)]);
        tree.remove(b);
        rows.stale = true;
        rows.update(&tree, "b");
        assert!(rows.list.is_empty());
        tree.insert("c", msg("3"));
        rows.built = Instant::now().checked_sub(REBUILD);
        assert!(!rows.update(&tree, ""));
        assert_eq!(rows.list.len(), 2);
    }

    #[test]
    fn rendered_bounds_text_and_diffs_only_whole_messages() {
        let mut tree = Tree::new(3);
        let a = tree.insert("a", msg("x")).unwrap();
        let long = Message {
            payload: vec![b'y'; 2 * LIMIT],
            ..msg("")
        };
        tree.insert("a", long);
        let r = Rendered::new(tree.node(a), 1, (a, 2, View::Auto, true, false));
        assert_eq!(r.parts, [("y".repeat(LIMIT), pal::TEXT)]);
        let cut = "Showing 64.0 KiB of 128.0 KiB · Too long to diff";
        assert_eq!(r.cut.as_deref(), Some(cut));
        let r = Rendered::new(tree.node(a), 1, (a, 2, View::Auto, true, true));
        assert_eq!(r.parts.len(), 2, "one line removed, one added");
        assert_eq!(r.cut, None);
        let j = tree.insert("j", msg(r#"{"k":1}"#)).unwrap();
        let r = Rendered::new(tree.node(j), 0, (j, 1, View::Auto, false, false));
        assert!(r.json.is_some());
    }

    #[test]
    fn password_is_cleared_once_for_another_broker() {
        let saved = Profile::default();
        let mut e = Editing {
            profile: saved.clone(),
            password: "pw".into(),
            cleared: false,
            error: None,
        };
        e.profile.keep_alive = 60;
        e.forget(&saved);
        assert_eq!(e.password, "pw", "same broker");
        e.profile.port = 8883;
        e.forget(&saved);
        assert_eq!(e.password, "");
        e.password = "new".into();
        e.profile.username = "u".into();
        e.forget(&saved);
        assert_eq!(e.password, "new", "re-entered text stays");
    }

    #[test]
    fn history_drops_the_oldest_beyond_the_byte_limit() {
        let draft = |topic: &str, len| Draft {
            topic: topic.into(),
            payload: "x".repeat(len),
            ..Draft::default()
        };
        let topics = |sent: &[Draft]| sent.iter().map(|d| d.topic.clone()).collect::<Vec<_>>();
        let mut sent = Vec::new();
        for topic in ["a", "b", "c"] {
            record(&mut sent, draft(topic, 30_000));
        }
        assert_eq!(topics(&sent), ["c", "b"]);
        record(&mut sent, draft("d", RECENT_BYTES));
        assert_eq!(topics(&sent), ["c", "b"], "too large alone");
        record(&mut sent, draft("b", 30_000));
        assert_eq!(topics(&sent), ["b", "c"]);
    }
}
