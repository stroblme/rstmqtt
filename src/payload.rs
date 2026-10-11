// SPDX-License-Identifier: MIT OR Apache-2.0

//! Payload rendering as text, pretty JSON, hex or base64, plus JSON paths and line diffs.

use std::fmt::Write;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use similar::{ChangeTag, TextDiff};

/// Hex dump line length, including the newline.
const HEX_LINE: usize = 79;

/// Time limit for a minimal diff; then it approximates.
const DIFF_TIME: Duration = Duration::from_millis(100);

/// Payload display format. `Auto` detects the format per message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum View {
    Auto,
    Text,
    Json,
    Hex,
    Base64,
}

impl View {
    pub const ALL: [View; 5] = [Self::Auto, Self::Text, Self::Json, Self::Hex, Self::Base64];

    pub fn label(self) -> &'static str {
        match self {
            Self::Auto => "Auto",
            Self::Text => "Text",
            Self::Json => "JSON",
            Self::Hex => "Hex",
            Self::Base64 => "Base64",
        }
    }
}

/// Selects JSON for objects and arrays, text for other UTF-8, or hex for binary.
///
/// JSON scalars stay as text to avoid a single-leaf tree.
pub fn detect(payload: &[u8]) -> View {
    match serde_json::from_slice(payload) {
        Ok(Value::Object(_) | Value::Array(_)) => View::Json,
        _ if std::str::from_utf8(payload).is_ok() => View::Text,
        _ => View::Hex,
    }
}

/// Renders the payload in `view` for display and "Copy value".
pub fn render(payload: &[u8], view: View) -> String {
    match view {
        View::Auto => render(payload, detect(payload)),
        View::Text => String::from_utf8_lossy(payload).into_owned(),
        // serde_json sorts object keys, so the same content always pretty
        // prints the same way and diffs only show real changes.
        View::Json => serde_json::from_slice::<Value>(payload)
            .and_then(|v| serde_json::to_string_pretty(&v))
            .unwrap_or_else(|_| render(payload, View::Text)),
        View::Hex => hex(payload),
        View::Base64 => STANDARD.encode(payload),
    }
}

/// Formats a `hexdump -C` style dump without the trailing offset line.
fn hex(payload: &[u8]) -> String {
    let mut out = String::with_capacity(payload.len().div_ceil(16) * HEX_LINE);
    for (i, line) in payload.chunks(16).enumerate() {
        if i > 0 {
            out.push('\n');
        }
        let _ = write!(out, "{:08x}  ", i * 16);
        for j in 0..16 {
            // Missing bytes are padded so the ascii column lines up.
            let _ = match line.get(j) {
                Some(b) => write!(out, "{b:02x} "),
                None => write!(out, "   "),
            };
            if j == 7 {
                out.push(' ');
            }
        }
        out.push_str(" |");
        out.extend(line.iter().map(|&b| match b {
            b' '..=b'~' => b as char,
            _ => '.',
        }));
        out.push('|');
    }
    out
}

/// Returns the view to display the payload in within about `max` bytes of
/// text. Payloads longer than `max` are not parsed as JSON, and `Auto` checks
/// only their start for UTF-8.
pub fn resolve(payload: &[u8], view: View, max: usize) -> View {
    let long = payload.len() > max;
    match view {
        View::Auto if !long => detect(payload),
        View::Auto if std::str::from_utf8(head(payload, max)).is_err() => View::Hex,
        View::Auto => View::Text,
        View::Json if long => View::Text,
        view => view,
    }
}

/// Renders the start of the payload in the [`resolve`]d `view`, processing only
/// as much as fits in about `max` bytes of text. Returns the text and how many
/// payload bytes it shows.
pub fn render_prefix(payload: &[u8], view: View, max: usize) -> (String, usize) {
    let view = resolve(payload, view, max);
    let part = match view {
        View::Hex => &payload[..payload.len().min(max / HEX_LINE * 16)],
        View::Base64 => &payload[..payload.len().min(max / 4 * 3)],
        View::Text => head(payload, max),
        // JSON within `max`.
        _ => payload,
    };
    (render(part, view), part.len())
}

/// Returns up to `max` bytes from the start of `payload`, backing off from a
/// UTF-8 char that would be split.
fn head(payload: &[u8], max: usize) -> &[u8] {
    if payload.len() <= max {
        return payload;
    }
    let mut end = max;
    // At most three continuation bytes follow a char's first byte.
    while end > 0 && max - end < 3 && payload[end] & 0xC0 == 0x80 {
        end -= 1;
    }
    &payload[..end]
}

/// Returns a single-line payload preview for a topic row. Examines only the
/// start of the payload, enough for `max` chars unless it is mostly whitespace.
pub fn preview(payload: &[u8], max: usize) -> String {
    if payload.is_empty() {
        return "(empty)".into();
    }
    let start = head(payload, 8 * (max + 1));
    let Ok(text) = std::str::from_utf8(start) else {
        return format!("<{} bytes>", payload.len());
    };
    // The words joined by single spaces, stopping one char past `max` so a
    // large payload is not copied just to be cut.
    let mut out: String = text
        .split_whitespace()
        .flat_map(|w| " ".chars().chain(w.chars()))
        .skip(1)
        .take(max + 1)
        .collect();
    let over = out.chars().count() > max;
    if over {
        out.pop();
    }
    if over || start.len() < payload.len() {
        out.push('…');
    }
    out
}

/// JSON path segment: an object key or array index.
#[derive(Clone, Copy, Debug)]
pub enum Seg<'a> {
    Key(&'a str),
    Index(usize),
}

/// Formats a JSON path as `a.b[0].c` for "Copy path".
///
/// Non-identifier keys use JSON-escaped bracket notation, such as `["x y"]`.
pub fn json_path(segs: &[Seg]) -> String {
    let mut out = String::new();
    for seg in segs {
        let _ = match seg {
            Seg::Index(i) => write!(out, "[{i}]"),
            Seg::Key(k) if ident(k) => {
                if !out.is_empty() {
                    out.push('.');
                }
                write!(out, "{k}")
            }
            Seg::Key(k) => write!(out, "[{}]", serde_json::to_string(k).unwrap()),
        };
    }
    out
}

fn ident(key: &str) -> bool {
    let mut chars = key.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Line change between two messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Change {
    Same,
    Added,
    Removed,
}

/// Diffs two rendered messages, including unchanged lines, within [`DIFF_TIME`].
///
/// Render both with the same view; JSON rendering ignores object key order.
pub fn diff(old: &str, new: &str) -> Vec<(Change, String)> {
    TextDiff::configure()
        .timeout(DIFF_TIME)
        .diff_lines(old, new)
        .iter_all_changes()
        .map(|c| {
            let change = match c.tag() {
                ChangeTag::Equal => Change::Same,
                ChangeTag::Insert => Change::Added,
                ChangeTag::Delete => Change::Removed,
            };
            (change, c.value().trim_end_matches(['\r', '\n']).to_owned())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_json_containers_text_and_binary() {
        assert_eq!(detect(br#"{"a": 1}"#), View::Json);
        assert_eq!(detect(b" [1, 2] "), View::Json);
        assert_eq!(detect(b"21.4"), View::Text);
        assert_eq!(detect(b"\"x\""), View::Text);
        assert_eq!(detect(b"on"), View::Text);
        assert_eq!(detect(b""), View::Text);
        assert_eq!(detect(&[0xff, 0x00, 0x10]), View::Hex);
    }

    #[test]
    fn hex_dump_layout() {
        let payload = b"Hello, MQTT!\x00\x01\x7f\xffab\n\x80";
        let expected = format!(
            "00000000  48 65 6c 6c 6f 2c 20 4d  51 54 54 21 00 01 7f ff  |Hello, MQTT!....|\n\
             00000010  61 62 0a 80{}|ab..|",
            " ".repeat(39)
        );
        assert_eq!(render(payload, View::Hex), expected);
        assert_eq!(render(b"", View::Hex), "");
    }

    #[test]
    fn json_is_pretty_printed_with_sorted_keys() {
        assert_eq!(
            render(br#"{"b":1,"a":[true,null]}"#, View::Json),
            "{\n  \"a\": [\n    true,\n    null\n  ],\n  \"b\": 1\n}"
        );
        assert_eq!(render(b"{not json", View::Json), "{not json");
    }

    #[test]
    fn base64_is_padded_standard() {
        assert_eq!(render(b"hello", View::Base64), "aGVsbG8=");
    }

    #[test]
    fn preview_is_one_line_cut_by_chars() {
        assert_eq!(preview(b"  {\n  \"a\":\t1\n}\n", 80), r#"{ "a": 1 }"#);
        assert_eq!(preview("héllo wörld".as_bytes(), 4), "héll…");
        assert_eq!(preview("héll".as_bytes(), 4), "héll");
        assert_eq!(preview(&[0xff, 0xfe, 0x00], 80), "<3 bytes>");
        assert_eq!(preview(b"", 80), "(empty)");
    }

    #[test]
    fn preview_examines_only_the_start() {
        // 24 bytes are examined, ending inside an `é`.
        let split = "a".to_owned() + &"é".repeat(100);
        assert_eq!(preview(split.as_bytes(), 2), "aé…");
        let mut late = b"on".to_vec();
        late.extend([b' '; 30]);
        late.push(0xff);
        assert_eq!(preview(&late, 2), "on…");
    }

    #[test]
    fn render_prefix_processes_only_what_fits() {
        // Two whole hex lines.
        let max = 2 * HEX_LINE + 10;
        assert_eq!(resolve(&[0xff; 100], View::Auto, max), View::Hex);
        let (text, shown) = render_prefix(&[0xff; 100], View::Hex, max);
        assert_eq!((text.lines().count(), shown), (2, 32));
        assert_eq!(render_prefix(&[0; 100], View::Base64, 8).1, 6);
        // Long JSON shows as text, cut before a split char.
        let json = "[\"é\"]".as_bytes();
        assert_eq!(resolve(json, View::Auto, 3), View::Text);
        assert_eq!(resolve(json, View::Json, 3), View::Text);
        assert_eq!(render_prefix(json, View::Json, 3), ("[\"".into(), 2));
        assert_eq!(resolve(json, View::Auto, usize::MAX), View::Json);
        let all = render_prefix(br#"{"a":1}"#, View::Json, usize::MAX);
        assert_eq!(all, ("{\n  \"a\": 1\n}".into(), 7));
    }

    #[test]
    fn json_paths() {
        use Seg::*;
        assert_eq!(
            json_path(&[Key("a"), Key("b"), Index(0), Key("c")]),
            "a.b[0].c"
        );
        assert_eq!(
            json_path(&[Key("a"), Key("x y"), Index(2)]),
            r#"a["x y"][2]"#
        );
        assert_eq!(
            json_path(&[Key("1st"), Key("q\"t"), Key("_ok")]),
            r#"["1st"]["q\"t"]._ok"#
        );
        assert_eq!(json_path(&[Index(3), Key("id")]), "[3].id");
        assert_eq!(json_path(&[]), "");
    }

    #[test]
    fn reordered_json_keys_diff_as_same() {
        let old = render(br#"{"a":1,"b":[1,2]}"#, View::Json);
        let new = render(br#"{"b":[1,2],"a":1}"#, View::Json);
        assert!(diff(&old, &new).iter().all(|(c, _)| *c == Change::Same));
    }

    #[test]
    fn changed_value_diffs_as_one_removed_and_one_added_line() {
        let old = render(br#"{"a":1,"b":2}"#, View::Json);
        let new = render(br#"{"a":1,"b":3}"#, View::Json);
        let changed: Vec<_> = diff(&old, &new)
            .into_iter()
            .filter(|(c, _)| *c != Change::Same)
            .collect();
        assert_eq!(
            changed,
            [
                (Change::Removed, r#"  "b": 2"#.to_owned()),
                (Change::Added, r#"  "b": 3"#.to_owned()),
            ]
        );
    }
}
