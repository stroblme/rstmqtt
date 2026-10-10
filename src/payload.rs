// SPDX-License-Identifier: MIT OR Apache-2.0

//! Turning payload bytes into something to read: text, pretty JSON, hex and
//! base64, JSON paths for copying, and line diffs between two messages.

use std::fmt::Write;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use similar::{ChangeTag, TextDiff};

/// How a payload is shown. `Auto` picks one of the others per message.
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

/// Picks the view a payload reads best in.
///
/// Only objects and arrays count as JSON: a sensor value like `21.4` or `true`
/// is valid JSON too, but a tree with a single leaf is no use.
pub fn detect(payload: &[u8]) -> View {
    match serde_json::from_slice(payload) {
        Ok(Value::Object(_) | Value::Array(_)) => View::Json,
        _ if std::str::from_utf8(payload).is_ok() => View::Text,
        _ => View::Hex,
    }
}

/// The payload as `view` shows it, which is also what "copy value" copies.
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

/// A `hexdump -C` style dump, minus its trailing offset line.
fn hex(payload: &[u8]) -> String {
    let lines: Vec<String> = payload
        .chunks(16)
        .enumerate()
        .map(|(i, line)| {
            let mut out = format!("{:08x}  ", i * 16);
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
            let ascii: String = line
                .iter()
                .map(|&b| match b {
                    b' '..=b'~' => b as char,
                    _ => '.',
                })
                .collect();
            let _ = write!(out, " |{ascii}|");
            out
        })
        .collect();
    lines.join("\n")
}

/// The one-line value shown next to a topic in the tree.
pub fn preview(payload: &[u8], max: usize) -> String {
    let Ok(text) = std::str::from_utf8(payload) else {
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
    if out.chars().count() > max {
        out.pop();
        out.push('…');
    }
    out
}

/// A step into a JSON value: an object key or an array index.
#[derive(Clone, Copy, Debug)]
pub enum Seg<'a> {
    Key(&'a str),
    Index(usize),
}

/// The path to a JSON node in the usual `a.b[0].c` notation, for "copy path".
///
/// Keys that are not identifiers are written as `["x y"]`, quoted and escaped
/// like a JSON string, so dots or brackets in a key cannot be misread.
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

/// What happened to a line between two messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Change {
    Same,
    Added,
    Removed,
}

/// A line diff of two rendered messages, every line of both included.
///
/// Both sides should be rendered with the same view; for JSON that makes
/// reordered keys compare equal.
pub fn diff(old: &str, new: &str) -> Vec<(Change, String)> {
    TextDiff::from_lines(old, new)
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
