// SPDX-License-Identifier: MIT OR Apache-2.0

//! Topic tree with per-topic message history. Arena indices keep GUI selections
//! stable as the session inserts nodes.

use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

/// Index of the root, which has no name or messages.
pub const ROOT: usize = 0;

/// Received MQTT publish for v3.1.1 or v5.
#[derive(Clone, Debug)]
pub struct Message {
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    /// Local receipt time; MQTT provides no message timestamp.
    pub time: DateTime<Local>,
    /// MQTT v5 only.
    pub props: Option<Box<Props>>,
}

/// MQTT v5 publish properties shown in the GUI.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Props {
    pub content_type: Option<String>,
    pub user: Vec<(String, String)>,
    pub response_topic: Option<String>,
    pub correlation: Option<Vec<u8>>,
    /// Message expiry interval in seconds.
    pub expiry: Option<u32>,
    /// Whether the sender marks the payload as UTF-8.
    pub utf8: Option<bool>,
}

/// Topic level with its children and any messages received on its full path.
pub struct Node {
    /// Level name; may be empty, as in `/a` or `a//b`.
    pub name: String,
    /// The root is its own parent.
    pub parent: usize,
    /// Child indices sorted by name.
    pub children: BTreeMap<String, usize>,
    /// Recent messages, oldest first, limited to the tree's cap.
    pub history: VecDeque<Message>,
    /// Total messages received on this topic, including evicted messages.
    pub received: u64,
    /// Total messages received in this subtree, including this topic.
    pub subtree_messages: u64,
    /// Topics in this subtree that have received a message.
    pub subtree_topics: u32,
    /// Last receipt time on this topic, used for highlighting.
    pub updated: Instant,
    /// Last receipt time in this subtree, used to highlight collapsed parents.
    pub subtree_updated: Instant,
    /// Whether the node is expanded; ignored while filtering.
    pub expanded: bool,
}

impl Node {
    fn new(name: String, parent: usize) -> Self {
        Self {
            name,
            parent,
            children: BTreeMap::new(),
            history: VecDeque::new(),
            received: 0,
            subtree_messages: 0,
            subtree_topics: 0,
            updated: Instant::now(),
            subtree_updated: Instant::now(),
            expanded: false,
        }
    }
}

/// Topic arena of [`Node`]s rooted at [`ROOT`].
pub struct Tree {
    nodes: Vec<Node>,
    /// Messages kept per topic.
    cap: usize,
    /// Running total of history payload bytes for the status bar.
    bytes: usize,
}

impl Tree {
    /// Creates an empty tree retaining the latest `cap` messages per topic.
    pub fn new(cap: usize) -> Self {
        Self {
            nodes: vec![Node::new(String::new(), ROOT)],
            cap,
            bytes: 0,
        }
    }

    /// Returns the node at `id`; panics if the index is out of bounds.
    pub fn node(&self, id: usize) -> &Node {
        &self.nodes[id]
    }

    /// Returns a mutable node for GUI expansion changes.
    pub fn node_mut(&mut self, id: usize) -> &mut Node {
        &mut self.nodes[id]
    }

    /// Returns the total payload bytes in all histories.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Inserts `msg` under `topic`, creating missing levels, and returns its node.
    /// Empty payloads remove leaf topics and return `None`, matching retained
    /// message clearing in MQTT Explorer; branches keep the empty message.
    pub fn insert(&mut self, topic: &str, msg: Message) -> Option<usize> {
        if msg.payload.is_empty() {
            match self.find(topic) {
                Some(id) if self.nodes[id].children.is_empty() => {
                    self.remove(id);
                    return None;
                }
                None => return None,
                Some(_) => {}
            }
        }
        let mut path = vec![ROOT];
        let mut id = ROOT;
        for level in topic.split('/') {
            id = match self.nodes[id].children.get(level) {
                Some(&child) => child,
                None => {
                    let child = self.nodes.len();
                    self.nodes.push(Node::new(level.into(), id));
                    self.nodes[id].children.insert(level.into(), child);
                    child
                }
            };
            path.push(id);
        }
        let first = self.nodes[id].received == 0;
        let now = Instant::now();
        for &up in &path {
            let node = &mut self.nodes[up];
            node.subtree_messages += 1;
            node.subtree_topics += u32::from(first);
            node.subtree_updated = now;
        }
        let node = &mut self.nodes[id];
        node.received += 1;
        node.updated = now;
        self.bytes += msg.payload.len();
        node.history.push_back(msg);
        self.bytes -= evict(&mut node.history, self.cap);
        Some(id)
    }

    /// Returns the node index for the exact topic path, if present.
    pub fn find(&self, topic: &str) -> Option<usize> {
        topic.split('/').try_fold(ROOT, |id, level| {
            self.nodes[id].children.get(level).copied()
        })
    }

    /// Checks whether `id` is still attached to the tree; excludes the root.
    /// Selections can outlive cleared topics.
    pub fn contains(&self, id: usize) -> bool {
        id != ROOT && self.find(&self.path(id)) == Some(id)
    }

    /// Returns the full topic path of `id`, or an empty string for the root.
    pub fn path(&self, id: usize) -> String {
        let mut levels = Vec::new();
        let mut id = id;
        while id != ROOT {
            levels.push(self.nodes[id].name.as_str());
            id = self.nodes[id].parent;
        }
        levels.reverse();
        levels.join("/")
    }

    /// Returns `id` and its descendants in preorder, with siblings sorted by name.
    pub fn subtree(&self, id: usize) -> Vec<usize> {
        let mut ids = Vec::new();
        let mut stack = vec![id];
        while let Some(id) = stack.pop() {
            ids.push(id);
            stack.extend(self.nodes[id].children.values().rev());
        }
        ids
    }

    /// Removes `id` and its descendants, then prunes ancestors with no children
    /// or messages. Reinserting a topic starts fresh. Removed indices remain
    /// valid for stale selections; removing them again or the root does nothing.
    // ponytail: the slots stay in the arena, so it grows with every clear; add a
    // free list or compact it if long sessions with many clears show the growth.
    pub fn remove(&mut self, id: usize) {
        if !self.contains(id) {
            return;
        }
        for i in self.subtree(id) {
            let history = std::mem::take(&mut self.nodes[i].history);
            self.bytes -= history.iter().map(|m| m.payload.len()).sum::<usize>();
        }
        let node = &self.nodes[id];
        let (messages, topics) = (node.subtree_messages, node.subtree_topics);
        let mut up = id;
        while up != ROOT {
            up = self.nodes[up].parent;
            let node = &mut self.nodes[up];
            node.subtree_messages -= messages;
            node.subtree_topics -= topics;
        }
        let mut gone = id;
        loop {
            let parent = self.nodes[gone].parent;
            let name = self.nodes[gone].name.clone();
            let node = &mut self.nodes[parent];
            node.children.remove(&name);
            if parent == ROOT || !node.children.is_empty() || node.received > 0 {
                break;
            }
            gone = parent;
        }
    }

    /// Returns visible `(id, depth)` rows in display order, excluding the root.
    /// An empty `filter` respects expansion flags. Otherwise, shows case-insensitive
    /// topic or latest-value matches and their ancestors, ignoring expansion.
    // ponytail: O(n) per call while filtering; cache the match set per tree
    // generation if 100k+ topics stutter.
    pub fn rows(&self, filter: &str) -> Vec<(usize, usize)> {
        let filter = filter.to_lowercase();
        let mut rows = Vec::new();
        let mut path = String::new();
        for &child in self.nodes[ROOT].children.values() {
            self.walk(child, 0, &filter, &mut path, &mut rows);
        }
        rows
    }

    /// Appends visible rows in `id`'s subtree and returns whether any matched.
    /// `path` is the parent's lowercase topic path and is restored before returning.
    fn walk(
        &self,
        id: usize,
        depth: usize,
        filter: &str,
        path: &mut String,
        rows: &mut Vec<(usize, usize)>,
    ) -> bool {
        let node = &self.nodes[id];
        let len = path.len();
        if depth > 0 {
            path.push('/');
        }
        path.push_str(&node.name.to_lowercase());
        let hit = filter.is_empty()
            || path.contains(filter)
            || node.history.back().is_some_and(|m| {
                String::from_utf8_lossy(&m.payload)
                    .to_lowercase()
                    .contains(filter)
            });
        let at = rows.len();
        rows.push((id, depth));
        let mut below = false;
        if node.expanded || !filter.is_empty() {
            for &child in node.children.values() {
                below |= self.walk(child, depth + 1, filter, path, rows);
            }
        }
        path.truncate(len);
        if !hit && !below {
            rows.truncate(at);
        }
        hit || below
    }
}

/// Evicts the oldest messages beyond `cap` and returns their total payload bytes.
fn evict(history: &mut VecDeque<Message>, cap: usize) -> usize {
    let excess = history.len().saturating_sub(cap);
    history.drain(..excess).map(|m| m.payload.len()).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(payload: &str) -> Message {
        Message {
            payload: payload.into(),
            qos: 0,
            retain: false,
            time: Local::now(),
            props: None,
        }
    }

    fn payloads(tree: &Tree, id: usize) -> Vec<&str> {
        let history = &tree.node(id).history;
        history
            .iter()
            .map(|m| std::str::from_utf8(&m.payload).unwrap())
            .collect()
    }

    /// `(received, subtree_messages, subtree_topics)`.
    fn counts(tree: &Tree, id: usize) -> (u64, u64, u32) {
        let node = tree.node(id);
        (node.received, node.subtree_messages, node.subtree_topics)
    }

    #[test]
    fn splits_levels_and_rebuilds_the_topic() {
        let mut tree = Tree::new(10);
        for topic in ["a/b/c", "/a", "a//b", "a/", "$SYS/broker/uptime"] {
            let id = tree.insert(topic, msg("x")).unwrap();
            assert_eq!(tree.path(id), topic);
            assert_eq!(tree.find(topic), Some(id), "{topic}");
        }
        assert_eq!(tree.path(ROOT), "");
        let top: Vec<_> = tree.node(ROOT).children.keys().collect();
        assert_eq!(top, ["", "$SYS", "a"]);
        // `a/` and `a//b` share the empty level below `a`.
        let empty = tree.find("a/").unwrap();
        assert_eq!(tree.node(tree.find("a//b").unwrap()).parent, empty);
        assert_eq!(tree.find("a/b/c/d"), None);
        assert_eq!(tree.find("b"), None);
    }

    #[test]
    fn counts_messages_and_topics_at_and_below_each_level() {
        let mut tree = Tree::new(10);
        tree.insert("home/kitchen/temp", msg("20"));
        tree.insert("home/kitchen/temp", msg("21"));
        tree.insert("home/hall", msg("on"));
        tree.insert("home", msg("x"));
        let [home, kitchen, temp] =
            ["home", "home/kitchen", "home/kitchen/temp"].map(|t| tree.find(t).unwrap());
        assert_eq!(counts(&tree, ROOT), (0, 4, 3));
        assert_eq!(counts(&tree, home), (1, 4, 3));
        assert_eq!(counts(&tree, kitchen), (0, 2, 1));
        assert_eq!(counts(&tree, temp), (2, 2, 1));
    }

    #[test]
    fn marks_ancestors_updated_but_keeps_their_own_time() {
        let mut tree = Tree::new(10);
        let a = tree.insert("a", msg("1")).unwrap();
        let own = tree.node(a).updated;
        let b = tree.insert("a/b", msg("2")).unwrap();
        assert_eq!(tree.node(a).updated, own);
        assert_eq!(tree.node(a).subtree_updated, tree.node(b).updated);
        assert_eq!(tree.node(ROOT).subtree_updated, tree.node(b).updated);
    }

    #[test]
    fn evicts_the_oldest_beyond_the_cap() {
        let mut tree = Tree::new(2);
        let a = tree.insert("a", msg("1")).unwrap();
        tree.insert("a", msg("22"));
        tree.insert("a", msg("333"));
        assert_eq!(payloads(&tree, a), ["22", "333"]);
        assert_eq!(tree.node(a).received, 3);
        assert_eq!(tree.bytes(), 5);
        tree.insert("b", msg("4444"));
        assert_eq!(tree.bytes(), 9);
    }

    #[test]
    fn remove_prunes_only_ancestors_left_empty() {
        let mut tree = Tree::new(10);
        tree.insert("a/b/c/d", msg("1"));
        tree.insert("a/b", msg("2"));
        tree.insert("a/x/y", msg("3"));
        tree.insert("a/x/z", msg("4"));
        tree.insert("p/q/r", msg("5"));
        // `c` goes, `b` stays for its own message.
        tree.remove(tree.find("a/b/c/d").unwrap());
        assert_eq!(tree.find("a/b/c"), None);
        assert!(tree.find("a/b").is_some());
        // `x` stays for `z`.
        tree.remove(tree.find("a/x/y").unwrap());
        assert_eq!(tree.find("a/x/y"), None);
        assert!(tree.find("a/x/z").is_some());
        // Everything up to the root goes, the root and `a` stay.
        tree.remove(tree.find("p/q/r").unwrap());
        let top: Vec<_> = tree.node(ROOT).children.keys().collect();
        assert_eq!(top, ["a"]);
    }

    #[test]
    fn remove_fixes_counts_and_bytes_and_allows_inserting_again() {
        let mut tree = Tree::new(10);
        tree.insert("a/b/c", msg("1"));
        tree.insert("a/b/c", msg("22"));
        tree.insert("a/b/d", msg("333"));
        tree.insert("a/e", msg("4444"));
        let a = tree.find("a").unwrap();
        tree.remove(tree.find("a/b").unwrap());
        assert_eq!(counts(&tree, a), (0, 1, 1));
        assert_eq!(counts(&tree, ROOT), (0, 1, 1));
        assert_eq!(tree.bytes(), 4);
        let c = tree.insert("a/b/c", msg("55555")).unwrap();
        assert_eq!(counts(&tree, c), (1, 1, 1));
        assert_eq!(payloads(&tree, c), ["55555"]);
        assert_eq!(counts(&tree, a), (0, 2, 2));
        assert_eq!(tree.bytes(), 9);
        tree.remove(a);
        assert!(tree.node(ROOT).children.is_empty());
        assert_eq!(counts(&tree, ROOT), (0, 0, 0));
        assert_eq!(tree.bytes(), 0);
    }

    #[test]
    fn rows_open_only_expanded_nodes() {
        let mut tree = Tree::new(10);
        tree.insert("a/b/c", msg("1"));
        tree.insert("a/d", msg("2"));
        tree.insert("e", msg("3"));
        let [a, b, c, d, e] = ["a", "a/b", "a/b/c", "a/d", "e"].map(|t| tree.find(t).unwrap());
        assert_eq!(tree.rows(""), [(a, 0), (e, 0)]);
        tree.node_mut(a).expanded = true;
        assert_eq!(tree.rows(""), [(a, 0), (b, 1), (d, 1), (e, 0)]);
        tree.node_mut(b).expanded = true;
        assert_eq!(tree.rows(""), [(a, 0), (b, 1), (c, 2), (d, 1), (e, 0)]);
        // A collapsed parent hides an expanded child.
        tree.node_mut(a).expanded = false;
        assert_eq!(tree.rows(""), [(a, 0), (e, 0)]);
    }

    /// Creates a sample tree with all nodes collapsed.
    fn house() -> Tree {
        let mut tree = Tree::new(10);
        tree.insert("home/kitchen/temp", msg("21.5"));
        tree.insert("home/hall/light", msg("dim"));
        tree.insert("home/hall/light", msg("ON"));
        tree.insert("home/hall/door", msg("closed"));
        tree.insert("garden/pump", msg("off"));
        tree
    }

    #[test]
    fn filter_matches_topic_or_latest_value_ignoring_case() {
        let tree = house();
        let [home, kitchen, temp, hall, light] = [
            "home",
            "home/kitchen",
            "home/kitchen/temp",
            "home/hall",
            "home/hall/light",
        ]
        .map(|t| tree.find(t).unwrap());
        assert_eq!(tree.rows("KITCHEN/T"), [(home, 0), (kitchen, 1), (temp, 2)]);
        assert_eq!(tree.rows("on"), [(home, 0), (hall, 1), (light, 2)]);
        assert!(tree.rows("dim").is_empty());
    }

    #[test]
    fn filter_keeps_ancestors_and_hides_unrelated_branches() {
        let tree = house();
        let [garden, pump, home, hall, door, light] = [
            "garden",
            "garden/pump",
            "home",
            "home/hall",
            "home/hall/door",
            "home/hall/light",
        ]
        .map(|t| tree.find(t).unwrap());
        // A matching level brings its whole subtree.
        assert_eq!(
            tree.rows("hall"),
            [(home, 0), (hall, 1), (door, 2), (light, 2)]
        );
        assert_eq!(tree.rows("pump"), [(garden, 0), (pump, 1)]);
        assert!(tree.rows("cellar").is_empty());
    }

    #[test]
    fn empty_payload_clears_a_leaf_but_not_a_branch() {
        let mut tree = Tree::new(10);
        let leaf = tree.insert("a/b/c", msg("1")).unwrap();
        tree.insert("a", msg("2"));
        assert_eq!(tree.insert("a/b/c", msg("")), None);
        assert!(!tree.contains(leaf));
        assert_eq!(tree.find("a/b"), None, "emptied ancestors go too");
        tree.remove(leaf);
        assert_eq!(
            counts(&tree, ROOT),
            (0, 1, 1),
            "a stale remove does nothing"
        );

        tree.insert("a/x", msg("3"));
        let a = tree.insert("a", msg("")).unwrap();
        assert_eq!(
            payloads(&tree, a),
            ["2", ""],
            "a branch keeps the empty message"
        );
        assert_eq!(tree.insert("never/seen", msg("")), None);
        assert_eq!(tree.find("never"), None);
    }
}
