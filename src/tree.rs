// SPDX-License-Identifier: MIT OR Apache-2.0

//! Topic tree with per-topic message history. Arena indices keep GUI selections
//! stable as the session inserts nodes; removed slots are reused.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Instant;

use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

/// Index of the root, which has no name or messages.
pub const ROOT: usize = 0;

/// Approximate memory limit for topics and messages; beyond it the oldest
/// messages tree-wide are evicted.
const BUDGET: usize = 256 << 20;

/// Most topic levels kept; messages that would create more are dropped.
const MAX_NODES: usize = 100_000;

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
    /// Whether any message received on this topic had the retain flag; survives
    /// history eviction.
    pub retained: bool,
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
            retained: false,
        }
    }
}

/// Topic arena of [`Node`]s rooted at [`ROOT`].
pub struct Tree {
    nodes: Vec<Node>,
    /// Removed slots for reuse.
    free: Vec<usize>,
    /// Messages kept per topic.
    cap: usize,
    /// [`BUDGET`], lowered by tests.
    budget: usize,
    /// Running total of accounted bytes for the status bar.
    bytes: usize,
    /// Accounted bytes of the nodes alone, which eviction can't reclaim.
    structure: usize,
    /// `(receipt time, node)` of each topic's oldest message, for global eviction.
    // ponytail: wall-clock order, so a clock step back evicts out of arrival
    // order; key by a message sequence number if that ever matters.
    oldest: BTreeSet<(DateTime<Local>, usize)>,
    /// Messages dropped for exceeding the node or memory limit.
    dropped: u64,
    /// Inserts and removals so far, for the GUI to notice changes.
    changes: u64,
}

impl Tree {
    /// Creates an empty tree retaining the latest `cap` messages per topic.
    pub fn new(cap: usize) -> Self {
        Self {
            nodes: vec![Node::new(String::new(), ROOT)],
            free: Vec::new(),
            cap,
            budget: BUDGET,
            bytes: 0,
            structure: 0,
            oldest: BTreeSet::new(),
            dropped: 0,
            changes: 0,
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

    /// Returns the accounted bytes of all topics and messages, within the budget.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Returns how many messages were dropped for exceeding the node or memory limit.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Returns a count that grows with every insert and removal.
    pub fn changes(&self) -> u64 {
        self.changes
    }

    /// Inserts `msg` under `topic`, creating missing levels, and returns its node.
    /// Evicts the oldest messages tree-wide to stay within the memory budget.
    /// Drops the message and returns `None` if its new levels would exceed the
    /// node limit, or if it can't fit even after evicting all other messages.
    pub fn insert(&mut self, topic: &str, msg: Message) -> Option<usize> {
        let (mut new, mut grow) = (0, 0);
        let mut parent = Some(ROOT);
        for level in topic.split('/') {
            parent = parent.and_then(|p| self.nodes[p].children.get(level).copied());
            if parent.is_none() {
                new += 1;
                grow += node_cost(level);
            }
        }
        let live = self.nodes.len() - 1 - self.free.len();
        if live + new > MAX_NODES || self.structure + grow + cost(&msg) > self.budget {
            self.dropped += 1;
            return None;
        }
        self.structure += grow;
        self.bytes += grow;
        let mut path = vec![ROOT];
        let mut id = ROOT;
        for level in topic.split('/') {
            id = match self.nodes[id].children.get(level) {
                Some(&child) => child,
                None => {
                    let node = Node::new(level.into(), id);
                    let child = match self.free.pop() {
                        Some(child) => {
                            self.nodes[child] = node;
                            child
                        }
                        None => {
                            self.nodes.push(node);
                            self.nodes.len() - 1
                        }
                    };
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
        node.retained |= msg.retain;
        if let Some(m) = node.history.front() {
            self.oldest.remove(&(m.time, id));
        }
        self.bytes += cost(&msg);
        node.history.push_back(msg);
        self.bytes -= evict(&mut node.history, self.cap);
        self.oldest
            .extend(node.history.front().map(|m| (m.time, id)));
        while self.bytes > self.budget
            && let Some((_, old)) = self.oldest.pop_first()
        {
            let history = &mut self.nodes[old].history;
            self.bytes -= history.pop_front().as_ref().map_or(0, cost);
            if let Some(m) = history.front() {
                self.oldest.insert((m.time, old));
            }
        }
        self.changes += 1;
        Some(id)
    }

    /// Returns the node index for the exact topic path, if present.
    pub fn find(&self, topic: &str) -> Option<usize> {
        topic.split('/').try_fold(ROOT, |id, level| {
            self.nodes[id].children.get(level).copied()
        })
    }

    /// Checks whether `id` is still attached to the tree; excludes the root.
    /// Selections can outlive removed topics, whose slots may hold newer topics.
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
    /// or messages. Reinserting a topic starts fresh. Freed slots are reused by
    /// later topics; removing a detached index or the root does nothing.
    pub fn remove(&mut self, id: usize) {
        if !self.contains(id) {
            return;
        }
        self.changes += 1;
        let node = &self.nodes[id];
        let (messages, topics) = (node.subtree_messages, node.subtree_topics);
        let mut up = id;
        while up != ROOT {
            up = self.nodes[up].parent;
            let node = &mut self.nodes[up];
            node.subtree_messages -= messages;
            node.subtree_topics -= topics;
        }
        let mut freed = self.subtree(id);
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
            freed.push(gone);
        }
        for i in freed {
            let node = &mut self.nodes[i];
            if let Some(m) = node.history.front() {
                self.oldest.remove(&(m.time, i));
            }
            let size = node_cost(&node.name);
            self.structure -= size;
            self.bytes -= size + node.history.iter().map(cost).sum::<usize>();
            // Detached at the root under an empty name, so `contains` is false.
            *node = Node::new(String::new(), ROOT);
            self.free.push(i);
        }
    }

    /// Returns visible `(id, depth)` rows in display order, excluding the root.
    /// An empty `filter` respects expansion flags. Otherwise, shows case-insensitive
    /// topic or latest-value matches and their ancestors, ignoring expansion.
    pub fn rows(&self, filter: &str) -> Vec<(usize, usize)> {
        let filter = filter.to_lowercase();
        let mut rows = Vec::new();
        // The lowercase topic path of the innermost open level.
        let mut path = String::new();
        // Open levels as `(parent path length, row, matched, unvisited children)`,
        // iterative since topics can nest 32,768 levels deep.
        let mut stack = vec![(0, 0, true, self.nodes[ROOT].children.values())];
        while let Some((_, _, _, children)) = stack.last_mut() {
            let Some(&id) = children.next() else {
                let (len, at, hit, _) = stack.pop().unwrap();
                path.truncate(len);
                // Keeps a non-matching level only for matches below it.
                if !hit && rows.len() == at + 1 {
                    rows.pop();
                }
                continue;
            };
            let node = &self.nodes[id];
            let depth = stack.len() - 1;
            let len = path.len();
            if depth > 0 {
                path.push('/');
            }
            path.push_str(&node.name.to_lowercase());
            let hit = filter.is_empty()
                || path.contains(&filter)
                || node
                    .history
                    .back()
                    .is_some_and(|m| contains(&m.payload, &filter));
            rows.push((id, depth));
            if node.expanded || !filter.is_empty() {
                stack.push((len, rows.len() - 1, hit, node.children.values()));
            } else {
                path.truncate(len);
            }
        }
        rows
    }
}

/// Checks whether `payload` contains the lowercase, non-empty `filter`, ignoring
/// case. Compares bytes for an ASCII filter rather than copying the payload.
fn contains(payload: &[u8], filter: &str) -> bool {
    if filter.is_ascii() {
        let filter = filter.as_bytes();
        payload
            .windows(filter.len())
            .any(|w| w.eq_ignore_ascii_case(filter))
    } else {
        String::from_utf8_lossy(payload)
            .to_lowercase()
            .contains(filter)
    }
}

/// Evicts the oldest messages beyond `cap` and returns their accounted bytes.
fn evict(history: &mut VecDeque<Message>, cap: usize) -> usize {
    let excess = history.len().saturating_sub(cap);
    history.drain(..excess).map(|m| cost(&m)).sum()
}

/// Accounted bytes of a stored message, including its MQTT v5 properties.
fn cost(msg: &Message) -> usize {
    let text = |s: &Option<String>| s.as_ref().map_or(0, String::len);
    let props = msg.props.as_deref().map_or(0, |p| {
        size_of::<Props>()
            + text(&p.content_type)
            + text(&p.response_topic)
            + p.correlation.as_ref().map_or(0, Vec::len)
            + p.user
                .iter()
                .map(|(k, v)| size_of::<(String, String)>() + k.len() + v.len())
                .sum::<usize>()
    });
    size_of::<Message>() + msg.payload.len() + props
}

/// Accounted bytes of a topic level: the node, its name twice (also the key in
/// its parent's children) and its entry in the eviction order.
fn node_cost(name: &str) -> usize {
    size_of::<Node>()
        + size_of::<(String, usize)>()
        + size_of::<(DateTime<Local>, usize)>()
        + 2 * name.len()
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

    fn size(payload: &str) -> usize {
        cost(&msg(payload))
    }

    /// Asserts the budget, and that bytes, eviction order and counters match a recount.
    fn check(tree: &Tree) {
        assert!(tree.bytes() <= tree.budget);
        let (mut bytes, mut fronts) = (0, BTreeSet::new());
        for id in tree.subtree(ROOT) {
            let node = tree.node(id);
            if id != ROOT {
                bytes += node_cost(&node.name);
            }
            bytes += node.history.iter().map(cost).sum::<usize>();
            fronts.extend(node.history.front().map(|m| (m.time, id)));
            let below = || node.children.values().map(|&c| tree.node(c));
            let messages: u64 = below().map(|c| c.subtree_messages).sum();
            let topics: u32 = below().map(|c| c.subtree_topics).sum();
            assert_eq!(node.subtree_messages, node.received + messages);
            assert_eq!(node.subtree_topics, u32::from(node.received > 0) + topics);
        }
        assert_eq!(tree.bytes(), bytes);
        assert_eq!(tree.oldest, fronts);
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
        let bytes = node_cost("a") + size("22") + size("333");
        assert_eq!(tree.bytes(), bytes);
        tree.insert("b", msg("4444"));
        assert_eq!(tree.bytes(), bytes + node_cost("b") + size("4444"));
        check(&tree);
    }

    #[test]
    fn evicts_the_oldest_messages_tree_wide_beyond_the_budget() {
        let mut tree = Tree::new(10);
        // Room for three topics and five messages.
        tree.budget = 3 * node_cost("t0") + 5 * size("00");
        let start = Local::now();
        for i in 0..30 {
            let time = start + chrono::TimeDelta::seconds(i);
            let m = Message {
                time,
                ..msg(&format!("{i:02}"))
            };
            assert!(tree.insert(&format!("t{}", i % 3), m).is_some());
            check(&tree);
        }
        let mut kept: Vec<_> = tree
            .subtree(ROOT)
            .into_iter()
            .flat_map(|id| payloads(&tree, id))
            .collect();
        kept.sort();
        assert_eq!(kept, ["25", "26", "27", "28", "29"]);
        assert_eq!(counts(&tree, ROOT), (0, 30, 3));
        assert_eq!(counts(&tree, tree.find("t0").unwrap()), (10, 10, 1));
        assert_eq!(tree.dropped(), 0);
    }

    #[test]
    fn drops_messages_that_cannot_fit() {
        let mut tree = Tree::new(10);
        tree.budget = node_cost("a") + size("x");
        let a = tree.insert("a", msg("x")).unwrap();
        assert_eq!(tree.insert("a", msg("xx")), None, "too large alone");
        assert_eq!(tree.insert("b", msg("")), None, "no room for the topic");
        assert_eq!(tree.find("b"), None);
        assert_eq!(payloads(&tree, a), ["x"]);
        assert_eq!(tree.dropped(), 2);
        check(&tree);
    }

    #[test]
    fn drops_messages_beyond_the_node_limit() {
        let mut tree = Tree::new(10);
        for i in 0..MAX_NODES {
            tree.insert(&i.to_string(), msg("x"));
        }
        assert_eq!(tree.insert("new", msg("x")), None);
        assert_eq!(tree.insert("0/new", msg("x")), None);
        assert_eq!(tree.find("0/new"), None);
        assert!(
            tree.insert("0", msg("y")).is_some(),
            "existing topics still work"
        );
        assert_eq!(tree.dropped(), 2);
        // Removed nodes no longer count.
        tree.remove(tree.find("0").unwrap());
        assert!(tree.insert("new", msg("x")).is_some());
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
        let bytes = node_cost("a") + node_cost("e") + size("4444");
        assert_eq!(tree.bytes(), bytes);
        let c = tree.insert("a/b/c", msg("55555")).unwrap();
        assert_eq!(counts(&tree, c), (1, 1, 1));
        assert_eq!(payloads(&tree, c), ["55555"]);
        assert_eq!(counts(&tree, a), (0, 2, 2));
        assert_eq!(
            tree.bytes(),
            bytes + node_cost("b") + node_cost("c") + size("55555")
        );
        check(&tree);
        tree.remove(a);
        assert!(tree.node(ROOT).children.is_empty());
        assert_eq!(counts(&tree, ROOT), (0, 0, 0));
        assert_eq!(tree.bytes(), 0);
        check(&tree);
    }

    #[test]
    fn reuses_removed_slots() {
        let mut tree = Tree::new(10);
        tree.insert("keep", msg("1"));
        for _ in 0..10_000 {
            let id = tree.insert("a/b/c", msg("2")).unwrap();
            tree.remove(id);
            assert!(!tree.contains(id));
            tree.remove(id);
        }
        assert_eq!(tree.nodes.len(), 5);
        assert_eq!(
            counts(&tree, ROOT),
            (0, 1, 1),
            "a stale remove does nothing"
        );
        check(&tree);
    }

    #[test]
    fn retained_flag_survives_eviction_but_not_removal() {
        let mut tree = Tree::new(1);
        let retained = Message {
            retain: true,
            ..msg("1")
        };
        let a = tree.insert("a", retained).unwrap();
        tree.insert("a", msg("2"));
        assert!(tree.node(a).retained);
        tree.remove(a);
        assert_eq!(tree.insert("b", msg("3")), Some(a), "the slot is reused");
        assert!(!tree.node(a).retained);
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
    fn filter_ignores_case_beyond_ascii() {
        let mut tree = Tree::new(10);
        let a = tree.insert("a", msg("ÄPFEL")).unwrap();
        assert_eq!(tree.rows("äpf"), [(a, 0)]);
        assert!(tree.rows("apf").is_empty());
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
    fn filters_a_maximum_length_topic() {
        // 65,535 bytes, the MQTT limit, in 32,768 levels.
        let topic = "a/".repeat(32767) + "a";
        let mut tree = Tree::new(10);
        let leaf = tree.insert(&topic, msg("x")).unwrap();
        assert_eq!(tree.rows("a").len(), 32768);
        for id in tree.subtree(ROOT) {
            tree.node_mut(id).expanded = true;
        }
        assert_eq!(tree.rows("").last(), Some(&(leaf, 32767)));
        assert!(tree.contains(leaf));
        assert_eq!(tree.path(leaf), topic);
        tree.remove(leaf);
        assert!(tree.node(ROOT).children.is_empty());
        check(&tree);
    }

    #[test]
    fn empty_payload_is_kept_like_any_message() {
        let mut tree = Tree::new(10);
        let leaf = tree.insert("a/b/c", msg("1")).unwrap();
        let a = tree.insert("a", msg("2")).unwrap();
        assert_eq!(tree.insert("a/b/c", msg("")), Some(leaf));
        assert_eq!(payloads(&tree, leaf), ["1", ""]);
        assert_eq!(tree.insert("a", msg("")), Some(a));
        assert_eq!(payloads(&tree, a), ["2", ""]);
        let seen = tree.insert("never/seen", msg("")).unwrap();
        assert_eq!(payloads(&tree, seen), [""]);
        assert_eq!(counts(&tree, a), (2, 4, 2));
        assert_eq!(counts(&tree, ROOT), (0, 5, 3));
        check(&tree);
    }
}
