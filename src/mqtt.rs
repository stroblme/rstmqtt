// SPDX-License-Identifier: MIT OR Apache-2.0

//! Connection profiles and sessions, with one Tokio task per connection updating
//! the shared topic tree.

use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Local;
use eframe::egui;
use rumqttc::v5::mqttbytes::v5::PublishProperties;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use crate::tree::{Message, Props, Tree};

/// MQTT packet size limit in both directions, above rumqttc's 10 KiB default.
/// Allocation follows packet size. A bigger incoming packet fails the session,
/// since a retained one would come back on every reconnect.
const MAX_PACKET: u32 = 16 << 20;

/// MQTT string length limit in bytes. rumqttc's v3.1.1 encoder truncates longer
/// lengths into malformed packets.
const MAX_STR: usize = 65_535;

/// Pending request capacity. `publish` returns an error when the queue is full;
/// `Session::clear` waits instead.
const QUEUE: usize = 1024;

/// Polling delay after a connection error.
const RETRY: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Version {
    V311,
    V5,
}

/// Connection settings persisted by eframe, excluding passwords.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Profile {
    pub name: String,
    pub host: String,
    pub port: u16,
    /// Whether to use TLS, verifying the broker's certificate chain and host name.
    pub tls: bool,
    /// PEM file of CA certificates to trust with TLS; empty for the system roots.
    pub ca_file: String,
    pub version: Version,
    pub client_id: String,
    pub username: String,
    /// Keep-alive interval in seconds.
    pub keep_alive: u16,
    pub clean: bool,
    /// Subscription pairs of topic filter and QoS.
    pub subscriptions: Vec<(String, u8)>,
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            name: String::new(),
            host: "localhost".into(),
            port: 1883,
            tls: false,
            ca_file: String::new(),
            version: Version::V311,
            // Random, since a broker drops the older of two clients with one id.
            client_id: format!("rstmqtt-{:08x}", RandomState::new().hash_one(()) as u32),
            username: String::new(),
            keep_alive: 30,
            clean: true,
            subscriptions: vec![("#".into(), 0), ("$SYS/#".into(), 0)],
        }
    }
}

impl Profile {
    /// Checks the strings sent on connecting, naming the field in the error.
    /// `Session::start` refuses a profile that fails this.
    pub fn check(&self, password: &str) -> Result<(), String> {
        check_str("client id", &self.client_id)?;
        check_str("username", &self.username)?;
        check_str("password", password)?;
        self.subscriptions
            .iter()
            .try_for_each(|(filter, _)| check_filter(filter))
    }
}

/// Filters a persistent session may still hold on the broker, saved so that a
/// resumed session can unsubscribe those the profile dropped. The broker
/// address and client id identify the session.
// ponytail: one record, so switching between persistent sessions forgets the
// other's filters; keep one per session if that shows.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Subscribed {
    host: String,
    port: u16,
    client_id: String,
    filters: Vec<String>,
}

impl Subscribed {
    /// Filters of `previous` missing from `self`, if both are the same session.
    fn dropped(&self, previous: &Subscribed) -> Vec<String> {
        fn id(s: &Subscribed) -> (&str, u16, &str) {
            (&s.host, s.port, &s.client_id)
        }
        if id(self) != id(previous) {
            return Vec::new();
        }
        let mut dropped = previous.filters.clone();
        dropped.retain(|f| !self.filters.contains(f));
        dropped
    }

    /// `self` with `stale` added, which the broker holds until it acknowledges
    /// unsubscribing them.
    fn with(&self, stale: &[String]) -> Self {
        let mut held = self.clone();
        held.filters.extend_from_slice(stale);
        held
    }
}

/// Checks a subscription filter: not empty, `+` and `#` only as whole levels,
/// `#` only last, and within MQTT's string limits.
pub fn check_filter(filter: &str) -> Result<(), String> {
    let field = format!("filter {filter:?}");
    if filter.is_empty() {
        return Err(format!("{field}: empty"));
    }
    if !rumqttc::valid_filter(filter) {
        return Err(format!(
            "{field}: `+` and `#` must be whole levels, `#` the last"
        ));
    }
    check_str(&field, filter)
}

/// Checks a topic name to publish to.
fn check_topic(topic: &str) -> Result<(), String> {
    // rumqttc reports a bad topic the same way as a full queue.
    if topic.is_empty() || topic.contains(['+', '#']) {
        return Err(format!("invalid topic {topic:?}"));
    }
    check_str("topic", topic)
}

/// Checks `s` against MQTT's string limits, which rumqttc does not enforce.
fn check_str(field: &str, s: &str) -> Result<(), String> {
    if s.len() > MAX_STR {
        return Err(format!("{field}: longer than {MAX_STR} bytes"));
    }
    if s.contains('\0') {
        return Err(format!("{field}: contains U+0000"));
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
pub enum State {
    Connecting,
    Connected,
    /// Last connection error; retries every second.
    Reconnecting(String),
    /// Error that ended the session because it would recur on every reconnect,
    /// such as a packet above the size limit.
    Failed(String),
    Disconnected,
}

pub struct Shared {
    pub tree: Tree,
    pub state: State,
    /// Events for the GUI to show and drain, such as rejected subscriptions,
    /// rejected MQTT v5 publishes and the outcome of `Session::clear`.
    pub notes: Vec<String>,
    /// Filters a persistent session may hold, for the GUI to save and pass to
    /// the next `Session::start`. Set on connecting and unsubscribing.
    pub subscribed: Option<Subscribed>,
}

#[derive(Clone)]
enum Client {
    V4(rumqttc::AsyncClient),
    V5(rumqttc::v5::AsyncClient),
}

/// Broker connection that disconnects when dropped.
pub struct Session {
    pub shared: Arc<Mutex<Shared>>,
    client: Client,
    /// Packet size limit, lowered by an MQTT v5 broker's Maximum Packet Size.
    limit: Arc<AtomicU32>,
    task: JoinHandle<()>,
    ctx: egui::Context,
}

impl Session {
    /// Connects in the background, reporting progress through `shared.state`.
    /// A resumed session unsubscribes the filters of `subscribed` that the
    /// profile dropped. Fails if the profile fails `Profile::check` or its CA
    /// file is unusable. Requires a Tokio runtime context.
    pub fn start(
        profile: &Profile,
        password: Option<&str>,
        subscribed: Option<&Subscribed>,
        cap: usize,
        ctx: egui::Context,
    ) -> Result<Self, String> {
        let password = password.unwrap_or_default();
        profile.check(password)?;
        let transport = match profile.tls {
            true => rumqttc::Transport::tls_with_config(tls(&profile.ca_file)?),
            false => rumqttc::Transport::Tcp,
        };
        let shared = Arc::new(Mutex::new(Shared {
            tree: Tree::new(cap),
            state: State::Connecting,
            notes: Vec::new(),
            subscribed: None,
        }));
        let limit = Arc::new(AtomicU32::new(MAX_PACKET));
        // A persistent session needs a client id to resume it by, and rumqttc's
        // v3.1.1 client panics without one.
        let clean = profile.clean || profile.client_id.is_empty();
        // None for a clean session, which ends with the connection.
        let record = (!clean).then(|| Subscribed {
            host: profile.host.clone(),
            port: profile.port,
            client_id: profile.client_id.clone(),
            filters: profile.subscriptions.iter().map(|s| s.0.clone()).collect(),
        });
        let stale = match (&record, subscribed) {
            (Some(record), Some(previous)) => record.dropped(previous),
            _ => Vec::new(),
        };
        let (client, task) = match profile.version {
            Version::V311 => {
                let mut options =
                    rumqttc::MqttOptions::new(&profile.client_id, &profile.host, profile.port);
                options
                    .set_transport(transport)
                    .set_keep_alive(Duration::from_secs(profile.keep_alive.into()))
                    .set_clean_session(clean)
                    .set_max_packet_size(MAX_PACKET as usize, MAX_PACKET as usize);
                if !profile.username.is_empty() {
                    options.set_credentials(&profile.username, password);
                }
                let filters = profile.subscriptions.iter().filter_map(|(filter, qos)| {
                    Some(rumqttc::SubscribeFilter::new(
                        filter.clone(),
                        rumqttc::qos(*qos).ok()?,
                    ))
                });
                let (client, eventloop) = rumqttc::AsyncClient::new(options, QUEUE);
                let task = run_v4(
                    eventloop,
                    filters.collect(),
                    stale,
                    record,
                    shared.clone(),
                    ctx.clone(),
                );
                (Client::V4(client), tokio::spawn(task))
            }
            Version::V5 => {
                let mut options =
                    rumqttc::v5::MqttOptions::new(&profile.client_id, &profile.host, profile.port);
                options
                    .set_transport(transport)
                    // rumqttc's v5 client panics below 5 s.
                    .set_keep_alive(Duration::from_secs(profile.keep_alive.max(5).into()))
                    .set_clean_start(clean)
                    // Never expires, as a v3.1.1 persistent session; brokers may
                    // cap it.
                    .set_session_expiry_interval((!clean).then_some(u32::MAX))
                    // Also tells the broker, which skips bigger messages for us.
                    .set_max_packet_size(Some(MAX_PACKET));
                if !profile.username.is_empty() {
                    options.set_credentials(&profile.username, password);
                }
                let filters = profile.subscriptions.iter().filter_map(|(filter, qos)| {
                    Some(rumqttc::v5::mqttbytes::v5::Filter::new(
                        filter,
                        rumqttc::v5::mqttbytes::qos(*qos)?,
                    ))
                });
                let (client, eventloop) = rumqttc::v5::AsyncClient::new(options, QUEUE);
                let task = run_v5(
                    eventloop,
                    filters.collect(),
                    stale,
                    record,
                    limit.clone(),
                    shared.clone(),
                    ctx.clone(),
                );
                (Client::V5(client), tokio::spawn(task))
            }
        };
        Ok(Self {
            shared,
            client,
            limit,
            task,
            ctx,
        })
    }

    /// Queues a message without waiting or locking `shared`; `Ok` means queued,
    /// not delivered. MQTT v5 sends content type and user properties; v3.1.1
    /// ignores `props`.
    pub fn publish(
        &self,
        topic: &str,
        payload: Vec<u8>,
        qos: u8,
        retain: bool,
        props: Option<&Props>,
    ) -> Result<(), String> {
        check_topic(topic)?;
        // Packet size bound: fixed header 5, topic length 2, packet id 2 and v5
        // property length 4 bytes. rumqttc fails the connection on bigger ones.
        let mut size = 13 + topic.len() + payload.len();
        if let (Client::V5(_), Some(p)) = (&self.client, props) {
            if let Some(content_type) = &p.content_type {
                check_str("content type", content_type)?;
                size += 3 + content_type.len();
            }
            for (key, value) in &p.user {
                check_str("user property key", key)?;
                check_str("user property value", value)?;
                size += 5 + key.len() + value.len();
            }
        }
        let limit = self.limit.load(Ordering::Relaxed);
        if size > limit as usize {
            return Err(match limit {
                MAX_PACKET => format!("message over the {} MiB packet limit", MAX_PACKET >> 20),
                _ => format!("message over the broker's {limit} byte packet limit"),
            });
        }
        let bad_qos = || format!("invalid QoS {qos}");
        let queued = match &self.client {
            Client::V4(client) => {
                let qos = rumqttc::qos(qos).map_err(|_| bad_qos())?;
                client.try_publish(topic, qos, retain, payload).is_ok()
            }
            Client::V5(client) => {
                let qos = rumqttc::v5::mqttbytes::qos(qos).ok_or_else(bad_qos)?;
                let props = props.map(|p| PublishProperties {
                    content_type: p.content_type.clone(),
                    user_properties: p.user.clone(),
                    ..Default::default()
                });
                client
                    .try_publish_with_properties(
                        topic,
                        qos,
                        retain,
                        payload,
                        props.unwrap_or_default(),
                    )
                    .is_ok()
            }
        };
        if !queued {
            return Err(format!("more than {QUEUE} messages waiting to be sent"));
        }
        Ok(())
    }

    /// Queues an empty retained QoS 0 message to each topic in the background,
    /// waiting for queue space, then notes the outcome in `shared`.
    /// Stops when the session ends.
    pub fn clear(&self, topics: Vec<String>) {
        let client = self.client.clone();
        let shared = self.shared.clone();
        let ctx = self.ctx.clone();
        tokio::spawn(async move {
            let n = topics.len();
            let mut note = format!("Queued clear of {n} topics");
            for (i, topic) in topics.iter().enumerate() {
                if let Err(e) = client.clear(topic).await {
                    note = format!("Queued clear of {i} of {n} topics: {e}");
                    break;
                }
            }
            shared.lock().unwrap().notes.push(note);
            ctx.request_repaint();
        });
    }

    /// Drops the session, setting `Disconnected` and attempting a clean DISCONNECT.
    pub fn disconnect(self) {
        drop(self);
    }
}

impl Drop for Session {
    /// Attempts to queue DISCONNECT when connected so the task exits.
    /// Aborts the task if DISCONNECT cannot be queued.
    fn drop(&mut self) {
        let state = std::mem::replace(&mut self.shared.lock().unwrap().state, State::Disconnected);
        let queued = state == State::Connected
            && match &self.client {
                Client::V4(client) => client.try_disconnect().is_ok(),
                Client::V5(client) => client.try_disconnect().is_ok(),
            };
        if !queued {
            self.task.abort();
        }
    }
}

impl Client {
    /// Queues an empty retained message to `topic`, waiting for queue space.
    /// Fails once the session task has ended.
    async fn clear(&self, topic: &str) -> Result<(), String> {
        check_topic(topic)?;
        let queued = match self {
            Client::V4(client) => {
                let qos = rumqttc::QoS::AtMostOnce;
                client.publish(topic, qos, true, Vec::new()).await.is_ok()
            }
            Client::V5(client) => {
                let qos = rumqttc::v5::mqttbytes::QoS::AtMostOnce;
                client.publish(topic, qos, true, Vec::new()).await.is_ok()
            }
        };
        queued.then_some(()).ok_or_else(|| "session ended".into())
    }
}

/// Builds a TLS configuration trusting `ca_file`, or the system roots if empty.
/// Unlike rumqttc's default, skips unreadable system certificates rather than
/// panicking.
fn tls(ca_file: &str) -> Result<rumqttc::TlsConfiguration, String> {
    use rumqttc::tokio_rustls::rustls::pki_types::{CertificateDer, pem::PemObject};
    use rumqttc::tokio_rustls::rustls::{ClientConfig, RootCertStore};
    let certs = if ca_file.is_empty() {
        rustls_native_certs::load_native_certs().certs
    } else {
        CertificateDer::pem_file_iter(ca_file)
            .and_then(|certs| certs.collect())
            .map_err(|e| format!("CA file {ca_file:?}: {e}"))?
    };
    let mut roots = RootCertStore::empty();
    if roots.add_parsable_certificates(certs).0 == 0 {
        return Err(match ca_file.is_empty() {
            true => "no system CA certificates found".into(),
            false => format!("CA file {ca_file:?}: no certificates"),
        });
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(rumqttc::TlsConfiguration::Rustls(Arc::new(config)))
}

/// Updates `shared` from an MQTT v3.1.1 connection until the session ends,
/// unsubscribing `stale` filters from a resumed session and reporting the
/// filters it may hold through `record`.
async fn run_v4(
    mut eventloop: rumqttc::EventLoop,
    filters: Vec<rumqttc::SubscribeFilter>,
    mut stale: Vec<String>,
    record: Option<Subscribed>,
    shared: Arc<Mutex<Shared>>,
    ctx: egui::Context,
) {
    use rumqttc::mqttbytes::Error::{OutgoingPacketTooLarge, PayloadSizeLimitExceeded};
    use rumqttc::{
        ConnectionError, Event, Outgoing, Packet, Request, StateError, Subscribe,
        SubscribeReasonCode, Unsubscribe,
    };
    // Whether the broker acknowledged the subscriptions, which a resumed
    // session keeps.
    let mut subscribed = false;
    loop {
        let event = eventloop.poll().await;
        let failed = {
            let mut shared = shared.lock().unwrap();
            match event {
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    shared.state = State::Connected;
                    // A new session holds no filters.
                    if !ack.session_present {
                        stale.clear();
                    }
                    // Into the event loop's unbounded queue, ahead of the
                    // requests that may fill the request queue.
                    if !(subscribed && ack.session_present) && !filters.is_empty() {
                        let subscribe = Subscribe::new_many(filters.clone());
                        eventloop.pending.push_front(Request::Subscribe(subscribe));
                    }
                    if !stale.is_empty() {
                        let topics = stale.clone();
                        let unsubscribe = Request::Unsubscribe(Unsubscribe { pkid: 0, topics });
                        eventloop.pending.push_front(unsubscribe);
                    }
                    shared.subscribed = record.as_ref().map(|r| r.with(&stale));
                    false
                }
                Ok(Event::Incoming(Packet::UnsubAck(_))) => {
                    stale.clear();
                    shared.subscribed = record.clone();
                    false
                }
                Ok(Event::Incoming(Packet::SubAck(ack))) => {
                    subscribed = true;
                    for (filter, code) in filters.iter().zip(&ack.return_codes) {
                        if !matches!(code, SubscribeReasonCode::Success(_)) {
                            let note = format!("subscription to `{}` rejected", filter.path);
                            shared.notes.push(note);
                        }
                    }
                    false
                }
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let msg = Message {
                        payload: p.payload.into(),
                        qos: p.qos as u8,
                        retain: p.retain,
                        time: Local::now(),
                        props: None,
                    };
                    shared.tree.insert(&p.topic, msg);
                    false
                }
                // Queued by `Session::drop`.
                Ok(Event::Outgoing(Outgoing::Disconnect)) => return,
                Ok(_) => continue,
                Err(_) if shared.state == State::Disconnected => return,
                // Would recur on every reconnect, as a retained message comes
                // again.
                Err(
                    e @ ConnectionError::MqttState(StateError::Deserialization(
                        PayloadSizeLimitExceeded(_) | OutgoingPacketTooLarge { .. },
                    )),
                ) => {
                    shared.state = State::Failed(e.to_string());
                    ctx.request_repaint();
                    return;
                }
                Err(e) => {
                    shared.state = State::Reconnecting(e.to_string());
                    true
                }
            }
        };
        ctx.request_repaint();
        if failed {
            // rumqttc reconnects on the next poll.
            tokio::time::sleep(RETRY).await;
        }
    }
}

/// Updates `shared` from an MQTT v5 connection until the session ends, as
/// `run_v4` does, and sets `limit` from the broker's Maximum Packet Size.
/// Topic aliases stay disabled to prevent empty aliased topic names.
async fn run_v5(
    mut eventloop: rumqttc::v5::EventLoop,
    filters: Vec<rumqttc::v5::mqttbytes::v5::Filter>,
    mut stale: Vec<String>,
    record: Option<Subscribed>,
    limit: Arc<AtomicU32>,
    shared: Arc<Mutex<Shared>>,
    ctx: egui::Context,
) {
    use rumqttc::Outgoing;
    use rumqttc::v5::mqttbytes::Error::{OutgoingPacketTooLarge, PayloadSizeLimitExceeded};
    use rumqttc::v5::mqttbytes::v5::{
        Packet, PubAck, PubAckReason, PubRec, PubRecReason, Subscribe, SubscribeReasonCode,
        UnsubAckReason, Unsubscribe,
    };
    use rumqttc::v5::{ConnectionError, Event, Request, StateError};
    // Whether the broker acknowledged the subscriptions, which a resumed
    // session keeps.
    let mut subscribed = false;
    loop {
        let event = eventloop.poll().await;
        let failed = {
            let mut shared = shared.lock().unwrap();
            match event {
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    shared.state = State::Connected;
                    let max = ack.properties.as_ref().and_then(|p| p.max_packet_size);
                    limit.store(max.unwrap_or(MAX_PACKET).min(MAX_PACKET), Ordering::Relaxed);
                    // A new session holds no filters.
                    if !ack.session_present {
                        stale.clear();
                    }
                    // Into the event loop's unbounded queue, ahead of the
                    // requests that may fill the request queue.
                    if !(subscribed && ack.session_present) && !filters.is_empty() {
                        let subscribe = Subscribe::new_many(filters.clone(), None);
                        eventloop.pending.push_front(Request::Subscribe(subscribe));
                    }
                    if !stale.is_empty() {
                        let unsubscribe = Request::Unsubscribe(Unsubscribe {
                            filters: stale.clone(),
                            ..Default::default()
                        });
                        eventloop.pending.push_front(unsubscribe);
                    }
                    shared.subscribed = record.as_ref().map(|r| r.with(&stale));
                    false
                }
                // Reason codes of 0x80 and above keep the filter, to retry on
                // the next resumed connection.
                Ok(Event::Incoming(Packet::UnsubAck(ack))) => {
                    let mut reasons = ack.reasons.iter();
                    stale.retain(|filter| match reasons.next() {
                        None
                        | Some(UnsubAckReason::Success | UnsubAckReason::NoSubscriptionExisted) => {
                            false
                        }
                        Some(reason) => {
                            let note = format!("unsubscribe from `{filter}` rejected: {reason:?}");
                            shared.notes.push(note);
                            true
                        }
                    });
                    shared.subscribed = record.as_ref().map(|r| r.with(&stale));
                    false
                }
                Ok(Event::Incoming(Packet::SubAck(ack))) => {
                    subscribed = true;
                    for (filter, code) in filters.iter().zip(&ack.return_codes) {
                        if !matches!(code, SubscribeReasonCode::Success(_)) {
                            let note =
                                format!("subscription to `{}` rejected: {code:?}", filter.path);
                            shared.notes.push(note);
                        }
                    }
                    false
                }
                // Reason codes of 0x80 and above.
                Ok(Event::Incoming(Packet::PubAck(PubAck { reason, .. })))
                    if !matches!(
                        reason,
                        PubAckReason::Success | PubAckReason::NoMatchingSubscribers
                    ) =>
                {
                    shared.notes.push(format!("publish rejected: {reason:?}"));
                    false
                }
                Ok(Event::Incoming(Packet::PubRec(PubRec { reason, .. })))
                    if !matches!(
                        reason,
                        PubRecReason::Success | PubRecReason::NoMatchingSubscribers
                    ) =>
                {
                    shared.notes.push(format!("publish rejected: {reason:?}"));
                    false
                }
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let msg = Message {
                        payload: p.payload.into(),
                        qos: p.qos as u8,
                        retain: p.retain,
                        time: Local::now(),
                        props: p.properties.map(|p| {
                            Box::new(Props {
                                content_type: p.content_type,
                                user: p.user_properties,
                                response_topic: p.response_topic,
                                correlation: p.correlation_data.map(Into::into),
                                expiry: p.message_expiry_interval,
                                utf8: p.payload_format_indicator.map(|f| f == 1),
                            })
                        }),
                    };
                    shared.tree.insert(&String::from_utf8_lossy(&p.topic), msg);
                    false
                }
                // Queued by `Session::drop`.
                Ok(Event::Outgoing(Outgoing::Disconnect)) => return,
                Ok(_) => continue,
                Err(_) if shared.state == State::Disconnected => return,
                // Would recur on every reconnect, as a retained message comes
                // again, also from brokers ignoring the advertised limit.
                // ponytail: also fails on a publish queued before a reconnect
                // lowered the limit, as to another cluster node; drop those
                // from `eventloop.pending` on ConnAck if that shows.
                Err(
                    e @ ConnectionError::MqttState(
                        StateError::Deserialization(
                            PayloadSizeLimitExceeded { .. } | OutgoingPacketTooLarge { .. },
                        )
                        | StateError::OutgoingPacketTooLarge { .. },
                    ),
                ) => {
                    shared.state = State::Failed(e.to_string());
                    ctx.request_repaint();
                    return;
                }
                Err(e) => {
                    shared.state = State::Reconnecting(e.to_string());
                    true
                }
            }
        };
        ctx.request_repaint();
        if failed {
            // rumqttc reconnects on the next poll.
            tokio::time::sleep(RETRY).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::{Ipv4Addr, TcpListener, TcpStream};
    use std::sync::OnceLock;
    use std::thread;
    use std::time::Instant;

    use super::*;

    /// Returns the shared test broker port for `version`, with or without TLS.
    /// Separate brokers avoid a rumqttd panic when forwarding v5 properties to
    /// v3.1.1 clients, which leaves its v3.1.1 listener rejecting connections.
    fn broker(version: Version, tls: bool) -> u16 {
        static PORTS: OnceLock<[[u16; 2]; 2]> = OnceLock::new();
        let ports = PORTS.get_or_init(|| {
            // Bound together, so the free ports differ.
            let listeners = [(); 4].map(|_| TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap());
            let [a, b, c, d] = listeners.map(|l| l.local_addr().unwrap().port());
            let ports = [[a, b], [c, d]];
            for (v, ports) in [Version::V311, Version::V5].into_iter().zip(ports) {
                let servers = [false, true].map(|tls| {
                    let server = rumqttd::ServerSettings {
                        name: format!("{v:?} tls={tls}"),
                        listen: (Ipv4Addr::LOCALHOST, ports[tls as usize]).into(),
                        tls: tls.then(|| rumqttd::TlsConfig::Rustls {
                            capath: None,
                            certpath: fixture("server.pem"),
                            keypath: fixture("server.key"),
                        }),
                        next_connection_delay_ms: 1,
                        connections: rumqttd::ConnectionSettings {
                            connection_timeout_ms: 60_000,
                            max_payload_size: 1 << 20,
                            max_inflight_count: 100,
                            auth: None,
                            external_auth: None,
                            dynamic_filters: true,
                        },
                    };
                    (server.name.clone(), server)
                });
                let servers = Some(HashMap::from(servers));
                let config = rumqttd::Config {
                    router: rumqttd::RouterConfig {
                        max_connections: 100,
                        max_outgoing_packet_count: 200,
                        max_segment_size: 1 << 20,
                        max_segment_count: 10,
                        ..Default::default()
                    },
                    v4: servers.clone().filter(|_| v == Version::V311),
                    v5: servers.filter(|_| v == Version::V5),
                    ..Default::default()
                };
                let mut broker = rumqttd::Broker::new(config);
                thread::spawn(move || broker.start().unwrap());
                for port in ports {
                    let listening = || TcpStream::connect((Ipv4Addr::LOCALHOST, port)).is_ok();
                    assert!(wait_until(listening));
                }
            }
            ports
        });
        ports[version as usize][tls as usize]
    }

    /// Returns the path of a test certificate file. `ca.pem` issued
    /// `server.pem` for `localhost` only; `other-ca.pem` is unrelated. Made with
    /// the openssl CLI, valid for 100 years.
    fn fixture(name: &str) -> String {
        format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    /// Polls `done` for up to 10 s.
    fn wait_until(mut done: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !done() {
            if Instant::now() > deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(10));
        }
        true
    }

    /// Connects a session to the test broker.
    fn connect(version: Version) -> Session {
        let profile = Profile {
            host: Ipv4Addr::LOCALHOST.to_string(),
            port: broker(version, false),
            version,
            // rumqttd drops a client that subscribes to `$SYS/#`.
            subscriptions: vec![("#".into(), 1)],
            ..Profile::default()
        };
        let session = Session::start(&profile, None, None, 10, egui::Context::default()).unwrap();
        let connected = wait_until(|| session.shared.lock().unwrap().state == State::Connected);
        // Not formatted inside the assert, whose panic would poison the lock.
        let state = format!("{:?}", session.shared.lock().unwrap().state);
        assert!(connected, "{state}");
        session
    }

    /// Returns the latest message on `topic`, if any.
    fn latest(session: &Session, topic: &str) -> Option<Message> {
        let shared = session.shared.lock().unwrap();
        let id = shared.tree.find(topic)?;
        shared.tree.node(id).history.back().cloned()
    }

    /// Waits for the latest message on `topic` to pass `check`.
    fn receive(session: &Session, topic: &str, check: impl Fn(&Message) -> bool) -> Message {
        assert!(
            wait_until(|| latest(session, topic).is_some_and(|m| check(&m))),
            "nothing on {topic}"
        );
        latest(session, topic).unwrap()
    }

    #[test]
    fn publish_and_receive_v311() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _rt = rt.enter();
        let session = connect(Version::V311);
        // Above rumqttc's default packet limit of 10 KiB.
        let payload = vec![b'x'; 64 * 1024];
        // The subscription was queued before `Connected` showed, so it reaches
        // the broker ahead of this.
        session
            .publish("test/v311/echo", payload.clone(), 1, false, None)
            .unwrap();
        let msg = receive(&session, "test/v311/echo", |_| true);
        assert_eq!(msg.payload, payload);
        assert_eq!(msg.qos, 1);
        assert!(!msg.retain);
        assert!(msg.props.is_none());

        let shared = session.shared.clone();
        session.disconnect();
        assert_eq!(shared.lock().unwrap().state, State::Disconnected);
        // The task ends once its DISCONNECT is out.
        assert!(wait_until(|| rt.metrics().num_alive_tasks() == 0));
    }

    #[test]
    fn retained_reaches_a_new_session() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _rt = rt.enter();
        let first = connect(Version::V311);
        first
            .publish("test/retained/kept", b"on".to_vec(), 0, true, None)
            .unwrap();
        // Back from the broker, so it has stored it.
        receive(&first, "test/retained/kept", |_| true);

        let second = connect(Version::V311);
        let msg = receive(&second, "test/retained/kept", |_| true);
        assert_eq!(msg.payload, b"on");
        assert!(msg.retain);
    }

    #[test]
    fn cleared_retained_stays_away() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _rt = rt.enter();
        let first = connect(Version::V311);
        first
            .publish("test/cleared/topic", b"on".to_vec(), 0, true, None)
            .unwrap();
        receive(&first, "test/cleared/topic", |m| m.payload == b"on");
        first
            .publish("test/cleared/topic", Vec::new(), 0, true, None)
            .unwrap();
        // The clear comes back as an empty message.
        let cleared = || latest(&first, "test/cleared/topic").is_some_and(|m| m.payload.is_empty());
        assert!(wait_until(cleared));

        // Retained messages arrive right after the subscription, ahead of
        // anything published later, so once the marker is in, the cleared
        // topic would be too.
        let fresh = connect(Version::V311);
        fresh
            .publish("test/cleared/marker", b"m".to_vec(), 0, false, None)
            .unwrap();
        receive(&fresh, "test/cleared/marker", |_| true);
        assert!(latest(&fresh, "test/cleared/topic").is_none());
    }

    #[test]
    fn properties_round_trip_v5() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _rt = rt.enter();
        let session = connect(Version::V5);
        let props = Props {
            content_type: Some("application/json".into()),
            user: vec![("source".into(), "test".into())],
            ..Props::default()
        };
        session
            .publish("test/v5/props", b"{}".to_vec(), 1, false, Some(&props))
            .unwrap();
        let msg = receive(&session, "test/v5/props", |_| true);
        assert_eq!(msg.payload, b"{}");
        assert_eq!(msg.qos, 1);
        assert_eq!(msg.props.as_deref(), Some(&props));
    }

    #[test]
    fn clear_notes_the_outcome() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _rt = rt.enter();
        let session = connect(Version::V311);
        session
            .publish("test/clear/a", b"on".to_vec(), 0, true, None)
            .unwrap();
        receive(&session, "test/clear/a", |m| m.payload == b"on");
        session.clear(vec!["test/clear/a".into(), "test/clear/b".into()]);
        let noted = || session.shared.lock().unwrap().notes == ["Queued clear of 2 topics"];
        assert!(wait_until(noted));
        let cleared = || latest(&session, "test/clear/a").is_some_and(|m| m.payload.is_empty());
        assert!(wait_until(cleared));
    }

    #[test]
    fn publish_checks_before_queueing() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _rt = rt.enter();
        let session = connect(Version::V5);
        let publish = |topic: &str, payload: Vec<u8>, props: Option<&Props>| {
            session.publish(topic, payload, 1, false, props).err()
        };
        let big = vec![0; MAX_PACKET as usize];
        let error = publish("test/big", big, None);
        assert_eq!(
            error.as_deref(),
            Some("message over the 16 MiB packet limit")
        );
        let error = publish("test/\0", Vec::new(), None);
        assert_eq!(error.as_deref(), Some("topic: contains U+0000"));
        let props = Props {
            user: vec![("key".into(), "ä".repeat(32_768))],
            ..Props::default()
        };
        let error = publish("test/props", Vec::new(), Some(&props));
        let expected = "user property value: longer than 65535 bytes";
        assert_eq!(error.as_deref(), Some(expected));
        // As from a broker advertising a Maximum Packet Size, which rumqttd
        // does not.
        session.limit.store(1024, Ordering::Relaxed);
        let error = publish("test/big", vec![0; 1024], None);
        let expected = "message over the broker's 1024 byte packet limit";
        assert_eq!(error.as_deref(), Some(expected));
        // Nothing reached the event loop, which would have dropped the connection.
        session
            .publish("test/after", b"ok".to_vec(), 0, false, None)
            .unwrap();
        receive(&session, "test/after", |_| true);
        assert_eq!(session.shared.lock().unwrap().state, State::Connected);
    }

    #[test]
    fn profile_strings_are_checked() {
        // Two-byte characters, so the limit counts bytes rather than characters.
        let at_limit = format!("a{}", "ä".repeat(32_767));
        let over = "ä".repeat(32_768);
        assert_eq!(at_limit.len(), MAX_STR);
        let check = |username: &str| {
            let profile = Profile {
                username: username.into(),
                ..Profile::default()
            };
            profile.check("")
        };
        assert_eq!(check(&at_limit), Ok(()));
        assert_eq!(
            check(&over).unwrap_err(),
            "username: longer than 65535 bytes"
        );
        assert_eq!(check("a\0b").unwrap_err(), "username: contains U+0000");
        let error = Profile::default().check(&over).unwrap_err();
        assert_eq!(error, "password: longer than 65535 bytes");
    }

    #[test]
    fn filters_are_checked() {
        for filter in ["#", "+", "a/+/b", "a/#", "/", "+/+/#", "$SYS/#"] {
            assert_eq!(check_filter(filter), Ok(()), "{filter:?}");
        }
        for filter in ["", "a#", "a/#/b", "#/a", "a+", "a/b+/c", "a\0b"] {
            assert!(check_filter(filter).is_err(), "{filter:?}");
        }
        let expected = "filter \"a/#/b\": `+` and `#` must be whole levels, `#` the last";
        assert_eq!(check_filter("a/#/b").unwrap_err(), expected);
        let profile = Profile {
            subscriptions: vec![("ok/#".into(), 0), ("a+".into(), 0)],
            ..Profile::default()
        };
        assert!(profile.check("").is_err());
    }

    /// rumqttd resumes sessions but does not acknowledge unsubscribing their
    /// filters, so only the filters to unsubscribe are tested here.
    #[test]
    fn dropped_filters_need_the_same_session() {
        let previous = Subscribed {
            host: "broker".into(),
            port: 1883,
            client_id: "a".into(),
            filters: vec!["kept".into(), "dropped".into()],
        };
        let current = Subscribed {
            filters: vec!["kept".into(), "added".into()],
            ..previous.clone()
        };
        assert_eq!(current.dropped(&previous), ["dropped"]);
        assert!(previous.dropped(&previous).is_empty());
        let other = Subscribed {
            client_id: "b".into(),
            ..current.clone()
        };
        assert!(other.dropped(&previous).is_empty());
    }

    /// Starts a TLS session to the test broker, returning its state once it is
    /// no longer connecting.
    fn tls_state(version: Version, host: &str, ca_file: &str) -> String {
        let profile = Profile {
            host: host.into(),
            port: broker(version, true),
            tls: true,
            ca_file: fixture(ca_file),
            version,
            subscriptions: Vec::new(),
            ..Profile::default()
        };
        let session = Session::start(&profile, None, None, 10, egui::Context::default()).unwrap();
        wait_until(|| session.shared.lock().unwrap().state != State::Connecting);
        format!("{:?}", session.shared.lock().unwrap().state)
    }

    #[test]
    fn tls_verifies_chain_and_host() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _rt = rt.enter();
        for version in [Version::V311, Version::V5] {
            assert_eq!(tls_state(version, "localhost", "ca.pem"), "Connected");
            // The certificate names `localhost`, not its address.
            let state = tls_state(version, "127.0.0.1", "ca.pem");
            assert!(state.contains("not valid for name"), "{state}");
            let state = tls_state(version, "localhost", "other-ca.pem");
            assert!(state.contains("UnknownIssuer"), "{state}");
        }
    }

    #[test]
    fn unusable_ca_file_is_refused() {
        let start = |ca_file: &str| {
            let profile = Profile {
                tls: true,
                ca_file: fixture(ca_file),
                ..Profile::default()
            };
            Session::start(&profile, None, None, 10, egui::Context::default()).err()
        };
        let error = start("missing.pem").unwrap();
        assert!(
            error.starts_with("CA file") && error.contains("I/O error"),
            "{error}"
        );
        // A key file holds no certificates.
        let error = start("server.key").unwrap();
        assert!(error.ends_with(": no certificates"), "{error}");
    }
}
