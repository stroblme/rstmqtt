// SPDX-License-Identifier: MIT OR Apache-2.0

//! Connection profiles and sessions, with one Tokio task per connection updating
//! the shared topic tree.

use std::hash::{BuildHasher, RandomState};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Local;
use eframe::egui;
use rumqttc::v5::mqttbytes::v5::PublishProperties;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use crate::tree::{Message, Props, Tree};

/// MQTT packet size limit, avoiding reconnect loops from retained packets above
/// rumqttc's 10 KiB default. The broker limits messages; allocation follows packet size.
const MAX_PACKET: u32 = 268_435_455;

/// Pending request capacity, sized for subtree clears that publish once per topic.
/// `publish` returns an error when the queue is full.
// ponytail: a queue that is full at a reconnect also drops the re-sent
// subscriptions, which takes QUEUE publishes while offline; have the GUI
// publish only while connected if that shows.
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

#[derive(Debug, PartialEq)]
pub enum State {
    Connecting,
    Connected,
    /// Last connection error; retries every second.
    Reconnecting(String),
    Disconnected,
}

pub struct Shared {
    pub tree: Tree,
    pub state: State,
}

enum Client {
    V4(rumqttc::AsyncClient),
    V5(rumqttc::v5::AsyncClient),
}

/// Broker connection that disconnects when dropped.
pub struct Session {
    pub shared: Arc<Mutex<Shared>>,
    client: Client,
    task: JoinHandle<()>,
}

impl Session {
    /// Connects in the background, reporting progress through `shared.state`.
    /// Requires a Tokio runtime context.
    pub fn start(
        profile: &Profile,
        password: Option<&str>,
        cap: usize,
        ctx: egui::Context,
    ) -> Self {
        let shared = Arc::new(Mutex::new(Shared {
            tree: Tree::new(cap),
            state: State::Connecting,
        }));
        let subscriptions = profile.subscriptions.clone();
        let password = password.unwrap_or_default();
        let (client, task) = match profile.version {
            Version::V311 => {
                let mut options =
                    rumqttc::MqttOptions::new(&profile.client_id, &profile.host, profile.port);
                options
                    .set_keep_alive(Duration::from_secs(profile.keep_alive.into()))
                    // rumqttc panics on a persistent session without a client
                    // id, which a broker would refuse anyway.
                    .set_clean_session(profile.clean || profile.client_id.is_empty())
                    .set_max_packet_size(MAX_PACKET as usize, MAX_PACKET as usize);
                if !profile.username.is_empty() {
                    options.set_credentials(&profile.username, password);
                }
                let (client, eventloop) = rumqttc::AsyncClient::new(options, QUEUE);
                let task = run_v4(
                    eventloop,
                    client.clone(),
                    subscriptions,
                    shared.clone(),
                    ctx,
                );
                (Client::V4(client), tokio::spawn(task))
            }
            Version::V5 => {
                let mut options =
                    rumqttc::v5::MqttOptions::new(&profile.client_id, &profile.host, profile.port);
                options
                    // rumqttc's v5 client panics below 5 s.
                    .set_keep_alive(Duration::from_secs(profile.keep_alive.max(5).into()))
                    .set_clean_start(profile.clean)
                    // Also tells the broker, which skips bigger messages for us.
                    .set_max_packet_size(Some(MAX_PACKET));
                if !profile.username.is_empty() {
                    options.set_credentials(&profile.username, password);
                }
                let (client, eventloop) = rumqttc::v5::AsyncClient::new(options, QUEUE);
                let task = run_v5(
                    eventloop,
                    client.clone(),
                    subscriptions,
                    shared.clone(),
                    ctx,
                );
                (Client::V5(client), tokio::spawn(task))
            }
        };
        Self {
            shared,
            client,
            task,
        }
    }

    /// Queues a message without waiting or locking `shared`.
    /// MQTT v5 sends content type and user properties; v3.1.1 ignores `props`.
    pub fn publish(
        &self,
        topic: &str,
        payload: Vec<u8>,
        qos: u8,
        retain: bool,
        props: Option<&Props>,
    ) -> Result<(), String> {
        // rumqttc reports a bad topic the same way as a full queue.
        if topic.is_empty() || topic.contains(['+', '#']) {
            return Err(format!("invalid topic {topic:?}"));
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

/// Updates `shared` from an MQTT v3.1.1 connection until the session ends.
async fn run_v4(
    mut eventloop: rumqttc::EventLoop,
    client: rumqttc::AsyncClient,
    subscriptions: Vec<(String, u8)>,
    shared: Arc<Mutex<Shared>>,
    ctx: egui::Context,
) {
    use rumqttc::{Event, Outgoing, Packet};
    loop {
        let event = eventloop.poll().await;
        let failed = {
            let mut shared = shared.lock().unwrap();
            match event {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    shared.state = State::Connected;
                    // A clean session forgets them on every reconnect.
                    for (filter, qos) in &subscriptions {
                        if let Ok(qos) = rumqttc::qos(*qos) {
                            let _ = client.try_subscribe(filter, qos);
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

/// Updates `shared` from an MQTT v5 connection until the session ends.
/// Topic aliases stay disabled to prevent empty aliased topic names.
async fn run_v5(
    mut eventloop: rumqttc::v5::EventLoop,
    client: rumqttc::v5::AsyncClient,
    subscriptions: Vec<(String, u8)>,
    shared: Arc<Mutex<Shared>>,
    ctx: egui::Context,
) {
    use rumqttc::Outgoing;
    use rumqttc::v5::{Event, mqttbytes::v5::Packet};
    loop {
        let event = eventloop.poll().await;
        let failed = {
            let mut shared = shared.lock().unwrap();
            match event {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    shared.state = State::Connected;
                    // A clean start forgets them on every reconnect.
                    for (filter, qos) in &subscriptions {
                        if let Some(qos) = rumqttc::v5::mqttbytes::qos(*qos) {
                            let _ = client.try_subscribe(filter, qos);
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

    /// Returns the shared test broker port for `version`.
    /// Separate brokers avoid a rumqttd panic when forwarding v5 properties to
    /// v3.1.1 clients, which leaves its v3.1.1 listener rejecting connections.
    fn broker(version: Version) -> u16 {
        static PORTS: OnceLock<[u16; 2]> = OnceLock::new();
        let ports = PORTS.get_or_init(|| {
            // Bound together, so the two free ports differ.
            let listeners = [(); 2].map(|_| TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap());
            let ports = listeners.map(|l| l.local_addr().unwrap().port());
            for (v, port) in [Version::V311, Version::V5].into_iter().zip(ports) {
                let server = rumqttd::ServerSettings {
                    name: format!("{v:?}"),
                    listen: (Ipv4Addr::LOCALHOST, port).into(),
                    tls: None,
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
                let servers = Some(HashMap::from([(server.name.clone(), server)]));
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
                let listening = || TcpStream::connect((Ipv4Addr::LOCALHOST, port)).is_ok();
                assert!(wait_until(listening));
            }
            ports
        });
        ports[version as usize]
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
            port: broker(version),
            version,
            // rumqttd drops a client that subscribes to `$SYS/#`.
            subscriptions: vec![("#".into(), 1)],
            ..Profile::default()
        };
        let session = Session::start(&profile, None, 10, egui::Context::default());
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
        // The clear comes back as an empty message, which removes the topic.
        assert!(wait_until(|| latest(&first, "test/cleared/topic").is_none()));

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
}
