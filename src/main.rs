// SPDX-License-Identifier: MIT OR Apache-2.0

//! MQTT explorer with a live topic tree, message inspection, history and publishing.

mod gui;
mod mqtt;
mod payload;
mod tree;

fn main() -> eframe::Result {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");
    // Entered on the GUI thread, so the session can `tokio::spawn` from there
    // without a handle being passed around.
    let _rt = rt.enter();
    gui::run()
}
