//! `HeartbeatClient::send` against a server that drops idle connections.
//!
//! This is the CI flake that started it: a leader is free to close a
//! keep-alive connection at any moment, and when it does so exactly as the
//! next heartbeat reuses it, the write fails before the request was ever
//! served. `ureq` will not retry a POST itself.

use smeltr::heartbeat::{HeartbeatClient, HeartbeatRequest, ThreadHeartbeat};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Serves `serve_then_close` requests on a connection and then hangs up,
/// so the *next* request on that pooled connection fails the way a real
/// idle-timeout does. Returns its address and a counter of requests
/// actually served.
fn flaky_leader(serve_then_close: usize) -> (String, Arc<AtomicUsize>) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let served = Arc::new(AtomicUsize::new(0));
    let counter = served.clone();
    std::thread::spawn(move || {
        for conn in l.incoming() {
            let Ok(mut c) = conn else { continue };
            let counter = counter.clone();
            let mut r = BufReader::new(c.try_clone().unwrap());
            let mut on_this_conn = 0usize;
            loop {
                let mut len = 0usize;
                let mut line = String::new();
                let mut saw_request = false;
                loop {
                    line.clear();
                    match r.read_line(&mut line) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                    saw_request = true;
                    let t = line.trim_end();
                    if t.is_empty() {
                        break;
                    }
                    if let Some(v) = t.to_ascii_lowercase().strip_prefix("content-length:") {
                        len = v.trim().parse().unwrap_or(0);
                    }
                }
                if !saw_request {
                    break;
                }
                let mut body = vec![0u8; len];
                if r.read_exact(&mut body).is_err() {
                    break;
                }
                if on_this_conn >= serve_then_close {
                    // Hang up without answering: the connection the client
                    // just reused is gone.
                    break;
                }
                on_this_conn += 1;
                counter.fetch_add(1, Ordering::SeqCst);
                let payload = br#"{"threads":[]}"#;
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                    payload.len()
                );
                if c.write_all(head.as_bytes()).is_err() || c.write_all(payload).is_err() {
                    break;
                }
            }
        }
    });
    (format!("http://{addr}"), served)
}

fn beat() -> HeartbeatRequest {
    HeartbeatRequest {
        heartbeat_interval_ms: None,
        single_worker: Vec::new(),
        progress: Vec::new(),
        streams: None,
        streams_window: None,
        stopping: false,
        authority_fingerprints: Vec::new(),
        takes_authority: true,
        logs: Vec::new(),
        boot_id: 0,
        group_id: "prod".to_string(),
        secret_versions: Vec::new(),
        supervising: None,
        resources: None,
        worker_id: "w1".to_string(),
        stop_after_unreachable_secs: None,
        commit: None,
        smeltr_version: String::new(),
        release: None,
        authority_expires_millis: None,
        threads: vec![ThreadHeartbeat {
            kind: Some(smeltr::heartbeat::ThreadKind::Reader),
            thread_id: "t0".to_string(),
            group_id: "g".to_string(),
            partitions_owned: vec![],
            metrics: vec![],
        }],
        accepts: Vec::new(),
        took: None,
    }
}

#[test]
fn a_closed_pooled_connection_is_retried() {
    // One request per connection, then the server hangs up. The second
    // heartbeat reuses the dead connection, fails, and must retry on a
    // fresh one rather than surfacing the error.
    let (url, served) = flaky_leader(1);
    let client = HeartbeatClient::new(&url, "token", None, true).expect("client");

    client.send(&beat()).expect("first heartbeat");
    client
        .send(&beat())
        .expect("second heartbeat survives the closed pooled connection");
    client.send(&beat()).expect("and so does the third");

    assert_eq!(
        served.load(Ordering::SeqCst),
        3,
        "every heartbeat was eventually served"
    );
}

#[test]
fn a_leader_that_is_simply_down_still_fails() {
    // The retry must not turn a real outage into a hang or a false
    // success: nothing is listening, both attempts fail, the caller is
    // told. This is what keeps the peek/commit cursor honest -- a failed
    // send is what leaves the window's counts owed rather than lost.
    let dead = {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let a = l.local_addr().unwrap();
        drop(l);
        format!("http://{a}")
    };
    let client = HeartbeatClient::new(&dead, "token", None, true).expect("client");
    assert!(
        client.send(&beat()).is_err(),
        "an unreachable leader is still an error after the retry"
    );
}
