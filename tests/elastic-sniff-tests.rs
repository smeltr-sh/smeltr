//! Sniffing (`io::elastic_nodes`): an Elasticsearch destination that learns the cluster's nodes from its `url`,
//! against a cluster of fake nodes in this process.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use smeltr::io::elastic::Elastic;
use smeltr::io::elastic_nodes::{Nodes, Sniffing};
use smeltr::io::http::{Protocol, Transport};
use smeltr::plan::topology::{HeldSecrets, Sniff};

/// A node that answers each request with `answer(request line)`, and keeps every request line it was sent.
struct Fake {
    url: String,
    seen: Arc<Mutex<Vec<String>>>,
}

fn fake(answer: impl Fn(&str) -> (u16, String) + Send + 'static) -> Fake {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let kept = seen.clone();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(mut conn) = conn else { return };
            let mut reader = BufReader::new(conn.try_clone().unwrap());
            let mut line = String::new();
            if reader.read_line(&mut line).is_err() {
                continue;
            }
            let mut length = 0;
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                if header.trim().is_empty() {
                    break;
                }
                if let Some(v) = header.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = v.trim().parse().unwrap();
                }
            }
            reader.read_exact(&mut vec![0; length]).unwrap();
            let line = line.trim().to_string();
            let (status, body) = answer(&line);
            kept.lock().unwrap().push(line);
            let _ = write!(
                conn,
                "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    Fake { url, seen }
}

fn bulk_ok() -> (u16, String) {
    (200, r#"{"errors":false}"#.to_string())
}

/// A node that takes bulk requests.
fn data_node() -> Fake {
    fake(|_| bulk_ok())
}

/// The `_nodes/http` answer naming `nodes` as (address, zone).
fn nodes_answer(nodes: &[(&str, &str)]) -> String {
    let named: Vec<String> = (nodes.iter().enumerate())
        .map(|(i, (url, zone))| {
            format!(
                r#""n{i}": {{"roles": ["data"], "attributes": {{"zone": "{zone}"}}, "http": {{"publish_address": "{}"}}}}"#,
                url.trim_start_matches("http://")
            )
        })
        .collect();
    format!(r#"{{"nodes": {{{}}}}}"#, named.join(","))
}

/// A seed that answers `_nodes/http` with `nodes`, and takes bulk requests itself.
fn seed(nodes: String) -> Fake {
    fake(move |line| {
        if line.contains("/_nodes/http") {
            (200, nodes.clone())
        } else {
            bulk_ok()
        }
    })
}

fn sniffing(seed: &str, zone: Option<&str>) -> (Arc<Nodes>, Sniffing) {
    let config = smeltr::io::http::agent_config(None, &HeldSecrets::default()).unwrap();
    let sniff = Sniff {
        zone_attribute: "zone".to_string(),
        zone: zone.map(str::to_string),
    };
    let nodes = Nodes::new(seed, &sniff, None, &config, "es".to_string());
    let transport = Sniffing::new(nodes.clone(), smeltr::io::elastic::transport(seed, None, &config));
    (nodes, transport)
}

fn post(transport: &mut Sniffing) -> Result<u16, String> {
    transport
        .post(Elastic::new("i").path(), b"{}\n", Duration::from_secs(5))
        .map(|r| r.status)
        .map_err(|e| e.to_string())
}

fn bulks(fake: &Fake) -> usize {
    fake.seen
        .lock()
        .unwrap()
        .iter()
        .filter(|l| l.contains("/_bulk"))
        .count()
}

/// Bulk requests go to the nodes the cluster names, in turn, and the seed takes none.
#[test]
fn requests_are_spread_across_the_nodes_the_cluster_names() {
    let (a, b) = (data_node(), data_node());
    let seed = seed(nodes_answer(&[(&a.url, "z1"), (&b.url, "z2")]));
    let (nodes, mut transport) = sniffing(&seed.url, None);
    for _ in 0..4 {
        assert_eq!(post(&mut transport), Ok(200));
    }
    assert_eq!(nodes.known().len(), 2);
    assert_eq!((bulks(&a), bulks(&b), bulks(&seed)), (2, 2, 0));
}

/// A worker in a zone sends to its zone's nodes while one is up, and to the others once none is.
#[test]
fn a_worker_prefers_its_zones_nodes_and_falls_to_the_others() {
    let (a, b) = (data_node(), data_node());
    let gone = TcpListener::bind("127.0.0.1:0").unwrap();
    let gone_url = format!("http://{}", gone.local_addr().unwrap());
    drop(gone);
    let seed = seed(nodes_answer(&[(&a.url, "z1"), (&b.url, "z2"), (&gone_url, "z2")]));
    let (_, mut transport) = sniffing(&seed.url, Some("z2"));
    for _ in 0..4 {
        assert_eq!(post(&mut transport), Ok(200));
    }
    // The gone node in z2 failed over to b, and is passed over after; a, in another zone, took nothing.
    assert_eq!((bulks(&a), bulks(&b)), (0, 4));
}

/// A credential the cluster will not list nodes for leaves the destination sending to its url.
#[test]
fn a_refused_lookup_sends_to_the_url() {
    let seed = fake(|line| {
        if line.contains("/_nodes/http") {
            (403, r#"{"error":"security_exception"}"#.to_string())
        } else {
            bulk_ok()
        }
    });
    let (nodes, mut transport) = sniffing(&seed.url, None);
    assert_eq!(post(&mut transport), Ok(200));
    assert!(nodes.known().is_empty());
    assert_eq!(bulks(&seed), 1);
}

/// Every node gone: the request still goes, to the url.
#[test]
fn with_every_node_gone_a_request_goes_to_the_url() {
    let gone = TcpListener::bind("127.0.0.1:0").unwrap();
    let gone_url = format!("http://{}", gone.local_addr().unwrap());
    drop(gone);
    let seed = seed(nodes_answer(&[(&gone_url, "z1")]));
    let (_, mut transport) = sniffing(&seed.url, None);
    assert_eq!(post(&mut transport), Ok(200));
    assert_eq!(bulks(&seed), 1);
}

/// An address that takes no connection: a listener whose backlog is full, so a connection to it neither opens nor
/// is refused. The listener and the connections that fill it are kept in what it returns.
fn unanswering() -> (String, socket2::Socket, Vec<std::net::TcpStream>) {
    let listener = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
    listener
        .bind(&"127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap().into())
        .unwrap();
    listener.listen(0).unwrap();
    let addr = listener.local_addr().unwrap().as_socket().unwrap();
    let mut filling = Vec::new();
    while let Ok(conn) = std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(300)) {
        filling.push(conn);
        assert!(filling.len() < 64, "the backlog never filled");
    }
    (format!("http://{addr}"), listener, filling)
}

/// A node a worker cannot reach is given up on once its connection fails to open, and the same request goes on to
/// the next node within its budget.
#[test]
fn a_node_that_never_answers_is_left_for_the_next_within_one_request() {
    let (unreachable, _listener, _filling) = unanswering();
    let a = data_node();
    let seed = seed(nodes_answer(&[(&unreachable, "z1"), (&a.url, "z1")]));
    let (_, mut transport) = sniffing(&seed.url, None);
    let started = std::time::Instant::now();
    for _ in 0..2 {
        let sent = transport.post(Elastic::new("i").path(), b"{}\n", Duration::from_secs(30));
        assert_eq!(sent.map(|r| r.status).map_err(|e| e.to_string()), Ok(200));
    }
    assert!(
        started.elapsed() < smeltr::io::elastic_nodes::CONNECT_WITHIN * 2,
        "took {:?}",
        started.elapsed()
    );
    assert_eq!(bulks(&a), 2);
}

/// A lookup that gets no answer holds up the request that ran it only until that request's deadline.
#[test]
fn a_lookup_ends_with_the_request_that_ran_it() {
    // Connections open, as the kernel accepts them, and no answer ever comes.
    let silent = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", silent.local_addr().unwrap());
    let (_, mut transport) = sniffing(&url, None);
    let started = std::time::Instant::now();
    let sent = transport.post(Elastic::new("i").path(), b"{}\n", Duration::from_secs(1));
    assert!(sent.is_err());
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "took {:?}",
        started.elapsed()
    );
}
