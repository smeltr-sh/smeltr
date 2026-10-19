//! A schema registry on 127.0.0.1 that answers Confluent's REST API from memory, for a test that has no live one.
//! Ids count from 1; a subject's versions from 1, in the order they were registered. The crate's own Avro tests
//! include it too (`src/io/avro/tests.rs`).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};

#[derive(Clone, Default)]
pub struct Registry {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    /// By id less one: its subject, its text, and what it references as JSON.
    schemas: Vec<(String, String, Value)>,
    /// Subjects whose compatibility refuses whatever is registered under them.
    refusing: Vec<String>,
    /// Calls still to answer 500.
    failing: usize,
    /// The `Authorization` every call must carry, where one must.
    authorization: Option<String>,
    /// Every request line and the `Authorization` it carried.
    seen: Vec<(String, Option<String>)>,
}

impl Registry {
    /// Serves it until the test ends, and answers its URL.
    pub fn serve(&self) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let registry = self.clone();
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(conn) = conn else { continue };
                let registry = registry.clone();
                std::thread::spawn(move || registry.answer_one(conn));
            }
        });
        url
    }

    /// Every call must sign in as `user` with `password`.
    pub fn sign_in(&self, user: &str, password: &str) {
        use base64::Engine;
        let token = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
        self.state.lock().unwrap().authorization = Some(format!("Basic {token}"));
    }

    /// `subject`'s compatibility refuses every schema registered under it.
    pub fn refuse(&self, subject: &str) {
        self.state.lock().unwrap().refusing.push(subject.to_string());
    }

    /// The next `n` calls answer 500.
    pub fn fail(&self, n: usize) {
        self.state.lock().unwrap().failing = n;
    }

    /// Every schema registered, by id less one: its subject and its text.
    pub fn schemas(&self) -> Vec<(String, String)> {
        let state = self.state.lock().unwrap();
        state.schemas.iter().map(|(s, t, _)| (s.clone(), t.clone())).collect()
    }

    /// Registers `text` under `subject`, referencing `references` (a JSON array of Confluent's references), and
    /// answers its id.
    pub fn hold(&self, subject: &str, text: &str, references: Value) -> u32 {
        let mut state = self.state.lock().unwrap();
        state.schemas.push((subject.into(), text.into(), references));
        state.schemas.len() as u32
    }

    /// Every request line so far, and the `Authorization` it carried.
    pub fn seen(&self) -> Vec<(String, Option<String>)> {
        self.state.lock().unwrap().seen.clone()
    }

    fn answer_one(&self, conn: std::net::TcpStream) {
        let mut reader = BufReader::new(conn);
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() {
            return;
        }
        let (mut length, mut authorization) = (0, None);
        loop {
            let mut header = String::new();
            if reader.read_line(&mut header).is_err() {
                return;
            }
            let Some((name, value)) = header.trim_end().split_once(':') else {
                break;
            };
            match name.to_ascii_lowercase().as_str() {
                "content-length" => length = value.trim().parse().unwrap_or(0),
                "authorization" => authorization = Some(value.trim().to_string()),
                _ => {}
            }
        }
        let mut body = vec![0; length];
        if reader.read_exact(&mut body).is_err() {
            return;
        }
        let (status, answer) = self.answer(line.trim_end(), authorization, &body);
        let _ = write!(
            reader.into_inner(),
            "HTTP/1.1 {status} X\r\nContent-Type: application/vnd.schemaregistry.v1+json\r\nContent-Length: {}\r\n\
             Connection: close\r\n\r\n{answer}",
            answer.len()
        );
    }

    fn answer(&self, line: &str, authorization: Option<String>, body: &[u8]) -> (u16, String) {
        let refused =
            |status: u16, message: &str| (status, json!({ "error_code": status, "message": message }).to_string());
        let mut state = self.state.lock().unwrap();
        state.seen.push((line.to_string(), authorization.clone()));
        if state.authorization.is_some() && state.authorization != authorization {
            return refused(401, "Unauthorized");
        }
        if state.failing > 0 {
            state.failing -= 1;
            return refused(500, "busy");
        }
        let mut parts = line.split(' ');
        let (method, path) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
        let path: Vec<String> = (path.trim_start_matches('/').split('/')).map(percent_decode).collect();
        let path: Vec<&str> = path.iter().map(String::as_str).collect();
        let found = |(_, text, references): &(String, String, Value)| {
            json!({ "schema": text, "references": references }).to_string()
        };
        match (method, path.as_slice()) {
            ("GET", ["schemas", "ids", id]) => match id
                .parse::<usize>()
                .ok()
                .and_then(|i| state.schemas.get(i.checked_sub(1)?))
            {
                Some(schema) => (200, found(schema)),
                None => refused(404, "Schema not found"),
            },
            ("GET", ["subjects", subject, "versions", version]) => {
                let at = version.parse::<usize>().ok().and_then(|v| v.checked_sub(1));
                match at.and_then(|at| state.schemas.iter().filter(|s| s.0 == *subject).nth(at)) {
                    Some(schema) => (200, found(schema)),
                    None => refused(404, "Version not found"),
                }
            }
            ("POST", ["subjects", subject, "versions"]) => {
                if state.refusing.iter().any(|s| s == subject) {
                    return refused(409, "Schema being registered is incompatible with an earlier schema");
                }
                let Some(text) = serde_json::from_slice::<Value>(body)
                    .ok()
                    .and_then(|v| Some(v["schema"].as_str()?.to_string()))
                else {
                    return refused(422, "Invalid schema");
                };
                let id = match state.schemas.iter().position(|s| s.0 == *subject && s.1 == text) {
                    Some(at) => at + 1,
                    None => {
                        state.schemas.push((subject.to_string(), text, json!([])));
                        state.schemas.len()
                    }
                };
                (200, json!({ "id": id }).to_string())
            }
            _ => refused(404, "HTTP 404 Not Found"),
        }
    }
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = (bytes[i] == b'%')
            .then(|| bytes.get(i + 1..i + 3))
            .flatten()
            .and_then(|h| u8::from_str_radix(std::str::from_utf8(h).ok()?, 16).ok());
        match hex {
            Some(b) => {
                out.push(b);
                i += 3;
            }
            None => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}
