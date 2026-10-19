//! `parseUserAgent(text)`: the browser, operating system and device a User-Agent header names.
//!
//! The rules are uap-core's (`useragent/regexes.yaml`, Apache-2.0), compiled into the binary, so every worker built
//! from one commit reads a header alike; `just uap-refresh` takes the current ones. Each of the three lists is tried in
//! order and its first match wins, as uap-core specifies and Elastic's `user_agent` processor does. The object names
//! its fields as that processor does: `name` and `device.name` are `Other` where no rule matches.

use std::collections::HashMap;
use std::sync::OnceLock;

use regex::{Regex, RegexBuilder, RegexSet, RegexSetBuilder};

use super::helpers::short_circuit_empty;
use super::stdfuncs::{Field, object_type};
use crate::core::column::{Column, DataType, StrBuilder, VarType};
use crate::core::ctx::Ctx;

const REGEXES: &str = include_str!("useragent/regexes.yaml");

/// What `parseUserAgent` returns.
pub const USER_AGENT: &[(&str, Field)] = &[
    ("name", Field::Leaf(DataType::Str)),
    ("version", Field::Leaf(DataType::Str)),
    ("os", Field::Object(OS)),
    ("device", Field::Object(DEVICE)),
];

const OS: &[(&str, Field)] = &[
    ("name", Field::Leaf(DataType::Str)),
    ("version", Field::Leaf(DataType::Str)),
    ("full", Field::Leaf(DataType::Str)),
];

const DEVICE: &[(&str, Field)] = &[("name", Field::Leaf(DataType::Str))];

/// One header, read.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct UserAgent {
    pub name: String,
    pub version: Option<String>,
    pub os_name: Option<String>,
    pub os_version: Option<String>,
    pub device_name: String,
}

impl UserAgent {
    /// `os.full`: the name, then the version where there is one.
    pub fn os_full(&self) -> Option<String> {
        let name = self.os_name.as_ref()?;
        Some(match &self.os_version {
            Some(version) => format!("{name} {version}"),
            None => name.clone(),
        })
    }
}

/// `parseUserAgent(text)`. A header met twice in a batch is read once.
pub fn parse_user_agent(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &object_type(USER_AGENT), len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let rules = rules();
    let mut seen: HashMap<&[u8], UserAgent> = HashMap::new();
    let mut name = StrBuilder::new(ctx.pool, VarType::Str, len);
    let mut version = StrBuilder::new(ctx.pool, VarType::Str, len);
    let mut os_name = StrBuilder::new(ctx.pool, VarType::Str, len);
    let mut os_version = StrBuilder::new(ctx.pool, VarType::Str, len);
    let mut os_full = StrBuilder::new(ctx.pool, VarType::Str, len);
    let mut device = StrBuilder::new(ctx.pool, VarType::Str, len);
    for i in 0..len {
        if !v.validity.is_valid(i) {
            for b in [
                &mut name,
                &mut version,
                &mut os_name,
                &mut os_version,
                &mut os_full,
                &mut device,
            ] {
                b.push_null();
            }
            continue;
        }
        let text = v.get(i);
        let ua = seen
            .entry(text)
            .or_insert_with(|| rules.read(&String::from_utf8_lossy(text)));
        let mut put = |b: &mut StrBuilder, s: Option<&str>| match s {
            Some(s) => b.push_grow(ctx.pool, s.as_bytes()),
            None => b.push_null(),
        };
        put(&mut name, Some(&ua.name));
        put(&mut version, ua.version.as_deref());
        put(&mut os_name, ua.os_name.as_deref());
        put(&mut os_version, ua.os_version.as_deref());
        put(&mut os_full, ua.os_full().as_deref());
        put(&mut device, Some(&ua.device_name));
    }
    let os = Column::struct_from_parts(
        vec![
            ("name".into(), os_name.finish()),
            ("version".into(), os_version.finish()),
            ("full".into(), os_full.finish()),
        ],
        len,
    );
    let device = Column::struct_from_parts(vec![("name".into(), device.finish())], len);
    Column::struct_from_parts(
        vec![
            ("name".into(), name.finish()),
            ("version".into(), version.finish()),
            ("os".into(), os),
            ("device".into(), device),
        ],
        len,
    )
}

/// The three lists of rules, compiled once per process, the first time a row needs them: about a second in a release
/// build.
pub fn rules() -> &'static Rules {
    static RULES: OnceLock<Rules> = OnceLock::new();
    RULES.get_or_init(|| Rules::parse(REGEXES))
}

pub struct Rules {
    agents: RuleList,
    systems: RuleList,
    devices: RuleList,
}

impl Rules {
    pub fn read(&self, text: &str) -> UserAgent {
        let agent = self.agents.first(text);
        let system = self.systems.first(text);
        let device = self.devices.first(text);
        UserAgent {
            name: agent
                .as_ref()
                .and_then(|m| m.name.clone())
                .unwrap_or_else(|| "Other".into()),
            version: agent.as_ref().and_then(Match::version),
            os_name: system.as_ref().and_then(|m| m.name.clone()),
            os_version: system.as_ref().filter(|m| m.name.is_some()).and_then(Match::version),
            device_name: device.and_then(|m| m.name).unwrap_or_else(|| "Other".into()),
        }
    }

    /// The YAML uap-core writes: three lists of maps, each value one single-quoted string on its own line.
    fn parse(yaml: &str) -> Rules {
        let mut lists: HashMap<&str, Vec<HashMap<&str, String>>> = HashMap::new();
        let mut list = "";
        for line in yaml.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if !line.starts_with(' ') {
                list = trimmed.trim_end_matches(':');
                continue;
            }
            let (entry, starts) = match trimmed.strip_prefix("- ") {
                Some(rest) => (rest, true),
                None => (trimmed, false),
            };
            let (key, value) = entry
                .split_once(": ")
                .unwrap_or_else(|| panic!("regexes.yaml: `{line}` is not `key: 'value'`"));
            let value = value
                .strip_prefix('\'')
                .and_then(|v| v.strip_suffix('\''))
                .unwrap_or_else(|| panic!("regexes.yaml: `{line}` is not single-quoted"))
                .replace("''", "'");
            let rules = lists.entry(list).or_default();
            if starts {
                rules.push(HashMap::new());
            }
            rules
                .last_mut()
                .unwrap_or_else(|| panic!("regexes.yaml: `{line}` before any rule"))
                .insert(key, value);
        }
        let mut take =
            |name: &str, keys: [&'static str; 5]| RuleList::new(lists.remove(name).unwrap_or_default(), keys);
        Rules {
            agents: take(
                "user_agent_parsers",
                [
                    "family_replacement",
                    "v1_replacement",
                    "v2_replacement",
                    "v3_replacement",
                    "v4_replacement",
                ],
            ),
            systems: take(
                "os_parsers",
                [
                    "os_replacement",
                    "os_v1_replacement",
                    "os_v2_replacement",
                    "os_v3_replacement",
                    "os_v4_replacement",
                ],
            ),
            devices: take("device_parsers", ["device_replacement", "", "", "", ""]),
        }
    }
}

/// uap-core's rules are long alternations; the regex crate's limits are sized for hand-written patterns.
const SIZE: usize = 64 << 20;

/// One list: its rules in order, and a set over them that names which match.
struct RuleList {
    set: RegexSet,
    rules: Vec<Rule>,
}

struct Rule {
    pattern: String,
    /// Compiled the first time the set says this rule is the first to match: most never are.
    regex: OnceLock<Regex>,
    /// The name's replacement, then the four version parts'. `None` takes the capture group of that place.
    replacements: [Option<String>; 5],
}

/// What a rule read: the name, and the version's four parts.
struct Match {
    name: Option<String>,
    parts: [Option<String>; 4],
}

impl Match {
    /// `major.minor.patch.build`, as far as the parts go without a gap.
    fn version(&self) -> Option<String> {
        let mut out = self.parts[0].clone()?;
        for part in &self.parts[1..] {
            match part {
                Some(p) => {
                    out.push('.');
                    out.push_str(p);
                }
                None => break,
            }
        }
        Some(out)
    }
}

impl RuleList {
    fn new(entries: Vec<HashMap<&str, String>>, keys: [&'static str; 5]) -> RuleList {
        let mut patterns = Vec::with_capacity(entries.len());
        let mut rules = Vec::with_capacity(entries.len());
        for mut e in entries {
            let source = e.remove("regex").expect("regexes.yaml: a rule with no regex");
            let pattern = match e.get("regex_flag").map(String::as_str) {
                Some("i") => format!("(?i){source}"),
                _ => source,
            };
            let replacements = keys.map(|k| (!k.is_empty()).then(|| e.get(k).cloned()).flatten());
            patterns.push(pattern.clone());
            rules.push(Rule {
                pattern,
                regex: OnceLock::new(),
                replacements,
            });
        }
        let set = RegexSetBuilder::new(&patterns)
            .size_limit(SIZE)
            .dfa_size_limit(SIZE)
            .build()
            .expect("regexes.yaml: the rules as one set");
        RuleList { set, rules }
    }

    /// The first rule that matches `text`, read.
    fn first(&self, text: &str) -> Option<Match> {
        let i = self.set.matches(text).into_iter().next()?;
        let rule = &self.rules[i];
        let regex = rule.regex.get_or_init(|| {
            RegexBuilder::new(&rule.pattern)
                .size_limit(SIZE)
                .build()
                .unwrap_or_else(|err| panic!("regexes.yaml: `{}`: {err}", rule.pattern))
        });
        let caps = regex.captures(text)?;
        let group = |n: usize| caps.get(n).map(|m| m.as_str()).filter(|s| !s.is_empty());
        // A replacement names a group as `$1`; the place's own group fills in where it has none.
        let fill = |place: usize| -> Option<String> {
            let out = match &rule.replacements[place] {
                Some(r) => substitute(r, &group),
                None => group(place + 1)?.to_string(),
            };
            let out = out.trim();
            (!out.is_empty()).then(|| out.to_string())
        };
        Some(Match {
            name: fill(0),
            parts: [fill(1), fill(2), fill(3), fill(4)],
        })
    }
}

/// `template` with each `$1` to `$9` replaced by that group, or by nothing where it did not take part.
fn substitute<'a>(template: &str, group: &dyn Fn(usize) -> Option<&'a str>) -> String {
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        match (c, chars.peek().and_then(|d| d.to_digit(10))) {
            ('$', Some(n)) if n > 0 => {
                chars.next();
                out.push_str(group(n as usize).unwrap_or(""));
            }
            _ => out.push(c),
        }
    }
    out
}
