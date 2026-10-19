//! Compiling a `GrokConfig` into regexes and matching one row against them.
//!
//! A `RegexSet` finds every candidate that matches in one pass; only the first match in declared order then pays for
//! capture extraction.
//!
//! Every pattern is built with `.` matching a newline. One `_raw` is one record and the source sets that boundary, so
//! a newline inside it is data like any other byte; line-bounded `.` is inherited from tools where the newline *is*
//! the record separator, and there it silently truncates a multi-line message at its first line. `(?-s)` inside a
//! pattern restores the line-bounded meaning for one element, which is why this needs no config surface.

use regex::{Captures, Regex, RegexBuilder, RegexSet, RegexSetBuilder};
use regex_syntax::ast::{self, Ast};
use regex_syntax::hir::{self, HirKind};

use crate::format::grok_pattern::{self, GrokError};
use crate::plan::config::{GrokConfig, GrokPatterns};

#[derive(Debug)]
pub struct Matcher {
    set: RegexSet,
    /// Each candidate's head: the whole pattern, or everything before a trailing greedy capture.
    patterns: Vec<Regex>,
    /// Per candidate, the name of a trailing greedy capture the head leaves off. Its value is whatever follows
    /// the head's match. The regex engine finds the same captures either way: the capture accepts every suffix,
    /// so the head's captures under the whole pattern are the head's own leftmost-first captures -- but
    /// resolving them over a long tail is what makes captures cost ten times a match. `split_tail` says which
    /// candidates qualify.
    tails: Vec<Option<String>>,
}

/// Which part of a candidate holds a named capture.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Group {
    Capture(usize),
    Tail,
}

/// One row's match: the candidate that matched and what it captured.
pub struct Matched<'m, 't> {
    pub pattern: usize,
    caps: Captures<'t>,
    tail: Option<(&'m str, &'t str)>,
}

impl<'t> Matched<'_, 't> {
    /// A named capture's text, or `None` where the group did not participate.
    pub fn name(&self, name: &str) -> Option<&'t str> {
        match self.tail {
            Some((tail, text)) if tail == name => Some(text),
            _ => self.caps.name(name).map(|m| m.as_str()),
        }
    }

    /// A capture's text by the index `Matcher::group_index` gave.
    pub fn get(&self, group: Group) -> Option<&'t str> {
        match group {
            Group::Capture(i) => self.caps.get(i).map(|m| m.as_str()),
            Group::Tail => self.tail.map(|(_, text)| text),
        }
    }
}

/// Split `expanded` into a head and the name of its trailing greedy capture.
///
/// The tail stands in for the capture only when two things hold: the capture really is the pattern's last
/// top-level element, and it accepts every suffix. Both views of the pattern are needed to say so. The AST
/// carries the byte offset the head ends at, which the HIR drops. The HIR resolves the inline flags the AST
/// leaves as written, so `(?U)` arrives as a lazy repetition and `(?-s)` as a class without `\n`. They must
/// agree on the name, and a pattern either view refuses is compiled whole.
fn split_tail(expanded: &str) -> (&str, Option<String>) {
    let Some((offset, name)) = ast_tail(expanded) else {
        return (expanded, None);
    };
    match hir_tail(expanded) {
        Some(resolved) if resolved == name => (&expanded[..offset], Some(name)),
        _ => (expanded, None),
    }
}

/// Where the pattern's last top-level element starts, and its capture name, when that element is a named group.
///
/// A top-level alternation answers `None`: `a|(?P<rest>.*)` ends in the group textually, but the group is one
/// branch of the whole pattern, not a piece that follows the rest of it. So does a group that only looks like
/// one, such as the trailing text of an `(?x)` comment, which parses to no node at all.
fn ast_tail(expanded: &str) -> Option<(usize, String)> {
    let ast = ast::parse::Parser::new().parse(expanded).ok()?;
    let last = match &ast {
        Ast::Concat(concat) => concat.asts.last()?,
        // A lone group is the whole pattern: `%{GREEDYDATA:rest}` on its own leaves an empty head.
        other => other,
    };
    let Ast::Group(group) = last else { return None };
    let ast::GroupKind::CaptureName { name, .. } = &group.kind else {
        return None;
    };
    Some((last.span().start.offset, name.name.clone()))
}

/// The name of the pattern's trailing capture, when the resolved pattern makes it a greedy `.*`.
fn hir_tail(expanded: &str) -> Option<String> {
    // The same `.` the compiled regex gets. A parse without it reads `.` as line-bounded and splits nothing.
    let mut parser = regex_syntax::ParserBuilder::new().dot_matches_new_line(true).build();
    let hir = parser.parse(expanded).ok()?;
    let last = match hir.kind() {
        HirKind::Concat(parts) => parts.last()?,
        _ => &hir,
    };
    let HirKind::Capture(capture) = last.kind() else {
        return None;
    };
    let name = capture.name.as_deref()?;
    let HirKind::Repetition(repetition) = capture.sub.kind() else {
        return None;
    };
    if !repetition.greedy || repetition.min != 0 || repetition.max.is_some() {
        return None;
    }
    accepts_every_char(repetition.sub.kind()).then(|| name.to_string())
}

/// Whether `kind` is a class holding every character, which is what makes the tail the whole rest of the text.
/// `(?-s)` narrows the class to everything but `\n` and fails here. Classes are canonical, so one range covers
/// everything or nothing does.
fn accepts_every_char(kind: &HirKind) -> bool {
    match kind {
        HirKind::Class(hir::Class::Unicode(class)) => {
            matches!(class.ranges(), [range] if range.start() == '\0' && range.end() == char::MAX)
        }
        HirKind::Class(hir::Class::Bytes(class)) => {
            matches!(class.ranges(), [range] if range.start() == 0 && range.end() == u8::MAX)
        }
        _ => false,
    }
}

/// A name `%{NAME}` can spell and every reader of it agrees on.
fn is_pattern_name(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

impl Matcher {
    /// Expand and compile every pattern in `config`, in order. A pattern the `regex` crate cannot compile is an `Err`.
    /// Called once per `Parser` step at plan-compile time and reused by every batch.
    pub fn compile(config: &GrokConfig) -> Result<Matcher, GrokError> {
        Self::compile_with(config, &GrokPatterns::new())
    }

    /// `compile`, naming the bundle's `shared` patterns too. A step's own pattern may not take a shared one's name:
    /// one name would then mean two patterns in one group.
    pub fn compile_with(config: &GrokConfig, shared: &GrokPatterns) -> Result<Matcher, GrokError> {
        if let Some((name, _)) = config.custom_patterns.iter().find(|(n, _)| shared.contains_key(n)) {
            return Err(GrokError(format!(
                "`{name}` is already on the Grok patterns page. Rename this one, or use that one"
            )));
        }
        if config.patterns.is_empty() {
            return Err(GrokError("grok parser needs at least one pattern".to_string()));
        }
        let mut exprs = Vec::with_capacity(config.patterns.len());
        let mut patterns = Vec::with_capacity(config.patterns.len());
        let mut tails = Vec::with_capacity(config.patterns.len());
        for p in &config.patterns {
            let expanded = grok_pattern::expand_with(p, config, shared)?;
            // The whole pattern compiles first, so a bad tail is still the author's error to see.
            RegexBuilder::new(&expanded)
                .dot_matches_new_line(true)
                .build()
                .map_err(|e| GrokError(format!("pattern `{p}` is not a valid regex: {e}")))?;
            let (head, tail) = split_tail(&expanded);
            let regex = RegexBuilder::new(head)
                .dot_matches_new_line(true)
                .build()
                .map_err(|e| GrokError(format!("pattern `{p}` is not a valid regex: {e}")))?;
            exprs.push(head.to_string());
            patterns.push(regex);
            tails.push(tail);
        }
        let set = RegexSetBuilder::new(&exprs)
            .dot_matches_new_line(true)
            .build()
            .map_err(|e| GrokError(format!("pattern set failed to compile: {e}")))?;
        Ok(Matcher { set, patterns, tails })
    }

    /// What is wrong with each of a bundle's patterns, by name: a built-in's name, or a definition that does not
    /// compile against the bundle's patterns and the built-ins alone. A step's own patterns are not in scope, so a
    /// shared pattern means the same in every step.
    pub fn check_shared(shared: &GrokPatterns) -> Vec<(String, GrokError)> {
        shared
            .keys()
            .filter_map(|name| {
                let refused = if grok_pattern::is_builtin(name) {
                    Err(GrokError(format!("`{name}` is a built-in pattern")))
                } else if !is_pattern_name(name) {
                    Err(GrokError(format!(
                        "`{name}` is not a pattern name. Use letters, digits and underscores"
                    )))
                } else {
                    let config = GrokConfig {
                        patterns: vec![format!("%{{{name}}}")],
                        custom_patterns: Vec::new(),
                        split_dots: false,
                    };
                    Self::compile_with(&config, shared).map(|_| ())
                };
                refused.err().map(|e| (name.clone(), e))
            })
            .collect()
    }

    /// Every name the patterns capture, in pattern order, each once. A capture can be minted by a nested pattern the
    /// top-level text never names, so this reads the compiled regexes rather than the pattern strings.
    pub fn capture_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = Vec::new();
        for (pattern, tail) in self.patterns.iter().zip(&self.tails) {
            for name in pattern.capture_names().flatten().chain(tail.as_deref()) {
                if !names.contains(&name) {
                    names.push(name);
                }
            }
        }
        names
    }

    /// The first candidate (declared order) that matches `text`, with its captures, or `None`. One pattern needs no
    /// set pass: its own capture search already answers whether it matches.
    pub fn match_row<'t>(&self, text: &'t str) -> Option<Matched<'_, 't>> {
        let pattern = if self.patterns.len() == 1 {
            0
        } else {
            let hits = self.set.matches(text);
            (0..self.patterns.len()).find(|i| hits.matched(*i))?
        };
        let caps = self.patterns[pattern].captures(text)?;
        let tail = self.tails[pattern].as_deref().map(|name| {
            let end = caps.get(0).expect("group 0 is the whole match").end();
            (name, &text[end..])
        });
        Some(Matched { pattern, caps, tail })
    }

    /// How many candidate patterns there are.
    pub fn pattern_count(&self) -> usize {
        self.patterns.len()
    }

    /// Where candidate `pattern` keeps the capture `name`, if it captures it.
    pub fn group_index(&self, pattern: usize, name: &str) -> Option<Group> {
        if self.tails[pattern].as_deref() == Some(name) {
            return Some(Group::Tail);
        }
        self.patterns[pattern]
            .capture_names()
            .position(|n| n == Some(name))
            .map(Group::Capture)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(patterns: &[&str]) -> GrokConfig {
        GrokConfig {
            split_dots: true,
            patterns: patterns.iter().map(|s| s.to_string()).collect(),
            custom_patterns: vec![],
        }
    }

    #[test]
    fn empty_pattern_list_is_a_compile_error() {
        assert!(Matcher::compile(&cfg(&[])).is_err());
    }

    #[test]
    fn a_backreference_fails_to_compile() {
        let config = GrokConfig {
            split_dots: true,
            patterns: vec![r#"(?P<q>['"])x\k<q>"#.into()],
            custom_patterns: vec![],
        };
        assert!(Matcher::compile(&config).is_err());
    }

    #[test]
    fn first_matching_pattern_wins() {
        let m = Matcher::compile(&cfg(&["%{INT:n}", "%{WORD:w}"])).unwrap();
        let caps = m.match_row("42").unwrap();
        assert_eq!(caps.name("n").unwrap(), "42");
        assert!(caps.name("w").is_none());
    }

    #[test]
    fn falls_through_to_a_later_candidate_when_an_earlier_one_does_not_match() {
        let m = Matcher::compile(&cfg(&["^%{INT:n}$", "%{WORD:w}"])).unwrap();
        let caps = m.match_row("hello").unwrap();
        assert_eq!(caps.name("w").unwrap(), "hello");
    }

    #[test]
    fn no_candidate_matching_is_none() {
        let m = Matcher::compile(&cfg(&["^%{INT:n}$"])).unwrap();
        assert!(m.match_row("hello").is_none());
    }

    /// The split exists to save work, never to change an answer. Every shape here is read twice: once through
    /// the matcher, once through the whole pattern compiled as one regex, which is what the split stands in for.
    #[test]
    fn a_split_reads_what_the_whole_pattern_reads() {
        for (pattern, input) in [
            ("<%{INT:pri}> %{WORD:w} %{GREEDYDATA:rest}", "x <14> ab cd\nef"),
            ("%{GREEDYDATA:rest}", "any line\nat all"),
            ("%{WORD:w}|%{GREEDYDATA:rest}", "hello world"),
            ("%{WORD:w}|%{GREEDYDATA:rest}", "!! not a word"),
            ("(?:a|b) %{GREEDYDATA:rest}", "b rest here"),
            ("(?i)^<%{INT:pri}> %{GREEDYDATA:rest}", "<14> abc\ndef"),
            ("^<%{INT:pri}> (?-s)%{GREEDYDATA:rest}", "<14> abc\ndef"),
            ("^(?-s:%{WORD:w}) %{GREEDYDATA:rest}", "ab cd\nef"),
            ("(?U)(?P<a>x)(?P<rest>.*)", "xyz"),
            ("^<%{INT:pri}> %{DATA:rest}", "<14> abc\ndef"),
            ("%{WORD:a} %{GREEDYDATA:b} %{GREEDYDATA:c}", "a b c d"),
            ("(?x) ^a\\ # (?P<rest>.*)", "a bcdef"),
            ("(?<U>x)(?:y)%{GREEDYDATA:rest}", "xyz rest here"),
            ("^%{INT:n}$", "42"),
        ] {
            let config = cfg(&[pattern]);
            let expanded = grok_pattern::expand(pattern, &config).unwrap();
            let whole = RegexBuilder::new(&expanded).dot_matches_new_line(true).build().unwrap();
            let m = Matcher::compile(&config).unwrap();

            let names: Vec<&str> = whole.capture_names().flatten().collect();
            assert_eq!(m.capture_names(), names, "{pattern}: capture names");

            let (got, want) = (m.match_row(input), whole.captures(input));
            assert_eq!(got.is_some(), want.is_some(), "{pattern}: matched");
            let (Some(got), Some(want)) = (got, want) else { continue };
            for name in &names {
                assert_eq!(
                    got.name(name),
                    want.name(name).map(|x| x.as_str()),
                    "{pattern}: capture `{name}`"
                );
            }
        }
    }

    #[test]
    fn a_greedy_tail_spans_newlines() {
        let m = Matcher::compile(&cfg(&["^<%{INT:pri}> %{GREEDYDATA:rest}"])).unwrap();
        let caps = m.match_row("<14> abc\ndef\nghi").unwrap();
        assert_eq!(caps.name("rest").unwrap(), "abc\ndef\nghi");
    }

    #[test]
    fn an_inline_negation_bounds_an_element_to_its_line() {
        let m = Matcher::compile(&cfg(&["^<%{INT:pri}> (?-s)%{GREEDYDATA:rest}"])).unwrap();
        let caps = m.match_row("<14> abc\ndef\nghi").unwrap();
        assert_eq!(caps.name("rest").unwrap(), "abc");
    }

    #[test]
    fn a_lazy_tail_still_captures_nothing() {
        let m = Matcher::compile(&cfg(&["^<%{INT:pri}> %{DATA:rest}"])).unwrap();
        assert_eq!(m.match_row("<14> abc\ndef").unwrap().name("rest").unwrap(), "");
    }

    /// Widening `.` only adds matches, so a pattern that needed a literal newline to be crossed now matches where it
    /// did not.
    #[test]
    fn dot_crosses_a_newline_between_elements() {
        let m = Matcher::compile(&cfg(&["^a.b$"])).unwrap();
        assert!(m.match_row("a\nb").is_some());
    }

    /// The wrapper sets one flag; every other flag an author writes still means what it meant, wherever it sits.
    #[test]
    fn other_inline_flags_still_apply() {
        for (pattern, input, want) in [
            ("(?i)^(?<w>hello)$", "HELLO", "HELLO"),
            // The `a` is outside the group, so only `bc` is captured.
            ("^a(?i)(?<w>bc)$", "aBC", "BC"),
            ("^(?i:(?<w>abc))$", "ABC", "ABC"),
        ] {
            let m = Matcher::compile(&cfg(&[pattern])).unwrap();
            assert_eq!(
                m.match_row(input).and_then(|c| c.name("w").map(|x| x.to_string())),
                Some(want.to_string()),
                "{pattern}"
            );
        }
    }

    #[test]
    fn extended_mode_still_ignores_whitespace() {
        let m = Matcher::compile(&cfg(&["(?x) ^ (?<w>a b c) $"])).unwrap();
        assert_eq!(m.match_row("abc").unwrap().name("w").unwrap(), "abc");
    }

    /// `(?s)` is orthogonal to `(?m)`: what `.` matches is not what `^` and `$` anchor to.
    #[test]
    fn anchors_are_unaffected() {
        let m = Matcher::compile(&cfg(&["(?m)^(?<w>second)$"])).unwrap();
        assert_eq!(m.match_row("first\nsecond").unwrap().name("w").unwrap(), "second");

        let m = Matcher::compile(&cfg(&["^(?<w>a)$"])).unwrap();
        assert!(
            m.match_row("a\nb").is_none(),
            "without `(?m)`, `$` is still the end of the record"
        );
    }

    #[test]
    fn numbered_groups_keep_their_index() {
        let m = Matcher::compile(&cfg(&["^(a)(?<w>b)$"])).unwrap();
        let caps = m.match_row("ab").unwrap();
        assert_eq!(caps.get(Group::Capture(1)).unwrap(), "a");
        assert_eq!(caps.name("w").unwrap(), "b");
    }

    /// The message quotes what the author wrote. Setting the flag on the builder rather than wrapping the pattern is
    /// what keeps it that way -- a `(?s:...)` wrapper put its own `)` inside the reported text, which turned an
    /// unclosed character class into a bad range and pointed the caret at neither.
    #[test]
    fn a_bad_pattern_is_reported_as_written() {
        let err = Matcher::compile(&cfg(&["^(?<w>[a-"])).unwrap_err().to_string();
        assert!(err.contains("^(?<w>[a-"), "{err}");
        assert!(err.contains("unclosed character class"), "{err}");
        assert!(
            !err.contains("(?s"),
            "the message must not leak how the flag is set: {err}"
        );
    }

    #[test]
    fn a_top_level_alternation_is_covered_by_the_flag() {
        let m = Matcher::compile(&cfg(&["^a.b$|^c.d$"])).unwrap();
        assert!(m.match_row("a\nb").is_some());
        assert!(m.match_row("c\nd").is_some());
    }

    #[test]
    fn captures_a_realistic_multi_field_line() {
        let m = Matcher::compile(&cfg(&[
            r#"%{IP:client} - - \[%{HTTPDATE:ts}\] \"%{WORD:method} %{URIPATHPARAM:path} HTTP/%{NUMBER:httpver}\" %{INT:status} %{INT:bytes}"#,
        ]))
        .unwrap();
        let line = r#"10.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.1" 200 1043"#;
        let caps = m.match_row(line).unwrap();
        assert_eq!(caps.name("client").unwrap(), "10.0.0.1");
        assert_eq!(caps.name("method").unwrap(), "GET");
        assert_eq!(caps.name("path").unwrap(), "/index.html");
        assert_eq!(caps.name("status").unwrap(), "200");
        assert_eq!(caps.name("bytes").unwrap(), "1043");
    }
}
