//! The user docs the website publishes, as the Markdown this build shipped with, for the AI agent to read: the same
//! Markdown the site serves beside each page.
//!
//! build.rs embeds them from the website's build, which `just frontends` runs; a binary built without that build has
//! none. Each page is cut into sections at its headings; the first section is the page's opening, under its title.

use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

/// The most hits one search answers.
pub const MAX_HITS: usize = 20;

/// About how much of a section a hit quotes, either side of the first word it matched.
const SNIPPET: usize = 120;

#[derive(Clone, Debug, Deserialize)]
pub struct Page {
    /// The page's path on the website, `/docs/...`.
    pub path: String,
    pub title: String,
    pub sections: Vec<Section>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Section {
    /// The heading's id on the page; empty for the page's opening.
    pub id: String,
    /// 1 for the page's opening, 2 and on for its headings.
    pub level: u8,
    pub heading: String,
    /// Markdown, from the heading on; the opening also holds what stands before the title.
    pub text: String,
}

/// A section a search matched.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Hit {
    pub path: String,
    pub title: String,
    pub section: String,
    pub heading: String,
    pub snippet: String,
}

/// The pages, by path.
pub struct Docs(Vec<Page>);

mod built {
    include!(concat!(env!("OUT_DIR"), "/website.rs"));
}

static DOCS: LazyLock<Docs> = LazyLock::new(|| {
    let json = built::FILES.iter().find(|(path, _)| *path == "docs.json");
    Docs(json.map_or_else(Vec::new, |(_, json)| {
        serde_json::from_slice(json).expect("docs.json is the generator's")
    }))
});

/// The docs this build shipped with.
pub fn docs() -> &'static Docs {
    &DOCS
}

impl Docs {
    /// Every page, by path.
    pub fn pages(&self) -> &[Page] {
        &self.0
    }

    /// The sections whose text holds every word of `query`, ignoring case, in page order. At most [`MAX_HITS`].
    pub fn search(&self, query: &str) -> Vec<Hit> {
        let words: Vec<String> = query.split_whitespace().map(str::to_lowercase).collect();
        if words.is_empty() {
            return Vec::new();
        }
        let mut hits = Vec::new();
        for page in self.pages() {
            for section in &page.sections {
                let text = section.text.to_lowercase();
                if words.iter().all(|w| text.contains(w.as_str())) {
                    hits.push(Hit {
                        path: page.path.clone(),
                        title: page.title.clone(),
                        section: section.id.clone(),
                        heading: section.heading.clone(),
                        snippet: snippet(&section.text, &text, &words[0]),
                    });
                    if hits.len() == MAX_HITS {
                        return hits;
                    }
                }
            }
        }
        hits
    }

    /// The page at `path`, `/docs/...`.
    pub fn page(&self, path: &str) -> Option<&Page> {
        self.pages().iter().find(|p| p.path == path)
    }

    /// One page's opening, or with `section` that heading's text and everything under it. `None` when there is no such
    /// page or heading.
    pub fn read(&self, path: &str, section: Option<&str>) -> Option<String> {
        let page = self.page(path)?;
        let sections = match section {
            None => &page.sections[..1],
            Some(id) => {
                let start = page.sections.iter().position(|s| s.id == id)?;
                let level = page.sections[start].level;
                let end = page.sections[start + 1..]
                    .iter()
                    .position(|s| s.level <= level)
                    .map_or(page.sections.len(), |n| start + 1 + n);
                &page.sections[start..end]
            }
        };
        Some(
            sections
                .iter()
                .map(|s| s.text.as_str())
                .collect::<Vec<_>>()
                .join("\n\n"),
        )
    }
}

/// Up to [`SNIPPET`] bytes of `text` either side of where `word` first falls in `lower`, its lowercase.
fn snippet(text: &str, lower: &str, word: &str) -> String {
    // Lowercasing can change a character's length; where it did, quote the section's start.
    let at = if lower.len() == text.len() {
        lower.find(word).unwrap_or(0)
    } else {
        0
    };
    let mut start = at.saturating_sub(SNIPPET);
    while !text.is_char_boundary(start) {
        start -= 1;
    }
    let mut end = (at + word.len() + SNIPPET).min(text.len());
    while !text.is_char_boundary(end) {
        end += 1;
    }
    let mut quoted = text[start..end].to_string();
    if start > 0 {
        quoted.insert(0, '…');
    }
    if end < text.len() {
        quoted.push('…');
    }
    quoted
}

#[cfg(test)]
mod tests {
    use super::*;

    fn docs() -> Docs {
        let section = |id: &str, level: u8, text: &str| Section {
            id: id.into(),
            level,
            heading: text.trim_start_matches('#').trim().into(),
            text: text.into(),
        };
        let page = |path: &str, sections: Vec<Section>| Page {
            path: path.into(),
            title: sections[0].heading.clone(),
            sections,
        };
        Docs(vec![
            page(
                "/docs/sources/kafka",
                vec![
                    section("", 1, "# Kafka\n\nReads topics."),
                    section("consumer", 2, "## Consumer group\n\nOne Kafka consumer per worker."),
                    section("offsets", 3, "### Offsets\n\nCommitted after a batch lands."),
                    section("tls", 2, "## TLS\n\nThe broker's certificate."),
                ],
            ),
            page(
                "/docs/sources/http",
                vec![section("", 1, "# HTTP\n\nThe consumer posts rows.")],
            ),
        ])
    }

    #[test]
    fn a_page_reads_its_opening_or_a_heading_with_what_is_under_it() {
        let docs = docs();
        let kafka = "/docs/sources/kafka";
        let consumer = docs.read(kafka, Some("consumer")).unwrap();
        assert!(consumer.starts_with("## Consumer group") && consumer.contains("### Offsets"));
        assert!(!consumer.contains("# Kafka") && !consumer.contains("## TLS"));
        assert_eq!(docs.read(kafka, None).unwrap(), "# Kafka\n\nReads topics.");
        assert_eq!(docs.read(kafka, Some("no-such-heading")), None);
        assert_eq!(docs.read("/docs/no-such-page", None), None);
    }

    #[test]
    fn a_search_finds_the_sections_holding_every_word() {
        let docs = docs();
        let found =
            |q: &str| -> Vec<(String, String)> { docs.search(q).into_iter().map(|h| (h.path, h.section)).collect() };
        assert_eq!(
            found("KAFKA consumer"),
            [("/docs/sources/kafka".into(), "consumer".into())]
        );
        assert_eq!(found("consumer").len(), 2);
        assert!(found("").is_empty());
        assert!(found("zzzz-no-such-word").is_empty());
    }

    #[test]
    fn a_snippet_quotes_around_the_word_on_char_boundaries() {
        let text = format!("{}needle{}", "é".repeat(100), "ü".repeat(100));
        let quoted = snippet(&text, &text, "needle");
        assert!(quoted.starts_with('…') && quoted.ends_with('…') && quoted.contains("needle"));
        assert_eq!(snippet("short needle", "short needle", "needle"), "short needle");
    }
}
