//! The user docs the website publishes, as the Markdown this build shipped with, for the AI agent to read: the same
//! Markdown the site serves beside each page.
//!
//! `docs.json` is written from the website's build by `just docs-markdown`, and `just website` fails when it is stale.
//! Each page is cut into sections at its headings; the first section is the page's opening, under its title.

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

static PAGES: LazyLock<Vec<Page>> =
    LazyLock::new(|| serde_json::from_str(include_str!("docs.json")).expect("docs.json is the generator's"));

/// Every page, by path.
pub fn pages() -> &'static [Page] {
    &PAGES
}

/// The sections whose text holds every word of `query`, ignoring case, in page order. At most [`MAX_HITS`].
pub fn search(query: &str) -> Vec<Hit> {
    let words: Vec<String> = query.split_whitespace().map(str::to_lowercase).collect();
    if words.is_empty() {
        return Vec::new();
    }
    let mut hits = Vec::new();
    for page in pages() {
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

/// One page's text, or with `section` that heading's text and everything under it. `None` when there is no such
/// page or heading.
pub fn read(path: &str, section: Option<&str>) -> Option<String> {
    let page = pages().iter().find(|p| p.path == path)?;
    let sections = match section {
        None => &page.sections[..],
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

    #[test]
    fn every_page_is_under_docs_and_opens_with_its_heading() {
        assert!(pages().len() > 50);
        for page in pages() {
            assert!(page.path.starts_with("/docs"), "{}", page.path);
            assert_eq!(page.sections[0].id, "");
            assert!(
                page.sections[0].text.lines().any(|l| l.starts_with("# ")),
                "{}",
                page.path
            );
        }
    }

    #[test]
    fn a_page_reads_whole_or_by_heading_with_what_is_under_it() {
        let page = pages()
            .iter()
            .find(|p| p.sections.windows(2).any(|w| w[0].level == 2 && w[1].level == 3))
            .unwrap();
        let at = page
            .sections
            .windows(2)
            .position(|w| w[0].level == 2 && w[1].level == 3)
            .unwrap();
        let (h2, h3) = (&page.sections[at], &page.sections[at + 1]);
        let section = read(&page.path, Some(&h2.id)).unwrap();
        assert!(section.starts_with(&h2.text) && section.contains(&h3.text));
        assert!(!section.contains(&page.sections[0].text));
        let whole = read(&page.path, None).unwrap();
        assert!(whole.starts_with(&page.sections[0].text) && whole.contains(&section));
        assert_eq!(read(&page.path, Some("no-such-heading")), None);
        assert_eq!(read("/docs/no-such-page", None), None);
    }

    #[test]
    fn a_search_finds_the_sections_holding_every_word() {
        let hits = search("KAFKA consumer");
        assert!(hits.iter().any(|h| h.path == "/docs/sources/kafka"));
        for hit in &hits {
            let text = read(
                &hit.path,
                Some(&hit.section).filter(|s| !s.is_empty()).map(String::as_str),
            )
            .unwrap();
            let text = text.to_lowercase();
            assert!(text.contains("kafka") && text.contains("consumer"), "{hit:?}");
        }
        assert!(search("").is_empty());
        assert!(search("zzzz-no-such-word").is_empty());
        assert_eq!(search("the").len(), MAX_HITS);
    }

    #[test]
    fn a_snippet_quotes_around_the_word_on_char_boundaries() {
        let text = format!("{}needle{}", "é".repeat(100), "ü".repeat(100));
        let quoted = snippet(&text, &text, "needle");
        assert!(quoted.starts_with('…') && quoted.ends_with('…') && quoted.contains("needle"));
        assert_eq!(snippet("short needle", "short needle", "needle"), "short needle");
    }
}
