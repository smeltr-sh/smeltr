//! Text from the web: `urlDecode`, `htmlStrip`, `parseUrl` and `registeredDomain`.
//!
//! Each reads text the way Elasticsearch's ingest processor of the same job does (urldecode, html_strip,
//! uri_parts, registered_domain), and an object it returns names its fields as that processor does, so a pipeline
//! moved from Elastic keeps its field names. Text that does not read is a null row with a `CoercionRefused`, as
//! `parseInt` gives one; a blank text is null and clean.

use super::helpers::short_circuit_empty;
use super::stdfuncs::{Field, object_type};
use crate::core::column::{Column, DataType, StrBuilder, VarLen, VarType};
use crate::core::ctx::Ctx;
use crate::core::error::ExceptionKind;

mod entities;

/// Whether `text` is blank: no exception where nothing was there to read.
fn blank(text: &[u8]) -> bool {
    text.iter().all(u8::is_ascii_whitespace)
}

/// A text-to-text kernel: `f` gives the row's text, or `None` where it does not read.
fn text_map(ctx: &mut Ctx, ins: &[&Column], f: impl Fn(&[u8], &mut Vec<u8>) -> bool) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
    let mut out = Vec::new();
    for i in 0..len {
        if !v.validity.is_valid(i) {
            b.push_null();
            continue;
        }
        out.clear();
        if f(v.get(i), &mut out) {
            b.push_grow(ctx.pool, &out);
        } else {
            b.push_null();
            if !blank(v.get(i)) {
                ctx.raise(i, ExceptionKind::CoercionRefused { to: DataType::Str });
            }
        }
    }
    b.finish()
}

// ---------------------------------------------------------------------------
// urlDecode
// ---------------------------------------------------------------------------

/// `urlDecode(text)`: `+` is a space and `%XX` a byte, the bytes read as UTF-8, as a form's fields are encoded. A `%`
/// that two hex digits do not follow does not read.
pub fn url_decode(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    text_map(ctx, ins, |text, out| form_decode(text, out).is_some())
}

/// A `%` that two hex digits do not follow stays a `%`, as the WHATWG URL standard decodes it. Bytes that do not
/// form UTF-8 become U+FFFD.
fn form_decode(text: &[u8], out: &mut Vec<u8>) -> Option<()> {
    let bytes = percent_decode(text, true, true)?;
    out.extend_from_slice(String::from_utf8_lossy(&bytes).as_bytes());
    Some(())
}

/// `text` with each `%XX` as its byte, and each `+` as a space where `plus` says so. A `%` that two hex digits do not
/// follow is kept where `keep_bare` says so, and is `None` where it does not.
pub(crate) fn percent_decode(text: &[u8], plus: bool, keep_bare: bool) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len());
    let mut i = 0;
    while i < text.len() {
        match text[i] {
            b'%' => {
                let byte = (|| Some(hex_digit(*text.get(i + 1)?)? << 4 | hex_digit(*text.get(i + 2)?)?))();
                match byte {
                    Some(b) => {
                        out.push(b);
                        i += 3;
                    }
                    None if keep_bare => {
                        out.push(b'%');
                        i += 1;
                    }
                    None => return None,
                }
            }
            b'+' if plus => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    Some(out)
}

fn hex_digit(b: u8) -> Option<u8> {
    (b as char).to_digit(16).map(|d| d as u8)
}

// ---------------------------------------------------------------------------
// htmlStrip
// ---------------------------------------------------------------------------

/// `htmlStrip(text)`: the text without its markup. A tag goes, a block-level one and `<br>` becoming a line break;
/// a comment, a `<script>` and a `<style>` go with what they hold; a character reference becomes its character. Text
/// without both `<` and `>` is returned as it is, as Elastic's `html_strip` does.
pub fn html_strip(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    text_map(ctx, ins, |text, out| {
        let text = String::from_utf8_lossy(text);
        if !(text.contains('<') && text.contains('>')) {
            out.extend_from_slice(text.as_bytes());
        } else {
            strip_html(&text, out);
        }
        true
    })
}

/// The tags whose start or end breaks the line, as Lucene's HTMLStripCharFilter has them.
const BLOCK_TAGS: &[&str] = &[
    "address",
    "article",
    "aside",
    "blockquote",
    "br",
    "dd",
    "div",
    "dl",
    "dt",
    "fieldset",
    "figcaption",
    "figure",
    "footer",
    "form",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "hgroup",
    "hr",
    "li",
    "nav",
    "noscript",
    "ol",
    "output",
    "p",
    "pre",
    "section",
    "table",
    "tbody",
    "td",
    "tfoot",
    "th",
    "thead",
    "tr",
    "ul",
];

fn strip_html(text: &str, out: &mut Vec<u8>) {
    let mut rest = text;
    let mut unclosed = Unclosed::default();
    while let Some(at) = rest.find(['<', '&']) {
        out.extend_from_slice(&rest.as_bytes()[..at]);
        rest = &rest[at..];
        if rest.starts_with('&') {
            match char_reference(rest) {
                Some((c, used)) => {
                    out.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes());
                    rest = &rest[used..];
                }
                None => {
                    out.push(b'&');
                    rest = &rest[1..];
                }
            }
            continue;
        }
        // Markup that never closes is text.
        let Some(skip) = markup(rest, &mut unclosed, out) else {
            out.push(b'<');
            rest = &rest[1..];
            continue;
        };
        rest = &rest[skip..];
    }
    out.extend_from_slice(rest.as_bytes());
}

/// The closers a search found nowhere in the rest of the text, so no later opener searches for them again: each
/// search would run to the text's end.
#[derive(Default)]
struct Unclosed {
    comment: bool,
    cdata: bool,
    declaration: bool,
}

/// `closer`'s offset in `text`, or `None` where it is nowhere in it, which `unclosed` then remembers.
fn find_closer(text: &str, closer: &str, unclosed: &mut bool) -> Option<usize> {
    if *unclosed {
        return None;
    }
    let found = text.find(closer);
    *unclosed = found.is_none();
    found
}

/// The length of the markup `text` starts with, having written what it stands for. `None` where `<` starts no markup.
fn markup(text: &str, unclosed: &mut Unclosed, out: &mut Vec<u8>) -> Option<usize> {
    if let Some(body) = text.strip_prefix("<!--") {
        return Some(4 + find_closer(body, "-->", &mut unclosed.comment)? + 3);
    }
    if let Some(body) = text.strip_prefix("<![CDATA[") {
        let end = find_closer(body, "]]>", &mut unclosed.cdata)?;
        out.extend_from_slice(&body.as_bytes()[..end]);
        return Some(9 + end + 3);
    }
    if text.starts_with("<!") || text.starts_with("<?") {
        return Some(find_closer(text, ">", &mut unclosed.declaration)? + 1);
    }
    let after = text[1..].strip_prefix('/').unwrap_or(&text[1..]);
    let name_len = after.find(|c: char| !c.is_ascii_alphanumeric()).unwrap_or(after.len());
    if name_len == 0 || !after.as_bytes()[0].is_ascii_alphabetic() {
        return None;
    }
    let end = tag_end(text)?;
    let name = after[..name_len].to_ascii_lowercase();
    let closing = text.as_bytes()[1] == b'/';
    if !closing && (name == "script" || name == "style") {
        let body = &text[end..];
        let stop = end_tag(body, &name);
        let tail = body[stop..].find('>').map_or(body.len(), |g| stop + g + 1);
        return Some(end + tail);
    }
    if BLOCK_TAGS.contains(&name.as_str()) {
        out.push(b'\n');
    }
    Some(end)
}

/// Where the first end tag of the element `name` starts in `body`, in any case: `</name` then a space, `/` or `>`.
/// `body.len()` where there is none, so the element runs to the text's end.
fn end_tag(body: &str, name: &str) -> usize {
    let bytes = body.as_bytes();
    let mut from = 0;
    while let Some(at) = body[from..].find("</").map(|i| from + i) {
        let after = &bytes[at + 2..];
        let named = after.len() >= name.len() && after[..name.len()].eq_ignore_ascii_case(name.as_bytes());
        if named
            && after
                .get(name.len())
                .is_none_or(|&b| b.is_ascii_whitespace() || b == b'/' || b == b'>')
        {
            return at;
        }
        from = at + 2;
    }
    body.len()
}

/// The longest tag read. A quote that never closes would otherwise send every `<` after it to the text's end.
const MAX_TAG: usize = 4096;

/// Where the tag `text` starts with ends, past its `>`: a quoted attribute value may hold one.
fn tag_end(text: &str) -> Option<usize> {
    let mut quote = None;
    for (i, c) in text.char_indices().skip(1).take_while(|&(i, _)| i < MAX_TAG) {
        match (quote, c) {
            (None, '"' | '\'') => quote = Some(c),
            (Some(q), c) if c == q => quote = None,
            (None, '>') => return Some(i + 1),
            (None, '<') => return None,
            _ => {}
        }
    }
    None
}

/// The longest reference read, `&` to `;`.
const MAX_REFERENCE: usize = 32;

/// The character the reference `text` starts with stands for, and its length.
fn char_reference(text: &str) -> Option<(char, usize)> {
    // No reference is longer, so a `&` with no `;` near it costs no scan to the text's end.
    let end = text.bytes().take(MAX_REFERENCE).position(|b| b == b';')?;
    let body = &text[1..end];
    let c = if let Some(num) = body.strip_prefix('#') {
        let (digits, radix) = match num.strip_prefix(['x', 'X']) {
            Some(hex) => (hex, 16),
            None => (num, 10),
        };
        // `from_str_radix` would take a leading `+`, which no reference has.
        if !digits.starts_with(|c: char| c.is_digit(radix)) {
            return None;
        }
        char::from_u32(u32::from_str_radix(digits, radix).ok()?)?
    } else {
        named_reference(body)?
    };
    Some((c, end + 1))
}

/// The character HTML 4 names `name`.
fn named_reference(name: &str) -> Option<char> {
    let i = entities::ENTITIES.binary_search_by(|&(n, _)| n.cmp(name)).ok()?;
    Some(entities::ENTITIES[i].1)
}

// ---------------------------------------------------------------------------
// Objects
// ---------------------------------------------------------------------------

/// One `str` field per leaf of a flat object, and the `i64` ones apart, filled a row at a time.
struct ObjectBuilder {
    fields: &'static [(&'static str, Field)],
    strs: Vec<Option<StrBuilder>>,
    ints: Vec<Option<Column>>,
    row: usize,
}

impl ObjectBuilder {
    fn new(ctx: &mut Ctx, fields: &'static [(&'static str, Field)], len: usize) -> Self {
        let mut strs = Vec::new();
        let mut ints = Vec::new();
        for (_, f) in fields {
            let is_int = *f == Field::Leaf(DataType::I64);
            strs.push((!is_int).then(|| StrBuilder::new(ctx.pool, VarType::Str, len)));
            ints.push(is_int.then(|| {
                let mut c = Column::alloc(ctx.pool, &DataType::I64, len);
                let v = c.i64_mut().unwrap();
                v.values.fill(0);
                for i in 0..len {
                    v.validity.set_valid(i, false);
                }
                c
            }));
        }
        ObjectBuilder {
            fields,
            strs,
            ints,
            row: 0,
        }
    }

    /// Write the row, each field taking what `value` gives its name.
    fn push(&mut self, ctx: &mut Ctx, value: impl Fn(&str) -> Option<Value>) {
        for (k, (name, _)) in self.fields.iter().enumerate() {
            match (value(name), &mut self.strs[k], &mut self.ints[k]) {
                (Some(Value::Str(s)), Some(b), _) => b.push_grow(ctx.pool, s.as_bytes()),
                (_, Some(b), _) => b.push_null(),
                (Some(Value::Int(n)), _, Some(c)) => {
                    let v = c.i64_mut().unwrap();
                    v.values[self.row] = n;
                    v.validity.set_valid(self.row, true);
                }
                _ => {}
            }
        }
        self.row += 1;
    }

    fn finish(self) -> Column {
        let len = self.row;
        let columns = self
            .fields
            .iter()
            .zip(self.strs.into_iter().zip(self.ints))
            .map(|((name, _), pair)| {
                let col = match pair {
                    (Some(b), _) => b.finish(),
                    (None, Some(c)) => c,
                    (None, None) => unreachable!("each field is a str or an i64"),
                };
                (name.to_string(), col)
            });
        let mut columns: Vec<(String, Column)> = columns.collect();
        for (_, c) in &mut columns {
            if let Ok(v) = c.i64_mut() {
                v.validity.recount();
            }
        }
        Column::struct_from_parts(columns, len)
    }
}

enum Value<'a> {
    Str(std::borrow::Cow<'a, str>),
    Int(i64),
}

/// An object kernel: `parse` gives the row's fields by name, or `None` where the text does not read.
fn object_map<'t, P>(ctx: &mut Ctx, ins: &[&'t Column], fields: &'static [(&'static str, Field)], parse: P) -> Column
where
    P: Fn(&str) -> Option<Box<dyn Fn(&str) -> Option<Value<'static>>>>,
{
    let len = ins[0].len();
    let ty = object_type(fields);
    if let Some(e) = short_circuit_empty(ins, &ty, len) {
        return e;
    }
    let v: VarLen<'t> = ins[0].var().unwrap();
    let mut b = ObjectBuilder::new(ctx, fields, len);
    for i in 0..len {
        let text = v.validity.is_valid(i).then(|| String::from_utf8_lossy(v.get(i)));
        match text.as_deref().and_then(&parse) {
            Some(get) => b.push(ctx, |name| get(name)),
            None => {
                b.push(ctx, |_| None);
                if text.is_some_and(|t| !blank(t.as_bytes())) {
                    ctx.raise(i, ExceptionKind::CoercionRefused { to: ty.clone() });
                }
            }
        }
    }
    b.finish()
}

// ---------------------------------------------------------------------------
// parseUrl
// ---------------------------------------------------------------------------

/// What `parseUrl` returns, named as Elastic's `uri_parts` names them.
pub const URL_PARTS: &[(&str, Field)] = &[
    ("domain", Field::Leaf(DataType::Str)),
    ("fragment", Field::Leaf(DataType::Str)),
    ("path", Field::Leaf(DataType::Str)),
    ("extension", Field::Leaf(DataType::Str)),
    ("port", Field::Leaf(DataType::I64)),
    ("query", Field::Leaf(DataType::Str)),
    ("scheme", Field::Leaf(DataType::Str)),
    ("user_info", Field::Leaf(DataType::Str)),
    ("username", Field::Leaf(DataType::Str)),
    ("password", Field::Leaf(DataType::Str)),
];

/// `parseUrl(text)`: a URL's parts, as `uri_parts` reads them. The path, query, fragment and user info are
/// percent-decoded. A URL that is not valid RFC 3986 is still read, its parts as written, where its scheme is
/// `http`, `https`, `ftp`, `file` or `jar`; any other does not read.
pub fn parse_url(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    object_map(ctx, ins, URL_PARTS, |text| {
        let parts = UrlParts::read(text)?;
        Some(Box::new(move |name| parts.get(name)))
    })
}

#[derive(Default, Debug, PartialEq)]
pub(crate) struct UrlParts {
    pub domain: Option<String>,
    pub fragment: Option<String>,
    pub path: Option<String>,
    pub port: Option<i64>,
    pub query: Option<String>,
    pub scheme: Option<String>,
    pub user_info: Option<String>,
}

impl UrlParts {
    fn get(&self, name: &str) -> Option<Value<'static>> {
        let s = |v: &Option<String>| v.clone().map(|s| Value::Str(s.into()));
        match name {
            "domain" => s(&self.domain),
            "fragment" => s(&self.fragment),
            "path" => s(&self.path),
            "extension" => self.extension().map(|e| Value::Str(e.to_string().into())),
            "port" => self.port.map(Value::Int),
            "query" => s(&self.query),
            "scheme" => s(&self.scheme),
            "user_info" => s(&self.user_info),
            "username" => self.credentials().map(|(u, _)| Value::Str(u.to_string().into())),
            "password" => self
                .credentials()
                .and_then(|(_, p)| p)
                .map(|p| Value::Str(p.to_string().into())),
            _ => None,
        }
    }

    /// What follows the last `.` of the path's last segment, the whole path where it has no `/`.
    pub(crate) fn extension(&self) -> Option<&str> {
        let path = self.path.as_deref()?;
        let last = &path[path.rfind('/').map_or(0, |slash| slash + 1)..];
        last.rfind('.').map(|dot| &last[dot + 1..])
    }

    /// The user info's name, and its password where a `:` follows the name.
    pub(crate) fn credentials(&self) -> Option<(&str, Option<&str>)> {
        let info = self.user_info.as_deref()?;
        Some(match info.split_once(':') {
            Some((name, password)) => (name, Some(password)),
            None => (info, None),
        })
    }

    /// RFC 3986's parts, decoded, as Java's `URI` gives them; failing that, as its `URL` does, as written.
    pub(crate) fn read(text: &str) -> Option<UrlParts> {
        match Self::split(text, true) {
            Some(parts) => Some(parts),
            None => {
                let parts = Self::split(text, false)?;
                let lenient = ["http", "https", "ftp", "file", "jar"];
                let scheme = parts.scheme.as_deref()?.to_ascii_lowercase();
                lenient.contains(&scheme.as_str()).then_some(parts)
            }
        }
    }

    /// `strict`: refuse a character RFC 3986 does not allow, and decode each part.
    fn split(text: &str, strict: bool) -> Option<UrlParts> {
        if strict && !text.chars().all(uri_char) {
            return None;
        }
        let decode = |s: &str| -> Option<String> {
            if !strict {
                return Some(s.to_string());
            }
            let bytes = percent_decode(s.as_bytes(), false, false)?;
            Some(String::from_utf8_lossy(&bytes).into_owned())
        };
        let mut parts = UrlParts::default();
        let (rest, fragment) = match text.split_once('#') {
            Some((r, f)) => (r, Some(f)),
            None => (text, None),
        };
        parts.fragment = decode_some(fragment, &decode)?;
        let scheme_end = rest.find(':').filter(|&end| {
            let s = &rest[..end];
            s.starts_with(|c: char| c.is_ascii_alphabetic())
                && s.chars().all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c))
        });
        let rest = match scheme_end {
            Some(end) => {
                parts.scheme = Some(rest[..end].to_string());
                &rest[end + 1..]
            }
            None => rest,
        };
        // An opaque URI, `mailto:a@b.c`, has no path, query or authority.
        if parts.scheme.is_some() && !rest.starts_with('/') {
            return (!strict || !rest.is_empty()).then_some(parts);
        }
        let (rest, query) = match rest.split_once('?') {
            Some((r, q)) => (r, Some(q)),
            None => (rest, None),
        };
        parts.query = decode_some(query, &decode)?;
        let path = match rest.strip_prefix("//") {
            Some(after) => {
                let end = after.find('/').unwrap_or(after.len());
                parts.read_authority(&after[..end], strict, &decode)?;
                &after[end..]
            }
            None => rest,
        };
        parts.path = Some(decode(path)?);
        Some(parts)
    }

    /// `user@host:port`. A port that is not a number leaves the authority unread, as `URI` treats it as a registry
    /// name.
    fn read_authority(&mut self, authority: &str, strict: bool, decode: &dyn Fn(&str) -> Option<String>) -> Option<()> {
        let (user_info, host_port) = match authority.rsplit_once('@') {
            Some((u, h)) => (Some(u), h),
            None => (None, authority),
        };
        let (host, port) = match host_port.rfind(':') {
            Some(colon) if !host_port[colon..].contains(']') => (&host_port[..colon], Some(&host_port[colon + 1..])),
            _ => (host_port, None),
        };
        let port = match port {
            None | Some("") => None,
            Some(p) => match p.parse::<u16>() {
                Ok(n) => Some(i64::from(n)),
                Err(_) if strict => return Some(()),
                Err(_) => return None,
            },
        };
        self.user_info = decode_some(user_info, decode)?;
        self.domain = (!host.is_empty()).then(|| host.to_string());
        self.port = port;
        Some(())
    }
}

/// `part` decoded, where there is one. `None` where it does not decode.
fn decode_some(part: Option<&str>, decode: &dyn Fn(&str) -> Option<String>) -> Option<Option<String>> {
    match part {
        Some(p) => Some(Some(decode(p)?)),
        None => Some(None),
    }
}

/// A character Java's `URI` takes: RFC 3986's, `%` among them, and any non-ASCII one that is not a control or a
/// space.
fn uri_char(c: char) -> bool {
    if c.is_ascii() {
        c.is_ascii_alphanumeric() || "-._~:/?#[]@!$&'()*+,;=%".contains(c)
    } else {
        !c.is_control() && !c.is_whitespace()
    }
}

// ---------------------------------------------------------------------------
// registeredDomain
// ---------------------------------------------------------------------------

/// What `registeredDomain` returns, named as Elastic's `registered_domain` names them.
pub const DOMAIN_PARTS: &[(&str, Field)] = &[
    ("domain", Field::Leaf(DataType::Str)),
    ("registered_domain", Field::Leaf(DataType::Str)),
    ("top_level_domain", Field::Leaf(DataType::Str)),
    ("subdomain", Field::Leaf(DataType::Str)),
];

/// `registeredDomain(host)`: the host, the name registered under its public suffix, that suffix, and what is left.
/// The suffix is one the ICANN section of the Public Suffix List names; a private section's, such as
/// `blogspot.com`, is a registered name as Elastic reads it. A host with no known suffix does not read.
pub fn registered_domain(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    object_map(ctx, ins, DOMAIN_PARTS, |text| {
        let parts = DomainParts::read(text)?;
        Some(Box::new(move |name| {
            let v = match name {
                "domain" => Some(&parts.domain),
                "registered_domain" => parts.registered_domain.as_ref(),
                "top_level_domain" => Some(&parts.top_level_domain),
                "subdomain" => parts.subdomain.as_ref(),
                _ => None,
            };
            v.map(|s| Value::Str(s.clone().into()))
        }))
    })
}

#[derive(Debug, PartialEq)]
pub(crate) struct DomainParts {
    pub domain: String,
    pub registered_domain: Option<String>,
    pub top_level_domain: String,
    pub subdomain: Option<String>,
}

impl DomainParts {
    pub(crate) fn read(fqdn: &str) -> Option<DomainParts> {
        let fqdn = fqdn.trim();
        let host = fqdn.to_ascii_lowercase();
        if host.is_empty() || host.starts_with('.') {
            return None;
        }
        let suffix_len = icann_suffix_len(&host)?;
        if suffix_len == host.len() {
            return Some(DomainParts {
                domain: fqdn.to_string(),
                registered_domain: None,
                top_level_domain: host,
                subdomain: None,
            });
        }
        let suffix_start = host.len() - suffix_len;
        let label_start = host[..suffix_start - 1].rfind('.').map_or(0, |d| d + 1);
        let registered = &host[label_start..];
        Some(DomainParts {
            domain: fqdn.to_string(),
            registered_domain: Some(registered.to_string()),
            top_level_domain: host[suffix_start..].to_string(),
            subdomain: (label_start > 0).then(|| fqdn[..label_start - 1].to_string()),
        })
    }
}

/// The length of `host`'s ICANN public suffix. A private suffix is stepped past to the ICANN one under it.
fn icann_suffix_len(host: &str) -> Option<usize> {
    use psl::Psl as _;
    let mut name = host;
    loop {
        let info = psl::List.find(name.rsplit('.').map(str::as_bytes));
        match info.typ {
            Some(psl::Type::Icann) => return Some(info.len).filter(|&n| n > 0 && n <= name.len()),
            Some(psl::Type::Private) => {
                let suffix = &name[name.len() - info.len..];
                name = suffix.split_once('.')?.1;
            }
            None => return None,
        }
    }
}
