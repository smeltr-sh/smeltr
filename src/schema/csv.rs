//! A schema from a CSV of fields: a spreadsheet's table, one row per field under a header row. `Columns` says which
//! column holds a field's path and type, and which its `required`, `description`, `example` and `elasticsearch` (an
//! Elasticsearch type); other columns are a spreadsheet's own and are passed over. Unsaid, a column is found by its
//! header's name, without regard to case. A type is smeltr's spelling, a common name for one (`schema::named`), or
//! a value the reader is told the type of.

use std::collections::BTreeMap;

use serde_json::json;

use crate::config_file::SchemaFieldDto;
use crate::format::csv_scan;

/// Which column holds what, each by its index from 0.
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Columns {
    pub field: Option<usize>,
    #[serde(rename = "type")]
    pub ty: Option<usize>,
    pub required: Option<usize>,
    pub description: Option<usize>,
    pub example: Option<usize>,
    pub elasticsearch: Option<usize>,
}

impl Columns {
    /// The columns `header` names, each by a name it may go by.
    fn named(header: &[String]) -> Columns {
        let at = |names: &[&str]| (header.iter()).position(|h| names.iter().any(|n| h.trim().eq_ignore_ascii_case(n)));
        Columns {
            field: at(&["field", "name", "path", "column"]),
            ty: at(&["type", "data type", "datatype"]),
            required: at(&["required"]),
            description: at(&["description"]),
            example: at(&["example"]),
            elasticsearch: at(&["elasticsearch"]),
        }
    }
}

/// What a file holds before its columns are chosen: its header, its rows to show, the columns its header names, and
/// each value of the type column.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize)]
pub struct Preview {
    pub header: Vec<String>,
    /// The first rows, and after them the first row of each type value they do not hold, so every value shows in one.
    pub rows: Vec<Vec<String>>,
    pub columns: Columns,
    /// In the order the file first holds them.
    pub types: Vec<TypeValue>,
}

/// One value of a CSV's type column.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TypeValue {
    pub value: String,
    /// The type it reads as, `None` where it is no type the reader knows.
    pub reads_as: Option<String>,
    /// How many rows hold it.
    pub fields: usize,
}

/// How many rows a preview shows before those it adds for type values.
const SHOWN: usize = 5;

/// `text`'s header and rows to show, the columns `columns` says or its header names, and the values its type column
/// holds.
pub fn preview(text: &str, columns: Option<&Columns>) -> Result<Preview, String> {
    let mut rows = rows(text)?.into_iter();
    let (_, header) = rows.next().ok_or("the file is empty")?;
    let columns = columns.cloned().unwrap_or_else(|| Columns::named(&header));
    let mut types: Vec<TypeValue> = Vec::new();
    let mut shown = Vec::new();
    for (n, (_, row)) in rows.enumerate() {
        let value = columns
            .ty
            .and_then(|i| row.get(i))
            .map(|t| t.trim())
            .unwrap_or_default();
        let new = match types.iter_mut().find(|t| t.value == value) {
            Some(t) => {
                t.fields += 1;
                false
            }
            None if value.is_empty() => false,
            None => {
                types.push(TypeValue {
                    value: value.to_string(),
                    reads_as: type_of(value, &BTreeMap::new()),
                    fields: 1,
                });
                true
            }
        };
        if n < SHOWN || new {
            shown.push(row);
        }
    }
    Ok(Preview {
        types,
        rows: shown,
        header,
        columns,
    })
}

/// The fields `text` declares, its rows in order, from the columns `columns` says or its header names. `types` gives
/// the type of a value the reader does not know. `Err` names the row at fault, counting the header as row 1.
pub fn read(
    text: &str,
    columns: Option<&Columns>,
    types: &BTreeMap<String, String>,
) -> Result<Vec<SchemaFieldDto>, String> {
    let mut rows = rows(text)?.into_iter();
    let (_, header) = rows.next().ok_or("the file is empty")?;
    let c = columns.cloned().unwrap_or_else(|| Columns::named(&header));
    let field = c.field.ok_or("the header names no `field` column")?;
    let ty = c.ty.ok_or("the header names no `type` column")?;
    let mut fields = Vec::new();
    for (n, row) in rows {
        let cell = |i: Option<usize>| i.and_then(|i| row.get(i)).map(|c| c.trim()).unwrap_or_default();
        let name = cell(Some(field));
        if name.is_empty() {
            return Err(format!("row {n}: no `field`"));
        }
        let said = cell(Some(ty));
        if said.is_empty() {
            return Err(format!("row {n}: no `type` for `{name}`"));
        }
        let ty = type_of(said, types).ok_or_else(|| format!("row {n}: `{said}` is no type smeltr knows"))?;
        fields.push(SchemaFieldDto {
            field: name.to_string(),
            ty,
            required: yes(cell(c.required))
                .map_err(|said| format!("row {n}: `required` is {said:?}, not yes or no"))?,
            description: cell(c.description).to_string(),
            example: cell(c.example).to_string(),
            elasticsearch: Some(cell(c.elasticsearch))
                .filter(|t| !t.is_empty())
                .map(|t| json!({ "type": t }).as_object().cloned().expect("an object")),
        });
    }
    Ok(fields)
}

/// The type `said` reads as: the one `types` gives it, smeltr's own spelling, or a common name for one.
fn type_of(said: &str, types: &BTreeMap<String, String>) -> Option<String> {
    if let Some(ty) = types.get(said) {
        return Some(ty.clone());
    }
    if crate::expr::parse::parse_type(said).is_ok() {
        return Some(said.to_string());
    }
    super::named(said).map(str::to_string)
}

/// Each record of `text` but blank ones, split into its cells, with its row number from 1. A quoted cell may hold a
/// line end.
pub(super) fn rows(text: &str) -> Result<Vec<(usize, Vec<String>)>, String> {
    let mut out = Vec::new();
    for (n, record) in records(text).into_iter().enumerate() {
        if record.trim().is_empty() {
            continue;
        }
        let cells = csv_scan::split_line(record.as_bytes(), b',')
            .ok_or_else(|| format!("row {}: a quote is not closed", n + 1))?;
        out.push((n + 1, cells.into_iter().map(|c| c.text.into_owned()).collect()));
    }
    Ok(out)
}

/// `text` cut at each line end outside quotes.
fn records(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let (mut start, mut quoted) = (0, false);
    for (i, b) in text.bytes().enumerate() {
        match b {
            b'"' => quoted = !quoted,
            b'\n' if !quoted => {
                out.push(text[start..i].trim_end_matches('\r'));
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&text[start..]);
    out
}

/// A spreadsheet's yes or no. Blank is no.
fn yes(cell: &str) -> Result<bool, &str> {
    match cell.to_ascii_lowercase().as_str() {
        "" | "false" | "no" | "n" | "0" => Ok(false),
        "true" | "yes" | "y" | "1" => Ok(true),
        _ => Err(cell),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn by_header(text: &str) -> Result<Vec<SchemaFieldDto>, String> {
        read(text, None, &BTreeMap::new())
    }

    #[test]
    fn a_spreadsheet_of_fields_reads_by_its_header() {
        let text = "Notes,FIELD,Type,Required,Description,Example,Elasticsearch\r\n\
                    x,client.ip,str,yes,\"Where it came from,\nthe address.\",10.0.0.1,ip\r\n\
                    ,status,i64,,The HTTP status.,200,\r\n\
                    \r\n";
        let fields = by_header(text).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].field, "client.ip");
        assert!(fields[0].required);
        assert_eq!(fields[0].description, "Where it came from,\nthe address.");
        assert_eq!(fields[0].example, "10.0.0.1");
        assert_eq!(fields[0].elasticsearch, json!({ "type": "ip" }).as_object().cloned());
        assert!(!fields[1].required);
        assert_eq!(fields[1].elasticsearch, None);
    }

    #[test]
    fn a_spreadsheet_says_what_it_lacks_by_row() {
        assert_eq!(by_header("").unwrap_err(), "the file is empty");
        assert_eq!(
            by_header("key,type\na,str").unwrap_err(),
            "the header names no `field` column"
        );
        assert_eq!(by_header("field,type\na,str\n,i64").unwrap_err(), "row 3: no `field`");
        assert_eq!(by_header("field,type\na,").unwrap_err(), "row 2: no `type` for `a`");
        assert_eq!(
            by_header("field,type,required\na,str,maybe").unwrap_err(),
            "row 2: `required` is \"maybe\", not yes or no"
        );
        assert_eq!(
            by_header("field,type\n\"a,str").unwrap_err(),
            "row 2: a quote is not closed"
        );
        assert_eq!(
            by_header("field,type\na,widget").unwrap_err(),
            "row 2: `widget` is no type smeltr knows"
        );
    }

    #[test]
    fn a_preview_shows_a_row_of_each_type_value_and_counts_them() {
        let mut text = String::from("field,type\n");
        for i in 0..8 {
            text += &format!("f{i},str\n");
        }
        text += "late,int\nlater,int\n";
        let preview = preview(&text, None).unwrap();
        let shown: Vec<&str> = preview.rows.iter().map(|r| r[0].as_str()).collect();
        assert_eq!(shown, ["f0", "f1", "f2", "f3", "f4", "late"]);
        let counts: Vec<(&str, usize)> = preview.types.iter().map(|t| (t.value.as_str(), t.fields)).collect();
        assert_eq!(counts, [("str", 8), ("int", 2)]);
    }

    #[test]
    fn a_type_reads_by_its_common_name_or_as_it_is_told() {
        let text = "Column,Data Type\nhost,VARCHAR\nport,int\nseen,timestamp\nshape,widget\n";
        let preview = preview(text, None).unwrap();
        assert_eq!((preview.columns.field, preview.columns.ty), (Some(0), Some(1)));
        assert_eq!(preview.header, vec!["Column", "Data Type"]);
        assert_eq!(preview.rows.len(), 4);
        let types: Vec<(&str, Option<&str>)> = (preview.types.iter())
            .map(|t| (t.value.as_str(), t.reads_as.as_deref()))
            .collect();
        assert_eq!(
            types,
            [
                ("VARCHAR", Some("str")),
                ("int", Some("i64")),
                ("timestamp", Some("datetime")),
                ("widget", None)
            ]
        );
        let columns = Columns {
            field: Some(0),
            ty: Some(1),
            ..Columns::default()
        };
        let told = BTreeMap::from([("widget".to_string(), "{...}".to_string())]);
        let fields = read(text, Some(&columns), &told).unwrap();
        let got: Vec<(&str, &str)> = fields.iter().map(|f| (f.field.as_str(), f.ty.as_str())).collect();
        assert_eq!(
            got,
            [
                ("host", "str"),
                ("port", "i64"),
                ("seen", "datetime"),
                ("shape", "{...}")
            ]
        );
    }
}
