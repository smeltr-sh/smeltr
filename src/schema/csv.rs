//! A schema from a CSV of fields: a spreadsheet's table, one row per field under a header row that names its columns.
//! `field` and `type` are named; `required`, `description`, `example` and `elasticsearch` (an Elasticsearch type) may
//! be. Other columns are a spreadsheet's own and are passed over. A header is matched without regard to case.

use serde_json::json;

use crate::config_file::SchemaFieldDto;
use crate::format::csv_scan;

/// The fields `text` declares, its rows in order. `Err` names the row at fault, counting the header as row 1.
pub fn read(text: &str) -> Result<Vec<SchemaFieldDto>, String> {
    let mut rows = rows(text)?.into_iter();
    let (_, header) = rows.next().ok_or("the file is empty")?;
    let at = |name: &str| header.iter().position(|h| h.trim().eq_ignore_ascii_case(name));
    let field = at("field").ok_or("the header names no `field` column")?;
    let ty = at("type").ok_or("the header names no `type` column")?;
    let (required, description, example, elasticsearch) =
        (at("required"), at("description"), at("example"), at("elasticsearch"));
    let mut fields = Vec::new();
    for (n, row) in rows {
        let cell = |i: Option<usize>| i.and_then(|i| row.get(i)).map(|c| c.trim()).unwrap_or_default();
        let name = cell(Some(field));
        if name.is_empty() {
            return Err(format!("row {n}: no `field`"));
        }
        let ty = cell(Some(ty));
        if ty.is_empty() {
            return Err(format!("row {n}: no `type` for `{name}`"));
        }
        fields.push(SchemaFieldDto {
            field: name.to_string(),
            ty: ty.to_string(),
            required: yes(cell(required)).map_err(|said| format!("row {n}: `required` is {said:?}, not yes or no"))?,
            description: cell(description).to_string(),
            example: cell(example).to_string(),
            elasticsearch: Some(cell(elasticsearch))
                .filter(|t| !t.is_empty())
                .map(|t| json!({ "type": t }).as_object().cloned().expect("an object")),
        });
    }
    Ok(fields)
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

    #[test]
    fn a_spreadsheet_of_fields_reads_by_its_header() {
        let text = "Notes,FIELD,Type,Required,Description,Example,Elasticsearch\r\n\
                    x,client.ip,str,yes,\"Where it came from,\nthe address.\",10.0.0.1,ip\r\n\
                    ,status,i64,,The HTTP status.,200,\r\n\
                    \r\n";
        let fields = read(text).unwrap();
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
        assert_eq!(read("").unwrap_err(), "the file is empty");
        assert_eq!(
            read("name,type\na,str").unwrap_err(),
            "the header names no `field` column"
        );
        assert_eq!(read("field,type\na,str\n,i64").unwrap_err(), "row 3: no `field`");
        assert_eq!(read("field,type\na,").unwrap_err(), "row 2: no `type` for `a`");
        assert_eq!(
            read("field,type,required\na,str,maybe").unwrap_err(),
            "row 2: `required` is \"maybe\", not yes or no"
        );
        assert_eq!(read("field,type\n\"a,str").unwrap_err(), "row 2: a quote is not closed");
    }
}
