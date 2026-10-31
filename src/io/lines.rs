//! Rows as lines on the file destination: NDJSON, a JSON array, one field's text, or bytes as they arrived, framed
//! and then compressed as one stream, so a file's bytes leave in takes while it is still being written.

use std::io::Write;

use parquet::file::metadata::ParquetMetaData;

use super::OwnedRecord;
use super::files::{FileEncoder, FileFormat, FileRow, Partition};
use crate::plan::topology::{Compression, Newlines, ObjectFormat};

/// Rows as lines: NDJSON, a JSON array, one field's text, or bytes as they arrived, compressed as a stream.
pub struct LineFormat {
    format: ObjectFormat,
    compression: Compression,
}

impl LineFormat {
    /// `format` is any object format but Parquet, Avro and CSV, which `files::ParquetFormat`, `avro::container` and
    /// `io::csv` write.
    pub fn new(format: ObjectFormat, compression: Compression) -> LineFormat {
        assert!(!format.writes_columns(), "{format:?} is not written as lines");
        LineFormat { format, compression }
    }
}

impl FileFormat for LineFormat {
    fn admit(&self, row: &OwnedRecord, _partition: &Partition) -> Result<FileRow, String> {
        // Egress made the row in this destination's form.
        let line = match &self.format {
            ObjectFormat::Parquet { .. } | ObjectFormat::Avro | ObjectFormat::Csv => {
                unreachable!("`LineFormat::new` refuses {:?}", self.format)
            }
            ObjectFormat::Ndjson | ObjectFormat::Json => row.value.trim_ascii().to_vec(),
            ObjectFormat::Text { .. } => row.value.clone(),
            ObjectFormat::Bytes { newlines, .. } => match newlines {
                Newlines::Escape => escape_line(&row.value),
                Newlines::Refuse if row.value.iter().any(|b| matches!(b, b'\n' | b'\r')) => {
                    return Err("it holds a line break, which this bytes object refuses".into());
                }
                Newlines::Refuse => row.value.clone(),
            },
        };
        Ok(FileRow::Line(line))
    }

    fn open(&self, _partition: &Partition) -> Box<dyn FileEncoder> {
        Box::new(LineEncoder::open(self.format == ObjectFormat::Json, self.compression))
    }
}

/// A new file of lines, compressed as one stream, opening with `header`. A format that renders its own lines
/// (`io::csv`) admits them as `FileRow::Line`.
pub fn encoder(compression: Compression, header: &[u8]) -> Box<dyn FileEncoder> {
    let mut encoder = LineEncoder::open(false, compression);
    encoder.put(header);
    Box::new(encoder)
}

/// One file of lines being written.
struct LineEncoder {
    /// The lines are one JSON array's elements.
    json: bool,
    stream: Stream,
    rows: u64,
    /// Bytes already handed out by `take`.
    taken: usize,
    /// Framed bytes written into the codec since the last `take`.
    plain_since_take: usize,
    /// What the rows written since the last `take` hold, by `FileRow::bytes`.
    row_bytes_since_take: usize,
}

impl LineEncoder {
    fn open(json: bool, compression: Compression) -> LineEncoder {
        LineEncoder {
            json,
            stream: Stream::open(compression),
            rows: 0,
            taken: 0,
            plain_since_take: 0,
            row_bytes_since_take: 0,
        }
    }

    fn put(&mut self, bytes: &[u8]) {
        self.stream.write(bytes);
        self.plain_since_take += bytes.len();
    }
}

impl FileEncoder for LineEncoder {
    fn write(&mut self, rows: &[FileRow]) {
        for row in rows {
            let FileRow::Line(line) = row else {
                unreachable!("a line format admits only lines")
            };
            if self.json {
                self.put(if self.rows == 0 { b"[" } else { b"," });
                self.put(line);
            } else {
                self.put(line);
                self.put(b"\n");
            }
            self.rows += 1;
            self.row_bytes_since_take += row.bytes();
        }
    }

    fn in_progress_bytes(&self) -> usize {
        self.row_bytes_since_take
    }

    fn take(&mut self) -> Vec<u8> {
        let out = self.stream.take();
        self.taken += out.len();
        self.plain_since_take = 0;
        self.row_bytes_since_take = 0;
        out
    }

    fn bytes_written(&self) -> usize {
        let pending = self.stream.pending();
        match self.stream {
            // Uncompressed, what is pending is exactly what was written.
            Stream::None(_) => self.taken + pending,
            // The codec may hold any of the plain bytes, so they count as written until a take says otherwise.
            _ => self.taken + pending + self.plain_since_take,
        }
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn finish(mut self: Box<Self>) -> (Vec<u8>, Option<ParquetMetaData>) {
        if self.json {
            // An empty array still opens.
            if self.rows == 0 {
                self.put(b"[");
            }
            self.put(b"]\n");
        }
        (self.stream.finish(), None)
    }
}

/// The codec over a file's framed bytes, writing into memory that `take` drains. Every codec only ever appends to
/// its output, so draining it between writes leaves the stream whole.
enum Stream {
    None(Vec<u8>),
    Gzip(Box<flate2::write::GzEncoder<Vec<u8>>>),
    // Snappy and lz4 are written framed, as their command-line tools read them.
    Snappy(Box<snap::write::FrameEncoder<Vec<u8>>>),
    Lz4(Box<lz4_flex::frame::FrameEncoder<Vec<u8>>>),
    Zstd(Box<zstd::stream::write::Encoder<'static, Vec<u8>>>),
}

impl Stream {
    fn open(compression: Compression) -> Stream {
        match compression {
            Compression::None => Stream::None(Vec::new()),
            Compression::Gzip => Stream::Gzip(Box::new(flate2::write::GzEncoder::new(
                Vec::new(),
                flate2::Compression::default(),
            ))),
            Compression::Snappy => Stream::Snappy(Box::new(snap::write::FrameEncoder::new(Vec::new()))),
            Compression::Lz4 => Stream::Lz4(Box::new(lz4_flex::frame::FrameEncoder::new(Vec::new()))),
            Compression::Zstd => Stream::Zstd(Box::new(
                zstd::stream::write::Encoder::new(Vec::new(), 3).expect("zstd level 3"),
            )),
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        match self {
            Stream::None(out) => out.extend_from_slice(bytes),
            Stream::Gzip(out) => out.write_all(bytes).expect("writing to memory"),
            Stream::Snappy(out) => out.write_all(bytes).expect("writing to memory"),
            Stream::Lz4(out) => out.write_all(bytes).expect("writing to memory"),
            Stream::Zstd(out) => out.write_all(bytes).expect("writing to memory"),
        }
    }

    /// Output the codec has produced and not yet handed out.
    fn pending(&self) -> usize {
        match self {
            Stream::None(out) => out.len(),
            Stream::Gzip(out) => out.get_ref().len(),
            Stream::Snappy(out) => out.get_ref().len(),
            Stream::Lz4(out) => out.get_ref().len(),
            Stream::Zstd(out) => out.get_ref().len(),
        }
    }

    /// Everything written so far that the codec will give up without ending its stream. Gzip keeps what it has not
    /// yet emitted: a flush there would cost a sync marker in the file for nothing a reader needs.
    fn take(&mut self) -> Vec<u8> {
        let out = match self {
            Stream::None(out) => out,
            Stream::Gzip(out) => out.get_mut(),
            Stream::Snappy(out) => {
                out.flush().expect("writing to memory");
                out.get_mut()
            }
            Stream::Lz4(out) => {
                out.flush().expect("writing to memory");
                out.get_mut()
            }
            Stream::Zstd(out) => {
                out.flush().expect("writing to memory");
                out.get_mut()
            }
        };
        std::mem::take(out)
    }

    /// The stream's remaining bytes, ending it.
    fn finish(self) -> Vec<u8> {
        match self {
            Stream::None(out) => out,
            Stream::Gzip(out) => (*out).finish().expect("writing to memory"),
            Stream::Snappy(out) => (*out)
                .into_inner()
                .map_err(|e| e.to_string())
                .expect("writing to memory"),
            Stream::Lz4(out) => (*out).finish().expect("writing to memory"),
            Stream::Zstd(out) => (*out).finish().expect("writing to memory"),
        }
    }
}

/// `bytes` kept to one line: `\` becomes `\\`, a newline `\n` and a carriage return `\r`.
pub fn escape_line(bytes: &[u8]) -> Vec<u8> {
    let mut line = Vec::with_capacity(bytes.len());
    for &b in bytes {
        match b {
            b'\\' => line.extend_from_slice(b"\\\\"),
            b'\n' => line.extend_from_slice(b"\\n"),
            b'\r' => line.extend_from_slice(b"\\r"),
            _ => line.push(b),
        }
    }
    line
}

/// The bytes `escape_line` kept to `line`. A `\` before any other byte stays as it is.
pub fn unescape_line(line: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(line.len());
    let mut rest = line.iter();
    while let Some(&b) = rest.next() {
        if b != b'\\' {
            bytes.push(b);
            continue;
        }
        match rest.next() {
            Some(b'n') => bytes.push(b'\n'),
            Some(b'r') => bytes.push(b'\r'),
            Some(b'\\') => bytes.push(b'\\'),
            Some(&other) => bytes.extend_from_slice(&[b'\\', other]),
            None => bytes.push(b'\\'),
        }
    }
    bytes
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    const CODECS: [Compression; 5] = [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ];

    fn partition() -> Partition {
        Partition {
            part: 0,
            path: Vec::new(),
            values: Vec::new(),
        }
    }

    fn record(value: &[u8]) -> OwnedRecord {
        OwnedRecord {
            value: value.to_vec(),
            ..OwnedRecord::default()
        }
    }

    fn decompress(compression: Compression, bytes: &[u8]) -> Vec<u8> {
        let mut plain = Vec::new();
        match compression {
            Compression::None => plain.extend_from_slice(bytes),
            Compression::Gzip => {
                flate2::read::GzDecoder::new(bytes).read_to_end(&mut plain).unwrap();
            }
            Compression::Snappy => {
                snap::read::FrameDecoder::new(bytes).read_to_end(&mut plain).unwrap();
            }
            Compression::Lz4 => {
                lz4_flex::frame::FrameDecoder::new(bytes)
                    .read_to_end(&mut plain)
                    .unwrap();
            }
            Compression::Zstd => plain = zstd::stream::decode_all(bytes).unwrap(),
        }
        plain
    }

    /// Writes `values` admitted by `format` in two writes with a take between, and returns the file's bytes
    /// decompressed, checking the encoder's counts on the way.
    fn written(format: &ObjectFormat, compression: Compression, values: &[&[u8]]) -> Vec<u8> {
        let lines = LineFormat::new(format.clone(), compression);
        let rows: Vec<FileRow> = values
            .iter()
            .map(|v| lines.admit(&record(v), &partition()).unwrap())
            .collect();
        let (first, second) = rows.split_at(rows.len() / 2);
        let mut encoder = lines.open(&partition());
        assert_eq!(encoder.bytes_written(), 0);

        encoder.write(first);
        assert_eq!(
            encoder.in_progress_bytes(),
            first.iter().map(FileRow::bytes).sum::<usize>()
        );
        let before = encoder.bytes_written();
        assert!(before > 0, "{format:?} {compression:?}");
        let mut file = encoder.take();
        assert_eq!(encoder.in_progress_bytes(), 0);
        assert_eq!(encoder.bytes_written(), file.len(), "{format:?} {compression:?}");

        encoder.write(second);
        assert!(encoder.bytes_written() > file.len(), "{format:?} {compression:?}");
        file.extend(encoder.take());
        assert_eq!(encoder.bytes_written(), file.len(), "{format:?} {compression:?}");
        assert_eq!(encoder.rows(), values.len() as u64);

        let (rest, metadata) = encoder.finish();
        assert!(metadata.is_none());
        file.extend(rest);
        decompress(compression, &file)
    }

    /// A format, the values of its rows, and the file text they make.
    type Case<'a> = (ObjectFormat, &'a [&'a [u8]], &'a [u8]);

    #[test]
    fn every_format_frames_its_lines_through_every_codec() {
        let values: [&[u8]; 4] = [b" {\"a\":1}\n", b"{\"b\":2}", b"{\"c\":\"x y\"}\t", b"{}"];
        let text: [&[u8]; 4] = [b" one", b"two ", b"", b"four"];
        let bytes: [&[u8]; 4] = [b"a\nb", b"c\\d", b"\r", b"\xff\x00"];
        let field: std::sync::Arc<str> = "f".into();
        let cases: Vec<Case> = vec![
            (
                ObjectFormat::Ndjson,
                &values,
                b"{\"a\":1}\n{\"b\":2}\n{\"c\":\"x y\"}\n{}\n",
            ),
            (
                ObjectFormat::Json,
                &values,
                b"[{\"a\":1},{\"b\":2},{\"c\":\"x y\"},{}]\n",
            ),
            (
                ObjectFormat::Text { field: field.clone() },
                &text,
                b" one\ntwo \n\nfour\n",
            ),
            (
                ObjectFormat::Bytes {
                    field: field.clone(),
                    newlines: Newlines::Escape,
                },
                &bytes,
                b"a\\nb\nc\\\\d\n\\r\n\xff\x00\n",
            ),
            (
                ObjectFormat::Bytes {
                    field,
                    newlines: Newlines::Refuse,
                },
                &[b"plain", b"c\\d", b"\t", b"\xff"],
                b"plain\nc\\d\n\t\n\xff\n",
            ),
        ];
        for (format, values, expected) in &cases {
            for compression in CODECS {
                assert_eq!(
                    written(format, compression, values),
                    *expected,
                    "{format:?} {compression:?}"
                );
            }
        }
    }

    #[test]
    fn an_empty_json_file_is_an_empty_array() {
        for compression in CODECS {
            let encoder = LineFormat::new(ObjectFormat::Json, compression).open(&partition());
            let (file, _) = encoder.finish();
            assert_eq!(decompress(compression, &file), b"[]\n", "{compression:?}");
        }
    }

    #[test]
    fn a_bytes_row_with_a_line_break_is_refused_when_newlines_refuse() {
        let lines = LineFormat::new(
            ObjectFormat::Bytes {
                field: "f".into(),
                newlines: Newlines::Refuse,
            },
            Compression::None,
        );
        for value in [&b"a\nb"[..], b"a\rb"] {
            let refused = lines.admit(&record(value), &partition()).unwrap_err();
            assert_eq!(refused, "it holds a line break, which this bytes object refuses");
        }
    }

    #[test]
    fn an_escaped_line_unescapes_to_its_bytes() {
        let bytes = b"a\\nb\n\r\\\\\xff\\";
        let line = escape_line(bytes);
        assert!(!line.contains(&b'\n') && !line.contains(&b'\r'));
        assert_eq!(unescape_line(&line), bytes);
    }
}
