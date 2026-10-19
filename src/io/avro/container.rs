//! Avro's object container file: a header holding the schema and the codec, then blocks of values, each block its
//! count, its size, its values compressed by the codec, and the header's sync marker.
//!
//! ```mermaid
//! flowchart LR
//!   H["Obj 1 · metadata · sync"] --> B1["count · size · values · sync"] --> B2["…"]
//! ```
//!
//! Read, each value's bytes are handed on whole, and `AvroReader` reads them with the file's schema. Written, each
//! `take` is one block.

use std::io::{BufRead, Read};
use std::sync::Arc;

use apache_avro::Schema;
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::writer::datum::GenericDatumWriter;
use parquet::file::metadata::ParquetMetaData;

use super::{Parsed, parsed_schema_of, row_of};
use crate::io::OwnedRecord;
use crate::io::files::{FileEncoder, FileFormat, FileRow, Partition, admit_columns};
use crate::io::parquet::Schema as Columns;
use crate::plan::topology::Compression;

/// What every container file starts with.
const MAGIC: &[u8; 4] = b"Obj\x01";

/// How a container's blocks are compressed: its `avro.codec`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Codec {
    Null,
    Deflate,
    Snappy,
    Zstandard,
}

impl Codec {
    fn named(name: &[u8]) -> Result<Codec, String> {
        Ok(match name {
            b"null" => Codec::Null,
            b"deflate" => Codec::Deflate,
            b"snappy" => Codec::Snappy,
            b"zstandard" => Codec::Zstandard,
            other => {
                return Err(format!(
                    "an Avro file compressed with {:?}, which smeltr does not read",
                    String::from_utf8_lossy(other)
                ));
            }
        })
    }

    fn name(self) -> &'static str {
        match self {
            Codec::Null => "null",
            Codec::Deflate => "deflate",
            Codec::Snappy => "snappy",
            Codec::Zstandard => "zstandard",
        }
    }

    /// The codec a destination's `compression` writes, or why Avro has none for it.
    fn of(compression: Compression) -> Result<Codec, String> {
        match compression {
            Compression::None => Ok(Codec::Null),
            Compression::Gzip => Ok(Codec::Deflate),
            Compression::Snappy => Ok(Codec::Snappy),
            Compression::Zstd => Ok(Codec::Zstandard),
            Compression::Lz4 => Err("Avro has no lz4 codec".to_string()),
        }
    }

    fn compress(self, data: &[u8]) -> Vec<u8> {
        match self {
            Codec::Null => data.to_vec(),
            Codec::Deflate => {
                let mut out = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
                std::io::Write::write_all(&mut out, data).expect("a Vec takes every byte");
                out.finish().expect("a Vec takes every byte")
            }
            Codec::Snappy => {
                let mut out = snap::raw::Encoder::new()
                    .compress_vec(data)
                    .expect("a block is smaller than snappy's limit");
                out.extend_from_slice(&crc32fast::hash(data).to_be_bytes());
                out
            }
            Codec::Zstandard => zstd::bulk::compress(data, 0).expect("a Vec takes every byte"),
        }
    }

    /// `data` decompressed, at most `max` bytes of it.
    fn decompress(self, data: &[u8], max: u64) -> Result<Vec<u8>, String> {
        let mut out = Vec::new();
        let read = match self {
            Codec::Null => return Ok(data.to_vec()),
            Codec::Deflate => flate2::read::DeflateDecoder::new(data)
                .take(max.saturating_add(1))
                .read_to_end(&mut out),
            Codec::Snappy => {
                let Some((compressed, crc)) = data.split_last_chunk::<4>() else {
                    return Err("a snappy block too short to hold its checksum".to_string());
                };
                let len = snap::raw::decompress_len(compressed).map_err(|e| e.to_string())?;
                if len as u64 > max {
                    return Err(format!("a block of more than {max} bytes"));
                }
                out = snap::raw::Decoder::new()
                    .decompress_vec(compressed)
                    .map_err(|e| format!("a snappy block that does not decompress: {e}"))?;
                if crc32fast::hash(&out).to_be_bytes() != *crc {
                    return Err("a snappy block whose checksum does not match".to_string());
                }
                return Ok(out);
            }
            Codec::Zstandard => zstd::stream::read::Decoder::new(data)
                .map_err(|e| e.to_string())?
                .take(max.saturating_add(1))
                .read_to_end(&mut out),
        };
        read.map_err(|e| format!("a {} block that does not decompress: {e}", self.name()))?;
        if out.len() as u64 > max {
            return Err(format!("a block of more than {max} bytes"));
        }
        Ok(out)
    }
}

/// A container file being read: its schema, its codec and its sync marker.
pub struct Container {
    pub schema: Arc<Parsed>,
    codec: Codec,
    sync: [u8; 16],
}

impl Container {
    /// Reads the header off `input`. Err says why it is no container file smeltr reads.
    pub fn open(input: &mut dyn BufRead) -> Result<Container, String> {
        let mut magic = [0; 4];
        input
            .read_exact(&mut magic)
            .map_err(|_| "an object too short to be an Avro file".to_string())?;
        if magic != *MAGIC {
            return Err("an object that is no Avro file: it does not start with Avro's `Obj` and 1".to_string());
        }
        let (mut schema, mut codec) = (None, Codec::Null);
        loop {
            let mut count = read_long(input)?;
            if count == 0 {
                break;
            }
            if count < 0 {
                count = count.checked_neg().ok_or("an Avro file's metadata of no length")?;
                read_long(input)?;
            }
            for _ in 0..count {
                let key = read_bytes(input, MAX_METADATA)?;
                let value = read_bytes(input, MAX_METADATA)?;
                match key.as_slice() {
                    b"avro.schema" => schema = Some(value),
                    b"avro.codec" => codec = Codec::named(&value)?,
                    _ => {}
                }
            }
        }
        let mut sync = [0; 16];
        input
            .read_exact(&mut sync)
            .map_err(|e| format!("an Avro file's header that ends early: {e}"))?;
        let text = schema.ok_or("an Avro file whose header holds no schema")?;
        let text = String::from_utf8(text).map_err(|_| "an Avro file whose schema is not UTF-8")?;
        let schema = Parsed::new(&text, &[]).map_err(|e| format!("an Avro file whose schema does not parse: {e}"))?;
        Ok(Container {
            schema: Arc::new(schema),
            codec,
            sync,
        })
    }

    /// The values of the next block off `input`, each its bytes, or `None` past the last block. A block or a value
    /// over `max` bytes is Err.
    pub fn block(&self, input: &mut dyn BufRead, max: u64) -> Result<Option<Vec<Vec<u8>>>, String> {
        if input.fill_buf().map_err(|e| e.to_string())?.is_empty() {
            return Ok(None);
        }
        let count = read_long(input)?;
        let size = read_long(input)?;
        let (Ok(count), Ok(size)) = (usize::try_from(count), u64::try_from(size)) else {
            return Err(format!("an Avro block of {count} values in {size} bytes"));
        };
        if size > max {
            return Err(format!("an Avro block of more than {max} bytes"));
        }
        let mut data = vec![0; size as usize];
        input
            .read_exact(&mut data)
            .map_err(|e| format!("an Avro block that ends early: {e}"))?;
        let mut sync = [0; 16];
        input
            .read_exact(&mut sync)
            .map_err(|e| format!("an Avro block that ends early: {e}"))?;
        if sync != self.sync {
            return Err("an Avro block whose sync marker is not its file's".to_string());
        }
        let data = self.codec.decompress(&data, max)?;
        let resolved = self.schema.resolved()?;
        let reader = GenericDatumReader::builder(&self.schema.schema)
            .resolved_writer_schemata(resolved)
            .build()
            .map_err(|e| e.to_string())?;
        let mut rest = data.as_slice();
        let mut values = Vec::with_capacity(count.min(data.len()));
        for _ in 0..count {
            let before = rest;
            reader
                .read_value(&mut rest)
                .map_err(|e| format!("an Avro value that does not read with its file's schema: {e}"))?;
            values.push(before[..before.len() - rest.len()].to_vec());
        }
        if !rest.is_empty() {
            return Err(format!("an Avro block with {} bytes after its values", rest.len()));
        }
        Ok(Some(values))
    }
}

/// The most a header's key or value may hold. A schema is the largest, and none is near it.
const MAX_METADATA: u64 = 16 * 1024 * 1024;

/// One of Avro's longs: zig-zag, in seven-bit groups, least significant first.
fn read_long(input: &mut dyn BufRead) -> Result<i64, String> {
    let mut n: u64 = 0;
    for shift in (0..64).step_by(7) {
        let mut byte = [0];
        input
            .read_exact(&mut byte)
            .map_err(|e| format!("an Avro file that ends early: {e}"))?;
        n |= u64::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 == 0 {
            return Ok((n >> 1) as i64 ^ -((n & 1) as i64));
        }
    }
    Err("an Avro long of more than ten bytes".to_string())
}

fn read_bytes(input: &mut dyn BufRead, max: u64) -> Result<Vec<u8>, String> {
    let len = read_long(input)?;
    let len = u64::try_from(len).map_err(|_| format!("Avro bytes of length {len}"))?;
    if len > max {
        return Err(format!("Avro bytes of more than {max}"));
    }
    let mut out = vec![0; len as usize];
    input
        .read_exact(&mut out)
        .map_err(|e| format!("an Avro file that ends early: {e}"))?;
    Ok(out)
}

/// An Avro long: zig-zag, then base-128 varint.
fn put_long(out: &mut Vec<u8>, n: i64) {
    let mut z = ((n << 1) ^ (n >> 63)) as u64;
    while z >= 0x80 {
        out.push(z as u8 | 0x80);
        z >>= 7;
    }
    out.push(z as u8);
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_long(out, bytes.len() as i64);
    out.extend_from_slice(bytes);
}

/// A container file's header: its schema's text, its codec's name and `metadata` as its metadata, then `sync`.
pub(crate) fn header(schema: &str, codec: &str, metadata: &[(&str, &[u8])], sync: &[u8; 16]) -> Vec<u8> {
    let mut out = MAGIC.to_vec();
    put_long(&mut out, 2 + metadata.len() as i64);
    put_bytes(&mut out, b"avro.schema");
    put_bytes(&mut out, schema.as_bytes());
    put_bytes(&mut out, b"avro.codec");
    put_bytes(&mut out, codec.as_bytes());
    for (key, value) in metadata {
        put_bytes(&mut out, key.as_bytes());
        put_bytes(&mut out, value);
    }
    put_long(&mut out, 0);
    out.extend_from_slice(sync);
    out
}

/// Container files, one record per part, as `files::ParquetFormat` writes Parquet: each row's chunk is cut to its
/// part's schema, and the file's schema is that record's (`schema_of`).
pub struct AvroFormat {
    columns: Vec<Columns>,
    /// Each part's schema: its text and parsed.
    schemas: Vec<Arc<(String, Schema)>>,
    codec: Codec,
}

impl AvroFormat {
    /// Err names a record Avro cannot write, or a compression it has no codec for.
    pub fn new(columns: Vec<Columns>, compression: Compression) -> Result<AvroFormat, String> {
        let schemas = (columns.iter())
            .map(|c| parsed_schema_of(c.arrow()).map(Arc::new))
            .collect::<Result<_, String>>()?;
        Ok(AvroFormat {
            columns,
            schemas,
            codec: Codec::of(compression)?,
        })
    }
}

impl FileFormat for AvroFormat {
    fn admit(&self, row: &OwnedRecord, partition: &Partition) -> Result<FileRow, String> {
        admit_columns(&self.columns, row, partition)
    }

    fn open(&self, partition: &Partition) -> Box<dyn FileEncoder> {
        let schema = self.schemas[partition.part].clone();
        // Any 16 bytes no value is likely to hold, so a reader can find a block's end.
        let mut sync = [0; 16];
        aws_lc_rs::rand::fill(&mut sync).expect("the system's random source answers");
        let header = header(&schema.0, self.codec.name(), &[], &sync);
        Box::new(AvroEncoder {
            schema,
            codec: self.codec,
            sync,
            header: Some(header),
            values: Vec::new(),
            count: 0,
            taken: 0,
            rows: 0,
        })
    }
}

/// One container file being written: the header until the first `take`, and the values of the block under way.
struct AvroEncoder {
    schema: Arc<(String, Schema)>,
    codec: Codec,
    sync: [u8; 16],
    header: Option<Vec<u8>>,
    values: Vec<u8>,
    count: i64,
    taken: usize,
    rows: u64,
}

impl FileEncoder for AvroEncoder {
    fn write(&mut self, rows: &[FileRow]) {
        let datum = GenericDatumWriter::builder(&self.schema.1)
            .build()
            .expect("a schema `schema_of` made resolves");
        for row in rows {
            let FileRow::Columns(c) = row else {
                unreachable!("AvroFormat admits only columns")
            };
            // `admit` let through only rows of the part's schema, whose values are values of its Avro schema.
            datum
                .write_value(&mut self.values, row_of(c.chunk.batch(), c.row as usize))
                .expect("a row of its part's schema writes as its Avro schema");
            self.count += 1;
            self.rows += 1;
        }
    }

    fn in_progress_bytes(&self) -> usize {
        self.values.len()
    }

    fn take(&mut self) -> Vec<u8> {
        let mut out = match self.count {
            0 => Vec::new(),
            _ => self.header.take().unwrap_or_default(),
        };
        if self.count > 0 {
            let block = self.codec.compress(&self.values);
            put_long(&mut out, self.count);
            put_bytes(&mut out, &block);
            out.extend_from_slice(&self.sync);
            self.values.clear();
            self.count = 0;
        }
        self.taken += out.len();
        out
    }

    fn bytes_written(&self) -> usize {
        self.taken + self.header.as_ref().map_or(0, Vec::len) + self.values.len()
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn finish(mut self: Box<Self>) -> (Vec<u8>, Option<ParquetMetaData>) {
        (self.take(), None)
    }
}
