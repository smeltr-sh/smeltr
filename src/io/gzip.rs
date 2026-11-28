//! Gzip and raw deflate, through ISA-L's igzip. igzip compresses at levels 0 to 3, the range the loader gives gzip
//! wherever smeltr compresses itself (`CompressionDto::levels`).

use isal_sys::igzip_lib as isal;

use crate::plan::topology::IgzipLevel;

/// `data` gzipped whole at `level`.
pub(crate) fn gzip(data: &[u8], level: IgzipLevel) -> Vec<u8> {
    whole(data, level, Wrap::Gzip)
}

/// `data` as a raw deflate stream at `level`, with no header or trailer: an Avro block's `deflate` codec.
pub(crate) fn deflate(data: &[u8], level: IgzipLevel) -> Vec<u8> {
    whole(data, level, Wrap::Raw)
}

/// A gzip stream written piece by piece. A caller may [`GzipStream::take`] what it has compressed between writes;
/// [`GzipStream::finish`] ends the stream.
pub(crate) struct GzipStream {
    deflater: Deflater,
    /// Input not yet handed to igzip, which is fed in large pieces because fed a row at a time it costs several
    /// times as much per byte.
    held: Vec<u8>,
    out: Vec<u8>,
}

/// The input [`GzipStream`] holds before compressing it.
const HOLD: usize = 64 << 10;

/// What a boxed [`GzipStream`] at `level` holds besides its output: itself, igzip's state, its level's working memory,
/// and the input it holds.
pub(crate) fn stream_bytes(level: IgzipLevel) -> usize {
    size_of::<GzipStream>() + size_of::<isal::isal_zstream>() + level_buf_size(level) as usize + HOLD
}

/// What [`gzip`] and [`deflate`] keep on each thread that calls them: igzip's state, with the top level's working
/// memory.
pub(crate) fn whole_bytes() -> usize {
    size_of::<isal::isal_zstream>() + level_buf_size(IgzipLevel::MAX) as usize
}

impl GzipStream {
    pub(crate) fn new(level: IgzipLevel) -> GzipStream {
        let mut deflater = Deflater::sized(level);
        deflater.start(level, Wrap::Gzip, Mode::Stream);
        GzipStream {
            deflater,
            held: Vec::with_capacity(HOLD),
            out: Vec::new(),
        }
    }

    pub(crate) fn write(&mut self, bytes: &[u8]) {
        if self.held.len() + bytes.len() > HOLD {
            self.deflater.push(&self.held, false, &mut self.out);
            self.held.clear();
        }
        if bytes.len() >= HOLD {
            self.deflater.push(bytes, false, &mut self.out);
        } else {
            self.held.extend_from_slice(bytes);
        }
    }

    /// Bytes compressed and not yet taken. Held input is not among them.
    pub(crate) fn pending(&self) -> usize {
        self.out.len()
    }

    /// The bytes compressed so far, which the stream gives up without ending. It keeps the input it holds.
    pub(crate) fn take(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.out)
    }

    /// The stream's remaining bytes, ending it.
    pub(crate) fn finish(mut self) -> Vec<u8> {
        self.deflater.push(&self.held, true, &mut self.out);
        self.out
    }
}

std::thread_local! {
    /// The state [`whole`] compresses with, kept by each thread that compresses: setting one up costs more than
    /// compressing a small body, and a thread compresses one body at a time.
    static WHOLE: std::cell::RefCell<Option<Deflater>> = const { std::cell::RefCell::new(None) };
}

fn whole(data: &[u8], level: IgzipLevel, wrap: Wrap) -> Vec<u8> {
    WHOLE.with_borrow_mut(|kept| {
        // Sized for the top level, so it serves a body at any.
        let deflater = kept.get_or_insert_with(|| Deflater::sized(IgzipLevel::MAX));
        deflater.start(level, wrap, Mode::Whole);
        compress_whole(deflater, data)
    })
}

fn compress_whole(deflater: &mut Deflater, data: &[u8]) -> Vec<u8> {
    // igzip's stateless output is at most the input, plus 5 bytes of stored-block header for each 64 KiB, plus the
    // stream's own header and trailer; 1 in 1024 of the input covers the block headers with room to spare.
    let bound = data.len() + data.len() / 1024 + isal::ISAL_DEF_MAX_HDR_SIZE as usize + 64;
    let mut out = Vec::with_capacity(bound);
    let stream = &mut *deflater.stream;
    stream.next_in = data.as_ptr().cast_mut();
    stream.avail_in = u32::try_from(data.len()).expect("a body under 4 GiB");
    stream.end_of_stream = 1;
    stream.next_out = out.as_mut_ptr();
    stream.avail_out = u32::try_from(bound).expect("a body under 4 GiB");
    // SAFETY: `stream` was initialized by `isal_deflate_stateless_init`, its level buffer is at least the size its
    // level asks, `next_in` reads `avail_in` bytes of `data`, and `next_out` writes at most `avail_out` bytes of
    // `out`'s capacity.
    let status = unsafe { isal::isal_deflate_stateless(stream) };
    assert_eq!(status, COMP_OK, "igzip into its bound");
    // SAFETY: igzip wrote the first `bound - avail_out` bytes of `out`'s capacity.
    unsafe { out.set_len(bound - stream.avail_out as usize) };
    stream.next_in = std::ptr::null_mut();
    stream.next_out = std::ptr::null_mut();
    // The output is held until it is delivered, retries included, so it keeps only what it holds, not the whole bound.
    out.shrink_to_fit();
    out
}

/// igzip's `COMP_OK`, which the bindings give as a `u32` and its calls return as an `int`.
const COMP_OK: i32 = isal::COMP_OK as i32;
/// igzip's `gzip_flag` values, which the bindings give as `u32`s for a `u16` field. `IGZIP_DEFLATE` is 0 and
/// `IGZIP_GZIP` is 1.
const GZIP: u16 = isal::IGZIP_GZIP as u16;
const RAW: u16 = isal::IGZIP_DEFLATE as u16;

#[derive(Clone, Copy)]
enum Wrap {
    Gzip,
    Raw,
}

#[derive(Clone, Copy)]
enum Mode {
    Whole,
    Stream,
}

/// The working memory igzip asks for at `level`.
fn level_buf_size(level: IgzipLevel) -> u32 {
    match level.get() {
        0 => isal::ISAL_DEF_LVL0_DEFAULT,
        1 => isal::ISAL_DEF_LVL1_DEFAULT,
        2 => isal::ISAL_DEF_LVL2_DEFAULT,
        _ => isal::ISAL_DEF_LVL3_DEFAULT,
    }
}

/// igzip's state for one stream, and the working memory its level asks for.
struct Deflater {
    // Boxed: igzip keeps pointers into the stream's own buffers, and the stream is 80 KiB.
    stream: Box<isal::isal_zstream>,
    level_buf: Vec<u8>,
}

// SAFETY: the stream's pointers point only into its own box and into `level_buf`'s heap, which move with it, and
// into the caller's buffers only for the length of one call.
unsafe impl Send for Deflater {}

impl Deflater {
    /// A deflater whose level buffer is the size `level` asks, for [`Deflater::start`] to begin a stream with at
    /// `level` or below.
    fn sized(level: IgzipLevel) -> Deflater {
        // SAFETY: an `isal_zstream` is integers, pointers and arrays of them, and all zeroes is a value of each;
        // `start` sets the ones igzip reads.
        let stream = unsafe { Box::<isal::isal_zstream>::new_zeroed().assume_init() };
        Deflater {
            stream,
            level_buf: vec![0; level_buf_size(level) as usize],
        }
    }

    /// Begins a new stream at `level`. The level buffer must be at least the size `level` asks.
    fn start(&mut self, level: IgzipLevel, wrap: Wrap, mode: Mode) {
        let size = level_buf_size(level);
        assert!(
            self.level_buf.len() >= size as usize,
            "a level buffer for level {level:?}"
        );
        let stream = &mut *self.stream;
        // SAFETY: `stream` is a whole, writable `isal_zstream`.
        unsafe {
            match mode {
                Mode::Whole => isal::isal_deflate_stateless_init(stream),
                Mode::Stream => isal::isal_deflate_init(stream),
            }
        }
        stream.level = level.get().into();
        stream.level_buf = if size == 0 {
            std::ptr::null_mut()
        } else {
            self.level_buf.as_mut_ptr()
        };
        // Fits: the buffer was sized from a `u32`, in `sized`.
        stream.level_buf_size = if size == 0 { 0 } else { self.level_buf.len() as u32 };
        stream.gzip_flag = match wrap {
            Wrap::Gzip => GZIP,
            Wrap::Raw => RAW,
        };
    }

    /// Compresses `input` onto `out`, and ends the stream when `end`.
    fn push(&mut self, input: &[u8], end: bool, out: &mut Vec<u8>) {
        let stream = &mut *self.stream;
        stream.next_in = input.as_ptr().cast_mut();
        stream.avail_in = u32::try_from(input.len()).expect("a write under 4 GiB");
        stream.end_of_stream = end.into();
        loop {
            out.reserve(HOLD);
            let room = (out.capacity() - out.len()).min(u32::MAX as usize);
            // SAFETY: `out.len()` is within its allocation.
            stream.next_out = unsafe { out.as_mut_ptr().add(out.len()) };
            stream.avail_out = u32::try_from(room).expect("capped at u32::MAX");
            // SAFETY: `stream` was initialized by `isal_deflate_init`, `next_in` reads `avail_in` bytes of `input`,
            // and `next_out` writes at most `avail_out` bytes of `out`'s spare capacity.
            let status = unsafe { isal::isal_deflate(stream) };
            assert_eq!(status, COMP_OK, "igzip a stream");
            // SAFETY: igzip wrote the first `room - avail_out` bytes of the spare capacity.
            unsafe { out.set_len(out.len() + room - stream.avail_out as usize) };
            // igzip returns when the input is spent or the output is full; at the end it must also have written its
            // trailer.
            let done = if end {
                stream.internal_state.state == isal::isal_zstate_state_ZSTATE_END
            } else {
                stream.avail_in == 0 && stream.avail_out > 0
            };
            if done {
                break;
            }
        }
        stream.next_in = std::ptr::null_mut();
        stream.next_out = std::ptr::null_mut();
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    fn at(level: u8) -> IgzipLevel {
        IgzipLevel::new(level).unwrap()
    }

    fn gunzip(bytes: &[u8]) -> Vec<u8> {
        let mut plain = Vec::new();
        flate2::read::MultiGzDecoder::new(bytes)
            .read_to_end(&mut plain)
            .unwrap();
        plain
    }

    fn lines() -> Vec<u8> {
        (0..20_000)
            .flat_map(|i| format!("{{\"n\":{i},\"msg\":\"line {}\"}}\n", i % 37).into_bytes())
            .collect()
    }

    /// Bytes no level shrinks, which igzip stores.
    fn noise() -> Vec<u8> {
        let mut x = 0x9E37_79B9_7F4A_7C15_u64;
        (0..300_000)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x.to_le_bytes()[0]
            })
            .collect()
    }

    #[test]
    fn every_level_gzips_what_any_gunzip_reads() {
        for data in [lines(), noise(), Vec::new()] {
            for level in 0..=3 {
                assert_eq!(gunzip(&gzip(&data, at(level))), data, "level {level}");
            }
        }
    }

    #[test]
    fn a_thread_compressing_at_one_level_then_another_starts_each_body_afresh() {
        let data = lines();
        for level in [3, 0, 2, 1, 3] {
            assert_eq!(gunzip(&gzip(&data, at(level))), data, "level {level}");
            let mut plain = Vec::new();
            flate2::read::DeflateDecoder::new(&deflate(&data, at(level))[..])
                .read_to_end(&mut plain)
                .unwrap();
            assert_eq!(plain, data, "deflate at level {level}");
        }
    }

    #[test]
    fn a_stream_drained_between_writes_is_one_gzip() {
        for data in [lines(), noise()] {
            let mut stream = GzipStream::new(at(1));
            let mut file = Vec::new();
            // Rows, a piece larger than what the stream holds, and rows again.
            for piece in data[..1000]
                .chunks(37)
                .chain([&data[1000..200_000]])
                .chain(data[200_000..].chunks(301))
            {
                stream.write(piece);
                file.extend(stream.take());
            }
            file.extend(stream.finish());
            assert_eq!(gunzip(&file), data);
        }
    }
}
