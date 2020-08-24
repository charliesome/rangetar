use std::cell::RefCell;
use std::cmp;
use std::convert::TryFrom;
use std::env;
use std::fs::{self, File};
use std::io::{self, ErrorKind, Seek, SeekFrom, Read};
use std::iter::Peekable;
use std::path::{Path, PathBuf};
use std::slice;

use tar::{Header, HeaderMode};

mod int;

const TAR_BLOCK_SIZE: u64 = 512;

#[derive(Debug)]
struct WriteIndex<'a> {
    root: &'a Path,
    entries: Vec<IndexEntry>,
}

impl<'a> WriteIndex<'a> {
    fn new(root: &'a Path) -> Self {
        WriteIndex {
            root,
            entries: Vec::new(),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn add(&mut self, disk_path: PathBuf, header: Header) {
        self.entries.push(IndexEntry {
            disk_path,
            header,
        });
    }
}

#[derive(Debug)]
struct IndexEntry {
    disk_path: PathBuf,
    header: Header,
}

fn traverse(write: &mut WriteIndex, path: &Path) -> Result<(), io::Error> {
    for entry in fs::read_dir(write.root().join(path))? {
        let file = entry?;
        let path = path.join(file.file_name());
        let meta = file.metadata()?;

        if meta.file_type().is_symlink() {
            // TODO parameterise symlink behaviour
            eprintln!("ignoring symlink: {}", file.path().display());
            continue;
        }

        let mut header = Header::new_ustar();
        header.set_path(&path)?;
        header.set_metadata_in_mode(&meta, HeaderMode::Deterministic);

        write.add(file.path(), header);

        if meta.file_type().is_dir() {
            traverse(write, &path)?;
        }
    }

    Ok(())
}

#[derive(Debug)]
struct Index {
    root: PathBuf,
    segments: Vec<Segment>,
}

#[derive(Debug)]
enum Segment {
    Static(Vec<u8>),
    File {
        // must wrap file in RefCell so that Segment::read can take &self
        // we must a peekable iterator to walk over segments which cannot
        // yield mutable references
        file: RefCell<File>,

        // keep file size separately to be resilient against change in file size
        size: u64,
    },
    Zeroes(u64),
}

impl Segment {
    pub fn byte_size(&self) -> u64 {
        match self {
            Segment::Static(bytes) => int::usize_to_u64(bytes.len()),
            Segment::File { size, .. } => *size,
            Segment::Zeroes(size) => *size,
        }
    }

    /// Reads `buf.len()` bytes from segment, starting at `offset`
    /// Returns number of bytes read. If return value is less than `buf.len()`,
    /// we have reached the end of the segment.
    pub fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, io::Error> {
        match self {
            Segment::Static(bytes) => {
                let offset = usize::try_from(offset).expect("u64 -> usize");

                let nbytes = cmp::min(bytes.len().saturating_sub(offset), buf.len());

                buf[0..nbytes].copy_from_slice(&bytes[offset..(offset + nbytes)]);

                Ok(nbytes)
            }
            Segment::File { file, size } => {
                // we need to be careful here to only read the number of bytes
                // we have promised to read - the file may have changed since
                // generating the index. we make no guarantees about the
                // coherence of the data actually read in this case, but we
                // must still behave reasonably and not crash or fall into an
                // infinite loop or do something else undesirable

                let mut file = file.borrow_mut();
                file.seek(SeekFrom::Start(offset))?;

                let mut nbytes = 0;

                while nbytes < buf.len() {
                    match file.read(&mut buf[nbytes..]) {
                        Ok(0) => {
                            // reached EOF

                            if offset + int::usize_to_u64(nbytes) < *size {
                                // we reached EOF earlier than expected - zero
                                // rest of bytes

                                let fill_to = int::converting_min(size.saturating_sub(offset), buf.len());

                                fill_slice(&mut buf[nbytes..fill_to], 0);
                            }

                            break;
                        }
                        Ok(n) => {
                            nbytes += n;

                            // usize -> u64 should always succeed
                            let nbytes_64 = int::usize_to_u64(nbytes);

                            if offset + nbytes_64 > *size {
                                // we reached EOF later than expected - break
                                // and adjust nbytes

                                nbytes = int::converting_min(size.saturating_sub(offset), buf.len());

                                break;
                            }
                        }
                        Err(e) if e.kind() == ErrorKind::Interrupted => { continue; }
                        Err(e) => { return Err(e); }
                    }
                }

                Ok(nbytes)
            }
            Segment::Zeroes(size) => {
                let nbytes = int::converting_min(size.saturating_sub(offset), buf.len());

                fill_slice(&mut buf[0..nbytes], 0);

                Ok(nbytes)
            }
        }
    }
}

// TODO - when slice_fill feature stabilises use that instead:
fn fill_slice(slice: &mut [u8], value: u8) {
    for byte in slice {
        *byte = value;
    }
}

impl Index {
    pub fn scan(path: PathBuf) -> Result<Self, io::Error> {
        let root = path.parent().unwrap_or(&path).to_owned();

        let path = path.file_name()
            .map(|p| Path::new(p))
            .unwrap_or(Path::new(""));

        // scan all files under path
        let mut write_index = WriteIndex::new(&root);
        traverse(&mut write_index, path)?;

        // map index entries into segments
        let mut segments = Vec::new();

        for entry in write_index.entries {
            let entry_size = entry.header.entry_size()?;

            let file = match File::open(&entry.disk_path) {
                Ok(file) => file,
                Err(e) => {
                    eprintln!("skipping unopenable file: {}\n    {:?}", entry.disk_path.display(), e);
                    continue;
                }
            };

            // file header
            segments.push(Segment::Static(entry.header.as_bytes().to_vec()));

            // file contents
            segments.push(Segment::File {
                file: RefCell::new(file),
                size: entry_size,
            });

            // pad to next tar block
            let size_modulo_block = entry_size % TAR_BLOCK_SIZE;

            if size_modulo_block > 0 {
                let required_padding = TAR_BLOCK_SIZE - size_modulo_block;

                segments.push(Segment::Zeroes(required_padding));
            }
        }

        // write two blocks of zeroes to mark end of tar
        segments.push(Segment::Zeroes(TAR_BLOCK_SIZE * 2));

        Ok(Index {
            root: root,
            segments,
        })
    }

    pub fn seek(&self, offset: u64) -> SeekReader<'_> {
        let mut skipped = 0;
        let mut iter = self.segments.iter().peekable();

        // skip past irrelevant segments
        while let Some(segment) = iter.peek() {
            let end = skipped + segment.byte_size();

            if end <= offset {
                skipped = end;
                iter.next();
            } else {
                break;
            }
        }

        SeekReader {
            offset: offset - skipped,
            segments: iter,
        }
    }
}

struct SeekReader<'a> {
    offset: u64,
    segments: Peekable<slice::Iter<'a, Segment>>,
}

impl<'a> SeekReader<'a> {
    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let mut nread = 0;

        while let Some(segment) = self.segments.peek() {
            if nread == buf.len() {
                break;
            }

            let segment_size = segment.byte_size();

            if self.offset >= segment_size {
                self.offset -= segment_size;
                self.segments.next();
                continue;
            }

            let segment_read = segment.read(self.offset, &mut buf[nread..])?;
            self.offset += int::usize_to_u64(segment_read);
            nread += segment_read;
        }

        Ok(nread)
    }
}

fn main() {
    let path = PathBuf::from(env::args_os().nth(1).expect("usage: rangetar <path>"));
    let index = Index::scan(path).expect("Index::scan");

    let mut buf = [0; 128];
    let nread = index.seek(0).read(&mut buf).unwrap();

    println!("read: {}\nbuf: {:?}", nread, &buf[0..nread]);
}
