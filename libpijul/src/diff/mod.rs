use crate::alive::{output_graph, Graph};
use crate::changestore::*;
use crate::pristine::*;
use crate::record::Recorded;
use crate::text_encoding::Encoding;

mod bin;

mod diff;
mod split;
mod vertex_buffer;
pub use diff::Algorithm;
mod delete;
mod replace;

lazy_static! {
    pub static ref DEFAULT_SEPARATOR: regex::bytes::Regex = regex::bytes::Regex::new("\n").unwrap();
}

#[derive(Hash, Clone, Copy)]
struct Line<'a> {
    l: &'a [u8],
    cyclic: bool,
    before_end_marker: bool,
    last: bool,
    ptr: *const u8,
}

impl<'a> std::fmt::Debug for Line<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "Line {{ l: {:?} }}", std::str::from_utf8(self.l))
    }
}

impl<'a> Default for Line<'a> {
    fn default() -> Self {
        Line {
            l: &[],
            cyclic: false,
            before_end_marker: false,
            last: false,
            ptr: std::ptr::null(),
        }
    }
}

impl<'a> PartialEq for Line<'a> {
    fn eq(&self, b: &Self) -> bool {
        if self.before_end_marker && !b.last && b.l.last() == Some(&b'\n') {
            return &b.l[..b.l.len() - 1] == self.l;
        }
        if b.before_end_marker && !self.last && self.l.last() == Some(&b'\n') {
            return &self.l[..self.l.len() - 1] == b.l;
        }
        ((self.ptr == b.ptr && self.l.len() == b.l.len()) || self.l == b.l)
            && self.cyclic == b.cyclic
    }
}
impl<'a> Eq for Line<'a> {}

#[derive(Error)]
pub enum DiffError<P: std::error::Error + 'static, T: GraphTxnT> {
    #[error(transparent)]
    Output(#[from] crate::output::FileError<P, T>),
    #[error(transparent)]
    Txn(#[from] TxnErr<T::GraphError>),
}

impl<P: std::error::Error, T: GraphTxnT> std::fmt::Debug for DiffError<P, T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DiffError::Output(e) => std::fmt::Debug::fmt(e, fmt),
            DiffError::Txn(e) => std::fmt::Debug::fmt(e, fmt),
        }
    }
}

fn make_old_lines<'a>(d: &'a vertex_buffer::Diff, r: &'a regex::bytes::Regex) -> Vec<Line<'a>> {
    d.lines(r)
        .map(|l| {
            let old_bytes = l.as_ptr() as usize - d.contents_a.as_ptr() as usize;
            let cyclic = if let Err(n) = d
                .cyclic_conflict_bytes
                .binary_search(&(old_bytes, std::usize::MAX))
            {
                n > 0 && {
                    let (a, b) = d.cyclic_conflict_bytes[n - 1];
                    a <= old_bytes && old_bytes < b
                }
            } else {
                false
            };
            let before_end_marker = if l.last() != Some(&b'\n') {
                let next_index = l.as_ptr() as usize + l.len() - d.contents_a.as_ptr() as usize + 1;
                d.marker.get(&next_index) == Some(&vertex_buffer::ConflictMarker::End)
            } else {
                false
            };
            if log_enabled!(log::Level::Debug) {
                if let Ok(l) = std::str::from_utf8(l) {
                    debug!("old = {:?}", l);
                } else {
                    debug!("old = {:?}", l);
                }
            }
            Line {
                l,
                cyclic,
                before_end_marker,
                last: l.as_ptr() as usize + l.len() - d.contents_a.as_ptr() as usize
                    >= d.contents_a.len(),
                ptr: l.as_ptr(),
            }
        })
        .collect()
}

fn make_new_lines<'a>(b: &'a [u8], sep: &'a regex::bytes::Regex) -> Vec<Line<'a>> {
    split::LineSplit::from_bytes_with_sep(b, sep)
        .map(|l| {
            if log_enabled!(log::Level::Debug) {
                if let Ok(l) = std::str::from_utf8(l) {
                    debug!("new = {:?}", l);
                } else {
                    debug!("new = {:?}", l);
                }
            }
            let next_index = l.as_ptr() as usize + l.len() - b.as_ptr() as usize;
            Line {
                l,
                cyclic: false,
                before_end_marker: false,
                last: next_index >= b.len(),
                ptr: l.as_ptr(),
            }
        })
        .collect()
}

impl Recorded {
    pub(crate) fn diff<T: ChannelTxnT, P: ChangeStore>(
        &mut self,
        changes: &P,
        txn: &ArcTxn<T>,
        channel: &ChannelRef<T>,
        algorithm: Algorithm,
        stop_early: bool,
        path: String,
        inode_: Inode,
        inode: Position<Option<ChangeId>>,
        a: &mut Graph,
        b: &[u8],
        encoding: &Option<Encoding>,
        separator: &regex::bytes::Regex,
    ) -> Result<(), DiffError<P::Error, T>> {
        self.largest_file = self.largest_file.max(b.len() as u64);
        let mut d = vertex_buffer::Diff::new(inode, path.clone(), a);
        output_graph(changes, txn, channel, &mut d, a, &mut self.redundant)?;
        let txn = txn.read();
        let channel = channel.read();
        // TODO pass through both encodings and use that to decide
        debug!("encoding = {:?}", encoding);
        let (lines_a, lines_b) = if encoding.is_none() {
            const ROLLING_SIZE: usize = 8192;
            debug!("contents_a: {:?}", d.contents_a.len());
            let (ah, old) = bin::make_old_chunks(ROLLING_SIZE, &d.contents_a);
            let (bb, new) = bin::make_new_chunks(ROLLING_SIZE, &ah, &b);
            debug!("bb = {:?}", bb);
            (old, new)
        } else {
            (make_old_lines(&d, separator), make_new_lines(&b, separator))
        };

        trace!("pos = {:?}", d.pos_a);
        if log::log_enabled!(log::Level::Trace) {
            for l in lines_a.iter() {
                trace!("a: {:?}", l)
            }
            for l in lines_b.iter() {
                trace!("b: {:?}", l)
            }
        }
        let dd = diff::diff(&lines_a, &lines_b, algorithm, stop_early);
        let mut conflict_contexts = replace::ConflictContexts::new();
        for r in 0..dd.len() {
            if dd[r].old_len > 0 {
                self.delete(
                    &*txn,
                    txn.graph(&*channel),
                    &d,
                    &dd,
                    &mut conflict_contexts,
                    &lines_a,
                    &lines_b,
                    inode_,
                    r,
                    encoding,
                )?;
            }
            if dd[r].new_len > 0 {
                self.replace(
                    &d,
                    &mut conflict_contexts,
                    &lines_a,
                    &lines_b,
                    inode_,
                    &dd,
                    r,
                    encoding,
                );
            }
        }
        debug!("Diff ended");
        Ok(())
    }
}
fn bytes_pos(chunks: &[Line], old: usize) -> usize {
    if old < chunks.len() {
        chunks[old].l.as_ptr() as usize - chunks[0].l.as_ptr() as usize
    } else if old > 0 {
        chunks[old - 1].l.as_ptr() as usize - chunks[0].l.as_ptr() as usize
            + chunks[old - 1].l.len()
    } else {
        0
    }
}
fn bytes_len(chunks: &[Line], old: usize, len: usize) -> usize {
    if let Some(p) = chunks.get(old + len) {
        p.l.as_ptr() as usize - chunks[old].l.as_ptr() as usize
    } else if old + len > 0 {
        chunks[old + len - 1].l.as_ptr() as usize + chunks[old + len - 1].l.len()
            - chunks[old].l.as_ptr() as usize
    } else {
        chunks[old + len].l.as_ptr() as usize - chunks[old].l.as_ptr() as usize
    }
}
