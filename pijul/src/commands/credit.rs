use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::bail;
use canonical_path::CanonicalPathBuf;
use clap::Parser;
use libpijul::vertex_buffer::VertexBuffer;
use libpijul::*;
use log::debug;

use crate::repository::Repository;

#[derive(Parser, Debug)]
pub struct Credit {
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,
    /// Use this channel instead of the current channel
    #[clap(long = "channel")]
    channel: Option<String>,
    /// The file to annotate
    file: PathBuf,
}

impl Credit {
    pub fn run(self) -> Result<(), anyhow::Error> {
        let has_repo_path = self.repo_path.is_some();
        let repo = Repository::find_root(self.repo_path)?;
        let txn = repo.pristine.txn_begin()?;
        let channel_name = if let Some(ref c) = self.channel {
            c
        } else {
            txn.current_channel().unwrap_or(crate::DEFAULT_CHANNEL)
        };
        let channel = if let Some(channel) = txn.load_channel(&channel_name)? {
            channel
        } else {
            bail!("No such channel: {:?}", channel_name)
        };
        let repo_path = CanonicalPathBuf::canonicalize(&repo.path)?;
        let (pos, _ambiguous) = if has_repo_path {
            let root = std::fs::canonicalize(repo.path.join(&self.file))?;
            let path = root.strip_prefix(&repo_path.as_path())?.to_str().unwrap();
            txn.follow_oldest_path(&repo.changes, &channel, &path)?
        } else {
            let mut root = crate::current_dir()?;
            root.push(&self.file);
            let root = std::fs::canonicalize(&root)?;
            let path = root.strip_prefix(&repo_path.as_path())?.to_str().unwrap();
            txn.follow_oldest_path(&repo.changes, &channel, &path)?
        };
        super::pager();
        let channel = channel.read();
        match libpijul::output::output_file(
            &repo.changes,
            &txn,
            &channel,
            pos,
            &mut Creditor::new(std::io::stdout(), &txn, &channel),
        ) {
            Ok(_) => {}
            Err(libpijul::output::FileError::Io(io)) => {
                if let std::io::ErrorKind::BrokenPipe = io.kind() {
                } else {
                    return Err(io.into());
                }
            }
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }
}

pub struct Creditor<'a, W: std::io::Write, T: ChannelTxnT> {
    w: W,
    buf: Vec<u8>,
    new_line: bool,
    changes: HashSet<Hash>,
    txn: &'a T,
    channel: &'a T::Channel,
}

impl<'a, W: std::io::Write, T: ChannelTxnT> Creditor<'a, W, T> {
    pub fn new(w: W, txn: &'a T, channel: &'a T::Channel) -> Self {
        Creditor {
            w,
            new_line: true,
            buf: Vec::new(),
            txn,
            channel,
            changes: HashSet::new(),
        }
    }
}

impl<'a, W: std::io::Write, T: TxnTExt> VertexBuffer for Creditor<'a, W, T> {
    fn output_line<E, C: FnOnce(&mut [u8]) -> Result<(), E>>(
        &mut self,
        v: Vertex<ChangeId>,
        c: C,
    ) -> Result<(), E>
    where
        E: From<std::io::Error>,
    {
        debug!("outputting vertex {:?}", v);
        self.buf.resize(v.end - v.start, 0);
        c(&mut self.buf)?;

        if !v.change.is_root() {
            self.changes.clear();
            for e in self
                .txn
                .iter_adjacent(self.channel, v, EdgeFlags::PARENT, EdgeFlags::all())
                .unwrap()
            {
                let e = e.unwrap();
                if e.introduced_by().is_root() {
                    continue;
                }
                if let Ok(Some(intro)) = self.txn.get_external(&e.introduced_by()) {
                    self.changes.insert(intro.into());
                }
            }
            if !self.new_line {
                writeln!(self.w)?;
            }
            writeln!(self.w)?;
            let mut is_first = true;
            for c in self.changes.drain() {
                let c = c.to_base32();
                write!(
                    self.w,
                    "{}{}",
                    if is_first { "" } else { ", " },
                    c.split_at(12).0,
                )?;
                is_first = false;
            }
            writeln!(self.w, "\n")?;
        }
        let ends_with_newline = self.buf.ends_with(b"\n");
        if let Ok(s) = std::str::from_utf8(&self.buf[..]) {
            for l in s.lines() {
                self.w.write_all(b"> ")?;
                self.w.write_all(l.as_bytes())?;
                self.w.write_all(b"\n")?;
            }
        }
        if !self.buf.is_empty() {
            // empty "lines" (such as in the beginning of a file)
            // don't change the status of self.new_line.
            self.new_line = ends_with_newline;
        }
        Ok(())
    }

    fn output_conflict_marker(
        &mut self,
        marker: &str,
        id: usize,
        sides: &[&Hash],
    ) -> Result<(), std::io::Error> {
        if !self.new_line {
            self.w.write_all(b"\n")?;
        }
        write!(self.w, "{} {}", marker, id)?;
        for side in sides {
            let h = side.to_base32();
            write!(self.w, " [{}]", h.split_at(8).0)?;
        }
        self.w.write_all(b"\n")?;
        Ok(())
    }
}
