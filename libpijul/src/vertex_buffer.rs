use crate::pristine::*;

pub const START_MARKER: &str = "\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n";

pub const SEPARATOR: &str = "\n================================\n";

pub const END_MARKER: &str = "\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n";

/// A trait for outputting keys and their contents. This trait allows
/// to retain more information about conflicts than directly
/// outputting as bytes to a `Write`. The diff algorithm uses that
/// information, for example.
pub trait VertexBuffer {
    fn output_line<E, F>(&mut self, key: Vertex<ChangeId>, contents: F) -> Result<(), E>
    where
        E: From<std::io::Error>,
        F: FnOnce(&mut Vec<u8>) -> Result<(), E>;

    fn output_conflict_marker(&mut self, s: &str) -> Result<(), std::io::Error>;
    fn begin_conflict(&mut self) -> Result<(), std::io::Error> {
        self.output_conflict_marker(START_MARKER)
    }
    fn begin_zombie_conflict(&mut self) -> Result<(), std::io::Error> {
        self.begin_conflict()
    }
    fn begin_cyclic_conflict(&mut self) -> Result<(), std::io::Error> {
        self.begin_conflict()
    }
    fn conflict_next(&mut self) -> Result<(), std::io::Error> {
        self.output_conflict_marker(SEPARATOR)
    }
    fn end_conflict(&mut self) -> Result<(), std::io::Error> {
        self.output_conflict_marker(END_MARKER)
    }
    fn end_zombie_conflict(&mut self) -> Result<(), std::io::Error> {
        self.end_conflict()
    }
    fn end_cyclic_conflict(&mut self) -> Result<(), std::io::Error> {
        self.output_conflict_marker(END_MARKER)
    }
}

pub(crate) struct ConflictsWriter<'a, 'b, W: std::io::Write> {
    pub w: W,
    pub lines: usize,
    pub new_line: bool,
    pub path: &'b str,
    pub conflicts: &'a mut Vec<crate::output::Conflict>,
    pub buf: Vec<u8>,
}

impl<'a, 'b, W: std::io::Write> ConflictsWriter<'a, 'b, W> {
    pub fn new(w: W, path: &'b str, conflicts: &'a mut Vec<crate::output::Conflict>) -> Self {
        ConflictsWriter {
            w,
            new_line: true,
            lines: 1,
            path,
            conflicts,
            buf: Vec::new(),
        }
    }
}

impl<'a, 'b, W: std::io::Write> std::ops::Deref for ConflictsWriter<'a, 'b, W> {
    type Target = W;
    fn deref(&self) -> &Self::Target {
        &self.w
    }
}

impl<'a, 'b, W: std::io::Write> std::ops::DerefMut for ConflictsWriter<'a, 'b, W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.w
    }
}

impl<'a, 'b, W: std::io::Write> VertexBuffer for ConflictsWriter<'a, 'b, W> {
    fn output_line<E, C>(&mut self, v: Vertex<ChangeId>, c: C) -> Result<(), E>
    where
        E: From<std::io::Error>,
        C: FnOnce(&mut Vec<u8>) -> Result<(), E>,
    {
        self.buf.clear();
        c(&mut self.buf)?;
        debug!("vbuf {:?} {:?}", v, std::str::from_utf8(&self.buf));
        let ends_with_newline = self.buf.ends_with(b"\n");
        self.lines += self.buf.iter().filter(|c| **c == b'\n').count();
        self.w.write_all(&self.buf)?;
        if !self.buf.is_empty() {
            // empty "lines" (such as in the beginning of a file)
            // don't change the status of self.new_line.
            self.new_line = ends_with_newline;
        }
        Ok(())
    }

    fn output_conflict_marker(&mut self, s: &str) -> Result<(), std::io::Error> {
        debug!("output_conflict_marker {:?}", self.new_line);
        if !self.new_line {
            self.lines += 2;
            self.w.write_all(s.as_bytes())?;
        } else {
            self.lines += 1;
            debug!("{:?}", &s.as_bytes()[1..]);
            self.w.write_all(&s.as_bytes()[1..])?;
        }
        self.new_line = true;
        Ok(())
    }

    fn begin_conflict(&mut self) -> Result<(), std::io::Error> {
        self.conflicts.push(crate::output::Conflict::Order {
            path: self.path.to_string(),
            line: self.lines,
        });
        self.output_conflict_marker(START_MARKER)
    }
    fn begin_zombie_conflict(&mut self) -> Result<(), std::io::Error> {
        self.conflicts.push(crate::output::Conflict::Zombie {
            path: self.path.to_string(),
            line: self.lines,
        });
        self.output_conflict_marker(START_MARKER)
    }
    fn begin_cyclic_conflict(&mut self) -> Result<(), std::io::Error> {
        self.conflicts.push(crate::output::Conflict::Cyclic {
            path: self.path.to_string(),
            line: self.lines,
        });
        self.output_conflict_marker(START_MARKER)
    }
}

pub struct Writer<W: std::io::Write> {
    w: W,
    buf: Vec<u8>,
    new_line: bool,
    is_zombie: bool,
}

impl<W: std::io::Write> Writer<W> {
    pub fn new(w: W) -> Self {
        Writer {
            w,
            new_line: true,
            buf: Vec::new(),
            is_zombie: false,
        }
    }
    pub fn into_inner(self) -> W {
        self.w
    }
}

impl<W: std::io::Write> std::ops::Deref for Writer<W> {
    type Target = W;
    fn deref(&self) -> &Self::Target {
        &self.w
    }
}

impl<W: std::io::Write> std::ops::DerefMut for Writer<W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.w
    }
}

impl<W: std::io::Write> VertexBuffer for Writer<W> {
    fn output_line<E, C>(&mut self, v: Vertex<ChangeId>, c: C) -> Result<(), E>
    where
        E: From<std::io::Error>,
        C: FnOnce(&mut Vec<u8>) -> Result<(), E>,
    {
        self.buf.clear();
        c(&mut self.buf)?;
        debug!("vbuf {:?} {:?}", v, std::str::from_utf8(&self.buf));
        let ends_with_newline = self.buf.ends_with(b"\n");
        self.w.write_all(&self.buf[..])?;
        if !self.buf.is_empty() {
            // empty "lines" (such as in the beginning of a file)
            // don't change the status of self.new_line.
            self.new_line = ends_with_newline;
        }
        Ok(())
    }

    fn output_conflict_marker(&mut self, s: &str) -> Result<(), std::io::Error> {
        debug!("output_conflict_marker {:?}", self.new_line);
        if !self.new_line {
            self.w.write_all(s.as_bytes())?;
        } else {
            debug!("{:?}", &s.as_bytes()[1..]);
            self.w.write_all(&s.as_bytes()[1..])?;
        }
        Ok(())
    }

    fn begin_conflict(&mut self) -> Result<(), std::io::Error> {
        self.output_conflict_marker(START_MARKER)
    }
    fn end_conflict(&mut self) -> Result<(), std::io::Error> {
        self.is_zombie = false;
        self.output_conflict_marker(END_MARKER)
    }
    fn begin_zombie_conflict(&mut self) -> Result<(), std::io::Error> {
        if self.is_zombie {
            Ok(())
        } else {
            self.is_zombie = true;
            self.begin_conflict()
        }
    }
    fn end_zombie_conflict(&mut self) -> Result<(), std::io::Error> {
        self.is_zombie = false;
        self.output_conflict_marker(END_MARKER)
    }
    fn begin_cyclic_conflict(&mut self) -> Result<(), std::io::Error> {
        self.output_conflict_marker(START_MARKER)
    }
}
