use std::fmt;

#[cfg(test)]
use quickcheck::{Arbitrary, Gen};

#[cfg(test)]
use PrintableHunk::*;

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringFragment<'a> {
    Literal(&'a str),
    EscapedChar(char),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PrintablePerms {
    IsDir,
    IsExecutable,
    IsFile,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PrintableEdgeFlags {
    pub block: bool,
    pub folder: bool,
    pub deleted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrintableEdge {
    pub previous: PrintableEdgeFlags,
    pub flag: PrintableEdgeFlags,
    pub from: PrintablePos,
    pub to_start: PrintablePos,
    pub to_end: u64,
    pub introduced_by: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrintableNewVertex {
    pub up_context: Vec<PrintablePos>,
    pub start: u64,
    pub end: u64,
    pub down_context: Vec<PrintablePos>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrintableAtom {
    NewVertex(PrintableNewVertex),
    Edges(Vec<PrintableEdge>),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PrintablePos(pub usize, pub u64);

// TODO: Some of these are untested: I didn't know how to create them from the
// command line
// NOTE: contents must be valid under the chosen encoding
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum PrintableHunk {
    FileMoveV {
        path: String,
        name: String,
        perms: PrintablePerms,
        pos: PrintablePos,
        up_context: Vec<PrintablePos>,
        down_context: Vec<PrintablePos>,
        del: Vec<PrintableEdge>,
    },
    FileMoveE {
        path: String,
        pos: PrintablePos,
        add: Vec<PrintableEdge>,
        del: Vec<PrintableEdge>,
    },
    FileAddition {
        name: String,
        parent: String,
        perms: PrintablePerms,
        encoding: Option<Encoding>,
        up_context: Vec<PrintablePos>,
        start: u64,
        end: u64,
        contents: Vec<u8>,
    },
    FileDel {
        path: String,
        pos: PrintablePos,
        encoding: Option<Encoding>,
        del_edges: Vec<PrintableEdge>,
        content_edges: Vec<PrintableEdge>,
        contents: Vec<u8>,
    },
    FileUndel {
        path: String,
        pos: PrintablePos,
        encoding: Option<Encoding>,
        undel_edges: Vec<PrintableEdge>,
        content_edges: Vec<PrintableEdge>,
        contents: Vec<u8>,
    },
    Edit {
        path: String,
        line: usize,
        pos: PrintablePos,
        encoding: Option<Encoding>,
        change: PrintableAtom,
        contents: Vec<u8>,
    },
    Replace {
        path: String,
        line: usize,
        pos: PrintablePos,
        encoding: Option<Encoding>,
        change: Vec<PrintableEdge>,
        replacement: PrintableNewVertex,
        change_contents: Vec<u8>,
        replacement_contents: Vec<u8>,
    },
    SolveNameConflict {
        path: String,
        pos: PrintablePos,
        names: Vec<String>,
        edges: Vec<PrintableEdge>,
    },
    UnsolveNameConflict {
        path: String,
        pos: PrintablePos,
        names: Vec<String>,
        edges: Vec<PrintableEdge>,
    },
    SolveOrderConflict {
        path: String,
        line: usize,
        pos: PrintablePos,
        encoding: Option<Encoding>,
        change: PrintableNewVertex,
        contents: Vec<u8>,
    },
    UnsolveOrderConflict {
        path: String,
        line: usize,
        pos: PrintablePos,
        encoding: Option<Encoding>,
        change: Vec<PrintableEdge>,
        contents: Vec<u8>,
    },
    ResurrectZombies {
        path: String,
        line: usize,
        pos: PrintablePos,
        encoding: Option<Encoding>,
        change: Vec<PrintableEdge>,
        contents: Vec<u8>,
    },
    AddRoot {
        start: u64,
    },
    DelRoot {
        name: Vec<PrintableEdge>,
        inode: Vec<PrintableEdge>,
    },
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct PrintableDep {
    pub type_: DepType,
    pub hash: String,
}

// TODO: make names more precise. I don't know what these should be named.
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum DepType {
    Numbered(usize, bool), // (number, plus-sign)
    ExtraKnown,
    ExtraUnknown,
}

pub struct Escaped<'a>(pub &'a str);

impl<'a> fmt::Display for Escaped<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "\"")?;
        for c in self.0.chars() {
            if c == '\n' {
                write!(fmt, "\\n")?
            } else if c == '\r' {
                write!(fmt, "\\r")?
            } else if c == '\t' {
                write!(fmt, "\\t")?
            } else if c == '\u{08}' {
                write!(fmt, "\\b")?
            } else if c == '\u{0C}' {
                write!(fmt, "\\f")?
            } else if c == '\\' {
                write!(fmt, "\\\\")?
            } else if c == '"' {
                write!(fmt, "\\\"")?
            } else {
                write!(fmt, "{}", c)?
            }
        }
        write!(fmt, "\"")?;
        Ok(())
    }
}

impl PrintablePerms {
    pub fn from_metadata(perms: InodeMetadata) -> Self {
        if perms.0 & 0o1000 == 0o1000 {
            PrintablePerms::IsDir
        } else if perms.0 & 0o100 == 0o100 {
            PrintablePerms::IsExecutable
        } else {
            PrintablePerms::IsFile
        }
    }

    pub fn to_metadata(self) -> InodeMetadata {
        InodeMetadata(match self {
            PrintablePerms::IsDir => 0o1100,
            PrintablePerms::IsExecutable => 0o100,
            PrintablePerms::IsFile => 0o0,
        })
    }
}

impl fmt::Display for PrintablePos {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}.{}", self.0, self.1)
    }
}

impl fmt::Display for PrintablePerms {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{}",
            match self {
                PrintablePerms::IsDir => " +dx",
                PrintablePerms::IsExecutable => " +x",
                PrintablePerms::IsFile => "",
            }
        )
    }
}

impl PrintableEdgeFlags {
    pub fn from(ef: EdgeFlags) -> Self {
        assert!(!ef.contains(EdgeFlags::PARENT));
        assert!(!ef.contains(EdgeFlags::PSEUDO));
        Self {
            block: ef.contains(EdgeFlags::BLOCK),
            folder: ef.contains(EdgeFlags::FOLDER),
            deleted: ef.contains(EdgeFlags::DELETED),
        }
    }

    // TODO: make this nicer
    pub fn to(self) -> EdgeFlags {
        let mut f = EdgeFlags::empty();
        if self.block {
            f |= EdgeFlags::BLOCK;
        }
        if self.folder {
            f |= EdgeFlags::FOLDER;
        }
        if self.deleted {
            f |= EdgeFlags::DELETED;
        }
        f
    }
}

impl fmt::Display for PrintableEdgeFlags {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        if self.block {
            write!(fmt, "B")?;
        }
        if self.folder {
            write!(fmt, "F")?;
        }
        if self.deleted {
            write!(fmt, "D")?;
        }
        Ok(())
    }
}

impl fmt::Display for PrintableEdge {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{}:{} {} -> {}:{}/{}",
            self.previous, self.flag, self.from, self.to_start, self.to_end, self.introduced_by
        )
    }
}

impl fmt::Display for PrintableNewVertex {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "  up")?;
        for c in self.up_context.iter() {
            write!(fmt, " {}", c)?
        }
        write!(fmt, ", new {}:{}, down", self.start, self.end)?;
        for c in self.down_context.iter() {
            write!(fmt, " {}", c)?;
        }
        Ok(())
    }
}

impl fmt::Display for PrintableAtom {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PrintableAtom::NewVertex(x) => write!(fmt, "{}", x),
            PrintableAtom::Edges(x) => {
                for (i, edge) in x.iter().enumerate() {
                    if i > 0 {
                        write!(fmt, ", ")?;
                    }
                    write!(fmt, "{}", edge)?;
                }
                Ok(())
            }
        }
    }
}

impl PrintableHunk {
    pub fn write<W: WriteChangeLine>(&self, w: &mut W) -> Result<(), std::io::Error> {
        use PrintableHunk::*;
        match self {
            FileMoveV {
                path,
                name,
                perms,
                pos,
                up_context,
                down_context,
                del,
            } => {
                writeln!(
                    w,
                    "Moved: {} {} {} {}",
                    Escaped(path),
                    Escaped(name),
                    perms,
                    pos,
                )?;
                writeln!(w, "{}", PrintableAtom::Edges(del.to_vec()))?;
                write!(w, "up")?;
                for c in up_context.iter() {
                    write!(w, " {}", c)?
                }
                write!(w, ", down")?;
                for c in down_context.iter() {
                    write!(w, " {}", c)?
                }
                writeln!(w)?;
            }
            FileMoveE {
                path,
                pos,
                add,
                del,
            } => {
                writeln!(w, "Moved: {} {}", Escaped(path), pos)?;
                writeln!(w, "{}", PrintableAtom::Edges(add.to_vec()))?;
                writeln!(w, "{}", PrintableAtom::Edges(del.to_vec()))?;
            }
            FileAddition {
                name,
                parent,
                perms,
                encoding,
                up_context,
                start,
                end,
                contents,
            } => {
                write!(
                    w,
                    "File addition: {} in {}{} {}\n  up",
                    Escaped(name),
                    Escaped(parent),
                    perms,
                    Escaped(encoding_label(encoding)),
                )?;
                for c in up_context.iter() {
                    write!(w, " {}", c)?
                }
                writeln!(w, ", new {}:{}", start, end)?;
                print_contents(w, "+", contents, encoding)?;
            }
            FileDel {
                path,
                pos,
                encoding,
                del_edges,
                content_edges,
                contents,
            } => {
                writeln!(
                    w,
                    "File deletion: {} {} {}",
                    Escaped(path),
                    pos,
                    Escaped(encoding_label(encoding)),
                )?;
                writeln!(w, "{}", PrintableAtom::Edges(del_edges.to_vec()))?;
                if !content_edges.is_empty() {
                    writeln!(w, "{}", PrintableAtom::Edges(content_edges.to_vec()))?;
                }
                print_contents(w, "-", contents, encoding)?;
            }
            FileUndel {
                path,
                pos,
                encoding,
                undel_edges,
                content_edges,
                contents,
            } => {
                writeln!(
                    w,
                    "File un-deletion: {} {} {}",
                    Escaped(path),
                    pos,
                    Escaped(encoding_label(encoding)),
                )?;
                writeln!(w, "{}", PrintableAtom::Edges(undel_edges.to_vec()))?;
                if !content_edges.is_empty() {
                    writeln!(w, "{}", PrintableAtom::Edges(content_edges.to_vec()))?;
                }
                print_contents(w, "+", contents, encoding)?;
            }

            Edit {
                path,
                line,
                pos,
                encoding,
                change,
                contents,
            } => {
                writeln!(
                    w,
                    "Edit in {}:{} {} {}",
                    Escaped(&path),
                    line,
                    pos,
                    Escaped(encoding_label(encoding))
                )?;
                writeln!(w, "{}", change)?;
                let sign = if let PrintableAtom::Edges(ref e) = change {
                    if e[0].flag.deleted {
                        "-"
                    } else {
                        "+"
                    }
                } else {
                    "+"
                };
                print_contents(w, sign, contents, encoding)?;
            }

            Replace {
                path,
                line,
                pos,
                encoding,
                change,
                replacement,
                change_contents,
                replacement_contents,
            } => {
                writeln!(
                    w,
                    "Replacement in {}:{} {} {}",
                    Escaped(&path),
                    line,
                    pos,
                    Escaped(encoding_label(encoding))
                )?;
                writeln!(w, "{}", PrintableAtom::Edges(change.clone()))?;
                writeln!(w, "{}", PrintableAtom::NewVertex(replacement.clone()))?;
                print_contents(w, "-", change_contents, encoding)?;
                print_contents(w, "+", replacement_contents, encoding)?;
            }
            SolveNameConflict {
                path,
                pos,
                names,
                edges,
            } => {
                write!(w, "Solving a name conflict in {} {}: ", Escaped(path), pos,)?;
                write_names(w, names)?;
                writeln!(w)?;
                writeln!(w, "{}", PrintableAtom::Edges(edges.clone()))?;
            }
            UnsolveNameConflict {
                path,
                pos,
                names,
                edges,
            } => {
                write!(
                    w,
                    "Un-solving a name conflict in {} {}: ",
                    Escaped(path),
                    pos,
                )?;
                write_names(w, names)?;
                writeln!(w)?;
                writeln!(w, "{}", PrintableAtom::Edges(edges.clone()))?;
            }
            SolveOrderConflict {
                path,
                line,
                pos,
                encoding,
                change,
                contents,
            } => {
                writeln!(
                    w,
                    "Solving an order conflict in {}:{} {} {}",
                    Escaped(path),
                    line,
                    pos,
                    Escaped(encoding_label(encoding)),
                )?;
                writeln!(w, "{}", change)?;
                print_contents(w, "+", contents, encoding)?;
            }
            UnsolveOrderConflict {
                path,
                line,
                pos,
                encoding,
                change,
                contents,
            } => {
                writeln!(
                    w,
                    "Un-solving an order conflict in {}:{} {} {}",
                    Escaped(path),
                    line,
                    pos,
                    Escaped(encoding_label(encoding))
                )?;
                writeln!(w, "{}", PrintableAtom::Edges(change.clone()))?;
                print_contents(w, "-", contents, encoding)?;
            }
            ResurrectZombies {
                path,
                line,
                pos,
                encoding,
                change,
                contents,
            } => {
                writeln!(
                    w,
                    "Resurrecting zombie lines in {}:{} {} {}",
                    Escaped(path),
                    line,
                    pos,
                    Escaped(encoding_label(encoding))
                )?;
                writeln!(w, "{}", PrintableAtom::Edges(change.clone()))?;
                print_contents(w, "+", contents, encoding)?;
            }
            AddRoot { start } => {
                writeln!(
                    w,
                    "Root add\n  up {}, new {}:{}",
                    PrintablePos(1, 0),
                    start,
                    start,
                )?;
            }
            DelRoot { name, inode } => {
                writeln!(w, "Root del",)?;
                writeln!(w, "{}", PrintableAtom::Edges(name.to_vec()))?;
                writeln!(w, "{}", PrintableAtom::Edges(inode.to_vec()))?;
            }
        };
        Ok(())
    }
}

pub fn write_names<W: std::io::Write>(w: &mut W, names: &[String]) -> Result<(), std::io::Error> {
    for (i, name) in names.iter().enumerate() {
        if i > 0 {
            write!(w, ", ")?;
        }
        write!(w, "{}", Escaped(name))?;
    }
    Ok(())
}

pub fn get_encoding(contents: &[u8]) -> Option<Encoding> {
    let mut detector = crate::chardetng::EncodingDetector::new();
    detector.feed(contents, true);
    if let Some(e) = detector.get_valid(None, true, &contents) {
        Some(Encoding(e))
    } else {
        None
    }
    // let (encoding_guess, may_be_right) = detector.guess_assess(None, true);
    // if may_be_right {
    //     Some(Encoding(encoding_guess))
    // } else {
    //     None
    // }
}

fn print_contents<W: WriteChangeLine>(
    w: &mut W,
    prefix: &str,
    contents: &[u8],
    encoding: &Option<Encoding>,
) -> Result<(), std::io::Error> {
    if contents.is_empty() {
        return Ok(());
    }
    if let Some(encoding) = encoding {
        let dec = encoding.decode(&contents);
        let ends_with_newline = dec.ends_with("\n");
        let dec = if ends_with_newline {
            &dec[..dec.len() - 1]
        } else {
            &dec
        };
        for a in dec.split('\n') {
            writeln!(w, "{} {}", prefix, a)?;
        }
        if !ends_with_newline {
            writeln!(w, "\\")?;
        }
        Ok(())
    } else if contents.len() <= 4096 {
        writeln!(w, "{}b{}", prefix, data_encoding::BASE64.encode(contents))
    } else {
        Ok(())
    }
}

// QuickCheck instances

#[cfg(test)]
#[rustfmt::skip]
// This may be nicer if it was generated by a macro
impl Arbitrary for PrintableHunk {
    fn arbitrary(g: &mut Gen) -> Self {
        fn f<A: Arbitrary>(g: &mut Gen) -> A {
            Arbitrary::arbitrary(g)
        }

        fix_encoding(Gen::new(g.size()).choose(&[
            FileMoveV {
                path: f(g), name: f(g), perms: f(g), pos: f(g), up_context: f(g), down_context: f(g), del: f(g),
            },
            FileMoveE {
                path: f(g), pos: f(g), add: f(g), del: f(g),
            },
            FileAddition {
                name: f(g), parent: f(g), perms: f(g), encoding: f(g), up_context: f(g), start: f(g), end: f(g), contents: f(g),
            },
            FileDel {
                path: f(g), pos: f(g), encoding: f(g), del_edges: f(g), content_edges: f(g), contents: f(g),
            },
            FileUndel {
                path: f(g), pos: f(g), encoding: f(g), undel_edges: f(g), content_edges: f(g), contents: f(g),
            },
            Edit {
                path: f(g), line: f(g), pos: f(g), encoding: f(g), change: f(g), contents: f(g),
            },
            Replace {
                path: f(g), line: f(g), pos: f(g), encoding: f(g), change: f(g), replacement: f(g), change_contents: f(g), replacement_contents: f(g),
            },
            SolveNameConflict {
                path: f(g), pos: f(g), names: f(g), edges: f(g),
            },
            UnsolveNameConflict {
                path: f(g), pos: f(g), names: f(g), edges: f(g),
            },
            SolveOrderConflict {
                path: f(g), line: f(g), pos: f(g), encoding: f(g), change: f(g), contents: f(g),
            },
            UnsolveOrderConflict {
                path: f(g), line: f(g), pos: f(g), encoding: f(g), change: f(g), contents: f(g),
            },
            ResurrectZombies {
                path: f(g), line: f(g), pos: f(g), encoding: f(g), change: f(g), contents: f(g),
            },
        ])
        .unwrap().clone())
    }

    // Shrinking frequently blows stack. Investigate how to fix it.
    // You can disable shrinking by commenting out this function.
    // This may be best solved by switching to proptest crate
    /*
    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self.clone() {
            FileMoveV { path, name, perms, pos, up_context, down_context, del } =>
                Box::new((path, name, perms, pos, up_context, down_context, del)
                .shrink().map(|(path, name, perms, pos, up_context, down_context, del)|
                fix_encoding(FileMoveV { path, name, perms, pos, up_context, down_context, del }))),

            FileMoveE { path, pos, add, del } =>
                Box::new((path, pos, add, del)
                .shrink().map(|(path, pos, add, del)|
                fix_encoding(FileMoveE { path, pos, add, del }))),

            FileAddition { name, parent, perms, encoding, up_context, start, end, contents } =>
                Box::new((name, parent, perms, encoding, up_context, start, end, contents)
                .shrink().map(|(name, parent, perms, encoding, up_context, start, end, contents)|
                fix_encoding(FileAddition { name, parent, perms, encoding, up_context, start, end, contents }))),

            FileDel { path, pos, encoding, del_edges, content_edges, contents } =>
                Box::new((path, pos, encoding, del_edges, content_edges, contents)
                .shrink().map(|(path, pos, encoding, del_edges, content_edges, contents)|
                fix_encoding(FileDel { path, pos, encoding, del_edges, content_edges, contents }))),

            FileUndel { path, pos, encoding, undel_edges, content_edges, contents } =>
                Box::new((path, pos, encoding, undel_edges, content_edges, contents)
                .shrink().map(|(path, pos, encoding, undel_edges, content_edges, contents)|
                fix_encoding(FileUndel { path, pos, encoding, undel_edges, content_edges, contents }))),

            Edit { path, line, pos, encoding, change, contents } =>
                Box::new((path, line, pos, encoding, change, contents)
                .shrink().map(|(path, line, pos, encoding, change, contents)|
                fix_encoding(Edit { path, line, pos, encoding, change, contents }))),

            Replace { path, line, pos, encoding, change, replacement, change_contents, replacement_contents } =>
                Box::new((path, line, pos, encoding, change, replacement, change_contents, replacement_contents)
                .shrink().map(|(path, line, pos, encoding, change, replacement, change_contents, replacement_contents)|
                fix_encoding(Replace { path, line, pos, encoding, change, replacement, change_contents, replacement_contents }))),

            SolveNameConflict { path, pos, names, edges } =>
                Box::new((path, pos, names, edges)
                .shrink().map(|(path, pos, names, edges)|
                fix_encoding(SolveNameConflict { path, pos, names, edges }))),

            UnsolveNameConflict { path, pos, names, edges } =>
                Box::new((path, pos, names, edges)
                .shrink().map(|(path, pos, names, edges)|
                fix_encoding(UnsolveNameConflict { path, pos, names, edges }))),

            SolveOrderConflict { path, line, pos, encoding, change, contents } =>
                Box::new((path, line, pos, encoding, change, contents)
                .shrink().map(|(path, line, pos, encoding, change, contents)|
                fix_encoding(SolveOrderConflict { path, line, pos, encoding, change, contents }))),

            UnsolveOrderConflict { path, line, pos, encoding, change, contents } =>
                Box::new((path, line, pos, encoding, change, contents)
                .shrink().map(|(path, line, pos, encoding, change, contents)|
                fix_encoding(UnsolveOrderConflict { path, line, pos, encoding, change, contents }))),

            ResurrectZombies { path, line, pos, encoding, change, contents } =>
                Box::new((path, line, pos, encoding, change, contents)
                .shrink().map(|(path, line, pos, encoding, change, contents)|
                fix_encoding(ResurrectZombies { path, line, pos, encoding, change, contents }))),
        }
    }
    */
}

#[cfg(test)]
/// This is the one thing that is not normalized on PrintableHunk: encoding
/// content must be valid, given the encoding. This function ensures that.
fn fix_encoding(mut hunk: PrintableHunk) -> PrintableHunk {
    // let mut h = hunk.clone();
    match &mut hunk {
        FileAddition {
            encoding, contents, ..
        } => *encoding = get_encoding(contents),
        FileDel {
            encoding, contents, ..
        } => *encoding = get_encoding(contents),
        FileUndel {
            encoding, contents, ..
        } => *encoding = get_encoding(contents),
        Edit {
            encoding, contents, ..
        } => *encoding = get_encoding(contents),
        Replace { encoding, .. } => *encoding = None,
        SolveOrderConflict {
            encoding, contents, ..
        } => *encoding = get_encoding(contents),
        UnsolveOrderConflict {
            encoding, contents, ..
        } => *encoding = get_encoding(contents),
        ResurrectZombies {
            encoding, contents, ..
        } => *encoding = get_encoding(contents),
        _ => (),
    };
    hunk
}

#[cfg(test)]
impl Arbitrary for PrintablePerms {
    fn arbitrary(g: &mut Gen) -> Self {
        *g.choose(&[
            PrintablePerms::IsDir,
            PrintablePerms::IsExecutable,
            PrintablePerms::IsFile,
        ])
        .unwrap()
    }
}

#[cfg(test)]
impl Arbitrary for PrintablePos {
    fn arbitrary(g: &mut Gen) -> Self {
        PrintablePos(usize::arbitrary(g), u64::arbitrary(g))
    }
    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(
            self.0
                .shrink()
                .zip(self.1.shrink())
                .map(|(a, b)| PrintablePos(a, b)),
        )
    }
}

#[cfg(test)]
impl Arbitrary for PrintableEdge {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            previous: Arbitrary::arbitrary(g),
            flag: Arbitrary::arbitrary(g),
            from: Arbitrary::arbitrary(g),
            to_start: Arbitrary::arbitrary(g),
            to_end: Arbitrary::arbitrary(g),
            introduced_by: Arbitrary::arbitrary(g),
        }
    }
    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let Self {
            previous,
            flag,
            from,
            to_start,
            to_end,
            introduced_by,
        } = self.clone();
        Box::new(
            (previous, flag, from, to_start, to_end, introduced_by)
                .shrink()
                .map(
                    |(previous, flag, from, to_start, to_end, introduced_by)| Self {
                        previous,
                        flag,
                        from,
                        to_start,
                        to_end,
                        introduced_by,
                    },
                ),
        )
    }
}

#[cfg(test)]
impl Arbitrary for PrintableNewVertex {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            up_context: Arbitrary::arbitrary(g),
            start: Arbitrary::arbitrary(g),
            end: Arbitrary::arbitrary(g),
            down_context: Arbitrary::arbitrary(g),
        }
    }
    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let Self {
            up_context,
            start,
            end,
            down_context,
        } = self.clone();
        Box::new((up_context, start, end, down_context).shrink().map(
            |(up_context, start, end, down_context)| Self {
                up_context,
                start,
                end,
                down_context,
            },
        ))
    }
}

#[cfg(test)]
impl Arbitrary for PrintableAtom {
    fn arbitrary(g: &mut Gen) -> Self {
        Gen::new(g.size())
            .choose(&[
                PrintableAtom::NewVertex(Arbitrary::arbitrary(g)),
                PrintableAtom::Edges(Arbitrary::arbitrary(g)),
            ])
            .unwrap()
            .clone()
    }
    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            PrintableAtom::NewVertex(x) => {
                Box::new(x.shrink().map(|x| PrintableAtom::NewVertex(x)))
            }
            PrintableAtom::Edges(x) => Box::new(x.shrink().map(|x| PrintableAtom::Edges(x))),
        }
    }
}

#[cfg(test)]
impl Arbitrary for PrintableEdgeFlags {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            block: Arbitrary::arbitrary(g),
            folder: Arbitrary::arbitrary(g),
            deleted: Arbitrary::arbitrary(g),
        }
    }
    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new((self.block, self.folder, self.deleted).shrink().map(
            |(block, folder, deleted)| Self {
                block,
                folder,
                deleted,
            },
        ))
    }
}
