use crate::HashMap;
use std::collections::hash_map::Entry;
use std::io::BufRead;

use regex::Captures;

use super::*;
use crate::changestore::*;

#[derive(Debug, Error)]
pub enum TextDeError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    TomlDe(#[from] toml::de::Error),
    #[error("Missing dependency [{0}]")]
    MissingChange(usize),
    #[error("Byte position {0} from this change missing")]
    MissingPosition(u64),
}

#[derive(Debug, Error)]
pub enum TextSerError<C: std::error::Error + 'static> {
    #[error(transparent)]
    C(C),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    TomlSer(#[from] toml::ser::Error),
    #[error("Missing contents in change {:?}", h)]
    MissingContents { h: Hash },
    #[error(transparent)]
    Change(#[from] ChangeError),
    #[error("Invalid change")]
    InvalidChange,
}

impl LocalChange<Hunk<Option<Hash>, Local>, Author> {
    const DEPS_LINE_: &'static str = "# Dependencies\n";
    const HUNKS_LINE_: &'static str = "# Hunks\n";

    pub fn write_all_deps<F: FnMut(Hash) -> Result<(), ChangeError>>(
        &self,
        mut f: F,
    ) -> Result<(), ChangeError> {
        for c in self.changes.iter() {
            for c in c.iter() {
                match *c {
                    Atom::NewVertex(ref n) => {
                        for change in n
                            .up_context
                            .iter()
                            .chain(n.down_context.iter())
                            .map(|c| c.change)
                            .chain(std::iter::once(n.inode.change))
                        {
                            if let Some(change) = change {
                                if let Hash::None = change {
                                    continue;
                                }
                                f(change)?
                            }
                        }
                    }
                    Atom::EdgeMap(ref e) => {
                        for edge in e.edges.iter() {
                            for change in &[
                                edge.from.change,
                                edge.to.change,
                                edge.introduced_by,
                                e.inode.change,
                            ] {
                                if let Some(change) = *change {
                                    if let Hash::None = change {
                                        continue;
                                    }
                                    f(change)?
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn write_old<W: WriteChangeLine, C: ChangeStore>(
        &self,
        changes: &C,
        hash: Option<Hash>,
        write_header: bool,
        mut w: W,
    ) -> Result<(), TextSerError<C::Error>> {
        if let Some(h) = hash {
            // Check if we have the full contents
            let mut hasher = Hasher::default();
            hasher.update(&self.contents);
            let hash = hasher.finish();
            if hash != self.contents_hash {
                return Err((TextSerError::MissingContents { h }).into());
            }
        }

        if write_header {
            let s = toml::ser::to_string_pretty(&self.header)?;
            writeln!(w, "{}", s)?;
        }
        let mut hashes = HashMap::default();
        let mut i = 2;
        let mut needs_newline = false;
        if !self.dependencies.is_empty() {
            w.write_all(Self::DEPS_LINE_.as_bytes())?;
            needs_newline = true;
            for dep in self.dependencies.iter() {
                hashes.insert(*dep, i);
                writeln!(w, "[{}] {}", i, dep.to_base32())?;
                i += 1;
            }
        }

        self.write_all_deps(|change| {
            if let Entry::Vacant(e) = hashes.entry(change) {
                e.insert(i);
                if !needs_newline {
                    w.write_all(Self::DEPS_LINE_.as_bytes())?;
                    needs_newline = true;
                }
                writeln!(w, "[{}]+{}", i, change.to_base32())?;
                i += 1;
            }
            Ok(())
        })?;

        if !self.extra_known.is_empty() {
            needs_newline = true;
            for dep in self.extra_known.iter() {
                writeln!(w, "[*] {}", dep.to_base32())?;
                i += 1;
            }
        }

        if !self.changes.is_empty() {
            if needs_newline {
                w.write_all(b"\n")?
            }
            w.write_all(Self::HUNKS_LINE_.as_bytes())?;
            for (n, rec) in self.changes.iter().enumerate() {
                write!(w, "\n{}. ", n + 1)?;
                rec.write_old(changes, &hashes, &self.contents, &mut w)?
            }
        }
        Ok(())
    }
}

impl Change {
    pub fn read_and_deps_old<
        R: BufRead,
        T: ChannelTxnT + DepsTxnT<DepsError = <T as GraphTxnT>::GraphError>,
    >(
        r: R,
        updatables: &mut HashMap<usize, crate::InodeUpdate>,
        txn: &T,
        channel: &ChannelRef<T>,
    ) -> Result<Self, TextDeError> {
        let (mut change, extra_dependencies) = Self::read_(r, updatables)?;
        let (mut deps, extra) =
            dependencies(txn, &channel.read(), change.hashed.changes.iter()).unwrap();
        deps.extend(extra_dependencies.into_iter());
        change.hashed.dependencies = deps;
        change.hashed.extra_known = extra;
        Ok(change)
    }

    pub fn read_old<R: BufRead>(
        r: R,
        updatables: &mut HashMap<usize, crate::InodeUpdate>,
    ) -> Result<Self, TextDeError> {
        Ok(Self::read_(r, updatables)?.0)
    }

    fn read_<R: BufRead>(
        mut r: R,
        updatables: &mut HashMap<usize, crate::InodeUpdate>,
    ) -> Result<(Self, HashSet<Hash>), TextDeError> {
        use self::text_changes_old::*;
        let mut section = Section::Header(String::new());
        let mut change = Change {
            offsets: Offsets::default(),
            hashed: Hashed {
                version: VERSION,
                header: ChangeHeader {
                    authors: Vec::new(),
                    message: String::new(),
                    description: None,
                    timestamp: chrono::Utc::now(),
                },
                dependencies: Vec::new(),
                extra_known: Vec::new(),
                metadata: Vec::new(),
                changes: Vec::new(),
                contents_hash: Hasher::default().finish(),
            },
            unhashed: None,
            contents: Vec::new(),
        };
        let conclude_section = |change: &mut Change,
                                section: Section,
                                contents: &mut Vec<u8>|
         -> Result<(), TextDeError> {
            match section {
                Section::Header(ref s) => {
                    debug!("header = {:?}", s);
                    change.header = toml::de::from_str(&s)?;
                    Ok(())
                }
                Section::Deps => Ok(()),
                Section::Changes {
                    mut changes,
                    current,
                    ..
                } => {
                    if has_newvertices(&current) {
                        contents.push(0)
                    }
                    if let Some(c) = current {
                        debug!("next action = {:?}", c);
                        changes.push(c)
                    }
                    change.changes = changes;
                    Ok(())
                }
            }
        };
        let mut h = String::new();
        let mut contents = Vec::new();
        let mut deps = HashMap::default();
        let mut extra_dependencies = HashSet::default();
        while r.read_line(&mut h)? > 0 {
            debug!("h = {:?}", h);
            if h == Self::DEPS_LINE_ {
                let section = std::mem::replace(&mut section, Section::Deps);
                conclude_section(&mut change, section, &mut contents)?;
            } else if h == Self::HUNKS_LINE_ {
                let section = std::mem::replace(
                    &mut section,
                    Section::Changes {
                        changes: Vec::new(),
                        current: None,
                        offsets: HashMap::default(),
                    },
                );
                conclude_section(&mut change, section, &mut contents)?;
            } else {
                use regex::Regex;
                lazy_static! {
                    static ref DEPS: Regex = Regex::new(r#"\[(\d*|\*)\](\+| ) *(\S*)"#).unwrap();
                    static ref KNOWN: Regex = Regex::new(r#"(\S*)"#).unwrap();
                }
                match section {
                    Section::Header(ref mut s) => s.push_str(&h),
                    Section::Deps => {
                        if let Some(d) = DEPS.captures(&h) {
                            let hash = Hash::from_base32(d[3].as_bytes()).unwrap();
                            if let Ok(n) = d[1].parse() {
                                if &d[2] == " " {
                                    change.hashed.dependencies.push(hash);
                                }
                                deps.insert(n, hash);
                            } else if &d[1] == "*" {
                                change.hashed.extra_known.push(hash);
                            } else {
                                extra_dependencies.insert(hash);
                            }
                        }
                    }
                    Section::Changes {
                        ref mut current,
                        ref mut changes,
                        ref mut offsets,
                    } => {
                        if let Some(next) =
                            Hunk::read(updatables, current, &mut contents, &deps, offsets, &h)?
                        {
                            debug!("next action = {:?}", next);
                            changes.push(next)
                        }
                    }
                }
            }
            h.clear();
        }
        conclude_section(&mut change, section, &mut contents)?;
        change.contents = contents;
        change.contents_hash = {
            let mut hasher = Hasher::default();
            hasher.update(&change.contents);
            hasher.finish()
        };
        Ok((change, extra_dependencies))
    }
}

const BINARY_LABEL: &str = "binary";

struct Escaped<'a>(&'a str);

impl<'a> std::fmt::Display for Escaped<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "\"")?;
        for c in self.0.chars() {
            if c == '"' {
                write!(fmt, "\\{}", c)?
            } else if c == '\\' {
                write!(fmt, "\\\\")?
            } else {
                write!(fmt, "{}", c)?
            }
        }
        write!(fmt, "\"")?;
        Ok(())
    }
}

fn unescape(s: &str) -> std::borrow::Cow<str> {
    let mut b = 0;
    let mut result = String::new();
    let mut ch = s.chars();
    while let Some(c) = ch.next() {
        if c == '\\' {
            if result.is_empty() {
                result = s.split_at(b).0.to_string();
            }
            if let Some(c) = ch.next() {
                result.push(c)
            }
        } else if !result.is_empty() {
            result.push(c)
        }
        b += c.len_utf8()
    }
    if result.is_empty() {
        s.into()
    } else {
        result.into()
    }
}

impl Hunk<Option<Hash>, Local> {
    fn write_old<W: WriteChangeLine, C: ChangeStore>(
        &self,
        changes: &C,
        hashes: &HashMap<Hash, usize>,
        change_contents: &[u8],
        mut w: &mut W,
    ) -> Result<(), TextSerError<C::Error>> {
        use self::text_changes_old::*;
        match self {
            Hunk::FileMove { del, add, path } => match add {
                Atom::NewVertex(ref add) => {
                    let FileMetadata {
                        basename: name,
                        metadata: perms,
                        ..
                    } = FileMetadata::read(&change_contents[add.start.0.into()..add.end.0.into()]);
                    write!(
                        w,
                        "Moved: {} {} {}",
                        Escaped(&path),
                        Escaped(&name),
                        if perms.0 & 0o1000 == 0o1000 {
                            "+dx "
                        } else if perms.0 & 0o100 == 0o100 {
                            "+x "
                        } else {
                            ""
                        }
                    )?;
                    write_pos(&mut w, hashes, del.inode())?;
                    writeln!(w)?;
                    write_atom(&mut w, hashes, &del)?;

                    write!(w, "up")?;
                    for c in add.up_context.iter() {
                        write!(w, " ")?;
                        write_pos(&mut w, hashes, *c)?
                    }
                    write!(w, ", down")?;
                    for c in add.down_context.iter() {
                        write!(w, " ")?;
                        write_pos(&mut w, hashes, *c)?
                    }
                    w.write_all(b"\n")?;
                }
                Atom::EdgeMap(_) => {
                    write!(w, "Moved: {:?} ", path)?;
                    write_pos(&mut w, hashes, del.inode())?;
                    writeln!(w)?;
                    write_atom(&mut w, hashes, &add)?;
                    write_atom(&mut w, hashes, &del)?;
                }
            },
            Hunk::FileDel {
                del,
                contents,
                path,
                encoding,
            } => {
                debug!("file del");
                write!(w, "File deletion: {} ", Escaped(path))?;
                write_pos(&mut w, hashes, del.inode())?;
                writeln!(w, " {:?}", encoding_label(encoding))?;

                write_atom(&mut w, hashes, &del)?;
                if let Some(ref contents) = contents {
                    write_atom(&mut w, hashes, &contents)?;
                    writeln!(w)?;
                    print_change_contents(w, changes, contents, change_contents, encoding)?;
                } else {
                    writeln!(w)?;
                }
            }
            Hunk::FileUndel {
                undel,
                contents,
                path,
                encoding,
            } => {
                debug!("file undel");
                write!(w, "File un-deletion: {} ", Escaped(path))?;
                write_pos(&mut w, hashes, undel.inode())?;
                writeln!(w, " {:?}", encoding_label(encoding))?;
                write_atom(&mut w, hashes, &undel)?;
                if let Some(ref contents) = contents {
                    write_atom(&mut w, hashes, &contents)?;
                    print_change_contents(w, changes, contents, change_contents, encoding)?;
                } else {
                    writeln!(w)?;
                }
            }
            Hunk::FileAdd {
                add_name,
                contents,
                path,
                encoding,
                ..
            } => {
                if let Atom::NewVertex(ref n) = add_name {
                    debug!("add_name {:?}", n);
                    let FileMetadata {
                        basename: name,
                        metadata: perms,
                        ..
                    } = FileMetadata::read(&change_contents[n.start.0.into()..n.end.0.into()]);
                    let parent = if let Some(p) = crate::path::parent(&path) {
                        if p.is_empty() {
                            "/"
                        } else {
                            p
                        }
                    } else {
                        "/"
                    };
                    write!(
                        w,
                        "File addition: {} in {}{} \"{}\"\n  up",
                        Escaped(name),
                        Escaped(parent),
                        if perms.0 & 0o1000 == 0o1000 {
                            " +dx"
                        } else if perms.0 & 0o100 == 0o100 {
                            " +x"
                        } else {
                            ""
                        },
                        encoding_label(encoding)
                    )?;
                    assert!(n.down_context.is_empty());
                    for c in n.up_context.iter() {
                        write!(w, " ")?;
                        write_pos(&mut w, hashes, *c)?
                    }
                    writeln!(w, ", new {}:{}", n.start.0, n.end.0)?;
                }
                if let Some(Atom::NewVertex(ref n)) = contents {
                    let c = &change_contents[n.start.us()..n.end.us()];
                    print_contents(w, "+", c, encoding)?;
                    if !c.ends_with(b"\n") {
                        writeln!(w, "\n\\")?
                    }
                }
            }
            Hunk::Edit {
                change,
                local,
                encoding,
            } => {
                debug!("edit");
                write!(w, "Edit in {}:{} ", Escaped(&local.path), local.line)?;
                write_pos(&mut w, hashes, change.inode())?;
                write!(w, " {:?}", encoding_label(encoding))?;
                writeln!(w)?;
                write_atom(&mut w, hashes, &change)?;
                print_change_contents(w, changes, change, change_contents, encoding)?;
            }
            Hunk::Replacement {
                change,
                replacement,
                local,
                encoding,
            } => {
                debug!("replacement");
                write!(w, "Replacement in {}:{} ", Escaped(&local.path), local.line)?;
                write_pos(&mut w, hashes, change.inode())?;
                write!(w, " {:?}", encoding_label(encoding))?;
                writeln!(w)?;
                write_atom(&mut w, hashes, &change)?;
                write_atom(&mut w, hashes, &replacement)?;
                print_change_contents(w, changes, change, change_contents, encoding)?;
                print_change_contents(w, changes, replacement, change_contents, encoding)?;
            }
            Hunk::SolveNameConflict { name, path } => {
                write!(w, "Solving a name conflict in {} ", Escaped(path))?;
                write_pos(&mut w, hashes, name.inode())?;
                write!(w, ": ")?;
                write_deleted_names(&mut w, changes, name)?;
                writeln!(w)?;
                write_atom(&mut w, hashes, &name)?;
            }
            Hunk::UnsolveNameConflict { name, path } => {
                write!(w, "Un-solving a name conflict in {} ", Escaped(path))?;
                write_pos(&mut w, hashes, name.inode())?;
                write!(w, ": ")?;
                write_deleted_names(&mut w, changes, name)?;
                writeln!(w)?;
                write_atom(&mut w, hashes, &name)?;
            }
            Hunk::SolveOrderConflict { change, local } => {
                debug!("solve order conflict");
                write!(
                    w,
                    "Solving an order conflict in {}:{} ",
                    Escaped(&local.path),
                    local.line,
                )?;
                write_pos(&mut w, hashes, change.inode())?;
                writeln!(w)?;
                write_atom(&mut w, hashes, &change)?;
                print_change_contents(w, changes, change, change_contents, &None)?;
            }
            Hunk::UnsolveOrderConflict { change, local } => {
                debug!("unsolve order conflict");
                write!(
                    w,
                    "Un-solving an order conflict in {}:{} ",
                    Escaped(&local.path),
                    local.line,
                )?;
                write_pos(&mut w, hashes, change.inode())?;
                writeln!(w)?;
                write_atom(&mut w, hashes, &change)?;
                print_change_contents(w, changes, change, change_contents, &None)?;
            }
            Hunk::ResurrectZombies {
                change,
                local,
                encoding,
            } => {
                debug!("resurrect zombies");
                write!(
                    w,
                    "Resurrecting zombie lines in {}:{} ",
                    Escaped(&local.path),
                    local.line
                )?;
                write_pos(&mut w, hashes, change.inode())?;
                write!(w, " \"{}\"", encoding_label(encoding))?;
                writeln!(w)?;
                write_atom(&mut w, hashes, &change)?;
                print_change_contents(w, changes, change, change_contents, encoding)?;
            }
            _ => {}
        }
        Ok(())
    }
}

fn encoding_label(encoding: &Option<Encoding>) -> &str {
    match encoding {
        Some(encoding) => encoding.label(),
        _ => BINARY_LABEL,
    }
}

impl Hunk<Option<Hash>, Local> {
    fn read(
        updatables: &mut HashMap<usize, crate::InodeUpdate>,
        current: &mut Option<Self>,
        contents_: &mut Vec<u8>,
        changes: &HashMap<usize, Hash>,
        offsets: &mut HashMap<u64, ChangePosition>,
        h: &str,
    ) -> Result<Option<Self>, TextDeError> {
        use self::text_changes_old::*;
        use regex::Regex;
        lazy_static! {
            static ref FILE_ADDITION: Regex =
                Regex::new(r#"^(?P<n>\d+)\. File addition: "(?P<name>[^"]*)" in "(?P<parent>[^"]*)"(?P<perm> \S+)? "(?P<encoding>[^"]*)""#).unwrap();
            static ref EDIT: Regex =
                Regex::new(r#"^([0-9]+)\. Edit in "([^:]+)":(\d+) (\d+\.\d+) "(?P<encoding>[^"]*)""#).unwrap();
            static ref REPLACEMENT: Regex =
                Regex::new(r#"^([0-9]+)\. Replacement in "([^:]+)":(\d+) (\d+\.\d+) "(?P<encoding>[^"]*)""#).unwrap();
            static ref FILE_DELETION: Regex =
                Regex::new(r#"^([0-9]+)\. File deletion: "([^"]*)" (\d+\.\d+) "(?P<encoding>[^"]*)""#).unwrap();
            static ref FILE_UNDELETION: Regex =
                Regex::new(r#"^([0-9]+)\. File un-deletion: "([^"]*)" (\d+\.\d+) "(?P<encoding>[^"]*)""#).unwrap();
            static ref MOVE: Regex =
                Regex::new(r#"^([0-9]+)\. Moved: "(?P<former>[^"]*)" "(?P<new>[^"]*)" (?P<perm>[^ ]+ )?(?P<inode>.*)"#).unwrap();
            static ref MOVE_: Regex = Regex::new(r#"^([0-9]+)\. Moved: "([^"]*)" (.*)"#).unwrap();
            static ref NAME_CONFLICT: Regex = Regex::new(
                r#"^([0-9]+)\. ((Solving)|(Un-solving)) a name conflict in "([^"]*)" (.*): .*"#
            )
            .unwrap();
            static ref ORDER_CONFLICT: Regex = Regex::new(
                r#"^([0-9]+)\. ((Solving)|(Un-solving)) an order conflict in (.*):(\d+) (\d+\.\d+)"#
            )
            .unwrap();
            static ref ZOMBIE: Regex =
                Regex::new(r#"^([0-9]+)\. Resurrecting zombie lines in (?P<path>"[^"]+"):(?P<line>\d+) (?P<inode>\d+\.\d+) "(?P<encoding>[^"]*)""#)
                    .unwrap();
            static ref CONTEXT: Regex = Regex::new(
                r#"up ((\d+\.\d+ )*\d+\.\d+)(, new (\d+):(\d+))?(, down ((\d+\.\d+ )*\d+\.\d+))?"#
            )
            .unwrap();
        }
        if let Some(cap) = FILE_ADDITION.captures(h) {
            if has_newvertices(current) {
                contents_.push(0)
            }
            let mut add_name = default_newvertex();
            add_name.start = ChangePosition(contents_.len().into());
            add_name.flag = EdgeFlags::FOLDER | EdgeFlags::BLOCK;
            let name = unescape(&cap.name("name").unwrap().as_str());
            let path = {
                let parent = cap.name("parent").unwrap().as_str();
                (if parent == "/" {
                    String::new()
                } else {
                    unescape(&parent).to_string() + "/"
                }) + &name
            };
            debug!("cap = {:?}", cap);
            let meta = if let Some(perm) = cap.name("perm") {
                if perm.as_str() == " +dx" {
                    0o1100
                } else if perm.as_str() == " +x" {
                    0o100
                } else {
                    0
                }
            } else {
                0
            };
            let n = cap.name("n").unwrap().as_str().parse().unwrap();
            let encoding = encoding_from_label(cap);
            let meta = FileMetadata {
                metadata: InodeMetadata(meta),
                basename: &name,
                encoding: encoding.clone(),
            };
            meta.write(contents_);
            add_name.end = ChangePosition(contents_.len().into());

            let mut add_inode = default_newvertex();
            add_inode.flag = EdgeFlags::FOLDER | EdgeFlags::BLOCK;
            add_inode.up_context.push(Position {
                change: None,
                pos: ChangePosition(contents_.len().into()),
            });

            contents_.push(0);
            add_inode.start = ChangePosition(contents_.len().into());
            add_inode.end = ChangePosition(contents_.len().into());
            contents_.push(0);
            if let Entry::Occupied(mut e) = updatables.entry(n) {
                if let crate::InodeUpdate::Add { ref mut pos, .. } = e.get_mut() {
                    offsets.insert(pos.0.into(), add_inode.start);

                    *pos = add_inode.start
                }
            }
            Ok(std::mem::replace(
                current,
                Some(Hunk::FileAdd {
                    add_name: Atom::NewVertex(add_name),
                    add_inode: Atom::NewVertex(add_inode),
                    contents: None,
                    path,
                    encoding,
                }),
            ))
        } else if let Some(cap) = EDIT.captures(h) {
            if has_newvertices(current) {
                contents_.push(0)
            }

            let mut v = default_newvertex();
            v.inode = parse_pos(changes, &cap[4])?;
            v.flag = EdgeFlags::BLOCK;
            Ok(std::mem::replace(
                current,
                Some(Hunk::Edit {
                    change: Atom::NewVertex(v),
                    local: Local {
                        path: unescape(&cap[2]).to_string(),
                        line: cap[3].parse().unwrap(),
                    },
                    encoding: encoding_from_label(cap),
                }),
            ))
        } else if let Some(cap) = REPLACEMENT.captures(h) {
            if has_newvertices(current) {
                contents_.push(0)
            }
            let mut v = default_newvertex();
            v.inode = parse_pos(changes, &cap[4])?;
            v.flag = EdgeFlags::BLOCK;
            Ok(std::mem::replace(
                current,
                Some(Hunk::Replacement {
                    change: Atom::NewVertex(v.clone()),
                    replacement: Atom::NewVertex(v),
                    local: Local {
                        path: unescape(&cap[2]).to_string(),
                        line: cap[3].parse().unwrap(),
                    },
                    encoding: encoding_from_label(cap),
                }),
            ))
        } else if let Some(cap) = FILE_DELETION.captures(h) {
            if has_newvertices(current) {
                contents_.push(0)
            }
            let mut del = default_edgemap();
            del.inode = parse_pos(changes, &cap[3])?;
            Ok(std::mem::replace(
                current,
                Some(Hunk::FileDel {
                    del: Atom::EdgeMap(del),
                    contents: None,
                    path: cap[2].to_string(),
                    encoding: encoding_from_label(cap),
                }),
            ))
        } else if let Some(cap) = FILE_UNDELETION.captures(h) {
            if has_newvertices(current) {
                contents_.push(0)
            }
            let mut undel = default_edgemap();
            undel.inode = parse_pos(changes, &cap[3])?;
            Ok(std::mem::replace(
                current,
                Some(Hunk::FileUndel {
                    undel: Atom::EdgeMap(undel),
                    contents: None,
                    path: cap[2].to_string(),
                    encoding: encoding_from_label(cap),
                }),
            ))
        } else if let Some(cap) = NAME_CONFLICT.captures(h) {
            if has_newvertices(current) {
                contents_.push(0)
            }
            let mut name = default_edgemap();
            debug!("cap = {:?}", cap);
            name.inode = parse_pos(changes, &cap[6])?;
            Ok(std::mem::replace(
                current,
                if &cap[2] == "Solving" {
                    Some(Hunk::SolveNameConflict {
                        name: Atom::EdgeMap(name),
                        path: cap[5].to_string(),
                    })
                } else {
                    Some(Hunk::UnsolveNameConflict {
                        name: Atom::EdgeMap(name),
                        path: cap[5].to_string(),
                    })
                },
            ))
        } else if let Some(cap) = MOVE.captures(h) {
            if has_newvertices(current) {
                contents_.push(0)
            }
            let mut add = default_newvertex();
            add.start = ChangePosition(contents_.len().into());
            add.flag = EdgeFlags::FOLDER | EdgeFlags::BLOCK;
            let name = unescape(cap.name("new").unwrap().as_str());
            let meta = if let Some(perm) = cap.name("perm") {
                debug!("perm = {:?}", perm.as_str());
                if perm.as_str() == "+dx " {
                    0o1100
                } else if perm.as_str() == "+x " {
                    0o100
                } else {
                    0
                }
            } else {
                0
            };
            let meta = FileMetadata {
                metadata: InodeMetadata(meta),
                basename: &name,
                encoding: None,
            };
            meta.write(contents_);
            add.end = ChangePosition(contents_.len().into());

            let mut del = default_edgemap();
            del.inode = parse_pos(changes, cap.name("inode").unwrap().as_str())?;
            Ok(std::mem::replace(
                current,
                Some(Hunk::FileMove {
                    del: Atom::EdgeMap(del),
                    add: Atom::NewVertex(add),
                    path: cap[2].to_string(),
                }),
            ))
        } else if let Some(cap) = MOVE_.captures(h) {
            if has_newvertices(current) {
                contents_.push(0)
            }
            let mut add = default_edgemap();
            let mut del = default_edgemap();
            add.inode = parse_pos(changes, &cap[3])?;
            del.inode = add.inode;
            Ok(std::mem::replace(
                current,
                Some(Hunk::FileMove {
                    del: Atom::EdgeMap(del),
                    add: Atom::EdgeMap(add),
                    path: cap[2].to_string(),
                }),
            ))
        } else if let Some(cap) = ORDER_CONFLICT.captures(h) {
            if has_newvertices(current) {
                contents_.push(0)
            }

            Ok(std::mem::replace(
                current,
                Some(if &cap[2] == "Solving" {
                    let mut v = default_newvertex();
                    v.inode = parse_pos(changes, &cap[7])?;
                    Hunk::SolveOrderConflict {
                        change: Atom::NewVertex(v),
                        local: Local {
                            path: cap[5].to_string(),
                            line: cap[6].parse().unwrap(),
                        },
                    }
                } else {
                    let mut v = default_edgemap();
                    v.inode = parse_pos(changes, &cap[7])?;
                    Hunk::UnsolveOrderConflict {
                        change: Atom::EdgeMap(v),
                        local: Local {
                            path: cap[5].to_string(),
                            line: cap[6].parse().unwrap(),
                        },
                    }
                }),
            ))
        } else if let Some(cap) = ZOMBIE.captures(h) {
            if has_newvertices(current) {
                contents_.push(0)
            }
            let mut v = default_edgemap();
            v.inode = parse_pos(changes, &cap.name("inode").unwrap().as_str())?;
            Ok(std::mem::replace(
                current,
                Some(Hunk::ResurrectZombies {
                    change: Atom::EdgeMap(v),
                    local: Local {
                        path: cap.name("path").unwrap().as_str().parse().unwrap(),
                        line: cap.name("line").unwrap().as_str().parse().unwrap(),
                    },
                    encoding: encoding_from_label(cap),
                }),
            ))
        } else {
            match current {
                Some(Hunk::FileAdd {
                    ref mut contents,
                    ref mut add_name,
                    encoding,
                    ..
                }) => {
                    if h.starts_with('+') {
                        if contents.is_none() {
                            let mut v = default_newvertex();
                            // The `-1` here comes from the extra 0
                            // padding bytes pushed onto `contents_`.
                            let inode = Position {
                                change: None,
                                pos: ChangePosition((contents_.len() - 1).into()),
                            };
                            v.up_context.push(inode);
                            v.inode = inode;
                            v.flag = EdgeFlags::BLOCK;
                            v.start = ChangePosition(contents_.len().into());
                            *contents = Some(Atom::NewVertex(v));
                        }
                        if let Some(Atom::NewVertex(ref mut contents)) = contents {
                            if h.starts_with('+') {
                                text_changes_old::parse_line_add(h, contents, contents_, encoding)
                            }
                        }
                    } else if h.starts_with('\\') {
                        if let Some(Atom::NewVertex(mut c)) = contents.take() {
                            if c.end > c.start {
                                if contents_[c.end.us() - 1] == b'\n' {
                                    assert_eq!(c.end.us(), contents_.len());
                                    contents_.pop();
                                    c.end.0 -= 1;
                                }
                                *contents = Some(Atom::NewVertex(c))
                            }
                        }
                    } else if let Some(cap) = CONTEXT.captures(h) {
                        if let Atom::NewVertex(ref mut name) = add_name {
                            name.up_context = parse_pos_vec(changes, offsets, &cap[1])?;
                            if let (Some(new_start), Some(new_end)) = (cap.get(4), cap.get(5)) {
                                offsets.insert(new_start.as_str().parse().unwrap(), name.start);
                                offsets.insert(new_end.as_str().parse().unwrap(), name.end);
                                offsets.insert(
                                    new_end.as_str().parse::<u64>().unwrap() + 1,
                                    name.end + 1,
                                );
                            }
                        }
                    }
                    Ok(None)
                }
                Some(Hunk::FileDel {
                    ref mut del,
                    ref mut contents,
                    ..
                }) => {
                    if let Some(edges) = parse_edges(changes, h)? {
                        if let Atom::EdgeMap(ref mut e) = del {
                            if edges[0].flag.contains(EdgeFlags::FOLDER) {
                                *e = EdgeMap {
                                    inode: e.inode,
                                    edges,
                                }
                            } else {
                                *contents = Some(Atom::EdgeMap(EdgeMap {
                                    inode: e.inode,
                                    edges,
                                }))
                            }
                        }
                    }
                    Ok(None)
                }
                Some(Hunk::FileUndel {
                    ref mut undel,
                    ref mut contents,
                    ..
                }) => {
                    if let Some(edges) = parse_edges(changes, h)? {
                        if let Atom::EdgeMap(ref mut e) = undel {
                            if edges[0].flag.contains(EdgeFlags::FOLDER) {
                                *e = EdgeMap {
                                    inode: e.inode,
                                    edges,
                                }
                            } else {
                                *contents = Some(Atom::EdgeMap(EdgeMap {
                                    inode: e.inode,
                                    edges,
                                }))
                            }
                        }
                    }
                    Ok(None)
                }
                Some(Hunk::FileMove {
                    ref mut del,
                    ref mut add,
                    ..
                }) => {
                    if let Some(edges) = parse_edges(changes, h)? {
                        if edges[0].flag.contains(EdgeFlags::DELETED) {
                            *del = Atom::EdgeMap(EdgeMap {
                                inode: del.inode(),
                                edges,
                            });
                            return Ok(None);
                        } else if let Atom::EdgeMap(ref mut add) = add {
                            if add.edges.is_empty() {
                                *add = EdgeMap {
                                    inode: add.inode,
                                    edges,
                                };
                                return Ok(None);
                            }
                        }
                    } else if let Some(cap) = CONTEXT.captures(h) {
                        if let Atom::NewVertex(ref mut c) = add {
                            debug!("cap = {:?}", cap);
                            c.up_context = parse_pos_vec(changes, offsets, &cap[1])?;
                            if let Some(cap) = cap.get(7) {
                                c.down_context = parse_pos_vec(changes, offsets, cap.as_str())?;
                            }
                        }
                    }
                    Ok(None)
                }
                Some(Hunk::Edit {
                    ref mut change,
                    encoding,
                    ..
                }) => {
                    debug!("edit {:?}", h);
                    if h.starts_with("+ ") {
                        if let Atom::NewVertex(ref mut change) = change {
                            if change.start == change.end {
                                change.start = ChangePosition(contents_.len().into());
                            }
                            text_changes_old::parse_line_add(h, change, contents_, encoding)
                        }
                    } else if h.starts_with('\\') {
                        if let Atom::NewVertex(ref mut change) = change {
                            if change.end > change.start && contents_[change.end.us() - 1] == b'\n'
                            {
                                assert_eq!(change.end.us(), contents_.len());
                                contents_.pop();
                                change.end.0 -= 1;
                            }
                        }
                    } else if let Some(cap) = CONTEXT.captures(h) {
                        if let Atom::NewVertex(ref mut c) = change {
                            debug!("cap = {:?}", cap);
                            c.up_context = parse_pos_vec(changes, offsets, &cap[1])?;
                            if let Some(cap) = cap.get(7) {
                                c.down_context = parse_pos_vec(changes, offsets, cap.as_str())?;
                            }
                        }
                    } else if let Some(edges) = parse_edges(changes, h)? {
                        *change = Atom::EdgeMap(EdgeMap {
                            inode: change.inode(),
                            edges,
                        });
                    }
                    Ok(None)
                }
                Some(Hunk::Replacement {
                    ref mut change,
                    ref mut replacement,
                    encoding,
                    ..
                }) => {
                    if h.starts_with("+ ") {
                        if let Atom::NewVertex(ref mut repl) = replacement {
                            if repl.start == repl.end {
                                repl.start = ChangePosition(contents_.len().into());
                            }
                            text_changes_old::parse_line_add(h, repl, contents_, encoding)
                        }
                    } else if h.starts_with('\\') {
                        if let Atom::NewVertex(ref mut repl) = replacement {
                            if repl.end > repl.start && contents_[repl.end.us() - 1] == b'\n' {
                                assert_eq!(repl.end.us(), contents_.len());
                                contents_.pop();
                                repl.end.0 -= 1;
                            }
                        }
                    } else if let Some(cap) = CONTEXT.captures(h) {
                        debug!("cap = {:?}", cap);
                        if let Atom::NewVertex(ref mut repl) = replacement {
                            repl.up_context = parse_pos_vec(changes, offsets, &cap[1])?;
                            if let Some(cap) = cap.get(7) {
                                repl.down_context = parse_pos_vec(changes, offsets, cap.as_str())?;
                            }
                        }
                    } else if let Some(edges) = parse_edges(changes, h)? {
                        *change = Atom::EdgeMap(EdgeMap {
                            inode: change.inode(),
                            edges,
                        });
                    }
                    Ok(None)
                }
                Some(Hunk::SolveNameConflict { ref mut name, .. })
                | Some(Hunk::UnsolveNameConflict { ref mut name, .. }) => {
                    if let Some(edges) = parse_edges(changes, h)? {
                        *name = Atom::EdgeMap(EdgeMap {
                            edges,
                            inode: name.inode(),
                        })
                    }
                    Ok(None)
                }
                Some(Hunk::SolveOrderConflict { ref mut change, .. }) => {
                    if h.starts_with("+ ") {
                        if let Atom::NewVertex(ref mut change) = change {
                            if change.start == change.end {
                                change.start = ChangePosition(contents_.len().into());
                            }
                            // TODO encoding
                            text_changes_old::parse_line_add(h, change, contents_, &None)
                        }
                    } else if let Some(cap) = CONTEXT.captures(h) {
                        debug!("cap = {:?}", cap);
                        if let Atom::NewVertex(ref mut change) = change {
                            change.up_context = parse_pos_vec(changes, offsets, &cap[1])?;
                            if let Some(cap) = cap.get(7) {
                                change.down_context =
                                    parse_pos_vec(changes, offsets, cap.as_str())?;
                            }
                            if let (Some(new_start), Some(new_end)) = (cap.get(4), cap.get(5)) {
                                let new_start = new_start.as_str().parse::<u64>().unwrap();
                                let new_end = new_end.as_str().parse::<u64>().unwrap();
                                change.start = ChangePosition(contents_.len().into());
                                change.end = ChangePosition(
                                    (contents_.len() as u64 + new_end - new_start).into(),
                                );
                                offsets.insert(new_end, change.end);
                            }
                        }
                    }
                    Ok(None)
                }
                Some(Hunk::UnsolveOrderConflict { ref mut change, .. }) => {
                    if let Some(edges) = parse_edges(changes, h)? {
                        if let Atom::EdgeMap(ref mut change) = change {
                            change.edges = edges
                        }
                    }
                    Ok(None)
                }
                Some(Hunk::ResurrectZombies { ref mut change, .. }) => {
                    if let Some(edges) = parse_edges(changes, h)? {
                        if let Atom::EdgeMap(ref mut change) = change {
                            change.edges = edges
                        }
                    }
                    Ok(None)
                }
                None => {
                    debug!("current = {:#?}", current);
                    debug!("h = {:?}", h);
                    Ok(None)
                }
                _ => {
                    unimplemented!()
                },
            }
        }
    }
}

fn encoding_from_label(cap: Captures) -> Option<Encoding> {
    let encoding_label = cap.name("encoding").unwrap().as_str();
    if encoding_label != BINARY_LABEL {
        Some(Encoding::for_label(encoding_label))
    } else {
        None
    }
}

lazy_static! {
    static ref POS: regex::Regex = regex::Regex::new(r#"(\d+)\.(\d+)"#).unwrap();
    static ref EDGE: regex::Regex =
        regex::Regex::new(r#"\s*(?P<prev>[BFD]*):(?P<flag>[BFD]*)\s+(?P<up_c>\d+)\.(?P<up_l>\d+)\s*->\s*(?P<c>\d+)\.(?P<l0>\d+):(?P<l1>\d+)/(?P<intro>\d+)\s*"#).unwrap();
}

pub fn default_newvertex() -> NewVertex<Option<Hash>> {
    NewVertex {
        start: ChangePosition(L64(0)),
        end: ChangePosition(L64(0)),
        flag: EdgeFlags::empty(),
        up_context: Vec::new(),
        down_context: Vec::new(),
        inode: Position {
            change: Some(Hash::None),
            pos: ChangePosition(L64(0)),
        },
    }
}

pub fn default_edgemap() -> EdgeMap<Option<Hash>> {
    EdgeMap {
        edges: Vec::new(),
        inode: Position {
            change: Some(Hash::None),
            pos: ChangePosition(L64(0)),
        },
    }
}

pub fn has_newvertices<L>(current: &Option<Hunk<Option<Hash>, L>>) -> bool {
    match current {
        Some(Hunk::FileAdd { contents: None, .. }) | None => false,
        Some(rec) => rec.iter().any(|e| matches!(e, Atom::NewVertex(_))),
    }
}

pub fn parse_pos_vec(
    changes: &HashMap<usize, Hash>,
    offsets: &HashMap<u64, ChangePosition>,
    s: &str,
) -> Result<Vec<Position<Option<Hash>>>, TextDeError> {
    let mut v = Vec::new();
    for pos in POS.captures_iter(s) {
        let change: usize = (&pos[1]).parse().unwrap();
        let pos: u64 = (&pos[2]).parse().unwrap();
        let pos = if change == 0 {
            if let Some(&pos) = offsets.get(&pos) {
                pos
            } else {
                debug!("inconsistent change: {:?} {:?}", s, offsets);
                return Err(TextDeError::MissingPosition(pos));
            }
        } else {
            ChangePosition(L64(pos.to_le()))
        };
        v.push(Position {
            change: change_ref(changes, change)?,
            pos,
        })
    }
    Ok(v)
}

fn change_ref(changes: &HashMap<usize, Hash>, change: usize) -> Result<Option<Hash>, TextDeError> {
    debug!("change_ref {:?} {:?}", changes, change);
    if change == 0 {
        Ok(None)
    } else if change == 1 {
        Ok(Some(Hash::None))
    } else if let Some(&c) = changes.get(&change) {
        Ok(Some(c))
    } else {
        Err(TextDeError::MissingChange(change))
    }
}

pub fn parse_pos(
    changes: &HashMap<usize, Hash>,
    s: &str,
) -> Result<Position<Option<Hash>>, TextDeError> {
    let pos = POS.captures(s).unwrap();
    let change: usize = (&pos[1]).parse().unwrap();
    let pos: u64 = (&pos[2]).parse().unwrap();
    Ok(Position {
        change: change_ref(changes, change)?,
        pos: ChangePosition(L64(pos.to_le())),
    })
}

pub fn parse_edges(
    changes: &HashMap<usize, Hash>,
    s: &str,
) -> Result<Option<Vec<NewEdge<Option<Hash>>>>, TextDeError> {
    debug!("parse_edges {:?}", s);
    let mut result = Vec::new();
    for edge in s.split(',') {
        debug!("parse edge {:?}", edge);
        if let Some(cap) = EDGE.captures(edge) {
            let previous = read_flag(cap.name("prev").unwrap().as_str());
            let flag = read_flag(cap.name("flag").unwrap().as_str());
            let change0: usize = cap.name("up_c").unwrap().as_str().parse().unwrap();
            let pos0: u64 = cap.name("up_l").unwrap().as_str().parse().unwrap();
            let change1: usize = cap.name("c").unwrap().as_str().parse().unwrap();
            let start1: u64 = cap.name("l0").unwrap().as_str().parse().unwrap();
            let end1: u64 = cap.name("l1").unwrap().as_str().parse().unwrap();
            let introduced_by: usize = cap.name("intro").unwrap().as_str().parse().unwrap();
            result.push(NewEdge {
                previous,
                flag,
                from: Position {
                    change: change_ref(changes, change0)?,
                    pos: ChangePosition(L64(pos0.to_le())),
                },
                to: Vertex {
                    change: change_ref(changes, change1)?,
                    start: ChangePosition(L64(start1.to_le())),
                    end: ChangePosition(L64(end1.to_le())),
                },
                introduced_by: change_ref(changes, introduced_by)?,
            })
        } else {
            debug!("not parsed");
            return Ok(None);
        }
    }
    Ok(Some(result))
}

pub fn parse_line_add(
    h: &str,
    change: &mut NewVertex<Option<Hash>>,
    contents_: &mut Vec<u8>,
    encoding: &Option<Encoding>,
) {
    let h = match encoding {
        Some(encoding) => encoding.encode(h),
        None => std::borrow::Cow::Borrowed(h.as_bytes()),
    };
    debug!("parse_line_add {:?} {:?}", change.end, change.start);
    debug!("parse_line_add {:?}", h);
    if h.len() > 2 {
        let h = &h[2..h.len()];
        contents_.extend(h);
    } else if h.len() > 1 {
        contents_.push(b'\n');
    }
    debug!("contents_.len() = {:?}", contents_.len());
    trace!("contents_ = {:?}", contents_);
    change.end = ChangePosition(contents_.len().into());
}

pub trait WriteChangeLine: std::io::Write {
    fn write_change_line(&mut self, pref: &str, contents: &str) -> Result<(), std::io::Error> {
        writeln!(self, "{} {}", pref, contents)
    }
    fn write_change_line_binary(
        &mut self,
        pref: &str,
        contents: &[u8],
    ) -> Result<(), std::io::Error> {
        write!(self, "{}b{}", pref, data_encoding::BASE64.encode(contents))
    }
}

impl WriteChangeLine for &mut Vec<u8> {}
impl WriteChangeLine for &mut std::io::Stderr {}
impl WriteChangeLine for &mut std::io::Stdout {}

pub fn print_contents<W: WriteChangeLine>(
    w: &mut W,
    pref: &str,
    contents: &[u8],
    encoding: &Option<Encoding>,
) -> Result<(), std::io::Error> {
    if let Some(encoding) = encoding {
        let dec = encoding.decode(&contents);
        let dec = if dec.ends_with("\n") {
            &dec[..dec.len() - 1]
        } else {
            &dec
        };
        for a in dec.split('\n') {
            w.write_change_line(pref, a)?
        }
    } else {
        writeln!(w, "{}b{}", pref, data_encoding::BASE64.encode(contents))?
    }
    Ok(())
}

pub fn print_change_contents<W: WriteChangeLine, C: ChangeStore>(
    w: &mut W,
    changes: &C,
    change: &Atom<Option<Hash>>,
    change_contents: &[u8],
    encoding: &Option<Encoding>,
) -> Result<(), TextSerError<C::Error>> {
    debug!("print_change_contents {:?}", change);
    match change {
        Atom::NewVertex(ref n) => {
            let c = &change_contents[n.start.us()..n.end.us()];
            print_contents(w, "+", c, encoding)?;
            if !c.ends_with(b"\n") {
                debug!("print_change_contents {:?}", c);
                writeln!(w, "\n\\")?
            }
            Ok(())
        }
        Atom::EdgeMap(ref n) if n.edges.is_empty() => return Err(TextSerError::InvalidChange),
        Atom::EdgeMap(ref n) if n.edges[0].flag.contains(EdgeFlags::DELETED) => {
            let mut buf = Vec::new();
            let mut current = None;
            for e in n.edges.iter() {
                if Some(e.to) == current {
                    continue;
                }
                buf.clear();
                changes
                    .get_contents_ext(e.to, &mut buf)
                    .map_err(TextSerError::C)?;
                print_contents(w, "-", &buf[..], &encoding)?;
                if !buf.ends_with(b"\n") {
                    debug!("print_change_contents {:?}", buf);
                    writeln!(w)?;
                }
                current = Some(e.to)
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub fn write_deleted_names<W: std::io::Write, C: ChangeStore>(
    w: &mut W,
    changes: &C,
    del: &Atom<Option<Hash>>,
) -> Result<(), TextSerError<C::Error>> {
    if let Atom::EdgeMap(ref e) = del {
        let mut buf = Vec::new();
        let mut is_first = true;
        for d in e.edges.iter() {
            buf.clear();
            changes
                .get_contents_ext(d.to, &mut buf)
                .map_err(TextSerError::C)?;
            if !buf.is_empty() {
                let FileMetadata { basename: name, .. } = FileMetadata::read(&buf);
                write!(w, "{}{:?}", if is_first { "" } else { ", " }, name)?;
                is_first = false;
            }
        }
    }
    Ok(())
}

pub fn write_flag<W: std::io::Write>(mut w: W, flag: EdgeFlags) -> Result<(), std::io::Error> {
    if flag.contains(EdgeFlags::BLOCK) {
        w.write_all(b"B")?;
    }
    if flag.contains(EdgeFlags::FOLDER) {
        w.write_all(b"F")?;
    }
    if flag.contains(EdgeFlags::DELETED) {
        w.write_all(b"D")?;
    }
    assert!(!flag.contains(EdgeFlags::PARENT));
    assert!(!flag.contains(EdgeFlags::PSEUDO));
    Ok(())
}

pub fn read_flag(s: &str) -> EdgeFlags {
    let mut f = EdgeFlags::empty();
    for i in s.chars() {
        match i {
            'B' => f |= EdgeFlags::BLOCK,
            'F' => f |= EdgeFlags::FOLDER,
            'D' => f |= EdgeFlags::DELETED,
            c => panic!("read_flag: {:?}", c),
        }
    }
    f
}

pub fn write_pos<W: std::io::Write>(
    mut w: W,
    hashes: &HashMap<Hash, usize>,
    pos: Position<Option<Hash>>,
) -> Result<(), std::io::Error> {
    let change = if let Some(Hash::None) = pos.change {
        1
    } else if let Some(ref c) = pos.change {
        *hashes.get(c).unwrap()
    } else {
        0
    };
    write!(w, "{}.{}", change, pos.pos.0)?;
    Ok(())
}

pub fn write_atom<W: std::io::Write>(
    w: &mut W,
    hashes: &HashMap<Hash, usize>,
    atom: &Atom<Option<Hash>>,
) -> Result<(), std::io::Error> {
    match atom {
        Atom::NewVertex(ref n) => write_newvertex(w, hashes, n),
        Atom::EdgeMap(ref n) => write_edgemap(w, hashes, n),
    }
}

pub fn write_newvertex<W: std::io::Write>(
    mut w: W,
    hashes: &HashMap<Hash, usize>,
    n: &NewVertex<Option<Hash>>,
) -> Result<(), std::io::Error> {
    write!(w, "  up")?;
    for c in n.up_context.iter() {
        write!(w, " ")?;
        write_pos(&mut w, hashes, *c)?
    }
    write!(w, ", new {}:{}", n.start.0, n.end.0)?;
    if !n.down_context.is_empty() {
        write!(w, ", down")?;
        for c in n.down_context.iter() {
            write!(w, " ")?;
            write_pos(&mut w, hashes, *c)?
        }
    }
    w.write_all(b"\n")?;
    Ok(())
}

pub fn write_edgemap<W: std::io::Write>(
    mut w: W,
    hashes: &HashMap<Hash, usize>,
    n: &EdgeMap<Option<Hash>>,
) -> Result<(), std::io::Error> {
    let mut is_first = true;
    for c in n.edges.iter() {
        if !is_first {
            write!(w, ", ")?;
        }
        is_first = false;
        write_flag(&mut w, c.previous)?;
        write!(w, ":")?;
        write_flag(&mut w, c.flag)?;
        write!(w, " ")?;
        write_pos(&mut w, hashes, c.from)?;
        write!(w, " -> ")?;
        write_pos(&mut w, hashes, c.to.start_pos())?;
        let h = if let Some(h) = hashes.get(c.introduced_by.as_ref().unwrap()) {
            h
        } else {
            panic!("introduced_by = {:?}, not found", c.introduced_by);
        };
        write!(w, ":{}/{}", c.to.end.0, h)?;
    }
    writeln!(w)?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Section {
    Header(String),
    Deps,
    Changes {
        changes: Vec<Hunk<Option<Hash>, Local>>,
        current: Option<Hunk<Option<Hash>, Local>>,
        offsets: HashMap<u64, ChangePosition>,
    },
}
