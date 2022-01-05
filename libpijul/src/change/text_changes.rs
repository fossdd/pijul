use crate::HashMap;
use std::collections::hash_map::Entry;
use std::io::BufRead;

use super::*;
use crate::change::parse::*;
use crate::change::printable::*;
use crate::changestore::*;

#[derive(Debug, Error)]
pub enum TextDeError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    TomlDe(#[from] toml::de::Error),
    #[error(transparent)]
    Nom(#[from] nom::Err<nom::error::Error<String>>),
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
    const DEPS_LINE: &'static str = "# Dependencies\n";
    const HUNKS_LINE: &'static str = "# Hunks\n";

    pub fn write_all_deps_old<F: FnMut(Hash) -> Result<(), ChangeError>>(
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

    pub fn write<W: WriteChangeLine, C: ChangeStore>(
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
            w.write_all(Self::DEPS_LINE.as_bytes())?;
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
                    w.write_all(Self::DEPS_LINE.as_bytes())?;
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
            w.write_all(Self::HUNKS_LINE.as_bytes())?;
            for (n, rec) in self.changes.iter().enumerate() {
                write!(w, "\n{}. ", n + 1)?;
                rec.write(changes, &hashes, &self.contents, &mut w)?
            }
        }
        Ok(())
    }
}

impl Change {
    pub fn read_and_deps<
        R: BufRead,
        T: ChannelTxnT + DepsTxnT<DepsError = <T as GraphTxnT>::GraphError>,
    >(
        r: R,
        updatables: &mut HashMap<usize, crate::InodeUpdate>,
        txn: &T,
        channel: &ChannelRef<T>,
    ) -> Result<Self, TextDeError> {
        let (mut change, extra_dependencies) = Self::read_impl(r, updatables)?;
        let (mut deps, extra) =
            dependencies(txn, &channel.read(), change.hashed.changes.iter()).unwrap();
        deps.extend(extra_dependencies.into_iter());
        change.hashed.dependencies = deps;
        change.hashed.extra_known = extra;
        Ok(change)
    }

    pub fn read<R: BufRead>(
        r: R,
        updatables: &mut HashMap<usize, crate::InodeUpdate>,
    ) -> Result<Self, TextDeError> {
        Ok(Self::read_impl(r, updatables)?.0)
    }

    fn read_impl<R: BufRead>(
        mut r: R,
        updatables: &mut HashMap<usize, crate::InodeUpdate>,
    ) -> Result<(Self, HashSet<Hash>), TextDeError> {
        // read the data
        // TODO: make this streaming
        let mut s = String::new();
        r.read_to_string(&mut s)?;
        let i = &s;

        // parse header
        let (i, m_header) = parse_header(i).map_err(|e| e.to_owned())?;
        let header = m_header?;

        // parse dependencies
        let (i, deps) = parse_dependencies(i).map_err(|e| e.to_owned())?;

        // parse hunks
        let (_, hunks) = parse_hunks(i).map_err(|e| e.to_owned())?;

        Change::update(header, deps, hunks, updatables)
    }

    fn update(
        header: ChangeHeader,
        dependencies: Vec<PrintableDep>,
        hunks: Vec<(u64, PrintableHunk)>,
        updatables: &mut HashMap<usize, crate::InodeUpdate>,
    ) -> Result<(Self, HashSet<Hash>), TextDeError> {
        // TODO: get rid of this whole default change if possible
        let mut change = Change {
            offsets: Offsets::default(),
            hashed: Hashed {
                version: VERSION,
                header,
                dependencies: Vec::new(),
                extra_known: Vec::new(),
                metadata: Vec::new(),
                changes: Vec::new(),
                contents_hash: Hasher::default().finish(),
            },
            unhashed: None,
            contents: Vec::new(),
        };

        // process dependencies
        let mut deps = HashMap::default();
        let mut extra_dependencies = HashSet::default();
        for dep in dependencies {
            let hash = Hash::from_base32(dep.hash.as_bytes()).unwrap();
            match dep.type_ {
                DepType::Numbered(n, false) => {
                    change.hashed.dependencies.push(hash);
                    deps.insert(n, hash);
                }
                DepType::Numbered(n, true) => {
                    deps.insert(n, hash);
                }
                DepType::ExtraKnown => {
                    change.hashed.extra_known.push(hash);
                }
                DepType::ExtraUnknown => {
                    extra_dependencies.insert(hash);
                }
            }
        }

        // process hunks
        let mut contents = Vec::new();
        let mut offsets = HashMap::default();
        for (n, hunk) in hunks {
            let res =
                Hunk::from_printable(updatables, &mut contents, &deps, &mut offsets, (n, hunk))?;
            debug!("res = {:?}", res);
            change.hashed.changes.push(res);
        }
        change.contents = contents;
        change.contents_hash = {
            let mut hasher = Hasher::default();
            hasher.update(&change.contents);
            hasher.finish()
        };
        Ok((change, extra_dependencies))
    }
}

pub fn to_printable_new_vertex(
    atom: &Atom<Option<Hash>>,
    hashes: &HashMap<Hash, usize>,
) -> PrintableNewVertex {
    if let PrintableAtom::NewVertex(v) = to_printable_atom(atom, hashes) {
        v
    } else {
        panic!("PrintableAtom::NewVertex expected here")
    }
}

pub fn to_printable_edge_map(
    atom: &Atom<Option<Hash>>,
    hashes: &HashMap<Hash, usize>,
) -> Vec<PrintableEdge> {
    if let PrintableAtom::Edges(v) = to_printable_atom(atom, hashes) {
        v
    } else {
        panic!("PrintableAtom::Edges expected here")
    }
}

/// Panics if the Atom is not an EdgeMap
fn to_printable_atom(atom: &Atom<Option<Hash>>, hashes: &HashMap<Hash, usize>) -> PrintableAtom {
    match atom {
        Atom::NewVertex(ref new_vertex) => PrintableAtom::NewVertex(PrintableNewVertex {
            up_context: new_vertex
                .up_context
                .iter()
                .map(|c| to_printable_pos(hashes, *c))
                .collect(),
            start: new_vertex.start.0 .0,
            end: new_vertex.end.0 .0,
            down_context: new_vertex
                .down_context
                .iter()
                .map(|c| to_printable_pos(hashes, *c))
                .collect(),
        }),
        Atom::EdgeMap(ref edge_map) => PrintableAtom::Edges(
            edge_map
                .edges
                .iter()
                .map(|c| PrintableEdge {
                    previous: PrintableEdgeFlags::from(c.previous),
                    flag: PrintableEdgeFlags::from(c.flag),
                    from: to_printable_pos(hashes, c.from),
                    to_start: to_printable_pos(hashes, c.to.start_pos()),
                    to_end: c.to.end.0 .0,
                    introduced_by: *hashes.get(&c.introduced_by.unwrap()).unwrap_or_else(|| {
                        panic!("introduced_by = {:?}, not found", c.introduced_by)
                    }),
                })
                .collect(),
        ),
    }
}

fn from_printable_edge_map(
    edges: &[PrintableEdge],
    changes: &HashMap<usize, Hash>,
) -> Result<Vec<NewEdge<Option<Hash>>>, TextDeError> {
    let mut res = Vec::new();
    for edge in edges {
        let Position { change, pos } = from_printable_pos(changes, edge.to_start)?;
        res.push(NewEdge {
            previous: edge.previous.to(),
            flag: edge.flag.to(),
            from: from_printable_pos(changes, edge.from)?,
            to: Vertex {
                change,
                start: pos,
                end: ChangePosition(L64(edge.to_end)),
            },
            introduced_by: change_ref(changes, edge.introduced_by)?,
        })
    }
    Ok(res)
}

impl Hunk<Option<Hash>, Local> {
    fn write<W: WriteChangeLine, C: ChangeStore>(
        &self,
        changes: &C,
        hashes: &HashMap<Hash, usize>,
        change_contents: &[u8],
        w: &mut W,
    ) -> Result<(), TextSerError<C::Error>> {
        use self::text_changes::*;
        debug!("write {:?}", self);
        match self {
            Hunk::FileMove { del, add, path } => match add {
                Atom::NewVertex(ref add) => {
                    let FileMetadata {
                        basename: name,
                        metadata,
                        ..
                    } = FileMetadata::read(&change_contents[add.start.0.into()..add.end.0.into()]);
                    PrintableHunk::FileMoveV {
                        path: path.to_string(),
                        name: name.to_string(),
                        perms: PrintablePerms::from_metadata(metadata),
                        pos: to_printable_pos(hashes, del.inode()),
                        up_context: to_printable_pos_vec(hashes, &add.up_context),
                        down_context: to_printable_pos_vec(hashes, &add.down_context),
                        del: to_printable_edge_map(del, hashes),
                    }
                }
                Atom::EdgeMap(_) => PrintableHunk::FileMoveE {
                    path: path.to_string(),
                    pos: to_printable_pos(hashes, del.inode()),
                    add: to_printable_edge_map(add, hashes),
                    del: to_printable_edge_map(del, hashes),
                },
            },
            Hunk::FileDel {
                del,
                contents,
                path,
                encoding,
            } => {
                debug!("file del");
                let (contents_data, content_edges) = if let Some(ref c) = contents {
                    (
                        get_change_contents(changes, c, change_contents)?,
                        to_printable_edge_map(c, hashes),
                    )
                } else {
                    (Vec::new(), Vec::new())
                };

                PrintableHunk::FileDel {
                    path: path.to_string(),
                    pos: to_printable_pos(hashes, del.inode()),
                    encoding: encoding.clone(),
                    del_edges: to_printable_edge_map(del, hashes),
                    content_edges: content_edges,
                    contents: contents_data,
                }
            }
            Hunk::FileUndel {
                undel,
                contents,
                path,
                encoding,
            } => {
                debug!("file undel");
                let (contents_data, content_edges) = if let Some(ref c) = contents {
                    (
                        get_change_contents(changes, c, change_contents)?,
                        to_printable_edge_map(c, hashes),
                    )
                } else {
                    (Vec::new(), Vec::new())
                };

                PrintableHunk::FileUndel {
                    path: path.to_string(),
                    pos: to_printable_pos(hashes, undel.inode()),
                    encoding: encoding.clone(),
                    undel_edges: to_printable_edge_map(undel, hashes),
                    content_edges: content_edges,
                    contents: contents_data,
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
                    let (name, metadata) = if n.start == n.end {
                        ("", InodeMetadata::DIR)
                    } else {
                        let FileMetadata {
                            basename: name,
                            metadata: perms,
                            ..
                        } = FileMetadata::read(&change_contents[n.start.0.into()..n.end.0.into()]);
                        (name, perms)
                    };

                    let contents = if let Some(Atom::NewVertex(ref n)) = contents {
                        change_contents[n.start.us()..n.end.us()].to_vec()
                    } else {
                        Vec::new()
                    };
                    assert!(n.down_context.is_empty());

                    PrintableHunk::FileAddition {
                        name: name.to_string(),
                        parent: crate::path::parent(&path).unwrap_or("").to_string(),
                        perms: PrintablePerms::from_metadata(metadata),
                        encoding: encoding.clone(),
                        up_context: to_printable_pos_vec(hashes, &n.up_context),
                        start: n.start.0 .0,
                        end: n.end.0 .0,
                        contents,
                    }
                } else {
                    panic!("Invalid Hunk::FileAdd field add_name: {:?}", add_name);
                }
            }
            Hunk::Edit {
                change,
                local,
                encoding,
            } => {
                debug!("edit");
                PrintableHunk::Edit {
                    path: local.path.clone(),
                    line: local.line,
                    pos: to_printable_pos(hashes, change.inode()),
                    encoding: encoding.clone(),
                    change: to_printable_atom(change, hashes),
                    contents: get_change_contents(changes, change, change_contents)?,
                }
            }
            Hunk::Replacement {
                change,
                replacement,
                local,
                encoding,
            } => {
                debug!("replacement");
                PrintableHunk::Replace {
                    path: local.path.clone(),
                    line: local.line,
                    pos: to_printable_pos(hashes, change.inode()),
                    encoding: encoding.clone(),
                    change: to_printable_edge_map(change, hashes),
                    replacement: to_printable_new_vertex(replacement, hashes),
                    change_contents: get_change_contents(changes, change, change_contents)?,
                    replacement_contents: get_change_contents(
                        changes,
                        replacement,
                        change_contents,
                    )?,
                }
            }
            Hunk::SolveNameConflict { name, path } => PrintableHunk::SolveNameConflict {
                path: path.clone(),
                pos: to_printable_pos(hashes, name.inode()),
                names: get_deleted_names(changes, name)?,
                edges: to_printable_edge_map(name, hashes),
            },
            Hunk::UnsolveNameConflict { name, path } => PrintableHunk::UnsolveNameConflict {
                path: path.clone(),
                pos: to_printable_pos(hashes, name.inode()),
                names: get_deleted_names(changes, name)?,
                edges: to_printable_edge_map(name, hashes),
            },
            Hunk::SolveOrderConflict { change, local } => {
                // TODO: pass in the encoding
                let contents = get_change_contents(changes, change, change_contents)?;
                let encoding = get_encoding(&contents);
                PrintableHunk::SolveOrderConflict {
                    path: local.path.clone(),
                    line: local.line,
                    pos: to_printable_pos(hashes, change.inode()),
                    encoding: encoding.clone(),
                    change: to_printable_new_vertex(change, hashes),
                    contents: get_change_contents(changes, change, change_contents)?,
                }
            }
            Hunk::UnsolveOrderConflict { change, local } => {
                // TODO: pass in the encoding
                let contents = get_change_contents(changes, change, change_contents)?;
                let encoding = get_encoding(&contents);
                PrintableHunk::UnsolveOrderConflict {
                    path: local.path.clone(),
                    line: local.line,
                    pos: to_printable_pos(hashes, change.inode()),
                    encoding: encoding.clone(),
                    change: to_printable_edge_map(change, hashes),
                    contents: get_change_contents(changes, change, change_contents)?,
                }
            }
            Hunk::ResurrectZombies {
                change,
                local,
                encoding,
            } => PrintableHunk::ResurrectZombies {
                path: local.path.clone(),
                line: local.line,
                pos: to_printable_pos(hashes, change.inode()),
                encoding: encoding.clone(),
                change: to_printable_edge_map(change, hashes),
                contents: get_change_contents(changes, change, change_contents)?,
            },
            Hunk::AddRoot { name, .. } => {
                if let Atom::NewVertex(ref n) = name {
                    PrintableHunk::AddRoot {
                        start: n.start.0 .0,
                    }
                } else {
                    unreachable!()
                }
            }
            Hunk::DelRoot { inode, name } => PrintableHunk::DelRoot {
                name: to_printable_edge_map(name, hashes),
                inode: to_printable_edge_map(inode, hashes),
            },
        }
        .write(w)?;
        Ok(())
    }
}

impl Hunk<Option<Hash>, Local> {
    fn from_printable(
        updatables: &mut HashMap<usize, crate::InodeUpdate>,
        contents_: &mut Vec<u8>,
        changes: &HashMap<usize, Hash>,
        offsets: &mut HashMap<u64, ChangePosition>,
        (hunk_id, hunk): (u64, PrintableHunk),
    ) -> Result<Self, TextDeError> {
        debug!("from_printable {:?}", hunk);
        match hunk {
            PrintableHunk::FileMoveV {
                path,
                name,
                perms,
                pos,
                up_context,
                down_context,
                del,
            } => {
                let mut add = default_newvertex();
                add.start = ChangePosition(contents_.len().into());
                add.flag = EdgeFlags::FOLDER | EdgeFlags::BLOCK;
                let meta = FileMetadata {
                    metadata: InodeMetadata(match perms {
                        // TODO: deduplicate
                        PrintablePerms::IsDir => 0o1100,
                        PrintablePerms::IsExecutable => 0o100,
                        PrintablePerms::IsFile => 0,
                    }),
                    basename: &name,
                    encoding: None,
                };
                meta.write(contents_);
                add.end = ChangePosition(contents_.len().into());
                add.up_context = from_printable_pos_vec_offsets(changes, offsets, &up_context)?;
                add.down_context = from_printable_pos_vec_offsets(changes, offsets, &down_context)?;
                contents_.push(0);

                Ok(Hunk::FileMove {
                    add: Atom::NewVertex(add),
                    del: Atom::EdgeMap(EdgeMap {
                        inode: from_printable_pos(changes, pos)?,
                        edges: from_printable_edge_map(&del, changes)?,
                    }),
                    path,
                })
            }
            PrintableHunk::FileMoveE {
                path,
                pos,
                add,
                del,
            } => {
                let inode = from_printable_pos(changes, pos)?;
                Ok(Hunk::FileMove {
                    add: Atom::EdgeMap(EdgeMap {
                        inode,
                        edges: from_printable_edge_map(&add, changes)?,
                    }),
                    del: Atom::EdgeMap(EdgeMap {
                        inode,
                        edges: from_printable_edge_map(&del, changes)?,
                    }),
                    path,
                })
            }
            PrintableHunk::FileAddition {
                name,
                parent,
                perms,
                encoding,
                up_context,
                start,
                end,
                contents,
            } => {
                let meta = FileMetadata {
                    metadata: InodeMetadata(match perms {
                        PrintablePerms::IsDir => 0o1100,
                        PrintablePerms::IsExecutable => 0o100,
                        PrintablePerms::IsFile => 0,
                    }),
                    basename: &name,
                    encoding: encoding.clone(),
                };

                let mut add_name = {
                    let mut x = default_newvertex();
                    x.start = ChangePosition(contents_.len().into());
                    meta.write(contents_);
                    x.end = ChangePosition(contents_.len().into());
                    x.flag = EdgeFlags::FOLDER | EdgeFlags::BLOCK;
                    x
                };

                let add_inode = {
                    let mut x = default_newvertex();
                    x.flag = EdgeFlags::FOLDER | EdgeFlags::BLOCK;
                    x.up_context.push(Position {
                        change: None,
                        pos: ChangePosition(contents_.len().into()),
                    });

                    contents_.push(0);
                    x.start = ChangePosition(contents_.len().into());
                    x.end = ChangePosition(contents_.len().into());
                    contents_.push(0);
                    x
                };

                if let Entry::Occupied(mut e) = updatables.entry(hunk_id as usize) {
                    if let crate::InodeUpdate::Add { ref mut pos, .. } = e.get_mut() {
                        offsets.insert(pos.0.into(), add_inode.start);
                        *pos = add_inode.start
                    }
                }

                // context
                add_name.up_context =
                    from_printable_pos_vec_offsets(changes, offsets, &up_context)?;
                offsets.insert(start, add_name.start);
                offsets.insert(end, add_name.end);
                offsets.insert(end + 1, add_name.end + 1);

                // contents
                let contents = if contents.len() > 0 {
                    let mut x = default_newvertex();
                    // The `-1` here comes from the extra 0
                    // padding bytes pushed onto `contents_`.
                    // TODO: verify this is correct
                    let inode = Position {
                        change: None,
                        pos: ChangePosition((contents_.len() - 1).into()),
                    };
                    x.up_context.push(inode);
                    x.inode = inode;
                    x.flag = EdgeFlags::BLOCK;
                    x.start = ChangePosition(contents_.len().into());
                    contents_.extend(&contents);
                    x.end = ChangePosition(contents_.len().into());
                    Some(Atom::NewVertex(x))
                } else {
                    None
                };
                contents_.push(0);

                Ok(Hunk::FileAdd {
                    add_name: Atom::NewVertex(add_name),
                    add_inode: Atom::NewVertex(add_inode),
                    contents,
                    path: if parent == "" {
                        name
                    } else {
                        parent + "/" + &name
                    },
                    encoding,
                })
            }
            PrintableHunk::FileDel {
                path,
                pos,
                encoding,
                del_edges,
                content_edges,
                contents: _,
            } => Ok(Hunk::FileDel {
                del: Atom::EdgeMap(EdgeMap {
                    edges: from_printable_edge_map(&del_edges, changes)?,
                    inode: from_printable_pos(changes, pos)?,
                }),
                contents: Some(Atom::EdgeMap(EdgeMap {
                    edges: from_printable_edge_map(&content_edges, changes)?,
                    inode: from_printable_pos(changes, pos)?,
                })),
                path,
                encoding,
            }),
            PrintableHunk::FileUndel {
                path,
                pos,
                encoding,
                undel_edges,
                content_edges,
                contents: _,
            } => Ok(Hunk::FileUndel {
                undel: Atom::EdgeMap(EdgeMap {
                    edges: from_printable_edge_map(&undel_edges, changes)?,
                    inode: from_printable_pos(changes, pos)?,
                }),
                contents: Some(Atom::EdgeMap(EdgeMap {
                    edges: from_printable_edge_map(&content_edges, changes)?,
                    inode: from_printable_pos(changes, pos)?,
                })),
                path,
                encoding,
            }),
            PrintableHunk::Edit {
                path,
                line,
                pos,
                encoding,
                change,
                contents,
            } => {
                let inode = from_printable_pos(changes, pos)?;
                let change = match change {
                    PrintableAtom::NewVertex(new_vertex) => {
                        assert!(!contents.is_empty());
                        let mut x = default_newvertex();
                        x.inode = inode;
                        x.flag = EdgeFlags::BLOCK;
                        x.up_context = from_printable_pos_vec_offsets(
                            changes,
                            offsets,
                            &new_vertex.up_context,
                        )?;
                        x.down_context = from_printable_pos_vec_offsets(
                            changes,
                            offsets,
                            &new_vertex.down_context,
                        )?;
                        x.start = ChangePosition(contents_.len().into());
                        contents_.extend(&contents);
                        x.end = ChangePosition(contents_.len().into());
                        contents_.push(0);
                        Atom::NewVertex(x)
                    }
                    PrintableAtom::Edges(edges) => Atom::EdgeMap(EdgeMap {
                        edges: from_printable_edge_map(&edges, changes)?,
                        inode: inode,
                    }),
                };

                Ok(Hunk::Edit {
                    change,
                    local: Local { path, line },
                    encoding,
                })
            }
            PrintableHunk::Replace {
                path,
                line,
                pos,
                encoding,
                change,
                replacement,
                change_contents: _,
                replacement_contents,
            } => {
                let inode = from_printable_pos(changes, pos)?;

                let replacement = {
                    let mut x = default_newvertex();
                    x.inode = inode;
                    x.flag = EdgeFlags::BLOCK;
                    x.up_context =
                        from_printable_pos_vec_offsets(changes, offsets, &replacement.up_context)?;
                    x.down_context = from_printable_pos_vec_offsets(
                        changes,
                        offsets,
                        &replacement.down_context,
                    )?;
                    x.start = ChangePosition(contents_.len().into());
                    contents_.extend(&replacement_contents);
                    x.end = ChangePosition(contents_.len().into());
                    Atom::NewVertex(x)
                };
                contents_.push(0);

                Ok(Hunk::Replacement {
                    change: Atom::EdgeMap(EdgeMap {
                        edges: from_printable_edge_map(&change, changes)?,
                        inode: inode,
                    }),
                    replacement,
                    local: Local { path, line },
                    encoding,
                })
            }
            PrintableHunk::SolveNameConflict {
                path,
                pos,
                names: _,
                edges,
            } => Ok(Hunk::SolveNameConflict {
                name: Atom::EdgeMap(EdgeMap {
                    inode: from_printable_pos(changes, pos)?,
                    edges: from_printable_edge_map(&edges, changes)?,
                }),
                path,
            }),
            PrintableHunk::UnsolveNameConflict {
                path,
                pos,
                names: _,
                edges,
            } => Ok(Hunk::UnsolveNameConflict {
                name: Atom::EdgeMap(EdgeMap {
                    inode: from_printable_pos(changes, pos)?,
                    edges: from_printable_edge_map(&edges, changes)?,
                }),
                path,
            }),
            PrintableHunk::SolveOrderConflict {
                path,
                line,
                pos,
                encoding: _,
                change,
                contents,
            } => {
                // If `contents.is_empty()`, we still need to add a
                // new empty vertex, so the following is ok:
                let mut c = default_newvertex();
                c.inode = from_printable_pos(changes, pos)?;
                c.up_context =
                    from_printable_pos_vec_offsets(changes, offsets, &change.up_context)?;
                c.down_context =
                    from_printable_pos_vec_offsets(changes, offsets, &change.down_context)?;
                c.start = ChangePosition(contents_.len().into());
                c.end = ChangePosition((contents_.len() as u64 + change.end - change.start).into());
                offsets.insert(change.end, c.end);
                c.start = ChangePosition(contents_.len().into());
                contents_.extend(&contents);
                c.end = ChangePosition(contents_.len().into());
                contents_.push(0);

                Ok(Hunk::SolveOrderConflict {
                    change: Atom::NewVertex(c),
                    local: Local { path, line },
                })
            }
            PrintableHunk::UnsolveOrderConflict {
                path,
                line,
                pos,
                encoding: _,
                change,
                contents: _,
            } => Ok(Hunk::UnsolveOrderConflict {
                change: Atom::EdgeMap(EdgeMap {
                    edges: from_printable_edge_map(&change, changes)?,
                    inode: from_printable_pos(changes, pos)?,
                }),
                local: Local { path, line },
            }),
            PrintableHunk::ResurrectZombies {
                path,
                line,
                pos,
                encoding,
                change,
                contents: _,
            } => Ok(Hunk::ResurrectZombies {
                change: Atom::EdgeMap(EdgeMap {
                    edges: from_printable_edge_map(&change, changes)?,
                    inode: from_printable_pos(changes, pos)?,
                }),
                local: Local { path, line },
                encoding,
            }),
            PrintableHunk::AddRoot { start } => {
                contents_.push(0);
                let root_inode = Position {
                    change: Some(Hash::None),
                    pos: ChangePosition(contents_.len().into()),
                };
                contents_.push(0);
                let inode = contents_.len();
                contents_.push(0);
                if let Entry::Occupied(mut e) = updatables.entry(hunk_id as usize) {
                    if let crate::InodeUpdate::Add { ref mut pos, .. } = e.get_mut() {
                        offsets.insert(pos.0.into(), ChangePosition((start + 1).into()));
                        *pos = ChangePosition((start + 1).into())
                    }
                }
                Ok(Hunk::AddRoot {
                    name: Atom::NewVertex(NewVertex {
                        up_context: vec![root_inode],
                        down_context: Vec::new(),
                        start: ChangePosition(start.into()),
                        end: ChangePosition(start.into()),
                        flag: EdgeFlags::FOLDER | EdgeFlags::BLOCK,
                        inode: root_inode,
                    }),
                    inode: Atom::NewVertex(NewVertex {
                        up_context: vec![Position {
                            change: None,
                            pos: ChangePosition(start.into()),
                        }],
                        down_context: Vec::new(),
                        start: ChangePosition(inode.into()),
                        end: ChangePosition(inode.into()),
                        flag: EdgeFlags::FOLDER | EdgeFlags::BLOCK,
                        inode: root_inode,
                    }),
                })
            }
            PrintableHunk::DelRoot { name, inode } => {
                let root_inode = PrintablePos(1, 0);
                Ok(Hunk::DelRoot {
                    name: Atom::EdgeMap(EdgeMap {
                        edges: from_printable_edge_map(&name, changes)?,
                        inode: from_printable_pos(changes, root_inode)?,
                    }),
                    inode: Atom::EdgeMap(EdgeMap {
                        edges: from_printable_edge_map(&inode, changes)?,
                        inode: from_printable_pos(changes, root_inode)?,
                    }),
                })
            }
        }
    }
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

// TODO: rename
pub fn from_printable_pos_vec_offsets(
    changes: &HashMap<usize, Hash>,
    offsets: &HashMap<u64, ChangePosition>,
    s: &[PrintablePos],
) -> Result<Vec<Position<Option<Hash>>>, TextDeError> {
    let mut v = Vec::new();
    for PrintablePos(change, pos) in s {
        let pos = if *change == 0 {
            if let Some(&pos) = offsets.get(&pos) {
                pos
            } else {
                debug!("inconsistent change: {:?} {:?}", s, offsets);
                return Err(TextDeError::MissingPosition(*pos));
            }
        } else {
            ChangePosition(L64(pos.to_le()))
        };
        v.push(Position {
            change: change_ref(changes, *change)?,
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

pub fn from_printable_pos(
    changes: &HashMap<usize, Hash>,
    pos: PrintablePos,
) -> Result<Position<Option<Hash>>, TextDeError> {
    Ok(Position {
        change: change_ref(changes, pos.0)?,
        pos: ChangePosition(L64(pos.1.to_le())),
    })
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
        writeln!(self, "{}b{}", pref, data_encoding::BASE64.encode(contents))
    }
}

impl WriteChangeLine for &mut Vec<u8> {}
impl WriteChangeLine for &mut std::io::Stderr {}
impl WriteChangeLine for &mut std::io::Stdout {}

pub fn get_change_contents<C: ChangeStore>(
    changes: &C,
    change: &Atom<Option<Hash>>,
    change_contents: &[u8],
) -> Result<Vec<u8>, TextSerError<C::Error>> {
    debug!("get_change_contents {:?}", change);
    match change {
        Atom::NewVertex(ref n) => Ok(change_contents[n.start.us()..n.end.us()].to_vec()),
        Atom::EdgeMap(ref n) if n.edges.is_empty() => Err(TextSerError::InvalidChange),
        Atom::EdgeMap(ref n) if n.edges[0].flag.contains(EdgeFlags::DELETED) => {
            // TODO: get rid of `tmp` and/or `buf`
            let mut buf = Vec::new();
            let mut tmp = Vec::new();
            let mut current = None;
            for e in n.edges.iter() {
                if Some(e.to) == current {
                    continue;
                }
                tmp.clear();
                changes
                    .get_contents_ext(e.to, &mut tmp)
                    .map_err(TextSerError::C)?;
                buf.extend_from_slice(&tmp);
                current = Some(e.to)
            }
            Ok(buf)
        }
        _ => Ok(Vec::new()),
    }
}

pub fn get_deleted_names<C: ChangeStore>(
    changes: &C,
    del: &Atom<Option<Hash>>,
) -> Result<Vec<String>, TextSerError<C::Error>> {
    let mut res = Vec::new();
    let mut h = HashSet::new();
    if let Atom::EdgeMap(ref e) = del {
        let mut tmp = Vec::new();
        for d in e.edges.iter() {
            if !h.insert(d.to) {
                continue;
            }
            tmp.clear();
            changes
                .get_contents_ext(d.to, &mut tmp)
                .map_err(TextSerError::C)?;
            if !tmp.is_empty() {
                let FileMetadata { basename: name, .. } = FileMetadata::read(&tmp);
                res.push(name.to_string());
            }
        }
    }
    Ok(res)
}

pub fn to_printable_pos(
    hashes: &HashMap<Hash, usize>,
    pos: Position<Option<Hash>>,
) -> PrintablePos {
    let change = if let Some(Hash::None) = pos.change {
        1
    } else if let Some(ref c) = pos.change {
        *hashes.get(c).unwrap()
    } else {
        0
    };
    PrintablePos(change, pos.pos.0 .0)
}

pub fn to_printable_pos_vec(
    hashes: &HashMap<Hash, usize>,
    pos: &[Position<Option<Hash>>],
) -> Vec<PrintablePos> {
    pos.iter().map(|c| to_printable_pos(hashes, *c)).collect()
}

impl LocalChange<Hunk<Option<Hash>, Local>, Author> {
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
}
