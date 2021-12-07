use super::*;
use crate::pristine::InodeMetadata;
use crate::HashMap;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct Memory(Arc<Mutex<Memory_>>);

#[derive(Debug)]
struct Memory_ {
    files: FileTree,
    last_modified: SystemTime,
}

#[derive(Debug, Default)]
struct FileTree {
    children: HashMap<String, Inode>,
}
#[derive(Debug)]
enum Inode {
    File {
        meta: InodeMetadata,
        last_modified: SystemTime,
        contents: Arc<Mutex<Vec<u8>>>,
    },
    Directory {
        meta: InodeMetadata,
        last_modified: SystemTime,
        children: FileTree,
    },
}

impl Default for Memory {
    fn default() -> Self {
        Memory(Arc::new(Mutex::new(Memory_ {
            files: FileTree::default(),
            last_modified: SystemTime::now(),
        })))
    }
}

impl Memory {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn list_files(&self) -> Vec<String> {
        let m = self.0.lock();
        let mut result = Vec::new();
        let mut current_files = vec![(String::new(), &m.files)];
        let mut next_files = Vec::new();
        loop {
            if current_files.is_empty() {
                break;
            }
            for (path, tree) in current_files.iter() {
                for (name, inode) in tree.children.iter() {
                    let mut path = path.clone();
                    crate::path::push(&mut path, name);
                    match inode {
                        Inode::File { .. } => {
                            result.push(path);
                        }
                        Inode::Directory { ref children, .. } => {
                            result.push(path.clone());
                            next_files.push((path, children))
                        }
                    }
                }
            }
            std::mem::swap(&mut current_files, &mut next_files);
            next_files.clear();
        }
        result
    }

    pub fn add_file(&self, file: &str, file_contents: Vec<u8>) {
        let file_meta = InodeMetadata::new(0, false);
        let last = SystemTime::now();
        self.add_inode(
            file,
            Inode::File {
                meta: file_meta,
                last_modified: last,
                contents: Arc::new(Mutex::new(file_contents)),
            },
        )
    }

    pub fn add_dir(&self, file: &str) {
        let file_meta = InodeMetadata::new(0o100, true);
        let last = SystemTime::now();
        self.add_inode(
            file,
            Inode::Directory {
                meta: file_meta,
                last_modified: last,
                children: FileTree {
                    children: HashMap::default(),
                },
            },
        )
    }

    fn add_inode(&self, file: &str, inode: Inode) {
        let mut m = self.0.lock();
        let last = SystemTime::now();
        m.last_modified = last;
        let mut file_tree = &mut m.files;
        let file = file.split('/').filter(|c| !c.is_empty());
        let mut p = file.peekable();
        while let Some(f) = p.next() {
            if p.peek().is_some() {
                let entry = file_tree
                    .children
                    .entry(f.to_string())
                    .or_insert(Inode::Directory {
                        meta: InodeMetadata::new(0o100, true),
                        children: FileTree {
                            children: HashMap::default(),
                        },
                        last_modified: last,
                    });
                match *entry {
                    Inode::Directory {
                        ref mut children, ..
                    } => file_tree = children,
                    _ => panic!("Not a directory"),
                }
            } else {
                file_tree.children.insert(f.to_string(), inode);
                break;
            }
        }
    }
}

impl Memory_ {
    fn get_file(&self, file: &str) -> Option<&Inode> {
        debug!("get_file {:?}", file);
        debug!("repo = {:?}", self);
        let mut t = Some(&self.files);
        let mut inode = None;
        let it = file.split('/').filter(|c| !c.is_empty());
        for c in it {
            debug!("c = {:?}", c);
            inode = t.take().unwrap().children.get(c);
            debug!("inode = {:?}", inode);
            match inode {
                Some(Inode::Directory { ref children, .. }) => t = Some(children),
                _ => break,
            }
        }
        inode
    }

    fn get_file_mut<'a>(&'a mut self, file: &str) -> Option<&'a mut Inode> {
        debug!("get_file_mut {:?}", file);
        debug!("repo = {:?}", self);
        let mut t = Some(&mut self.files);
        let mut it = file.split('/').filter(|c| !c.is_empty()).peekable();
        self.last_modified = SystemTime::now();
        while let Some(c) = it.next() {
            debug!("c = {:?}", c);
            let inode_ = t.take().unwrap().children.get_mut(c);
            debug!("inode = {:?}", inode_);
            if it.peek().is_none() {
                return inode_;
            }
            match inode_ {
                Some(Inode::Directory {
                    ref mut children, ..
                }) => t = Some(children),
                _ => return None,
            }
        }
        None
    }

    fn remove_path_(&mut self, path: &str) -> Option<Inode> {
        debug!("remove_path {:?}", path);
        debug!("repo = {:?}", self);
        self.last_modified = SystemTime::now();
        let mut t = Some(&mut self.files);
        let mut it = path.split('/').filter(|c| !c.is_empty());
        let mut c = it.next().unwrap();
        loop {
            debug!("c = {:?}", c);
            let next_c = it.next();
            let t_ = t.take().unwrap();
            let next_c = if let Some(next_c) = next_c {
                next_c
            } else {
                return t_.children.remove(c);
            };
            let inode = t_.children.get_mut(c);
            c = next_c;
            debug!("inode = {:?}", inode);
            match inode {
                Some(Inode::Directory {
                    ref mut children, ..
                }) => t = Some(children),
                _ => return None,
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Path not found: {path}")]
    NotFound { path: String },
}

impl WorkingCopyRead for Memory {
    type Error = Error;
    fn file_metadata(&self, file: &str) -> Result<InodeMetadata, Self::Error> {
        let m = self.0.lock();
        match m.get_file(file) {
            Some(Inode::Directory { meta, .. }) => Ok(*meta),
            Some(Inode::File { meta, .. }) => Ok(*meta),
            None => Err(Error::NotFound {
                path: file.to_string(),
            }),
        }
    }
    fn read_file(&self, file: &str, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        let m = self.0.lock();
        match m.get_file(file) {
            Some(Inode::Directory { .. }) => panic!("Not a file: {:?}", file),
            Some(Inode::File { ref contents, .. }) => {
                buffer.extend(&contents.lock()[..]);
                Ok(())
            }
            None => Err(Error::NotFound {
                path: file.to_string(),
            }),
        }
    }
    fn modified_time(&self, file: &str) -> Result<std::time::SystemTime, Self::Error> {
        let m = self.0.lock();
        match m.get_file(file) {
            Some(Inode::Directory { last_modified, .. })
            | Some(Inode::File { last_modified, .. }) => Ok(*last_modified),
            _ => Ok(m.last_modified),
        }
    }
}

impl WorkingCopy for Memory {
    fn create_dir_all(&self, file: &str) -> Result<(), Self::Error> {
        let not_already_exists = {
            let m = self.0.lock();
            m.get_file(file).is_none()
        };
        if not_already_exists {
            let last = SystemTime::now();
            self.add_inode(
                file,
                Inode::Directory {
                    meta: InodeMetadata::new(0o100, true),
                    children: FileTree {
                        children: HashMap::default(),
                    },
                    last_modified: last,
                },
            );
        }
        Ok(())
    }

    fn remove_path(&self, path: &str, _rec: bool) -> Result<(), Self::Error> {
        self.0.lock().remove_path_(path);
        Ok(())
    }

    fn rename(&self, old: &str, new: &str) -> Result<(), Self::Error> {
        debug!("rename {:?} to {:?}", old, new);
        let inode = {
            let mut m = self.0.lock();
            m.remove_path_(old)
        };
        if let Some(inode) = inode {
            self.add_inode(new, inode)
        }
        Ok(())
    }
    fn set_permissions(&self, file: &str, permissions: u16) -> Result<(), Self::Error> {
        debug!("set_permissions {:?}", file);
        let mut m = self.0.lock();
        match m.get_file_mut(file) {
            Some(Inode::File { ref mut meta, .. }) => {
                *meta = InodeMetadata::new(permissions as usize & 0o100, false);
            }
            Some(Inode::Directory { ref mut meta, .. }) => {
                *meta = InodeMetadata::new(permissions as usize & 0o100, true);
            }
            None => panic!("file not found: {:?}", file),
        }
        Ok(())
    }

    type Writer = Writer;
    fn write_file(&self, file: &str, _: crate::Inode) -> Result<Self::Writer, Self::Error> {
        let mut m = self.0.lock();
        if let Some(f) = m.get_file_mut(file) {
            if let Inode::File {
                ref mut contents, ..
            } = f
            {
                contents.lock().clear();
                return Ok(Writer {
                    w: contents.clone(),
                });
            } else {
                unreachable!()
            }
        }
        std::mem::drop(m);
        let contents = Arc::new(Mutex::new(Vec::new()));
        let last_modified = SystemTime::now();
        self.add_inode(
            file,
            Inode::File {
                meta: InodeMetadata::new(0, false),
                contents: contents.clone(),
                last_modified,
            },
        );
        Ok(Writer { w: contents })
    }
}

pub struct Writer {
    w: Arc<Mutex<Vec<u8>>>,
}

impl std::io::Write for Writer {
    fn write(&mut self, b: &[u8]) -> Result<usize, std::io::Error> {
        self.w.lock().write(b)
    }
    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}
