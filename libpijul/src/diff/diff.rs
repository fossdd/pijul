use super::Line;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Algorithm used to compute the diff.
pub enum Algorithm {
    Myers,
    Patience,
}

impl Default for Algorithm {
    fn default() -> Self {
        Algorithm::Myers
    }
}

pub(super) fn diff(
    lines_a: &[Line],
    lines_b: &[Line],
    algorithm: Algorithm,
    stop_early: bool,
) -> D {
    let mut dd = diffs::Replace::new(D {
        r: Vec::with_capacity(lines_a.len() + lines_b.len()),
        stop_early,
    });
    match algorithm {
        Algorithm::Patience => diffs::patience::diff(
            &mut dd,
            lines_a,
            0,
            lines_a.len(),
            lines_b,
            0,
            lines_b.len(),
        )
        .unwrap_or(()),
        Algorithm::Myers => diffs::myers::diff(
            &mut dd,
            lines_a,
            0,
            lines_a.len(),
            lines_b,
            0,
            lines_b.len(),
        )
        .unwrap_or(()),
    }
    dd.into_inner()
}
#[derive(Debug)]
pub struct D {
    pub r: Vec<Replacement>,
    pub stop_early: bool,
}

impl D {
    pub fn len(&self) -> usize {
        self.r.len()
    }
}

impl std::ops::Index<usize> for D {
    type Output = Replacement;
    fn index(&self, i: usize) -> &Replacement {
        self.r.index(i)
    }
}

impl std::ops::IndexMut<usize> for D {
    fn index_mut(&mut self, i: usize) -> &mut Replacement {
        self.r.index_mut(i)
    }
}

#[derive(Debug)]
pub struct Replacement {
    pub old: usize,
    pub old_len: usize,
    pub new: usize,
    pub new_len: usize,
    pub is_cyclic: bool,
}

impl diffs::Diff for D {
    type Error = ();
    fn delete(&mut self, old: usize, old_len: usize, new: usize) -> std::result::Result<(), ()> {
        debug!("Diff::delete {:?} {:?} {:?}", old, old_len, new);
        self.r.push(Replacement {
            old,
            old_len,
            new,
            new_len: 0,
            is_cyclic: false,
        });
        if self.stop_early {
            Err(())
        } else {
            Ok(())
        }
    }
    fn insert(&mut self, old: usize, new: usize, new_len: usize) -> std::result::Result<(), ()> {
        debug!("Diff::insert {:?} {:?} {:?}", old, new, new_len);
        self.r.push(Replacement {
            old,
            old_len: 0,
            new,
            new_len,
            is_cyclic: false,
        });
        if self.stop_early {
            Err(())
        } else {
            Ok(())
        }
    }
    fn replace(
        &mut self,
        old: usize,
        old_len: usize,
        new: usize,
        new_len: usize,
    ) -> std::result::Result<(), ()> {
        debug!(
            "Diff::replace {:?} {:?} {:?} {:?}",
            old, old_len, new, new_len
        );
        self.r.push(Replacement {
            old,
            old_len,
            new,
            new_len,
            is_cyclic: false,
        });
        if self.stop_early {
            Err(())
        } else {
            Ok(())
        }
    }
}
fn line_index(lines_a: &[Line], pos_bytes: usize) -> usize {
    lines_a
        .binary_search_by(|line| {
            (line.l.as_ptr() as usize - lines_a[0].l.as_ptr() as usize).cmp(&pos_bytes)
        })
        .unwrap()
}
pub struct Deleted {
    pub replaced: bool,
    pub next: usize,
}

impl D {
    pub(super) fn is_deleted(&self, lines_a: &[Line], pos: usize) -> Option<Deleted> {
        let line = line_index(lines_a, pos);
        match self.r.binary_search_by(|repl| repl.old.cmp(&line)) {
            Ok(i) if self.r[i].old_len > 0 => Some(Deleted {
                replaced: self.r[i].new_len > 0,
                next: pos + lines_a[line].l.len(),
            }),
            Err(i) if i == 0 => None,
            Err(i) if line < self.r[i - 1].old + self.r[i - 1].old_len => Some(Deleted {
                replaced: self.r[i - 1].new_len > 0,
                next: pos + lines_a[line].l.len(),
            }),
            _ => None,
        }
    }
}
