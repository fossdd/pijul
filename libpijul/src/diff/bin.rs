use adler32::*;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

pub(super) fn make_old_chunks(
    window: usize,
    a: &[u8],
) -> (HashMap<u32, Vec<(usize, &[u8])>>, Vec<super::Line>) {
    let mut a_ad = 0;
    let mut a_h = HashMap::with_capacity(a.len() / window + 1);
    let mut lines = Vec::new();
    'outer: for ch in a.chunks(window) {
        debug!("chunk {:?}", ch.len());
        lines.push(super::Line {
            l: ch,
            ptr: ch.as_ptr(),
            ..super::Line::default()
        });
        let ad = adler32(ch).unwrap();
        match a_h.entry(ad) {
            Entry::Vacant(e) => {
                e.insert(vec![(a_ad, ch)]);
            }
            Entry::Occupied(mut e) => {
                let e = e.get_mut();
                for (_, old) in e.iter() {
                    if *old == ch {
                        continue 'outer;
                    }
                }
                e.push((a_ad, ch));
            }
        }
        a_ad += 1
    }
    if let Some(l) = lines.last_mut() {
        l.last = true
    }
    (a_h, lines)
}

pub(super) fn make_new_chunks<'a>(
    window: usize,
    a_h: &HashMap<u32, Vec<(usize, &[u8])>>,
    b: &'a [u8],
) -> (Vec<Chunk>, Vec<super::Line<'a>>) {
    let mut ad = RollingAdler32::from_buffer(&b[..window.min(b.len())]);

    let mut bb = Vec::new();
    let mut i = window.min(b.len());
    let mut j = 0;
    let mut lines = Vec::new();
    while j < b.len() {
        let h = ad.hash();
        if let Some(v) = a_h.get(&h) {
            // We've found a match from the old version.
            for &(v, old) in v.iter() {
                if old == &b[j..i] {
                    bb.push(Chunk::Old {
                        start: j,
                        end: i,
                        old_pos: v,
                        ptr: old.as_ptr(),
                    });
                    for _ in 0..window {
                        if j < b.len() {
                            ad.remove(i - j, b[j]);
                            j += 1;
                        } else {
                            break;
                        }
                        if i < b.len() {
                            ad.update(b[i]);
                            i += 1;
                        }
                    }
                    break;
                }
            }
        } else {
            if let Some(Chunk::New { ref mut len, .. }) = bb.last_mut() {
                *len += 1
            } else {
                bb.push(Chunk::New { start: j, len: 1 })
            }
            ad.remove(i - j, b[j]);
            j += 1;
            if i < b.len() {
                ad.update(b[i]);
                i += 1;
            }
        }
    }
    for chunk in bb.iter() {
        match *chunk {
            Chunk::Old {
                start, end, ptr, ..
            } => lines.push(super::Line {
                l: &b[start..end],
                ptr,
                ..super::Line::default()
            }),
            Chunk::New { start, len } => lines.push(super::Line {
                l: &b[start..start + len],
                ..super::Line::default()
            }),
        }
    }
    if let Some(l) = lines.last_mut() {
        l.last = true
    }
    (bb, lines)
}

#[derive(Debug)]
pub(super) enum Chunk {
    Old {
        start: usize,
        end: usize,
        old_pos: usize,
        ptr: *const u8,
    },
    New {
        start: usize,
        len: usize,
    },
}

/*
pub fn diff<D: diffs::Diff>(window: usize, a: &[u8], b: &[u8], d: D)
where
    D::Error: std::fmt::Debug,
{
    let a_h = make_old_chunks(window, a);
    let bb = make_new_chunks(window, &a_h, b);
    // Make a dummy vector (because `std::ops::Index` wants a borrow).
    let mut aa = Vec::with_capacity(a.len() / window + 1);
    for pos in 0..(a.len() + window - 1) / window {
        aa.push(pos)
    }
    diffs::myers::diff(
        &mut W {
            d,
            window,
            old_len: a.len(),
            a: &aa,
            b: &bb,
        },
        &aa,
        0,
        aa.len(),
        &bb,
        0,
        bb.len(),
    )
    .unwrap();
}


impl Chunk {
    fn start(&self) -> usize {
        match *self {
            Chunk::Old { start, .. } => start,
            Chunk::New { start, .. } => start,
        }
    }
}

impl PartialEq<usize> for Chunk {
    fn eq(&self, b: &usize) -> bool {
        if let Chunk::Old { old_pos, .. } = *self {
            old_pos == *b
        } else {
            false
        }
    }
}

#[derive(Debug)]
struct W<'a, D> {
    d: D,
    window: usize,
    old_len: usize,
    a: &'a [usize],
    b: &'a [Chunk],
}

impl<'a, D: diffs::Diff> diffs::Diff for W<'a, D>
where
    D::Error: std::fmt::Debug,
{
    type Error = D::Error;
    fn equal(&mut self, old: usize, new: usize, len: usize) -> Result<(), Self::Error> {
        let old = old * self.window;
        let new = self.b[new].start();
        let len = (len * self.window).min(self.old_len - old);
        self.d.equal(old, new, len)
    }
    fn delete(&mut self, old: usize, len: usize, new: usize) -> Result<(), Self::Error> {
        let old = old * self.window;
        let new = self.b[new].start();
        let len = (len * self.window).min(self.old_len - old);
        self.d.delete(old, len, new)
    }
    fn insert(&mut self, old: usize, new: usize, new_len: usize) -> Result<(), Self::Error> {
        let old = old * self.window;
        let new = self.b[new].start();
        let mut new_len_ = 0;
        for b in &self.b[new .. new + new_len] {
            match b {
                Chunk::Old { start, .. } => {
                    new_len_ += self.window.min(self.old_len - start)
                }
                Chunk::New { len, .. } => {
                    new_len_ += len
                }
            }
        }
        self.d.insert(old, new, new_len_)
    }
    fn replace(
        &mut self,
        old: usize,
        old_len: usize,
        new: usize,
        new_len: usize,
    ) -> Result<(), Self::Error> {
        let old = old * self.window;
        let old_len = (old_len * self.window).min(self.old_len - old);
        let new = self.b[new].start();
        let mut new_len_ = 0;
        for b in &self.b[new .. new + new_len] {
            match b {
                Chunk::Old { start, .. } => {
                    new_len_ += self.window.min(self.old_len - start)
                }
                Chunk::New { len, .. } => {
                    new_len_ += len
                }
            }
        }
        self.d.replace(old, old_len, new, new_len_)
    }
    fn finish(&mut self) -> Result<(), Self::Error> {
        self.d.finish()
    }
}
 */
