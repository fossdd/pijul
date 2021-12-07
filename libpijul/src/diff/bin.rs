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
        debug!("processing {:?}", j);
        let h = ad.hash();
        let mut found = false;
        if let Some(v) = a_h.get(&h) {
            // We've found a match from the old version.
            debug!("matched {:?}", h);
            for &(_, old) in v.iter() {
                found = old == &b[j..i];
                if found {
                    debug!("old matched from {:?}-{:?}", j, i);
                    bb.push(Chunk::Old {
                        start: j,
                        end: i,
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
        }
        if !found {
            debug!("new {:?}", h);
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
        ptr: *const u8,
    },
    New {
        start: usize,
        len: usize,
    },
}
