use crate::change::*;
use crate::changestore::*;
use crate::pristine::*;
use crate::working_copy::*;
use crate::*;
use std::io::Write;

use super::*;

#[cfg(feature = "text-changes")]
#[test]
/// Test the new text_changes.rs against the old text_changes.rs
/// TODO: add test-cases for all kinds of hunks
fn text_changes() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";
    let repo = working_copy::memory::Memory::new();
    let store = changestore::memory::Memory::new();
    repo.add_file("file", contents.to_vec());
    repo.add_file("file2", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let txn = env.arc_txn_begin().unwrap();
    let channel = txn.write().open_or_create_channel("main")?;
    txn.write().add_file("file", 0)?;
    txn.write().add_file("file2", 0)?;
    let h0 = record_all(&repo, &store, &txn, &channel, "")?;

    write!(repo.write_file("file", Inode::ROOT)?, "a\nx\nc\ne\ny\nf\n")?;

    let h1 = record_all(&repo, &store, &txn, &channel, "")?;
    let _change1 = store.get_change(&h1).unwrap();

    repo.remove_path("file2", false)?;
    let h2 = record_all(&repo, &store, &txn, &channel, "")?;
    let _change2 = store.get_change(&h2).unwrap();

    repo.rename("file", "file3")?;
    txn.write().move_file("file", "file3", 0)?;
    let h3 = record_all(&repo, &store, &txn, &channel, "")?;
    let _change3 = store.get_change(&h3).unwrap();

    // name conflicts
    let env2 = pristine::sanakirja::Pristine::new_anon()?;
    let txn2 = env2.arc_txn_begin().unwrap();
    let channel2 = txn2.write().open_or_create_channel("main")?;
    let repo2 = working_copy::memory::Memory::new();
    apply::apply_change(&store, &mut *txn2.write(), &mut *channel2.write(), &h0)?;
    apply::apply_change(&store, &mut *txn2.write(), &mut *channel2.write(), &h1)?;
    apply::apply_change(&store, &mut *txn2.write(), &mut *channel2.write(), &h2)?;
    output::output_repository_no_pending(&repo2, &store, &txn2, &channel2, "", true, None, 1, 0)?;
    repo2.rename("file", "file4")?;
    txn2.write().move_file("file", "file4", 0)?;
    record_all(&repo2, &store, &txn2, &channel2, "")?;

    apply::apply_change(&store, &mut *txn2.write(), &mut *channel2.write(), &h3)?;
    output::output_repository_no_pending(&repo2, &store, &txn2, &channel2, "", true, None, 1, 0)?;
    let h = record_all(&repo2, &store, &txn2, &channel2, "")?;
    let _solution = store.get_change(&h).unwrap();

    Ok(())
}

quickcheck! {
  fn string_roundtrip(s1: String) -> () {
      let mut w = Vec::new();
      write!(&mut w, "{}", Escaped(&s1)).unwrap();
      let utf = std::str::from_utf8(&w).unwrap();
      let (_, s2) = parse_string(&utf).unwrap();
      assert_eq!(s1, s2);
  }
}

use crate::quickcheck::{Arbitrary, Gen};

#[test]
#[cfg(feature = "text-changes")]
fn hunk_roundtrip_test() {
    fn go(mut hunk: PrintableHunk) {
        match hunk {
            PrintableHunk::FileAddition {
                ref perms,
                ref mut contents,
                ..
            } => {
                if let PrintablePerms::IsDir = perms {
                    contents.clear()
                }
            }
            PrintableHunk::FileDel {
                ref mut del_edges, ..
            } => {
                if del_edges.is_empty() {
                    del_edges.push(PrintableEdge::arbitrary(&mut Gen::new(3)))
                }
            }
            PrintableHunk::FileUndel {
                ref mut undel_edges,
                ..
            } => {
                if undel_edges.is_empty() {
                    undel_edges.push(PrintableEdge::arbitrary(&mut Gen::new(3)))
                }
            }
            PrintableHunk::Replace {
                ref mut change_contents,
                ref mut replacement_contents,
                ..
            } => {
                if change_contents.is_empty() {
                    change_contents.push(b'a')
                }
                if replacement_contents.is_empty() {
                    replacement_contents.push(b'b')
                }
            }
            PrintableHunk::Edit {
                ref mut change,
                ref mut contents,
                ..
            } => {
                if let PrintableAtom::Edges(ref mut change) = change {
                    if change.is_empty() {
                        change.push(PrintableEdge::arbitrary(&mut Gen::new(3)))
                    }
                }
                if std::str::from_utf8(contents).is_err() {
                    contents.clear();
                    contents.extend(b"bla\n".iter().cloned())
                }
            }
            _ => {}
        }
        let mut w = Vec::new();
        hunk.write(&mut &mut w).unwrap();
        let s = std::str::from_utf8(&w).unwrap();
        match parse_hunk(&s) {
            Ok((_, hunk2)) => {
                if hunk != hunk2 {
                    eprintln!("## Hunk: \n\n{}", s);
                    eprintln!("In:  {:?}\n\nOut: {:?}\n", hunk, hunk2);
                    panic!("Hunks don't match.");
                }
            }
            Err(e) => {
                eprintln!("Hunk: \n\n{}", s);
                panic!("Can't parse hunk. Error: {:?}", e);
            }
        }
    }
    // Create a new thread with custom stack size
    std::thread::Builder::new()
        // frequently blown by shrinking :(
        // You can disable shrinking by commenting out the shrink function
        // for PrintableHunk
        .stack_size(20 * 1024 * 1024)
        .spawn(move || {
            quickcheck::QuickCheck::new()
                .tests(1000)
                .gen(quickcheck::Gen::new(3))
                .max_tests(100000000)
                .quickcheck(go as fn(PrintableHunk) -> ());
        })
        .unwrap()
        .join()
        .unwrap();
}
