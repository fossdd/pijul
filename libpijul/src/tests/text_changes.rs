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
    let change0 = store.get_change(&h0).unwrap();

    text_test(&store, &change0, h0);

    write!(repo.write_file("file")?, "a\nx\nc\ne\ny\nf\n")?;

    let h1 = record_all(&repo, &store, &txn, &channel, "")?;
    let change1 = store.get_change(&h1).unwrap();
    text_test(&store, &change1, h1);

    repo.remove_path("file2", false)?;
    let h2 = record_all(&repo, &store, &txn, &channel, "")?;
    let change2 = store.get_change(&h2).unwrap();
    text_test(&store, &change2, h2);

    repo.rename("file", "file3")?;
    txn.write().move_file("file", "file3", 0)?;
    let h3 = record_all(&repo, &store, &txn, &channel, "")?;
    let change3 = store.get_change(&h3).unwrap();
    text_test(&store, &change3, h3);

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
    let solution = store.get_change(&h).unwrap();
    text_test(&store, &solution, h);

    Ok(())
}

fn text_test<C: ChangeStore>(c: &C, change0: &Change, h: Hash) {
    let mut v = Vec::new();
    let mut v_old = Vec::new();
    // let channel = channel.borrow();
    change0.write_old(c, Some(h), true, &mut v_old).unwrap();
    change0.write(c, Some(h), true, &mut v).unwrap();

    println!("{}", String::from_utf8_lossy(&v_old));

    for i in std::str::from_utf8(&v).unwrap().lines() {
        debug!("{}", i);
    }
    let change0 =
        Change::read_old(std::io::Cursor::new(&v_old[..]), &mut HashMap::default()).unwrap();
    let change1 = Change::read(std::io::Cursor::new(&v[..]), &mut HashMap::default()).unwrap();
    if change0.header != change1.header {
        error!("header: {:#?} != {:#?}", change0.header, change1.header);
    }
    if change0.dependencies != change1.dependencies {
        error!(
            "deps: {:#?} != {:#?}",
            change0.dependencies, change1.dependencies
        );
    }
    if change0.extra_known != change1.extra_known {
        error!(
            "extra: {:#?} != {:#?}",
            change0.extra_known, change1.extra_known
        );
    }
    if change0.metadata != change1.metadata {
        error!("meta: {:#?} != {:#?}", change0.metadata, change1.metadata);
    }
    if change0.changes != change1.changes {
        if change0.changes.len() != change1.changes.len() {
            trace!("change0.changes = {:#?}", change0.changes);
            trace!("change1.changes = {:#?}", change1.changes);
        } else {
            for (a, b) in change0.changes.iter().zip(change1.changes.iter()) {
                trace!("change0: {:#?}", a);
                trace!("change1: {:#?}", b);
                for (a, b) in a.iter().zip(b.iter()) {
                    if a != b {
                        error!("change0 -> {:#?}", a);
                        error!("change1 -> {:#?}", b);
                    }
                }
            }
        }
    }
    if change0.contents != change1.contents {
        error!("change0.contents = {:?}", change0.contents);
        error!("change1.contents = {:?}", change1.contents);
    }
    assert_eq!(change0, change1);
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

#[test]
#[cfg(feature = "text-changes")]
fn hunk_roundtrip_test() {
    fn go(hunk: PrintableHunk) {
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
