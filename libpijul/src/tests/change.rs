use crate::change::*;
use crate::changestore::*;
use crate::pristine::*;
use crate::record::*;
use crate::working_copy::*;
use crate::*;
use std::io::Write;

use super::*;

fn hash_mismatch(change: &mut Change) -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());
    use crate::change::*;
    let mut buf = tempfile::NamedTempFile::new()?;
    let mut h = change.serialize(&mut buf, |_, _| Ok::<_, anyhow::Error>(()))?;
    match h {
        crate::pristine::Hash::Blake3(ref mut h) => h[0] = h[0].wrapping_add(1),
        _ => unreachable!(),
    }
    match Change::deserialize(buf.path().to_str().unwrap(), Some(&h)) {
        Err(ChangeError::ChangeHashMismatch { .. }) => {}
        _ => unreachable!(),
    }

    let f = ChangeFile::open(h, buf.path().to_str().unwrap())?;
    assert_eq!(f.hashed(), &change.hashed);
    Ok(())
}

#[test]
fn hash_mism() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";
    let repo = working_copy::memory::Memory::new();
    let store = changestore::memory::Memory::new();
    repo.add_file("file", contents.to_vec());
    repo.add_file("file2", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let txn = env.arc_txn_begin().unwrap();
    let mut channel = txn.write().open_or_create_channel("main")?;
    txn.write().add_file("file", 0)?;
    txn.write().add_file("file2", 0)?;

    let mut state = Builder::new();
    state
        .record(
            txn.clone(),
            Algorithm::Myers,
            false,
            &crate::DEFAULT_SEPARATOR,
            channel.clone(),
            &repo,
            &store,
            "",
            0,
        )
        .unwrap();
    let rec = state.finish();
    let changes: Vec<_> = rec
        .actions
        .into_iter()
        .map(|rec| rec.globalize(&*txn.read()).unwrap())
        .collect();
    info!("changes = {:?}", changes);
    let mut change0 = crate::change::Change::make_change(
        &*txn.read(),
        &channel,
        changes,
        std::mem::take(&mut *rec.contents.lock()),
        crate::change::ChangeHeader {
            message: "test".to_string(),
            authors: vec![],
            description: None,
            timestamp: chrono::Utc::now(),
        },
        Vec::new(),
    )
    .unwrap();
    let hash0 = store.save_change(&mut change0, |_, _| Ok::<_, anyhow::Error>(()))?;
    apply::apply_local_change(
        &mut *txn.write(),
        &mut channel,
        &change0,
        &hash0,
        &rec.updatables,
    )?;

    hash_mismatch(&mut change0)?;

    Ok(())
}

#[cfg(feature = "text-changes")]
#[test]
#[ignore]
fn text() -> Result<(), anyhow::Error> {
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

    write!(repo.write_file("file", Inode::ROOT)?, "a\nx\nc\ne\ny\nf\n")?;

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
    // let channel = channel.borrow();
    change0.write(c, Some(h), true, &mut v).unwrap();
    for i in std::str::from_utf8(&v).unwrap().lines() {
        debug!("{}", i);
    }
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
    assert_eq!(change0, &change1);
}
