use crate::change::*;
use crate::changestore::*;
use crate::pristine::*;
use crate::record::*;
use crate::working_copy::*;
use crate::*;

fn hash_mismatch(change: &Change) -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());
    use crate::change::*;
    let mut buf = tempfile::NamedTempFile::new()?;
    let mut h = change.serialize(&mut buf)?;
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
    let mut repo = working_copy::memory::Memory::new();
    let store = changestore::memory::Memory::new();
    repo.add_file("file", contents.to_vec());
    repo.add_file("file2", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel = txn.open_or_create_channel("main")?;
    txn.add_file("file")?;
    txn.add_file("file2")?;

    let mut state = Builder::new();
    state
        .record(
            &mut txn,
            Algorithm::Myers,
            &mut channel.borrow_mut(),
            &mut repo,
            &store,
            "",
        )
        .unwrap();
    let rec = state.finish();
    let changes: Vec<_> = rec
        .actions
        .into_iter()
        .map(|rec| rec.globalize(&txn).unwrap())
        .collect();
    info!("changes = {:?}", changes);
    let change0 = crate::change::Change::make_change(
        &txn,
        &channel,
        changes,
        rec.contents,
        crate::change::ChangeHeader {
            message: "test".to_string(),
            authors: vec![],
            description: None,
            timestamp: chrono::Utc::now(),
        },
        Vec::new(),
    )
    .unwrap();
    let hash0 = store.save_change(&change0)?;
    apply::apply_local_change(&mut txn, &mut channel, &change0, &hash0, &rec.updatables)?;

    hash_mismatch(&change0)?;

    debug_to_file(&txn, &channel.borrow(), "debug")?;

    Ok(())
}

fn record_all<T: MutTxnT, R: WorkingCopy, P: ChangeStore>(
    repo: &mut R,
    store: &P,
    txn: &mut T,
    channel: &mut ChannelRef<T>,
    prefix: &str,
) -> Result<(Hash, Change), anyhow::Error>
where
    R::Error: Send + Sync + 'static,
{
    let mut state = Builder::new();
    state.record(
        txn,
        Algorithm::default(),
        &mut channel.borrow_mut(),
        repo,
        store,
        prefix,
    )?;

    let rec = state.finish();
    let changes = rec
        .actions
        .into_iter()
        .map(|rec| rec.globalize(txn).unwrap())
        .collect();
    let change0 = crate::change::Change::make_change(
        txn,
        &channel,
        changes,
        rec.contents,
        crate::change::ChangeHeader {
            message: "test".to_string(),
            authors: vec![],
            description: None,
            // Beware of changing the following line: two changes
            // doing the same thing will be equal. Sometimes we don't
            // want that, as in tests::unrecord::unrecord_double.
            timestamp: chrono::Utc::now(),
        },
        Vec::new(),
    )
    .unwrap();
    let hash = store.save_change(&change0)?;
    apply::apply_local_change(txn, channel, &change0, &hash, &rec.updatables)?;
    Ok((hash, change0))
}

#[cfg(feature = "text-changes")]
#[test]
fn text() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";
    let mut repo = working_copy::memory::Memory::new();
    let store = changestore::memory::Memory::new();
    repo.add_file("file", contents.to_vec());
    repo.add_file("file2", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel = txn.open_or_create_channel("main")?;
    txn.add_file("file")?;
    txn.add_file("file2")?;
    let (h0, change0) = record_all(&mut repo, &store, &mut txn, &mut channel, "")?;
    text_test(&store, &change0, h0);

    repo.write_file::<_, std::io::Error, _>("file", |w| {
        write!(w, "a\nx\nc\ne\ny\nf\n")?;
        Ok(())
    })?;
    let (h1, change1) = record_all(&mut repo, &store, &mut txn, &mut channel, "")?;
    text_test(&store, &change1, h1);

    repo.remove_path("file2")?;
    let (h2, change2) = record_all(&mut repo, &store, &mut txn, &mut channel, "")?;
    text_test(&store, &change2, h2);

    repo.rename("file", "file3")?;
    txn.move_file("file", "file3")?;
    let (h3, change3) = record_all(&mut repo, &store, &mut txn, &mut channel, "")?;
    text_test(&store, &change3, h3);

    // name conflicts
    let env2 = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn2 = env2.mut_txn_begin().unwrap();
    let mut channel2 = txn2.open_or_create_channel("main")?;
    let mut repo2 = working_copy::memory::Memory::new();

    apply::apply_change(&store, &mut txn2, &mut channel2, &h0)?;
    apply::apply_change(&store, &mut txn2, &mut channel2, &h1)?;
    apply::apply_change(&store, &mut txn2, &mut channel2, &h2)?;
    output::output_repository_no_pending(
        &mut repo2,
        &store,
        &mut txn2,
        &mut channel2,
        "",
        true,
        None,
    )?;
    repo2.rename("file", "file4")?;
    txn2.move_file("file", "file4")?;
    let (_, _) = record_all(&mut repo2, &store, &mut txn2, &mut channel2, "")?;

    apply::apply_change(&store, &mut txn2, &mut channel2, &h3)?;
    output::output_repository_no_pending(
        &mut repo2,
        &store,
        &mut txn2,
        &mut channel2,
        "",
        true,
        None,
    )?;
    let (h, solution) = record_all(&mut repo2, &store, &mut txn2, &mut channel2, "")?;

    text_test(&store, &solution, h);

    Ok(())
}

fn text_test<C: ChangeStore>(c: &C, change0: &Change, h: Hash) {
    let mut v = Vec::new();
    // let channel = channel.borrow();
    change0
        .write(
            c,
            Some(h),
            |l, _p| format!("{}:{}", l.path, l.line),
            true,
            &mut v,
        )
        .unwrap();
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
