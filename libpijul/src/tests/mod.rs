use crate::change::Change;
use crate::changestore::ChangeStore;
use crate::pristine::*;
use crate::record::{Algorithm, Builder};
use crate::working_copy::WorkingCopy;
use crate::*;
use chrono::*;

mod add_file;
mod change;
mod clone;
mod conflict;
mod diff;
mod file_conflicts;
mod filesystem;
mod missing_context;
mod partial;
mod performance;
mod rm_file;
mod rollback;
mod text;
mod text_changes;
mod unrecord;

fn record_all_change<
    T: MutTxnT + Send + Sync + 'static,
    R: WorkingCopy + Clone + Send + Sync + 'static,
    P: ChangeStore + Clone + Send + 'static,
>(
    repo: &R,
    store: &P,
    txn: &ArcTxn<T>,
    channel: &ChannelRef<T>,
    prefix: &str,
) -> Result<(Hash, Change), anyhow::Error>
where
    R::Error: Send + Sync + 'static,
{
    let mut state = Builder::new();
    state.record(
        txn.clone(),
        Algorithm::default(),
        false,
        &crate::DEFAULT_SEPARATOR,
        channel.clone(),
        repo,
        store,
        prefix,
        1,
    )?;

    let rec = state.finish();
    let changes = rec
        .actions
        .into_iter()
        .map(|rec| rec.globalize(&*txn.read()).unwrap())
        .collect();
    let mut change0 = crate::change::Change::make_change(
        &*txn.read(),
        &channel.clone(),
        changes,
        std::mem::take(&mut *rec.contents.lock()),
        crate::change::ChangeHeader {
            message: "test".to_string(),
            authors: vec![],
            description: None,
            // Beware of changing the following line: two changes
            // doing the same thing will be equal. Sometimes we don't
            // want that, as in tests::unrecord::unrecord_double.
            timestamp: Utc::now(),
        },
        Vec::new(),
    )
    .unwrap();
    let hash = store.save_change(&mut change0, |_, _| Ok::<_, anyhow::Error>(()))?;
    if log_enabled!(log::Level::Debug) {
        change0
            .write(store, Some(hash), true, &mut std::io::stderr())
            .unwrap();
    }
    apply::apply_local_change(
        &mut *txn.write(),
        &channel,
        &change0,
        &hash,
        &rec.updatables,
    )?;
    Ok((hash, change0))
}

fn record_all<T: MutTxnT, R: WorkingCopy, P: ChangeStore>(
    repo: &R,
    store: &P,
    txn: &ArcTxn<T>,
    channel: &ChannelRef<T>,
    prefix: &str,
) -> Result<Hash, anyhow::Error>
where
    T: MutTxnT + Send + Sync + 'static,
    R: WorkingCopy + Clone + Send + Sync + 'static,
    P: ChangeStore + Clone + Send + 'static,
    R::Error: Send + Sync + 'static,
{
    let (hash, _) = record_all_change(repo, store, txn, channel, prefix)?;
    Ok(hash)
}

fn record_all_output<
    T: MutTxnT + Send + Sync + 'static,
    R: WorkingCopy + Clone + Send + Sync + 'static,
    P: ChangeStore + Clone + Send + Sync + 'static,
>(
    repo: &R,
    changes: P,
    txn: &ArcTxn<T>,
    channel: &ChannelRef<T>,
    prefix: &str,
) -> Result<Hash, anyhow::Error>
where
    T: MutTxnT + Send + Sync + 'static,
    R: WorkingCopy + Clone + Send + Sync + 'static,
    P: ChangeStore + Clone + Send + Sync + 'static,
    R::Error: Send + Sync + 'static,
{
    let hash = record_all(repo, &changes, txn, channel, prefix)?;
    output::output_repository_no_pending(repo, &changes, txn, channel, "", true, None, 1, 0)
        .unwrap();
    Ok(hash)
}
