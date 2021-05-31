use crate::changestore::ChangeStore;
use crate::pristine::*;
use crate::record::{Algorithm, Builder};
use crate::working_copy::WorkingCopy;
use crate::*;
use chrono::*;

use std::sync::{Arc, RwLock};

mod add_file;
mod change;
mod clone;
mod conflict;
mod file_conflicts;
mod filesystem;
mod missing_context;
mod partial;
mod performance;
mod rm_file;
mod rollback;
mod unrecord;

fn record_all<T: MutTxnT, R: WorkingCopy, P: ChangeStore>(
    repo: &R,
    store: &P,
    txn: &mut T,
    channel: &ChannelRef<T>,
    prefix: &str,
) -> Result<Hash, anyhow::Error>
where
    R::Error: Send + Sync + 'static,
{
    let mut state = Builder::new();
    state.record(
        txn,
        Algorithm::default(),
        &mut *channel.lock().unwrap(),
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
            timestamp: Utc::now(),
        },
        Vec::new(),
    )
    .unwrap();
    let hash = store.save_change(&change0)?;
    if log_enabled!(log::Level::Debug) {
        change0
            .write(
                store,
                Some(hash),
                |l, _p| format!("{}:{}", l.path, l.line),
                true,
                &mut std::io::stderr(),
            )
            .unwrap();
    }
    apply::apply_local_change(txn, channel, &change0, &hash, &rec.updatables)?;
    Ok(hash)
}

fn record_all_output<
    T: MutTxnT + Send + Sync + 'static,
    R: WorkingCopy + Send + Sync + 'static,
    P: ChangeStore + Clone + Send + Sync + 'static,
>(
    repo: Arc<R>,
    changes: Arc<P>,
    txn: Arc<RwLock<T>>,
    channel: &ChannelRef<T>,
    prefix: &str,
) -> Result<Hash, anyhow::Error>
where
    R::Error: Send + Sync + 'static,
    T::Channel: Send + Sync + 'static,
{
    let hash = record_all(
        repo.as_ref(),
        changes.as_ref(),
        &mut *txn.write().unwrap(),
        channel,
        prefix,
    )?;
    output::output_repository_no_pending(repo, changes, txn, channel.clone(), "", true, None, 1)
        .unwrap();
    Ok(hash)
}
