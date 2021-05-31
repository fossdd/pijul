mod init;
pub use init::Init;

mod clone;
pub use clone::Clone;

mod pushpull;
pub use pushpull::*;

mod log;
pub use self::log::Log;

mod record;
pub use record::Record;

mod diff;
pub use diff::Diff;

mod change;
pub use change::Change;

mod protocol;
pub use protocol::Protocol;

#[cfg(feature = "git")]
mod git;
#[cfg(feature = "git")]
pub use git::Git;

mod channel;
pub use channel::*;

mod reset;
pub use reset::*;

mod fork;
pub use fork::*;

mod unrecord;
pub use unrecord::*;

mod file_operations;
pub use file_operations::*;

mod apply;
pub use apply::*;

mod archive;
pub use archive::*;

mod credit;
pub use credit::*;

mod tag;
pub use tag::*;

// #[cfg(debug_assertions)]
mod debug;
// #[cfg(debug_assertions)]
pub use debug::*;

/// Record the pending change (i.e. any unrecorded modifications in
/// the working copy), returning its hash.
fn pending<T: libpijul::MutTxnTExt + libpijul::TxnT + Send + Sync + 'static>(
    txn: std::sync::Arc<std::sync::RwLock<T>>,
    channel: &libpijul::ChannelRef<T>,
    repo: &mut crate::repository::Repository,
) -> Result<Option<libpijul::Hash>, anyhow::Error> {
    use libpijul::changestore::ChangeStore;

    let mut builder = libpijul::record::Builder::new();
    builder.record(
        txn.clone(),
        libpijul::Algorithm::default(),
        channel.clone(),
        repo.working_copy.clone(),
        &repo.changes,
        "",
        num_cpus::get(),
    )?;
    let recorded = builder.finish();
    if recorded.actions.is_empty() {
        return Ok(None);
    }
    let mut txn = txn.write().unwrap();
    let actions = recorded
        .actions
        .into_iter()
        .map(|rec| rec.globalize(&*txn).unwrap())
        .collect();
    let contents = if let Ok(c) = std::sync::Arc::try_unwrap(recorded.contents) {
        c.into_inner().unwrap()
    } else {
        unreachable!()
    };
    let mut pending_change = libpijul::change::Change::make_change(
        &*txn,
        channel,
        actions,
        contents,
        libpijul::change::ChangeHeader::default(),
        Vec::new(),
    )?;
    let (dependencies, extra_known) =
        libpijul::change::dependencies(&*txn, &*channel.read()?, pending_change.changes.iter())?;
    pending_change.dependencies = dependencies;
    pending_change.extra_known = extra_known;
    let hash = repo.changes.save_change(&pending_change).unwrap();
    txn.apply_local_change(channel, &pending_change, &hash, &recorded.updatables)?;
    Ok(Some(hash))
}

#[cfg(unix)]
fn pager() -> bool {
    if let Ok(less) = std::process::Command::new("less")
        .args(&["--version"])
        .output()
    {
        let regex = regex::bytes::Regex::new("less ([0-9]+)").unwrap();
        if let Some(caps) = regex.captures(&less.stdout) {
            if std::str::from_utf8(&caps[1])
                .unwrap()
                .parse::<usize>()
                .unwrap()
                >= 530
            {
                pager::Pager::with_pager("less -RF").setup();
                return true;
            } else {
                pager::Pager::new().setup();
            }
        }
    }
    false
}

#[cfg(not(unix))]
fn pager() -> bool {
    false
}

/// Make a "changelist", i.e. a list of patches that can be edited in
/// a text editor.
fn make_changelist<S: libpijul::changestore::ChangeStore>(
    changes: &S,
    pullable: &[libpijul::Hash],
    verb: &str,
) -> Result<Vec<u8>, anyhow::Error> {
    use libpijul::Base32;
    use std::io::Write;

    let mut v = Vec::new();
    // TODO: This message should probably be customizable
    writeln!(
        v,
        "# Please select the changes to {}. The lines that contain just a
# valid hash, and no other character (except possibly a newline), will
# be {}ed.\n",
        verb, verb,
    )
    .unwrap();
    let mut first_p = true;
    for p in pullable {
        if !first_p {
            writeln!(v, "").unwrap();
        }
        first_p = false;
        writeln!(v, "{}\n", p.to_base32()).unwrap();
        let deps = changes.get_dependencies(&p)?;
        if !deps.is_empty() {
            write!(v, "  Dependencies:").unwrap();
            for d in deps {
                write!(v, " {}", d.to_base32()).unwrap();
            }
            writeln!(v).unwrap();
        }
        let change = changes.get_header(&p)?;
        write!(v, "  Author: [").unwrap();
        let mut first = true;
        for a in change.authors.iter() {
            if !first {
                write!(v, ", ").unwrap();
            }
            first = false;
            write!(v, "{}", a).unwrap();
        }
        writeln!(v, "]").unwrap();
        writeln!(v, "  Date: {}\n", change.timestamp).unwrap();
        for l in change.message.lines() {
            writeln!(v, "    {}", l).unwrap();
        }
        if let Some(desc) = change.description {
            writeln!(v).unwrap();
            for l in desc.lines() {
                writeln!(v, "    {}", l).unwrap();
            }
        }
    }
    Ok(v)
}

/// Parses a list of hashes from a slice of bytes.
/// Everything that is not a line consisting of a
/// valid hash and nothing else will be ignored.
fn parse_changelist(o: &[u8]) -> Vec<libpijul::Hash> {
    use libpijul::Base32;
    if let Ok(o) = std::str::from_utf8(o) {
        o.lines()
            .filter_map(|l| libpijul::Hash::from_base32(l.as_bytes()))
            .collect()
    } else {
        Vec::new()
    }
}
