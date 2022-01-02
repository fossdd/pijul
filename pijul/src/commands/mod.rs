use anyhow::bail;

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

mod key;
pub use key::*;

// #[cfg(debug_assertions)]
mod debug;
// #[cfg(debug_assertions)]
pub use debug::*;

/// Record the pending change (i.e. any unrecorded modifications in
/// the working copy), returning its hash.
fn pending<T: libpijul::MutTxnTExt + libpijul::TxnT + Send + Sync + 'static>(
    txn: libpijul::ArcTxn<T>,
    channel: &libpijul::ChannelRef<T>,
    repo: &mut crate::repository::Repository,
) -> Result<Option<libpijul::Hash>, anyhow::Error> {
    use libpijul::changestore::ChangeStore;

    let mut builder = libpijul::record::Builder::new();
    builder.record(
        txn.clone(),
        libpijul::Algorithm::default(),
        false,
        &libpijul::DEFAULT_SEPARATOR,
        channel.clone(),
        &repo.working_copy,
        &repo.changes,
        "",
        num_cpus::get(),
    )?;
    let recorded = builder.finish();
    if recorded.actions.is_empty() {
        return Ok(None);
    }
    let mut txn = txn.write();
    let actions = recorded
        .actions
        .into_iter()
        .map(|rec| rec.globalize(&*txn).unwrap())
        .collect();
    let contents = if let Ok(c) = std::sync::Arc::try_unwrap(recorded.contents) {
        c.into_inner()
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
        libpijul::change::dependencies(&*txn, &*channel.read(), pending_change.changes.iter())?;
    pending_change.dependencies = dependencies;
    pending_change.extra_known = extra_known;
    let hash = repo
        .changes
        .save_change(&mut pending_change, |_, _| Ok::<_, anyhow::Error>(()))
        .unwrap();
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

use crate::remote::CS;

/// Make a "changelist", i.e. a list of patches that can be edited in
/// a text editor.
fn make_changelist<S: libpijul::changestore::ChangeStore>(
    changes: &S,
    pullable: &[CS],
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
        let header = match p {
            CS::Change(p) => {
                writeln!(v, "{}\n", p.to_base32()).unwrap();
                let deps = changes.get_dependencies(&p)?;
                if !deps.is_empty() {
                    write!(v, "  Dependencies:").unwrap();
                    for d in deps {
                        write!(v, " {}", d.to_base32()).unwrap();
                    }
                    writeln!(v).unwrap();
                }
                changes.get_header(&p)?
            }
            CS::State(p) => {
                writeln!(v, "t{}\n", p.to_base32()).unwrap();
                changes.get_tag_header(&p)?
            }
        };
        write!(v, "  Author: [").unwrap();
        let mut first = true;
        for a in header.authors.iter() {
            if !first {
                write!(v, ", ").unwrap();
            }
            first = false;
            if let Some(s) = a.0.get("name") {
                write!(v, "{}", s).unwrap()
            } else if let Some(k) = a.0.get("key") {
                write!(v, "{}", k).unwrap()
            }
        }
        writeln!(v, "]").unwrap();
        writeln!(v, "  Date: {}\n", header.timestamp).unwrap();
        for l in header.message.lines() {
            writeln!(v, "    {}", l).unwrap();
        }
        if let Some(desc) = header.description {
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
fn parse_changelist(o: &[u8]) -> Vec<crate::remote::CS> {
    use libpijul::Base32;
    if let Ok(o) = std::str::from_utf8(o) {
        o.lines()
            .filter_map(|l| {
                ::log::debug!(
                    "l = {:?} {:?}",
                    l,
                    libpijul::Merkle::from_base32(l.as_bytes())
                );
                if l.starts_with("t") {
                    libpijul::Merkle::from_base32(&l.as_bytes()[1..]).map(crate::remote::CS::State)
                } else {
                    libpijul::Hash::from_base32(l.as_bytes()).map(crate::remote::CS::Change)
                }
            })
            .collect()
    } else {
        Vec::new()
    }
}

use serde_derive::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct Identity {
    pub public_key: libpijul::key::PublicKey,
    pub login: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub origin: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default)]
    pub last_modified: u64,
}

fn load_key() -> Result<(libpijul::key::SecretKey, libpijul::key::SKey), anyhow::Error> {
    if let Some(mut dir) = crate::config::global_config_dir() {
        dir.push("secretkey.json");
        if let Ok(key) = std::fs::File::open(&dir) {
            let k: libpijul::key::SecretKey = serde_json::from_reader(key)?;
            let pass = if k.encryption.is_some() {
                Some(rpassword::read_password_from_tty(Some(&format!(
                    "Password for {:?}: ",
                    dir
                )))?)
            } else {
                None
            };
            let sk = k.load(pass.as_deref())?;
            Ok((k, sk))
        } else {
            bail!("Secret key not found, please use `pijul key generate` and try again")
        }
    } else {
        bail!("Secret key not found, please use `pijul key generate` and try again")
    }
}

fn find_hash<B: libpijul::Base32>(
    path: &mut std::path::PathBuf,
    hash: &str,
) -> Result<B, anyhow::Error> {
    if hash.len() < 2 {
        bail!("Ambiguous hash, need at least two characters")
    }
    let (a, b) = hash.split_at(2);
    path.push(a);
    let mut result = None;
    for f in std::fs::read_dir(&path)? {
        let e = f?;
        let p = if let Ok(p) = e.file_name().into_string() {
            p
        } else {
            continue;
        };
        if p.starts_with(b) {
            if result.is_none() {
                result = Some(p)
            } else {
                bail!("Ambiguous hash");
            }
        }
    }
    if let Some(mut r) = result {
        path.push(&r);
        if let Some(i) = r.find('.') {
            r.truncate(i)
        }
        let f = format!("{}{}", a, r);
        if let Some(h) = B::from_base32(f.as_bytes()) {
            return Ok(h);
        }
    }
    bail!("Hash not found")
}

use libpijul::Conflict;
fn print_conflicts(conflicts: &[Conflict]) -> Result<(), std::io::Error> {
    if conflicts.is_empty() {
        return Ok(());
    }
    let mut w = termcolor::StandardStream::stderr(termcolor::ColorChoice::Auto);
    use std::io::Write;
    use termcolor::*;
    w.set_color(ColorSpec::new().set_fg(Some(Color::Red)))?;
    writeln!(w, "\nThere were conflicts:\n")?;
    w.set_color(ColorSpec::new().set_fg(None))?;
    for c in conflicts.iter() {
        match c {
            Conflict::Name { ref path } => writeln!(w, "  - Name conflict on \"{}\"", path)?,
            Conflict::ZombieFile { ref path } => {
                writeln!(w, "  - Path deletion conflict \"{}\"", path)?
            }
            Conflict::MultipleNames { ref path, .. } => {
                writeln!(w, "  - File has multiple names: \"{}\"", path)?
            }
            Conflict::Zombie { ref path, ref line } => writeln!(
                w,
                "  - Deletion conflict in \"{}\" starting on line {}",
                path, line
            )?,
            Conflict::Cyclic { ref path, ref line } => writeln!(
                w,
                "  - Cycle conflict in \"{}\" starting on line {}",
                path, line
            )?,
            Conflict::Order { ref path, ref line } => writeln!(
                w,
                "  - Order conflict in \"{}\" starting on line {}",
                path, line
            )?,
        }
    }
    Ok(())
}
