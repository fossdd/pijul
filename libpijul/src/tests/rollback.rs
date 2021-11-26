use super::*;
use crate::working_copy::{WorkingCopy, WorkingCopyRead};
use std::io::Write;

#[test]
fn rollback_conflict_resolution_simple() {
    env_logger::try_init().unwrap_or(());

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon().unwrap();

    let mut txn = env.arc_txn_begin().unwrap();

    let mut channela = txn.write().open_or_create_channel("main").unwrap();

    // Create a simple conflict between axb and ayb
    repo.add_file("file", b"a\nb\n".to_vec());
    txn.write().add_file("file", 0).unwrap();
    record_all(&repo, &changes, &txn, &channela, "").unwrap();

    let channelb = txn.write().fork(&channela, "other").unwrap();

    repo.write_file("file")
        .unwrap()
        .write_all(b"a\nx\nb\n")
        .unwrap();
    let ha = record_all(&repo, &changes, &txn, &channela, "").unwrap();

    repo.write_file("file")
        .unwrap()
        .write_all(b"a\ny\nb\n")
        .unwrap();
    let hb = record_all(&repo, &changes, &txn, &channelb, "").unwrap();

    apply::apply_change_arc(&changes, &txn, &channelb, &ha).unwrap();
    apply::apply_change_arc(&changes, &txn, &channela, &hb).unwrap();

    output::output_repository_no_pending(&repo, &changes, &txn, &channela, "", true, None, 1, 0)
        .unwrap();
    let mut buf = Vec::new();
    repo.read_file("file", &mut buf).unwrap();
    debug!("{}", std::str::from_utf8(&buf).unwrap());

    // Solve the conflict.
    let conflict: Vec<_> = std::str::from_utf8(&buf).unwrap().lines().collect();
    {
        let mut w = repo.write_file("file").unwrap();
        for l in conflict.iter().filter(|l| l.len() == 1) {
            writeln!(w, "{}", l).unwrap()
        }
    }

    buf.clear();
    repo.read_file("file", &mut buf).unwrap();
    debug!("{}", std::str::from_utf8(&buf).unwrap());
    let resb = record_all(&mut repo, &changes, &mut txn, &mut channela, "").unwrap();

    let mut p_inv = changes.get_change(&resb).unwrap().inverse(
        &resb,
        crate::change::ChangeHeader {
            authors: vec![],
            message: "rollback".to_string(),
            description: None,
            timestamp: chrono::Utc::now(),
        },
        Vec::new(),
    );
    let h_inv = changes
        .save_change(&mut p_inv, |_, _| Ok::<_, anyhow::Error>(()))
        .unwrap();
    apply::apply_change_arc(&changes, &txn, &channela, &h_inv).unwrap();
}

#[test]
fn rollback_conflict_resolution_swap() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let txn = env.arc_txn_begin().unwrap();

    let channela = txn.write().open_or_create_channel("main")?;

    // Create a simple conflict between axb and ayb
    repo.add_file("file", b"a\nb\n".to_vec());
    txn.write().add_file("file", 0)?;
    record_all(&mut repo, &changes, &txn, &channela, "")?;

    let channelb = txn.write().fork(&channela, "other")?;

    repo.write_file("file")
        .unwrap()
        .write_all(b"a\nx\nb\n")
        .unwrap();
    let ha = record_all(&repo, &changes, &txn, &channela, "")?;

    repo.write_file("file").unwrap().write_all(b"a\ny\nb\n")?;
    let hb = record_all(&repo, &changes, &txn, &channelb, "")?;

    apply::apply_change_arc(&changes, &txn, &channelb, &ha)?;
    apply::apply_change_arc(&changes, &txn, &channela, &hb)?;

    output::output_repository_no_pending(&repo, &changes, &txn, &channela, "", true, None, 1, 0)?;
    let mut buf = Vec::new();
    repo.read_file("file", &mut buf)?;
    debug!("{}", std::str::from_utf8(&buf).unwrap());

    // Solve the conflict.
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    {
        let mut w = repo.write_file("file").unwrap();
        for l in conflict.iter().filter(|l| l.len() == 1) {
            writeln!(w, "{}", l)?
        }
    }

    buf.clear();
    repo.read_file("file", &mut buf)?;
    debug!("{}", std::str::from_utf8(&buf).unwrap());
    let resb = record_all(&repo, &changes, &txn, &channelb, "")?;

    let mut p_inv = changes.get_change(&resb).unwrap().inverse(
        &resb,
        crate::change::ChangeHeader {
            authors: vec![],
            message: "rollback".to_string(),
            description: None,
            timestamp: chrono::Utc::now(),
        },
        Vec::new(),
    );
    let h_inv = changes.save_change(&mut p_inv, |_, _| Ok::<_, anyhow::Error>(()))?;
    apply::apply_change_arc(&changes, &txn, &channelb, &h_inv)?;

    Ok(())
}
