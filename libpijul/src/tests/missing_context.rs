use super::*;
use crate::working_copy::{WorkingCopy, WorkingCopyRead};
use std::io::Write;

#[test]
fn missing_context_newnodes_lines() -> Result<(), anyhow::Error> {
    missing_context_newnodes(Some("a\nf\n"))
}

#[test]
fn missing_context_newnodes_file() -> Result<(), anyhow::Error> {
    missing_context_newnodes(None)
}

fn missing_context_newnodes(alice: Option<&str>) -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";
    let bob = b"a\nb\nc\nx\nz\nd\ne\nf\n";
    let bob2 = b"a\nb\nc\nx\ny\nz\nd\ne\nf\n";

    let repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let txn_alice = env_alice.arc_txn_begin().unwrap();
    let channel_alice = txn_alice.write().open_or_create_channel("main").unwrap();
    txn_alice.write().add_file("file", 0).unwrap();
    let init_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "").unwrap();

    // Bob clones
    let repo_bob = working_copy::memory::Memory::new();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let txn_bob = env_bob.arc_txn_begin().unwrap();
    let mut channel_bob = txn_bob.write().open_or_create_channel("main").unwrap();
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &init_h).unwrap();
    output::output_repository_no_pending(
        &repo_bob,
        &changes,
        &txn_bob,
        &channel_bob,
        "",
        true,
        None,
        1,
        0,
    )
    .unwrap();

    // Bob edits and records
    repo_bob
        .write_file("file", Inode::ROOT)
        .unwrap()
        .write_all(bob)
        .unwrap();
    let bob_h = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "").unwrap();

    repo_bob
        .write_file("file", Inode::ROOT)
        .unwrap()
        .write_all(bob2)
        .unwrap();
    let bob_h2 = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "").unwrap();

    // Alice edits and records
    if let Some(alice) = alice {
        repo_alice
            .write_file("file", Inode::ROOT)
            .unwrap()
            .write_all(alice.as_bytes())
            .unwrap();
    } else {
        repo_alice.remove_path("file", false)?;
    }
    let alice_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    // Alice applies Bob's change
    debug!("applying Bob's change");
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &bob_h)?;
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &bob_h2)?;
    debug!("done applying Bob's change");
    output::output_repository_no_pending(
        &repo_alice,
        &changes,
        &txn_alice,
        &channel_alice,
        "",
        true,
        None,
        1,
        0,
    )?;

    crate::unrecord::unrecord(
        &mut *txn_alice.write(),
        &channel_alice,
        &changes,
        &bob_h2,
        0,
    )?;
    crate::unrecord::unrecord(&mut *txn_alice.write(), &channel_alice, &changes, &bob_h, 0)?;

    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &bob_h)?;
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &bob_h2)?;

    let mut buf = Vec::new();
    repo_alice.read_file("file", &mut buf)?;

    let re = regex::bytes::Regex::new(r#" \[[^\]]*\]"#).unwrap();
    let buf_ = re.replace_all(&buf, &[][..]);
    if alice.is_some() {
        assert_eq!(
            std::str::from_utf8(&buf_),
            Ok(&"a\n>>>>>>> 0\nx\ny\nz\n<<<<<<< 0\nf\n"[..])
        );
    } else {
        assert_eq!(
            std::str::from_utf8(&buf_),
            Ok(&">>>>>>> 0\nx\ny\nz\n<<<<<<< 0\n"[..])
        );
    }

    // Alice solves the conflict by confirming the deads.
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    {
        let mut w = repo_alice.write_file("file", Inode::ROOT).unwrap();
        for l in conflict.iter().filter(|l| l.len() <= 3) {
            writeln!(w, "{}", l)?
        }
    }
    info!("starting fix_deletion");
    let _fix_deletion = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;
    info!("fix_deletion over");

    // Bob applies Alice's change
    info!("Bob applies Alice's change");
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &alice_h).unwrap();
    info!("Outputting Bob's working_copy");

    crate::unrecord::unrecord(
        &mut *txn_bob.write(),
        &mut channel_bob,
        &changes,
        &alice_h,
        0,
    )?;

    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &alice_h)?;

    let mut buf = Vec::new();

    output::output_repository_no_pending(
        &repo_bob,
        &changes,
        &txn_bob,
        &channel_bob,
        "",
        true,
        None,
        1,
        0,
    )?;

    repo_bob.read_file("file", &mut buf)?;
    if alice.is_some() {
        let buf_ = re.replace_all(&buf, &[][..]);
        assert_eq!(
            std::str::from_utf8(&buf_),
            Ok(&"a\n>>>>>>> 0\nx\n<<<<<<< 0\ny\n>>>>>>> 1\nz\n<<<<<<< 1\nf\n"[..])
        );
    } else {
        assert_eq!(
            std::str::from_utf8(&buf_),
            Ok(&">>>>>>> 0\nx\ny\nz\n<<<<<<< 0\n"[..])
        );
    }

    // Bob solves the conflict by deleting the offending line.
    {
        let mut w = repo_bob.write_file("file", Inode::ROOT).unwrap();
        for l in conflict.iter().filter(|&&l| l != "xyz") {
            writeln!(w, "{}", l)?
        }
    }
    info!("starting fix_insertion");
    let _fix_insertion = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "")?;
    info!("fix_insertion over");
    Ok(())
}

#[test]
fn missing_context_newedges() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";
    let alice = b"d\nf\n";
    let bob = b"a\nb\nc\ne\nf\n";

    let repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let txn_alice = env_alice.arc_txn_begin().unwrap();
    let channel_alice = txn_alice.write().open_or_create_channel("main")?;
    txn_alice.write().add_file("file", 0)?;
    let init_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    // Bob clones
    let repo_bob = working_copy::memory::Memory::new();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let txn_bob = env_bob.arc_txn_begin().unwrap();
    let channel_bob = txn_bob.write().open_or_create_channel("main")?;
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &init_h)?;
    output::output_repository_no_pending(
        &repo_bob,
        &changes,
        &txn_bob,
        &channel_bob,
        "",
        true,
        None,
        1,
        0,
    )?;
    info!("Done outputting Bob's working_copy");
    {
        let mut buf = Vec::new();
        repo_bob.read_file("file", &mut buf).unwrap();
        info!("Bob = {:?}", std::str::from_utf8(&buf));
    }
    // Bob edits and records
    debug!("Bob edits and records");
    repo_bob
        .write_file("file", Inode::ROOT)
        .unwrap()
        .write_all(bob)
        .unwrap();
    let bob_h = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "")?;

    // Alice edits and records
    debug!("Alice edits and records");
    repo_alice
        .write_file("file", Inode::ROOT)
        .unwrap()
        .write_all(alice)
        .unwrap();
    let _alice_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    // Alice applies Bob's change
    debug!("Alice applies Bob's change");
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &bob_h)?;
    output::output_repository_no_pending(
        &repo_alice,
        &changes,
        &txn_alice,
        &channel_alice,
        "",
        true,
        None,
        1,
        0,
    )?;

    // Bob reverts his change.
    debug!("Bob reverts");
    let bob_change = changes.get_change(&bob_h)?;
    let mut inv = bob_change.inverse(
        &bob_h,
        crate::change::ChangeHeader {
            authors: vec![],
            message: "rollback".to_string(),
            description: None,
            timestamp: chrono::Utc::now(),
        },
        Vec::new(),
    );
    let inv_h = changes.save_change(&mut inv, |_, _| Ok::<_, anyhow::Error>(()))?;
    // Alice applies Bob's inverse change.
    info!("Applying inverse change");
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &inv_h)?;
    output::output_repository_no_pending(
        &repo_alice,
        &changes,
        &txn_alice,
        &channel_alice,
        "",
        true,
        None,
        1,
        0,
    )?;

    Ok(())
}
