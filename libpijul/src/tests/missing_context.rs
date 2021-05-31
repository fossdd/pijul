use super::*;

use crate::working_copy::WorkingCopy;

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

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let mut channel_alice = txn_alice.open_or_create_channel("main").unwrap();
    txn_alice.add_file("file").unwrap();
    let init_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )
    .unwrap();

    // Bob clones
    let mut repo_bob = working_copy::memory::Memory::new();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();
    let mut channel_bob = txn_bob.open_or_create_channel("main").unwrap();
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &init_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )
    .unwrap();

    // Bob edits and records
    repo_bob
        .write_file::<_, std::io::Error, _>("file", |w| {
            w.write_all(bob).unwrap();
            Ok(())
        })
        .unwrap();
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();

    repo_bob
        .write_file::<_, std::io::Error, _>("file", |w| {
            w.write_all(bob2).unwrap();
            Ok(())
        })
        .unwrap();
    let bob_h2 = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "bob0")?;

    // Alice edits and records
    if let Some(alice) = alice {
        repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
            w.write_all(alice.as_bytes()).unwrap();
            Ok(())
        })?
    } else {
        repo_alice.remove_path("file")?;
    }
    let alice_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug0")?;

    // Alice applies Bob's change
    debug!("applying Bob's change");
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &bob_h)?;
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &bob_h2)?;
    debug!("done applying Bob's change");
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1")?;

    crate::unrecord::unrecord(&mut txn_alice, &mut channel_alice, &changes, &bob_h2)?;
    crate::unrecord::unrecord(&mut txn_alice, &mut channel_alice, &changes, &bob_h)?;

    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1_unrec")?;
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &bob_h)?;
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &bob_h2)?;

    let mut buf = Vec::new();
    repo_alice.read_file("file", &mut buf)?;

    if alice.is_some() {
        assert_eq!(
            std::str::from_utf8(&buf),
            Ok(
                &"a\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\nx\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\ny\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\nz\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\nf\n"
                    [..]
            )
        );
    } else {
        assert_eq!(
            std::str::from_utf8(&buf),
            Ok(&">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\nx\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\ny\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\nz\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n"[..])
        );
    }

    // Alice solves the conflict by confirming the deads.
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        for l in conflict.iter().filter(|l| l.len() <= 3) {
            writeln!(w, "{}", l)?
        }
        Ok(())
    })?;
    info!("starting fix_deletion");
    let _fix_deletion = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice2")?;
    info!("fix_deletion over");

    // Bob applies Alice's change
    info!("Bob applies Alice's change");
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob0")?;
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &alice_h).unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1")?;
    info!("Outputting Bob's working_copy");

    crate::unrecord::unrecord(&mut txn_bob, &mut channel_bob, &changes, &alice_h)?;

    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob2_unrec")?;
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &alice_h)?;

    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob2_unrec_app")?;
    let mut buf = Vec::new();

    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob2")?;

    repo_bob.read_file("file", &mut buf)?;
    if alice.is_some() {
        assert_eq!(
            std::str::from_utf8(&buf),
            Ok(
                &"a\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\nx\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\ny\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\nz\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\nf\n"
                    [..]
            )
        );
    } else {
        assert_eq!(
            std::str::from_utf8(&buf),
            Ok(&">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\nx\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\ny\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\nz\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n"[..])
        );
    }

    // Bob solves the conflict by deleting the offending line.
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        for l in conflict.iter().filter(|&&l| l != "xyz") {
            writeln!(w, "{}", l)?
        }
        Ok(())
    })?;
    info!("starting fix_insertion");
    let _fix_insertion = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "")?;
    info!("fix_insertion over");
    Ok(())
}

#[test]
fn missing_context_newedges() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";
    let alice = b"d\nf\n";
    let bob = b"a\nb\nc\ne\nf\n";

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let mut channel_alice = txn_alice.open_or_create_channel("main")?;
    txn_alice.add_file("file")?;
    let init_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_init").unwrap();

    // Bob clones
    let mut repo_bob = working_copy::memory::Memory::new();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();
    let mut channel_bob = txn_bob.open_or_create_channel("main")?;
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &init_h)?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    info!("Done outputting Bob's working_copy");
    {
        let mut buf = Vec::new();
        repo_bob.read_file("file", &mut buf).unwrap();
        info!("Bob = {:?}", std::str::from_utf8(&buf));
    }
    // Bob edits and records
    debug!("Bob edits and records");
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(bob).unwrap();
        Ok(())
    })?;
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "")?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob0").unwrap();

    // Alice edits and records
    debug!("Alice edits and records");
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(alice).unwrap();
        Ok(())
    })?;
    let _alice_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice0").unwrap();

    // Alice applies Bob's change
    debug!("Alice applies Bob's change");
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &bob_h)?;
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1").unwrap();

    // Bob reverts his change.
    debug!("Bob reverts");
    let bob_change = changes.get_change(&bob_h)?;
    let inv = bob_change.inverse(
        &bob_h,
        crate::change::ChangeHeader {
            authors: vec![],
            message: "rollback".to_string(),
            description: None,
            timestamp: chrono::Utc::now(),
        },
        Vec::new(),
    );
    let inv_h = changes.save_change(&inv)?;
    // Alice applies Bob's inverse change.
    info!("Applying inverse change");
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &inv_h)?;
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice2").unwrap();

    Ok(())
}
