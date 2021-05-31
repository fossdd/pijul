use super::*;

#[test]
fn solve_order_conflict() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\n";
    let alice = b"a\nx\ny\nz\nb\n";
    let bob = b"a\nu\nv\nw\nb\n";

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel_alice = txn.open_or_create_channel("alice")?;
    txn.add_file("file")?;
    let init_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_init").unwrap();

    // Bob clones
    let mut repo_bob = working_copy::memory::Memory::new();
    let mut channel_bob = txn.open_or_create_channel("bob")?;
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &init_h)?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
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
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(bob).unwrap();
        Ok(())
    })?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice").unwrap();
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob").unwrap();
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob0").unwrap();

    // Alice edits and records
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(alice).unwrap();
        Ok(())
    })?;
    let alice_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice0").unwrap();

    // Alice applies Bob's change
    apply::apply_change(&changes, &mut txn, &mut channel_alice, &bob_h)?;
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf = Vec::new();
    repo_alice.read_file("file", &mut buf)?;

    let check_conflict = |buf: &[u8]| -> Result<(), anyhow::Error> {
        let conflict: Vec<_> = std::str::from_utf8(buf)?.lines().collect();
        debug!("{:?}", conflict);
        {
            let mut conflict = conflict.clone();
            (&mut conflict[2..9]).sort_unstable();
            assert_eq!(
                conflict,
                vec![
                    "a",
                    ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
                    "================================",
                    "u",
                    "v",
                    "w",
                    "x",
                    "y",
                    "z",
                    "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",
                    "b"
                ]
            );
        }
        Ok(())
    };
    // check_conflict(&buf)?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice1").unwrap();

    // Alice solves the conflict.
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        for (n, l) in conflict.iter().enumerate() {
            if n == 0 || n == 2 || n == 3 || n == 7 || n == 8 || n == 10 {
                writeln!(w, "{}", l)?
            } else if n == 4 {
                writeln!(w, "{}\nbla!", l)?
            } else if n == 6 {
                writeln!(w, "{}", l)?
            }
        }
        Ok(())
    })?;
    info!("resolving");
    let resolution = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice2").unwrap();

    // Bob applies Alice's change
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &alice_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob1").unwrap();
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    check_conflict(&buf)?;

    // Bob applies Alice's resolution
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &resolution).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob2").unwrap();
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    assert!(std::str::from_utf8(&buf)?.lines().all(|l| l.len() < 10));

    crate::unrecord::unrecord(&mut txn, &mut channel_bob, &changes, &resolution).unwrap();
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob3").unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    check_conflict(&buf)?;
    Ok(())
}
#[test]
fn order_conflict_simple() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\n";
    let alice = b"a\nx\nb\n";
    let bob = b"a\ny\nb\n";
    let charlie = b"a\nz\nb\n";

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel_alice = txn.open_or_create_channel("alice")?;
    txn.add_file("file")?;
    let init_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_init").unwrap();

    // Bob clones
    let mut repo_bob = working_copy::memory::Memory::new();
    let mut channel_bob = txn.open_or_create_channel("bob")?;
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &init_h)?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
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

    // Charlie clones
    let mut repo_charlie = working_copy::memory::Memory::new();
    let mut channel_charlie = txn.open_or_create_channel("charlie")?;
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &init_h)?;
    output::output_repository_no_pending(
        &mut repo_charlie,
        &changes,
        &mut txn,
        &mut channel_charlie,
        "",
        true,
        None,
    )?;

    // Bob edits and records
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(bob).unwrap();
        Ok(())
    })?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice").unwrap();
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob").unwrap();
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob0").unwrap();

    // Charlie edits and records
    repo_charlie.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(charlie).unwrap();
        Ok(())
    })?;
    let charlie_h = record_all(
        &mut repo_charlie,
        &changes,
        &mut txn,
        &mut channel_charlie,
        "",
    )?;
    debug_to_file(&txn, &channel_charlie.borrow(), "debug_charlie0").unwrap();

    // Alice edits and records
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(alice).unwrap();
        Ok(())
    })?;
    let alice_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice0").unwrap();

    // Alice applies Bob's change
    apply::apply_change(&changes, &mut txn, &mut channel_alice, &bob_h)?;
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf = Vec::new();
    repo_alice.read_file("file", &mut buf)?;

    let check_conflict = |buf: &[u8]| -> Result<(), anyhow::Error> {
        let conflict: Vec<_> = std::str::from_utf8(buf)?.lines().collect();
        debug!("{:?}", conflict);
        {
            let mut conflict = conflict.clone();
            (&mut conflict[2..7]).sort_unstable();
            assert_eq!(
                conflict,
                vec![
                    "a",
                    ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
                    "================================",
                    "================================",
                    "x",
                    "y",
                    "z",
                    "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",
                    "b"
                ]
            );
        }
        Ok(())
    };
    // check_conflict(&buf)?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice1").unwrap();

    // Alice solves the conflict.
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        for l in conflict.iter().filter(|l| l.len() == 1) {
            writeln!(w, "{}", l)?
        }
        Ok(())
    })?;
    let mut alice_resolution = Vec::new();
    repo_alice.read_file("file", &mut alice_resolution)?;
    info!("resolving");
    let resolution = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice2").unwrap();

    // Bob applies Alice's change
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &alice_h).unwrap();
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &charlie_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob1").unwrap();
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    check_conflict(&buf)?;

    apply::apply_change(&changes, &mut txn, &mut channel_bob, &resolution).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    {
        let mut conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
        (&mut conflict[2..6]).sort_unstable();
        assert_eq!(
            conflict,
            vec![
                "a",
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
                "================================",
                "x",
                "y",
                "z",
                "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",
                "b"
            ]
        )
    }
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        for l in conflict.iter().filter(|l| l.len() == 1) {
            writeln!(w, "{}", l)?
        }
        Ok(())
    })?;
    let mut bob_resolution = Vec::new();
    repo_bob.read_file("file", &mut bob_resolution)?;
    info!("resolving");
    let resolution2 = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;

    // Charlie applies Alice's change
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &alice_h).unwrap();
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &bob_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_charlie,
        &changes,
        &mut txn,
        &mut channel_charlie,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_charlie.borrow(), "debug_charlie1").unwrap();
    buf.clear();
    repo_charlie.read_file("file", &mut buf)?;
    check_conflict(&buf)?;

    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &resolution).unwrap();
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &resolution2).unwrap();
    output::output_repository_no_pending(
        &mut repo_charlie,
        &changes,
        &mut txn,
        &mut channel_charlie,
        "",
        true,
        None,
    )?;
    buf.clear();
    repo_charlie.read_file("file", &mut buf)?;
    assert_eq!(
        std::str::from_utf8(&bob_resolution),
        std::str::from_utf8(&buf)
    );

    Ok(())
}

#[test]
fn order_conflict_edit() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\n";
    let alice = b"a\nx\ny\nb\n";
    let bob = b"a\nu\nv\nb\n";

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel_alice = txn.open_or_create_channel("alice")?;
    txn.add_file("file")?;
    let init_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_init").unwrap();

    // Bob clones
    let mut repo_bob = working_copy::memory::Memory::new();
    let mut channel_bob = txn.open_or_create_channel("bob")?;
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &init_h)?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    info!("Done outputting Bob's working_copy");

    // Bob edits and records
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(bob).unwrap();
        Ok(())
    })?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice").unwrap();
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob").unwrap();
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob0").unwrap();

    // Alice edits and records
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(alice).unwrap();
        Ok(())
    })?;
    let alice_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice0").unwrap();

    // Alice applies Bob's change
    apply::apply_change(&changes, &mut txn, &mut channel_alice, &bob_h)?;
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice1").unwrap();

    // Alice solves the conflict.
    let mut buf = Vec::new();
    repo_alice.read_file("file", &mut buf)?;
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    let mut is_conflict = 0;
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        for l in conflict.iter() {
            if l.len() == 1 {
                if is_conflict < 2 {
                    writeln!(w, "{}", l)?
                }
                is_conflict += 1
            } else if l.as_bytes()[0] == b'<' {
                is_conflict = 0
            } else {
                // === or >>>
                is_conflict = 1
            }
        }
        Ok(())
    })?;
    let mut alice_resolution = Vec::new();
    repo_alice.read_file("file", &mut alice_resolution)?;
    info!("resolving {:?}", std::str::from_utf8(&alice_resolution));
    let resolution = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice2").unwrap();

    // Bob applies Alice's change
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &alice_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob1").unwrap();
    let mut buf = Vec::new();
    repo_bob.read_file("file", &mut buf)?;

    apply::apply_change(&changes, &mut txn, &mut channel_bob, &resolution).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob1").unwrap();
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    assert_eq!(alice_resolution, buf);
    Ok(())
}

#[test]
fn edit_conflict_sides() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\n";
    let alice = b"a\nx\nb\nc\n";
    let bob = b"a\ny\nb\nc\n";

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel_alice = txn.open_or_create_channel("alice")?;
    txn.add_file("file")?;
    let init_h = record_all(&mut repo, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_init").unwrap();

    // Bob clones
    let mut channel_bob = txn.open_or_create_channel("bob")?;
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &init_h)?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    info!("Done outputting Bob's working_copy");

    // Bob edits and records
    repo.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(bob).unwrap();
        Ok(())
    })?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice").unwrap();
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob").unwrap();
    let bob_h = record_all(&mut repo, &changes, &mut txn, &mut channel_bob, "")?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob0").unwrap();

    // Alice edits and records
    repo.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(alice).unwrap();
        Ok(())
    })?;
    let alice_h = record_all(&mut repo, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice0").unwrap();

    // Alice applies Bob's change
    apply::apply_change(&changes, &mut txn, &mut channel_alice, &bob_h)?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf = Vec::new();
    repo.read_file("file", &mut buf)?;

    // Alice edits sides of the conflict.
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    repo.write_file::<_, std::io::Error, _>("file", |w| {
        let mut ended = false;
        let mut n = 0;
        for l in conflict.iter() {
            debug!("line: {:?}", l);
            if l.len() > 5 {
                if l.as_bytes()[0] == b'<' {
                    ended = true
                }
                if true {
                    writeln!(w, "pre{}\n{}\npost{}", n, l, n)?;
                } else {
                    writeln!(w, "{}", l)?;
                }
                n += 1
            } else if !ended {
                writeln!(w, "{}", l)?
            } else {
                debug!("writing c: {:?}", l);
                writeln!(w, "c")?
            }
        }
        Ok(())
    })?;
    let mut buf = Vec::new();
    repo.read_file("file", &mut buf)?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice1").unwrap();
    info!("resolving");
    let resolution = record_all(&mut repo, &changes, &mut txn, &mut channel_alice, "")?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf2 = Vec::new();
    repo.read_file("file", &mut buf2)?;
    info!("{:?}", std::str::from_utf8(&buf2).unwrap());
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice2").unwrap();
    assert_eq!(std::str::from_utf8(&buf), std::str::from_utf8(&buf2));

    // Bob applies
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &alice_h)?;
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &resolution)?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    let mut buf3 = Vec::new();
    repo.read_file("file", &mut buf3)?;
    let mut lines2: Vec<_> = std::str::from_utf8(&buf2).unwrap().lines().collect();
    lines2.sort_unstable();
    let mut lines3: Vec<_> = std::str::from_utf8(&buf3).unwrap().lines().collect();
    lines3.sort_unstable();
    assert_eq!(lines2, lines3);
    Ok(())
}

#[test]
fn edit_after_conflict() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\n";
    let alice = b"a\nx\ny\nb\nc\n";
    let bob = b"a\nx\ny\nb\nc\n";

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel_alice = txn.open_or_create_channel("alice")?;
    txn.add_file("file")?;
    let init_h = record_all(&mut repo, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_init").unwrap();

    // Bob clones
    let mut channel_bob = txn.open_or_create_channel("bob")?;
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &init_h)?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    info!("Done outputting Bob's working_copy");

    // Bob edits and records
    repo.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(bob).unwrap();
        Ok(())
    })?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice").unwrap();
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob").unwrap();
    let bob_h = record_all(&mut repo, &changes, &mut txn, &mut channel_bob, "")?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob0").unwrap();

    // Alice edits and records
    repo.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(alice).unwrap();
        Ok(())
    })?;
    let alice_h = record_all(&mut repo, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice0").unwrap();

    // Alice applies Bob's change
    apply::apply_change(&changes, &mut txn, &mut channel_alice, &bob_h)?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf = Vec::new();
    repo.read_file("file", &mut buf)?;

    // Alice edits sides of the conflict.
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    repo.write_file::<_, std::io::Error, _>("file", |w| {
        for l in conflict.iter() {
            debug!("line: {:?}", l);
            if l.len() > 5 && l.as_bytes()[0] != b'<' {
                writeln!(w, "pre\n{}\npost", l)?;
            } else if *l != "b" && *l != "x" {
                writeln!(w, "{}", l)?
            }
        }
        Ok(())
    })?;
    let mut buf = Vec::new();
    repo.read_file("file", &mut buf)?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice1").unwrap();

    info!("resolving");
    let resolution = record_all(&mut repo, &changes, &mut txn, &mut channel_alice, "")?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf2 = Vec::new();
    repo.read_file("file", &mut buf2)?;
    info!("{:?}", std::str::from_utf8(&buf2).unwrap());
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice2").unwrap();
    assert_eq!(std::str::from_utf8(&buf), std::str::from_utf8(&buf2));

    apply::apply_change(&changes, &mut txn, &mut channel_bob, &alice_h)?;
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &resolution)?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;

    let mut buf3 = Vec::new();
    repo.read_file("file", &mut buf3)?;

    let mut lines2: Vec<_> = std::str::from_utf8(&buf2)?.lines().collect();
    lines2.sort_unstable();
    let mut lines3: Vec<_> = std::str::from_utf8(&buf3)?.lines().collect();
    lines3.sort_unstable();
    assert_eq!(lines2, lines3);
    Ok(())
}

#[test]
fn delete_before_marker() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\n";
    let alice0 = b"a\nx\ny\nb\nc\n";
    let alice1 = b"a\nx\ny\nz\nb\nc\n";
    let bob0 = b"a\nu\nv\nb\nc\n";
    let bob1 = b"a\nu\nv\nw\nb\nc\n";

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel_alice = txn.open_or_create_channel("alice")?;
    txn.add_file("file")?;
    let init_h = record_all(&mut repo, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_init").unwrap();

    // Bob clones
    let mut channel_bob = txn.open_or_create_channel("bob")?;
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &init_h)?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    info!("Done outputting Bob's working_copy");

    // Bob edits and records
    let bob_edits: &[&[u8]] = &[bob0, bob1];
    let bob_changes: Vec<_> = bob_edits
        .iter()
        .map(|bob| {
            repo.write_file::<_, std::io::Error, _>("file", |w| {
                w.write_all(bob).unwrap();
                Ok(())
            })
            .unwrap();
            record_all(&mut repo, &changes, &mut txn, &mut channel_bob, "").unwrap()
        })
        .collect();

    // Alice edits and records
    let alice_edits: &[&[u8]] = &[alice0, alice1];
    let alice_changes: Vec<_> = alice_edits
        .iter()
        .map(|alice| {
            repo.write_file::<_, std::io::Error, _>("file", |w| {
                w.write_all(alice).unwrap();
                Ok(())
            })
            .unwrap();
            record_all(&mut repo, &changes, &mut txn, &mut channel_alice, "").unwrap()
        })
        .collect();

    // Alice applies Bob's changes
    for bob_h in bob_changes.iter() {
        apply::apply_change(&changes, &mut txn, &mut channel_alice, bob_h)?;
    }
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf = Vec::new();
    repo.read_file("file", &mut buf)?;

    // Alice edits sides of the conflict.
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    repo.write_file::<_, std::io::Error, _>("file", |w| {
        let mut ended = false;
        for l in conflict.iter() {
            debug!("line: {:?}", l);
            if *l == "z" || *l == "w" {
            } else if l.starts_with("<<<") {
                writeln!(w, "{}", l)?;
                ended = true
            } else if ended {
                writeln!(w, "end\n{}", l)?;
                ended = false
            } else {
                writeln!(w, "{}", l)?;
            }
        }
        Ok(())
    })?;
    let mut buf = Vec::new();
    repo.read_file("file", &mut buf)?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice1").unwrap();

    info!("resolving");
    let conflict_edits = record_all(&mut repo, &changes, &mut txn, &mut channel_alice, "")?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf2 = Vec::new();
    repo.read_file("file", &mut buf2)?;
    info!("{:?}", std::str::from_utf8(&buf2).unwrap());
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice2").unwrap();
    assert_eq!(std::str::from_utf8(&buf), std::str::from_utf8(&buf2));

    // Bob pulls
    for alice_h in alice_changes.iter() {
        apply::apply_change(&changes, &mut txn, &mut channel_bob, &*alice_h)?;
    }
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &conflict_edits)?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob").unwrap();
    buf2.clear();
    repo.read_file("file", &mut buf2)?;
    let mut lines: Vec<_> = std::str::from_utf8(&buf).unwrap().lines().collect();
    lines.sort_unstable();
    let mut lines2: Vec<_> = std::str::from_utf8(&buf2).unwrap().lines().collect();
    lines2.sort_unstable();
    assert_eq!(lines, lines2);

    Ok(())
}

#[test]
fn conflict_last_line() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\n";
    let alice = b"a\nx";
    let bob = b"a\ny";

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel_alice = txn.open_or_create_channel("alice")?;
    txn.add_file("file")?;
    let init_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_init").unwrap();

    // Bob clones
    let mut repo_bob = working_copy::memory::Memory::new();
    let mut channel_bob = txn.open_or_create_channel("bob")?;
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &init_h)?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
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
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(bob).unwrap();
        Ok(())
    })?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice").unwrap();
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob").unwrap();
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob0").unwrap();

    // Alice edits and records
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(alice).unwrap();
        Ok(())
    })?;
    let alice_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice0").unwrap();

    // Alice applies Bob's change
    apply::apply_change(&changes, &mut txn, &mut channel_alice, &bob_h)?;
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf = Vec::new();
    repo_alice.read_file("file", &mut buf)?;

    let check_conflict = |buf: &[u8]| -> Result<(), anyhow::Error> {
        let conflict: Vec<_> = std::str::from_utf8(buf)?.lines().collect();
        debug!("{:?}", conflict);
        {
            let mut conflict = conflict.clone();
            (&mut conflict[2..5]).sort_unstable();
            assert_eq!(
                conflict,
                vec![
                    "a",
                    ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
                    "================================",
                    "x",
                    "y",
                    "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
                ]
            );
        }
        Ok(())
    };
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice1").unwrap();

    // Alice solves the conflict.
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        for l in conflict.iter().filter(|l| l.len() <= 2) {
            writeln!(w, "{}", l)?
        }
        Ok(())
    })?;
    info!("resolving");
    let mut buf_alice = Vec::new();
    repo_alice.read_file("file", &mut buf_alice)?;

    let resolution = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice2").unwrap();

    // Bob applies Alice's change
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &alice_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob1").unwrap();
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    check_conflict(&buf)?;

    apply::apply_change(&changes, &mut txn, &mut channel_bob, &resolution).unwrap();
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob2").unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    assert_eq!(std::str::from_utf8(&buf), std::str::from_utf8(&buf_alice));
    Ok(())
}

#[test]
fn zombie_last_line() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb";
    let alice = b"a\nx";
    let bob = b"";

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel_alice = txn.open_or_create_channel("alice")?;
    txn.add_file("file")?;
    let init_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_init").unwrap();

    // Bob clones
    let mut repo_bob = working_copy::memory::Memory::new();
    let mut channel_bob = txn.open_or_create_channel("bob")?;
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &init_h)?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
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
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(bob).unwrap();
        Ok(())
    })?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice").unwrap();
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob").unwrap();
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob0").unwrap();

    // Alice edits and records
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(alice).unwrap();
        Ok(())
    })?;
    let alice_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice0").unwrap();

    // Alice applies Bob's change
    apply::apply_change(&changes, &mut txn, &mut channel_alice, &bob_h)?;
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf = Vec::new();
    repo_alice.read_file("file", &mut buf)?;

    let check_conflict = |buf: &[u8]| -> Result<(), anyhow::Error> {
        let conflict: Vec<_> = std::str::from_utf8(buf)?.lines().collect();
        assert_eq!(
            conflict,
            vec![
                ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
                "x",
                "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
            ]
        );
        Ok(())
    };
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice1").unwrap();

    {
        let mut state = Builder::new();
        state
            .record(
                &mut txn,
                Algorithm::default(),
                &mut channel_alice.borrow_mut(),
                &mut repo_alice,
                &changes,
                "",
            )
            .unwrap();
        let rec = state.finish();
        assert!(rec.actions.is_empty())
    }

    // Alice solves the conflict.
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        write!(w, "x")?;
        Ok(())
    })?;
    info!("resolving");
    let mut buf_alice = Vec::new();
    repo_alice.read_file("file", &mut buf_alice)?;

    let resolution = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice2").unwrap();

    // Bob applies Alice's change
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &alice_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob1").unwrap();
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    check_conflict(&buf)?;

    apply::apply_change(&changes, &mut txn, &mut channel_bob, &resolution).unwrap();
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob2").unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    assert_eq!(std::str::from_utf8(&buf), std::str::from_utf8(&buf_alice));
    Ok(())
}

#[test]
fn edit_post_conflict() -> Result<(), anyhow::Error> {
    edit_post_conflict_(
        |buf| {
            let buf: Vec<_> = std::str::from_utf8(&buf).unwrap().lines().collect();
            assert!(
                buf == [
                    "a",
                    ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
                    "0",
                    "1",
                    "2",
                    "================================",
                    "3",
                    "4",
                    "5",
                    "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",
                    "b",
                ] || buf
                    == [
                        "a",
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
                        "3",
                        "4",
                        "5",
                        "================================",
                        "0",
                        "1",
                        "2",
                        "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",
                        "b",
                    ]
            )
        },
        |buf, w| {
            let conflict: Vec<_> = std::str::from_utf8(&buf).unwrap().lines().collect();
            for l in conflict.iter() {
                if *l == "a" {
                    writeln!(w, "a\na'")?
                } else if l.len() == 1 && *l != "0" && *l != "3" {
                    writeln!(w, "{}", l)?
                }
            }
            Ok(())
        },
    )
}

#[test]
fn edit_around_conflict() -> Result<(), anyhow::Error> {
    edit_post_conflict_(
        |buf| {
            let buf: Vec<_> = std::str::from_utf8(&buf).unwrap().lines().collect();
            assert!(
                buf == [
                    "a",
                    ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
                    "0",
                    "1",
                    "2",
                    "================================",
                    "3",
                    "4",
                    "5",
                    "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",
                    "b",
                ] || buf
                    == [
                        "a",
                        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
                        "3",
                        "4",
                        "5",
                        "================================",
                        "0",
                        "1",
                        "2",
                        "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",
                        "b",
                    ]
            )
        },
        |buf, w| {
            let conflict: Vec<_> = std::str::from_utf8(&buf).unwrap().lines().collect();
            for l in conflict.iter() {
                if *l == "a" {
                    writeln!(w, "a\na'")?
                } else if *l == "b" {
                    writeln!(w, "c")?
                } else {
                    writeln!(w, "{}", l)?
                }
            }
            Ok(())
        },
    )
}

fn edit_post_conflict_<
    Check: FnMut(&[u8]),
    Resolve: FnOnce(&[u8], &mut dyn std::io::Write) -> Result<(), std::io::Error>,
>(
    mut check: Check,
    resolve: Resolve,
) -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\n";
    let alice = b"a\n0\n1\n2\nb\n";
    let bob = b"a\n3\n4\n5\nb\n";

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel_alice = txn.open_or_create_channel("alice")?;
    txn.add_file("file")?;
    let init_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_init").unwrap();

    // Bob clones
    let mut repo_bob = working_copy::memory::Memory::new();
    let mut channel_bob = txn.open_or_create_channel("bob")?;
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &init_h)?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
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
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(bob).unwrap();
        Ok(())
    })?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice").unwrap();
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob").unwrap();
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob0").unwrap();

    // Alice edits and records
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(alice).unwrap();
        Ok(())
    })?;
    let alice_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice0").unwrap();

    // Alice applies Bob's change
    apply::apply_change(&changes, &mut txn, &mut channel_alice, &bob_h)?;
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf = Vec::new();
    repo_alice.read_file("file", &mut buf)?;

    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice1").unwrap();
    check(&buf);

    // Alice solves the conflict.
    repo_alice.write_file::<_, std::io::Error, _>("file", |mut w| {
        resolve(&buf, &mut w)?;
        Ok(())
    })?;
    info!("resolving");
    let resolution = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice2").unwrap();

    // Bob applies Alice's change
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &alice_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob1").unwrap();
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    check(&buf);

    // Bob applies Alice's solution.
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &resolution).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob2").unwrap();

    let mut buf2 = Vec::new();
    repo_bob.read_file("file", &mut buf2)?;
    buf.clear();
    repo_alice.read_file("file", &mut buf)?;
    let mut lines: Vec<_> = std::str::from_utf8(&buf).unwrap().lines().collect();
    lines.sort_unstable();
    let mut lines2: Vec<_> = std::str::from_utf8(&buf2).unwrap().lines().collect();
    lines2.sort_unstable();
    assert_eq!(lines, lines2);

    Ok(())
}

#[test]
fn nested_conflict() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\n";
    let alice = b"a\nx\nb\n";
    let bob = b"a\ny\nb\n";

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel_alice = txn.open_or_create_channel("alice")?;
    txn.add_file("file")?;
    let init_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_init").unwrap();

    // Bob clones
    let mut repo_bob = working_copy::memory::Memory::new();
    let mut channel_bob = txn.open_or_create_channel("bob")?;
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &init_h)?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
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
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(bob).unwrap();
        Ok(())
    })?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice").unwrap();
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob").unwrap();
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob0").unwrap();

    // Alice edits and records
    debug!("Alice records");
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(alice).unwrap();
        Ok(())
    })?;
    let alice_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice0").unwrap();

    // Alice applies Bob's change
    debug!("Alice applies");
    apply::apply_change(&changes, &mut txn, &mut channel_alice, &bob_h)?;
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice1").unwrap();

    // Alice solves the conflict.
    let mut buf = Vec::new();
    repo_alice.read_file("file", &mut buf)?;
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        let buf = std::str::from_utf8(&buf).unwrap();
        w.write_all(buf.replace("x\n", "u\nx\n").as_bytes())?;
        Ok(())
    })?;
    info!("resolving");
    let resolution_alice = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice2").unwrap();

    // Bob applies Alice's change
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &alice_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;

    // Bob resolves.
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        let buf = std::str::from_utf8(&buf).unwrap();
        w.write_all(buf.replace("x\n", "i\nx\n").as_bytes())?;
        Ok(())
    })?;
    info!("resolving");
    let resolution_bob = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;

    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob1").unwrap();
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;

    // Alice applies Bob's resolution.
    apply::apply_change(&changes, &mut txn, &mut channel_alice, &resolution_bob).unwrap();
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice3").unwrap();
    buf.clear();
    repo_alice.read_file("file", &mut buf)?;
    debug!("{}", std::str::from_utf8(&buf).unwrap());

    // Bob applies Alice's resolution
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &resolution_alice).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    let mut buf2 = Vec::new();
    repo_bob.read_file("file", &mut buf2)?;

    let mut lines: Vec<_> = std::str::from_utf8(&buf).unwrap().lines().collect();
    lines.sort_unstable();
    let mut lines2: Vec<_> = std::str::from_utf8(&buf2).unwrap().lines().collect();
    lines2.sort_unstable();
    assert_eq!(lines, lines2);

    Ok(())
}
#[test]
fn zombie_context_resolution() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo_alice = working_copy::memory::Memory::new();
    let mut repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();

    let mut channel_alice = txn_alice.open_or_create_channel("alice").unwrap();

    // Alice records
    txn_alice.add_file("file").unwrap();
    repo_alice.add_file("file", b"".to_vec());
    let x: &[&[u8]] = &[b"c\n", b"a\nc\n", b"a\nb\nc\n", b"a\n", b""];
    let p_alice: Vec<_> = x
        .iter()
        .map(|c| {
            repo_alice
                .write_file::<_, std::io::Error, _>("file", |w| {
                    w.write_all(c)?;
                    Ok(())
                })
                .unwrap();
            record_all(
                &mut repo_alice,
                &changes,
                &mut txn_alice,
                &mut channel_alice,
                "",
            )
            .unwrap()
        })
        .collect();

    // Bob clones
    let mut channel_bob = txn_bob.open_or_create_channel("bob").unwrap();
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &p_alice[0]).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob")?;

    // Bob creates an order conflict just to keep line "c" connected
    // to the root.
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(b"x\nc\n")?;
        Ok(())
    })?;
    debug!("bob records conflict");
    let p_bob = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob0")?;

    // Bob applies all of Alice's other changes
    for (n, p) in (&p_alice[1..]).iter().enumerate() {
        info!("{}. Applying {:?}", n, p);
        apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, p).unwrap();
        debug_to_file(&txn_bob, &channel_bob.borrow(), &format!("debug_bob_{}", n))?;
        // if n == 2 {
        //     panic!("n")
        // }
    }
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    let mut buf = Vec::new();
    repo_bob.read_file("file", &mut buf)?;
    debug!("file = {:?}", std::str::from_utf8(&buf));
    assert_eq!(
        std::str::from_utf8(&buf),
        Ok(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\nx\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
    );

    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(b"x\nc\n")?;
        Ok(())
    })?;
    let resolution =
        record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob2")?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    assert_eq!(buf, b"x\nc\n");

    // Alice applies Bob's change and resolution.
    debug!("Alice applies Bob's change");
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &p_bob).unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1")?;
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf2 = Vec::new();
    repo_alice.read_file("file", &mut buf2)?;
    assert_eq!(
        std::str::from_utf8(&buf2),
        Ok(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\nx\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
    );
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &resolution).unwrap();
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice2")?;
    let mut buf2 = Vec::new();
    repo_alice.read_file("file", &mut buf2)?;
    assert_eq!(std::str::from_utf8(&buf), std::str::from_utf8(&buf2));
    Ok(())
}
#[test]
fn zombie_half_survivor() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo_alice = working_copy::memory::Memory::new();
    let mut repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();

    let mut channel_alice = txn_alice.open_or_create_channel("alice").unwrap();

    // Alice records
    txn_alice.add_file("file").unwrap();
    repo_alice.add_file("file", b"".to_vec());
    let x: &[&[u8]] = &[b"a\nb\nc\nd\n", b""];
    let p_alice: Vec<_> = x
        .iter()
        .map(|c| {
            repo_alice
                .write_file::<_, std::io::Error, _>("file", |w| {
                    w.write_all(c)?;
                    Ok(())
                })
                .unwrap();
            record_all(
                &mut repo_alice,
                &changes,
                &mut txn_alice,
                &mut channel_alice,
                "",
            )
            .unwrap()
        })
        .collect();

    // Bob clones
    let mut channel_bob = txn_bob.open_or_create_channel("bob").unwrap();
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &p_alice[0]).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;

    // Bob creates an order conflict just to keep line "c" connected
    // to the root.
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(b"a\nb\nx\ny\nz\nc\nd\n")?;
        Ok(())
    })?;
    let p_bob = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob0")?;

    // Bob applies all of Alice's other changes
    for p in &p_alice[1..] {
        apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, p).unwrap();
    }
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1")?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1_")?;
    let mut buf = Vec::new();
    repo_bob.read_file("file", &mut buf)?;
    assert_eq!(
        std::str::from_utf8(&buf),
        Ok(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\nx\ny\nz\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
    );

    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(b"a\nz\nd\n")?;
        Ok(())
    })?;
    let resolution =
        record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
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
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    assert_eq!(buf, b"a\nz\nd\n");

    // Alice applies Bob's change and resolution.
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &p_bob).unwrap();
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &resolution).unwrap();
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf2 = Vec::new();
    repo_alice.read_file("file", &mut buf2)?;
    assert_eq!(std::str::from_utf8(&buf), std::str::from_utf8(&buf2));
    Ok(())
}

#[test]
fn three_way_zombie() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"u\na\nb\nc\nd\nv\n";
    let alice = b"u\na\nb\nx\nc\nd\nv\n";
    let bob = b"u\na\nd\nv\n";
    let alice_bob = b"u\na\nx\nd\nv\n";
    let charlie = b"u\nv\n";

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel_alice = txn.open_or_create_channel("alice")?;
    txn.add_file("file")?;
    let init_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_init").unwrap();

    // Bob clones
    let mut repo_bob = working_copy::memory::Memory::new();
    let mut channel_bob = txn.open_or_create_channel("bob")?;
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &init_h)?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
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

    // Charlie clones
    let mut repo_charlie = working_copy::memory::Memory::new();
    let mut channel_charlie = txn.open_or_create_channel("charlie")?;
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &init_h)?;
    output::output_repository_no_pending(
        &mut repo_charlie,
        &changes,
        &mut txn,
        &mut channel_charlie,
        "",
        true,
        None,
    )?;

    // Alice adds a line.
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(alice).unwrap();
        Ok(())
    })?;
    let alice_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice0").unwrap();

    // Bob deletes the context.
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(bob).unwrap();
        Ok(())
    })?;
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob0").unwrap();

    // Charlie also deletes the context.
    repo_charlie.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(charlie).unwrap();
        Ok(())
    })?;
    record_all(
        &mut repo_charlie,
        &changes,
        &mut txn,
        &mut channel_charlie,
        "",
    )?;
    debug_to_file(&txn, &channel_charlie.borrow(), "debug_charlie0").unwrap();

    // Alice applies Bob's change
    apply::apply_change(&changes, &mut txn, &mut channel_alice, &bob_h)?;
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf = Vec::new();
    repo_alice.read_file("file", &mut buf)?;
    debug!("alice = {:?}", std::str::from_utf8(&buf));
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice1").unwrap();

    // Alice solves the conflict.
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| Ok(w.write_all(alice_bob)?))?;

    let resolution = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice2").unwrap();

    // Bob applies Alice's edits and resolution.
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &alice_h)?;
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &resolution)?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob1").unwrap();

    // Charlie applies all changes
    /*output::output_repository_no_pending(
    &mut repo_charlie,
    &changes,
    &mut txn,
    &mut channel_charlie,
    "",
    )?;*/
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &bob_h)?;
    debug_to_file(&txn, &channel_charlie.borrow(), "debug_charlie1").unwrap();
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &alice_h)?;
    debug_to_file(&txn, &channel_charlie.borrow(), "debug_charlie2").unwrap();
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &resolution)?;
    debug_to_file(&txn, &channel_charlie.borrow(), "debug_charlie3").unwrap();

    Ok(())
}

#[test]
fn cyclic_conflict_resolution() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\n";
    let alice = b"a\nx\ny\nz\nb\n";
    let bob = b"a\nu\nv\nw\nb\n";
    let charlie = b"a\nU\nV\nW\nb\n";

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel_alice = txn.open_or_create_channel("alice")?;
    txn.add_file("file")?;
    let init_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_init").unwrap();

    // Bob clones
    let mut repo_bob = working_copy::memory::Memory::new();
    let mut channel_bob = txn.open_or_create_channel("bob")?;
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &init_h)?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
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

    // Charlie clones and makes something independent.
    let mut repo_charlie = working_copy::memory::Memory::new();
    let mut channel_charlie = txn.open_or_create_channel("charlie")?;
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &init_h)?;
    output::output_repository_no_pending(
        &mut repo_charlie,
        &changes,
        &mut txn,
        &mut channel_charlie,
        "",
        true,
        None,
    )?;
    repo_charlie.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(charlie).unwrap();
        Ok(())
    })?;
    let charlie_h = record_all(
        &mut repo_charlie,
        &changes,
        &mut txn,
        &mut channel_charlie,
        "",
    )?;
    info!("Done outputting Charlie's working_copy");
    {
        let mut buf = Vec::new();
        repo_charlie.read_file("file", &mut buf).unwrap();
        info!("Charlie = {:?}", std::str::from_utf8(&buf));
    }

    // Bob edits and records
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(bob).unwrap();
        Ok(())
    })?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice").unwrap();
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob").unwrap();
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob0").unwrap();

    // Alice edits and records
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(alice).unwrap();
        Ok(())
    })?;
    let alice_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice0").unwrap();

    // Alice applies Bob's change
    apply::apply_change(&changes, &mut txn, &mut channel_alice, &bob_h)?;
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf = Vec::new();
    repo_alice.read_file("file", &mut buf)?;
    debug!("alice: {:?}", std::str::from_utf8(&buf));
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice1").unwrap();

    // Alice solves the conflict.
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        for l in conflict.iter() {
            if l.len() < 10 {
                writeln!(w, "{}", l)?
            }
        }
        Ok(())
    })?;
    info!("resolving");
    let alices_resolution =
        record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice2").unwrap();

    // Bob applies Alice's change
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &alice_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob1").unwrap();
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    debug!("bob: {:?}", std::str::from_utf8(&buf));
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        for l in conflict.iter() {
            if l.len() < 10 {
                writeln!(w, "{}", l)?
            }
        }
        Ok(())
    })?;
    info!("resolving");
    let _bobs_resolution = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;

    // Bob applies Alice's resolution
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &alices_resolution).unwrap();
    // Bob applies Charlie's side
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &charlie_h).unwrap();
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob2").unwrap();
    debug!("outputting bob2");
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob3").unwrap();
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    // Check that there is a conflict.
    assert!(std::str::from_utf8(&buf)?.lines().any(|l| l.len() >= 10));
    debug!("{:?}", std::str::from_utf8(&buf));
    // Solve it again, in the same way and output the result.
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        for l in conflict.iter() {
            if l.len() < 10 {
                writeln!(w, "{}", l)?
            }
        }
        Ok(())
    })?;
    debug!("resolving again");
    let second_resolution = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob4").unwrap();

    // Check that the conflict is gone.
    assert!(std::str::from_utf8(&buf)?.lines().all(|l| l.len() < 10));

    // Unrecord
    crate::unrecord::unrecord(&mut txn, &mut channel_bob, &changes, &second_resolution).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob5").unwrap();
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;

    // Check that the conflict is back.
    assert!(std::str::from_utf8(&buf)?.lines().any(|l| l.len() >= 10));

    Ok(())
}

#[test]
fn cyclic_zombies() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\n";
    let alice = b"a\nx\ny\nz\nb\nc\n";
    let alice2 = b"a\nx\nX\ny\nz\nb\nc\n";
    let alice3 = b"a\nx\nX\nY\ny\nz\nb\nc\n";
    let bob = b"a\nu\nv\nw\nb\nc\n";
    let bob2 = b"a\nu\nU\nv\nw\nb\nc\n";
    let bob3 = b"a\nu\nU\nV\nv\nw\nb\nc\n";
    let charlie = b"a\nc\n";

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel_alice = txn.open_or_create_channel("alice")?;
    txn.add_file("file")?;
    let init_h = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_init").unwrap();

    // Bob clones
    let mut repo_bob = working_copy::memory::Memory::new();
    let mut channel_bob = txn.open_or_create_channel("bob")?;
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &init_h)?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
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
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(bob).unwrap();
        Ok(())
    })?;
    let bob_h1 = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(bob2).unwrap();
        Ok(())
    })?;
    let bob_h2 = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(bob3).unwrap();
        Ok(())
    })?;
    let bob_h3 = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob0").unwrap();

    // Alice edits and records
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(alice).unwrap();
        Ok(())
    })?;
    let alice_h1 = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(alice2).unwrap();
        Ok(())
    })?;
    let alice_h2 = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(alice3).unwrap();
        Ok(())
    })?;
    let alice_h3 = record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice0").unwrap();

    // Alice applies Bob's change
    apply::apply_change(&changes, &mut txn, &mut channel_alice, &bob_h1)?;
    apply::apply_change(&changes, &mut txn, &mut channel_alice, &bob_h2)?;
    apply::apply_change(&changes, &mut txn, &mut channel_alice, &bob_h3)?;
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf = Vec::new();
    repo_alice.read_file("file", &mut buf)?;
    debug!("alice: {:?}", std::str::from_utf8(&buf));
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice1").unwrap();

    // Alice solves the conflict.
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        for l in conflict.iter() {
            if l.len() < 10 {
                writeln!(w, "{}", l)?
            }
        }
        Ok(())
    })?;
    info!("resolving");
    let alices_resolution =
        record_all(&mut repo_alice, &changes, &mut txn, &mut channel_alice, "")?;
    debug_to_file(&txn, &channel_alice.borrow(), "debug_alice2").unwrap();

    // Bob applies Alice's change
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &alice_h1).unwrap();
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &alice_h2).unwrap();
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &alice_h3).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob1").unwrap();
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;
    debug!("bob: {:?}", std::str::from_utf8(&buf));
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        for l in conflict.iter() {
            if l.len() < 10 {
                writeln!(w, "{}", l)?
            }
        }
        Ok(())
    })?;
    info!("resolving");
    // Bob solves the conflict
    let bobs_resolution = record_all(&mut repo_bob, &changes, &mut txn, &mut channel_bob, "")?;

    // Charlie clones and deletes
    let mut repo_charlie = working_copy::memory::Memory::new();
    let mut channel_charlie = txn.open_or_create_channel("charlie")?;
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &init_h)?;
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &alice_h1)?;
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &alice_h2)?;
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &alice_h3)?;
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &bob_h1)?;
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &bob_h2)?;
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &bob_h3)?;
    output::output_repository_no_pending(
        &mut repo_charlie,
        &changes,
        &mut txn,
        &mut channel_charlie,
        "",
        true,
        None,
    )?;
    repo_charlie.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(charlie).unwrap();
        Ok(())
    })?;
    let charlie_h = record_all(
        &mut repo_charlie,
        &changes,
        &mut txn,
        &mut channel_charlie,
        "",
    )?;

    // Bob applies Alice's resolution
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &alices_resolution).unwrap();
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob2").unwrap();
    debug!("outputting bob2");
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob3").unwrap();
    buf.clear();
    repo_bob.read_file("file", &mut buf)?;

    // Bob applies Charlie's side
    debug!("applying charlie's patch");
    apply::apply_change(&changes, &mut txn, &mut channel_bob, &charlie_h).unwrap();
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob4").unwrap();
    let (alive_, reachable_) = check_alive(&txn, &channel_bob.borrow().graph);
    if !alive_.is_empty() {
        error!("alive (bob0): {:?}", alive_);
    }
    if !reachable_.is_empty() {
        error!("reachable (bob0): {:?}", reachable_);
    }
    debug!("outputting bob's repo");
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_bob.borrow(), "debug_bob5").unwrap();
    let (alive, reachable) = check_alive(&txn, &channel_bob.borrow().graph);
    if !alive.is_empty() {
        panic!("alive (bob1): {:?}", alive);
    } else if !alive_.is_empty() {
        panic!("alive_ (bob1): {:?}", alive_);
    }
    if !reachable.is_empty() {
        panic!("reachable (bob1): {:?}", reachable);
    } else if !reachable_.is_empty() {
        panic!("reachable_ (bob1): {:?}", reachable_);
    }

    // Symmetric: Charlie applies the other sides.
    debug!("Charlie applies");
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &alices_resolution).unwrap();
    apply::apply_change(&changes, &mut txn, &mut channel_charlie, &bobs_resolution).unwrap();
    debug_to_file(&txn, &channel_charlie.borrow(), "debug_charlie").unwrap();
    let (alive, reachable) = check_alive(&txn, &channel_charlie.borrow().graph);
    if !alive.is_empty() {
        panic!("alive (charlie0): {:?}", alive);
    }
    if !reachable.is_empty() {
        panic!("reachable (charlie0): {:?}", reachable);
    }
    output::output_repository_no_pending(
        &mut repo_charlie,
        &changes,
        &mut txn,
        &mut channel_charlie,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel_charlie.borrow(), "debug_charlie1").unwrap();

    let (alive, reachable) = check_alive(&txn, &channel_charlie.borrow().graph);
    if !alive.is_empty() {
        panic!("alive (charlie1): {:?}", alive);
    }
    if !reachable.is_empty() {
        panic!("reachable (charlie1): {:?}", reachable);
    }

    Ok(())
}

#[test]
fn cyclic_files() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\n";

    let mut repo_alice = working_copy::memory::Memory::new();
    let mut repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("a/file", contents.to_vec());
    repo_alice.add_file("b/file", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();
    let mut channel_alice = txn_alice.open_or_create_channel("alice")?;
    txn_alice.add_file("a/file")?;
    txn_alice.add_file("b/file")?;
    let init_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_init").unwrap();

    // Bob clones and moves a -> a/b
    let mut channel_bob = txn_bob.open_or_create_channel("bob")?;
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
    txn_bob.move_file("a", "b/a").unwrap();
    repo_bob.rename("a", "b/a").unwrap();
    let ab = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "")?;

    // Alice moves b -> b/a
    txn_alice.move_file("b", "a/b").unwrap();
    repo_alice.rename("b", "a/b").unwrap();
    let _ba = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &ab)?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug").unwrap();
    debug!("outputting cycle");
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;

    let v: Vec<_> = txn_alice.iter_working_copy().collect();
    println!("{:?}", v);
    let (alive, reachable) = check_alive(&txn_alice, &channel_alice.borrow().graph);
    if !alive.is_empty() {
        panic!("alive: {:?}", alive);
    }
    if !reachable.is_empty() {
        panic!("reachable: {:?}", reachable);
    }
    debug!("recording the resolution");
    let _resolution = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug2").unwrap();
    Ok(())
}

#[test]
fn tree_inodes_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\n";
    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("a/b/file", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let mut channel_alice = txn_alice.open_or_create_channel("alice")?;
    txn_alice.add_file("a/b/file")?;

    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();
    let mut channel_bob = txn_bob.open_or_create_channel("bob")?;
    txn_bob.add_file("a/b/file")?;

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

    // Bob moves and deletes a/b
    repo_bob.rename("a/b/file", "c/d/file")?;
    txn_bob.move_file("a/b/file", "c/d/file")?;
    repo_bob.remove_path("a")?;
    txn_bob.remove_file("a")?;
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "")?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob0").unwrap();

    // Alice applies
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

    check_tree_inodes(&txn_alice, &channel_alice.borrow());
    Ok(())
}

fn check_tree_inodes<T: TxnT>(txn: &T, channel: &T::Channel) {
    // Sanity check
    for x in txn.iter_inodes().unwrap() {
        let (&inode, &vertex) = x.unwrap();
        debug!("inode = {:?}, vertex = {:?}", inode, vertex);
        let mut inode_ = inode;
        while !inode_.is_root() {
            if let Some(next) = txn.get_revtree(&inode_, None).unwrap() {
                debug!("next = {:?}", next);
                inode_ = next.parent_inode;
            } else {
                panic!("inode = {:?}, inode_ = {:?}", inode, inode_);
            }
        }
        if !is_alive(txn, T::graph(txn, &channel), &vertex.inode_vertex()).unwrap() {
            for e in iter_adjacent(
                txn,
                T::graph(txn, &channel),
                vertex.inode_vertex(),
                EdgeFlags::empty(),
                EdgeFlags::all(),
            )
            .unwrap()
            {
                error!("{:?} {:?} {:?}", inode, vertex, e)
            }
            panic!(
                "inode {:?}, vertex {:?}, is not alive, {:?}",
                inode,
                vertex,
                tree_path(txn, &vertex)
            )
        }
    }
}
