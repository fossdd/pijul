use super::*;

/// Add a simple file and clone.
#[test]
fn add_file_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("dir/file", b"a\nb\nc\nd\ne\nf\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;

    {
        let mut txn = env.mut_txn_begin().unwrap();
        txn.add_file("dir/file").unwrap();

        let mut channel = txn.open_or_create_channel("main").unwrap();
        record_all(&mut repo, &changes, &mut txn, &mut channel, "").unwrap();

        // Adding the inode another time.
        assert!(txn.add_file("dir/file").is_err());
        debug_to_file(&txn, &channel, "debug").unwrap();
        txn.commit().unwrap();
    }
    {
        let txn = env.txn_begin()?;
        let files: Vec<_> = crate::fs::iter_working_copy(&txn, Inode::ROOT)
            .map(|n| n.unwrap().1)
            .collect();
        assert_eq!(files, vec!["dir", "dir/file"]);

        let channel_ = txn.load_channel("main").unwrap().unwrap();
        let channel = channel_.lock().unwrap();
        let mut it =
            crate::fs::iter_graph_children(&txn, &changes, &channel.graph, Position::ROOT).unwrap();
        let (key, _, meta, file) = it.next().unwrap().unwrap();
        assert!(meta.is_dir());
        assert_eq!(file, "dir");
        assert!(it.next().is_none());
        let mut it = crate::fs::iter_graph_children(&txn, &changes, &channel.graph, key).unwrap();
        let (file_key, _, _, _) = it.next().unwrap().unwrap();
        crate::fs::iter_paths(&txn, &channel.graph, file_key, |path| {
            debug!("begin path");
            for path in path {
                debug!("path = {:?}", path);
            }
            debug!("end path");
            true
        })
        .unwrap();
        debug_to_file(&txn, &channel_, "debug2").unwrap();

        let mut it = crate::fs::iter_basenames(&txn, &changes, &channel.graph, key).unwrap();
        let (key, _, name) = it.next().unwrap().unwrap();
        assert_eq!(key, Position::ROOT);
        assert_eq!(name, "dir");
        assert!(it.next().is_none());
        assert!(txn.is_tracked("dir/file").unwrap());
    }
    Ok(())
}

/// Test that we can add a directory with a file in it.
#[test]
fn add_dir_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo = working_copy::memory::Memory::new();
    repo.add_file("dir/file", b"a\nb\nc\nd\ne\nf\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn = env.mut_txn_begin().unwrap();
    txn.add_dir("dir/file")?;
    assert!(txn.is_tracked("dir").unwrap());
    assert!(txn.is_tracked("dir/file").unwrap());

    let (name, inode) = crate::fs::working_copy_children(&txn, Inode::ROOT)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(name.as_str(), "dir");
    assert!(txn.is_directory(inode).unwrap());
    debug!("name = {:?}", inode);
    debug_tree(&txn, "debug_tree")?;
    let mut it = crate::fs::working_copy_children(&txn, inode).unwrap();
    let (name, _) = it.next().unwrap().unwrap();
    assert_eq!(name.as_str(), "file");
    assert!(it.next().is_none());

    Ok(())
}

/// Test that we can delete a file.
#[test]
fn del_file_test() {
    env_logger::try_init().unwrap_or(());

    let mut repo = Arc::new(working_copy::memory::Memory::new());
    let changes = Arc::new(changestore::memory::Memory::new());

    let env = pristine::sanakirja::Pristine::new_anon().unwrap();
    {
        let mut txn = Arc::new(RwLock::new(env.mut_txn_begin().unwrap()));
        txn.write().unwrap().add_file("dir/file").unwrap();
        let mut channel = txn.write().unwrap().open_or_create_channel("main").unwrap();

        repo.add_file("dir/file", b"a\nb\nc\nd\ne\nf\n".to_vec());
        record_all_output(
            repo.clone(),
            changes.clone(),
            txn.clone(),
            &channel.clone(),
            "",
        )
        .unwrap();
        debug_to_file(&*txn.read().unwrap(), &channel, "debug0").unwrap();
        let files: Vec<_> = crate::fs::iter_working_copy(&*txn.read().unwrap(), Inode::ROOT)
            .map(|f| f.unwrap().1)
            .collect();
        assert_eq!(files, vec!["dir", "dir/file"]);

        repo.remove_path("dir/file").unwrap();
        txn.write().unwrap().remove_file("dir").unwrap();

        let files: Vec<_> = crate::fs::iter_working_copy(&*txn.read().unwrap(), Inode::ROOT)
            .map(|n| n.unwrap().1)
            .collect();
        debug!("files = {:?}", files);
        assert!(files.is_empty());

        record_all_output(repo, changes, txn, &channel, "").unwrap();
        debug_to_file(&*txn.read().unwrap(), &channel, "debug").unwrap();

        let files: Vec<_> = crate::fs::iter_working_copy(&*txn.read().unwrap(), Inode::ROOT)
            .map(|n| n.unwrap().1)
            .collect();
        debug!("files = {:?}", files);
        assert!(files.is_empty());

        // Test deletions without recording.
        txn.write().unwrap().add_file("dir2/file").unwrap();
        txn.write().unwrap().remove_file("dir2").unwrap();
        assert!(
            crate::fs::iter_working_copy(&*txn.read().unwrap(), Inode::ROOT)
                .all(|f| f.unwrap().1 != "dir2")
        );
        assert!(txn.write().unwrap().remove_file("dir2").is_err());
        txn.write().unwrap().commit().unwrap();
    }

    {
        let txn = env.txn_begin().unwrap();
        let files: Vec<_> = crate::fs::iter_working_copy(&txn, Inode::ROOT)
            .map(|n| n.unwrap().1)
            .collect();
        debug!("files = {:?}", files);
        assert!(files.is_empty());
    }
}

/// Test that `record` notices the deletion of a file.
#[test]
fn del_obsolete_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo = Arc::new(working_copy::memory::Memory::new());
    let changes = Arc::new(changestore::memory::Memory::new());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = Arc::new(RwLock::new(env.mut_txn_begin().unwrap()));
    txn.write().unwrap().add_file("a/b/c/d/e")?;
    let mut channel = txn.write().unwrap().open_or_create_channel("main")?;

    repo.add_file("a/b/c/d/e", b"a\nb\nc\nd\ne\nf\n".to_vec());
    record_all_output(repo.clone(), changes.clone(), txn.clone(), &channel, "")?;
    debug_to_file(&*txn.read().unwrap(), &channel, "debug0").unwrap();
    let files: Vec<_> = crate::fs::iter_working_copy(&*txn.read().unwrap(), Inode::ROOT)
        .map(|f| f.unwrap().1)
        .collect();
    assert_eq!(files, vec!["a", "a/b", "a/b/c", "a/b/c/d", "a/b/c/d/e"]);

    repo.remove_path("a/b/c")?;
    debug!("Recording the deletion");
    record_all_output(repo.clone(), changes.clone(), txn.clone(), &channel, "")?;
    debug_to_file(&*txn.read().unwrap(), &channel, "debug").unwrap();

    let mut repo2 = Arc::new(working_copy::memory::Memory::new());
    output::output_repository_no_pending(
        repo2,
        changes,
        txn.clone(),
        channel.clone(),
        "",
        true,
        None,
        1,
    )?;
    assert_eq!(repo2.list_files(), vec!["a", "a/b"]);

    let files: Vec<_> = crate::fs::iter_working_copy(&*txn.read().unwrap(), Inode::ROOT)
        .map(|n| n.unwrap().1)
        .collect();
    debug!("files = {:?}", files);
    assert_eq!(files, vec!["a", "a/b"]);
    Ok(())
}

/// Test that we can delete the end of a file.
#[test]
fn del_eof_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo = Arc::new(working_copy::memory::Memory::new());
    let changes = Arc::new(changestore::memory::Memory::new());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = Arc::new(RwLock::new(env.mut_txn_begin().unwrap()));
    txn.write().unwrap().add_file("dir/file")?;
    let mut channel = txn.write().unwrap().open_or_create_channel("main")?;

    repo.add_file("dir/file", b"a\nb\nc\nd\ne\nf\n".to_vec());
    record_all_output(repo.clone(), changes.clone(), txn.clone(), &channel, "").unwrap();
    debug_to_file(&*txn.read().unwrap(), &channel, "debug").unwrap();

    repo.write_file("dir/file")
        .unwrap()
        .write_all(b"a\nb\nc\n")
        .unwrap();
    record_all_output(repo.clone(), changes.clone(), txn.clone(), &channel, "").unwrap();
    let mut file = Vec::new();
    repo.read_file("dir/file", &mut file).unwrap();
    assert_eq!(std::str::from_utf8(&file), Ok("a\nb\nc\n"));
    debug_to_file(&txn, &channel.borrow(), "debug").unwrap();
    txn.write().unwrap().commit()?;
    Ok(())
}

/// Just delete a few lines of a file.
#[test]
fn del_nonzombie_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    txn.write().unwrap().add_file("dir/file")?;
    let mut channel = txn.write().unwrap().open_or_create_channel("main")?;

    repo.add_file("dir/file", b"a\nb\nc\nd\ne\nf\n".to_vec());
    record_all_output(&mut repo, &changes, &mut txn, &mut channel, "")?;

    repo.write_file::<_, std::io::Error, _>("dir/file", |w| {
        w.write_all(b"a\nb\nc\ne\nf\n")?;
        Ok(())
    })?;
    record_all_output(&mut repo, &changes, &mut txn, &mut channel, "")?;
    repo.write_file::<_, std::io::Error, _>("dir/file", |w| {
        w.write_all(b"a\nb\nc\nf\n")?;
        Ok(())
    })?;
    debug_to_file(&txn, &channel.borrow(), "debug0").unwrap();
    record_all_output(&mut repo, &changes, &mut txn, &mut channel, "")?;
    debug_to_file(&txn, &channel.borrow(), "debug1").unwrap();
    repo.write_file::<_, std::io::Error, _>("dir/file", |w| {
        w.write_all(b"a\nb\nc\n")?;
        Ok(())
    })?;
    record_all_output(&mut repo, &changes, &mut txn, &mut channel, "")?;
    let mut file = Vec::new();
    repo.read_file("dir/file", &mut file).unwrap();
    assert_eq!(std::str::from_utf8(&file), Ok("a\nb\nc\n"));
    debug_to_file(&txn, &channel.borrow(), "debug2").unwrap();
    txn.write().unwrap().commit()?;
    Ok(())
}

/// Are permissions properly recorded?
#[test]
fn permissions_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", b"a\nb\nc\nd\ne\nf\n".to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    txn_alice.add_file("file")?;

    let mut channel = txn_alice.open_or_create_channel("main")?;
    let alice0 = record_all(&mut repo_alice, &changes, &mut txn_alice, &mut channel, "")?;
    debug_to_file(&txn_alice, &channel.borrow(), "debug0").unwrap();

    repo_alice.set_permissions("file", 0o755)?;
    let alice1 = record_all(&mut repo_alice, &changes, &mut txn_alice, &mut channel, "")?;
    debug_to_file(&txn_alice, &channel.borrow(), "debug1").unwrap();

    let mut repo_bob = working_copy::memory::Memory::new();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();
    let mut channel = txn_bob.open_or_create_channel("main")?;
    apply::apply_change(&changes, &mut txn_bob, &mut channel, &alice0)?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_bob, &channel.borrow(), "debug_bob1").unwrap();
    let bob_perm = repo_bob.file_metadata("file")?;
    assert_eq!(bob_perm.0, 0);

    apply::apply_change(&changes, &mut txn_bob, &mut channel, &alice1)?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_bob, &channel.borrow(), "debug_bob2").unwrap();
    let bob_perm = repo_bob.file_metadata("file")?;
    assert_eq!(bob_perm.0, 0o100);
    let alice_perm = repo_alice.file_metadata("file")?;
    assert_eq!(alice_perm.0, 0o100);
    Ok(())
}

/// Move a file to a directory, then delete the file and clone the whole thing.
#[test]
fn move_file_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", b"a\nb\nc\nd\ne\nf\n".to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    txn_alice.add_file("file")?;

    let mut channel = txn_alice.open_or_create_channel("main")?;
    let alice0 = record_all(&mut repo_alice, &changes, &mut txn_alice, &mut channel, "")?;
    debug!("alice0 = {:?}", alice0);
    debug_to_file(&txn_alice, &channel.borrow(), "debug0").unwrap();
    txn_alice.add_dir("dir")?;
    txn_alice.move_file("file", "dir/file2")?;

    repo_alice.add_dir("dir");
    repo_alice.rename("file", "dir/file2")?;
    debug_tree(&txn_alice, "debug_tree")?;
    let alice1 = record_all(&mut repo_alice, &changes, &mut txn_alice, &mut channel, "")?;
    debug!("alice1 = {:?}", alice1);
    debug_to_file(&txn_alice, &channel.borrow(), "debug1").unwrap();
    debug_tree(&txn_alice, "debug_tree")?;
    debug_inodes(&txn_alice);
    debug!("{:?}", repo_alice);

    repo_alice.remove_path("dir/file2")?;
    debug!("{:?}", repo_alice);
    let alice2 = record_all(&mut repo_alice, &changes, &mut txn_alice, &mut channel, "")?;
    debug_to_file(&txn_alice, &channel.borrow(), "debug2").unwrap();
    txn_alice.commit()?;

    let mut repo_bob = working_copy::memory::Memory::new();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();
    let mut channel = txn_bob.open_or_create_channel("main")?;
    apply::apply_change(&changes, &mut txn_bob, &mut channel, &alice0)?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel,
        "",
        true,
        None,
    )?;
    assert_eq!(repo_bob.list_files(), &["file"]);

    apply::apply_change(&changes, &mut txn_bob, &mut channel, &alice1)?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel,
        "",
        true,
        None,
    )?;
    let mut files = repo_bob.list_files();
    files.sort();
    assert_eq!(files, &["dir", "dir/file2"]);

    apply::apply_change(&changes, &mut txn_bob, &mut channel, &alice2)?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel,
        "",
        true,
        None,
    )?;
    assert_eq!(repo_bob.list_files(), &["dir"]);

    Ok(())
}

/// Overwrite a file with a move.
#[test]
fn move_file_existing_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", b"a\nb\nc\nd\ne\nf\n".to_vec());
    repo_alice.add_file("file2", b"a\nb\nc\nd\ne\nf\n".to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    txn_alice.add_file("file")?;
    txn_alice.add_file("file2")?;

    let mut channel = txn_alice.open_or_create_channel("main")?;
    record_all(&mut repo_alice, &changes, &mut txn_alice, &mut channel, "")?;
    txn_alice.move_file("file", "file2")?;
    repo_alice.rename("file", "file2")?;
    record_all(&mut repo_alice, &changes, &mut txn_alice, &mut channel, "")?;
    debug_to_file(&txn_alice, &channel.borrow(), "debug1").unwrap();
    let mut files = repo_alice.list_files();
    files.sort();
    assert_eq!(files, &["file2"]);
    Ok(())
}

#[test]
fn move_back_delete_test() -> Result<(), anyhow::Error> {
    move_back_test_(true)
}

#[test]
fn move_back_test() -> Result<(), anyhow::Error> {
    move_back_test_(false)
}

fn move_back_test_(resolve_by_deleting: bool) -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("a", b"a\nb\nc\nd\ne\nf\n".to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    txn_alice.add_file("a")?;

    let mut channel = txn_alice.open_or_create_channel("main")?;
    let alice1 = record_all(&mut repo_alice, &changes, &mut txn_alice, &mut channel, "")?;
    // Alice moves a -> b
    txn_alice.move_file("a", "b")?;
    repo_alice.rename("a", "b")?;
    let alice2 = record_all(&mut repo_alice, &changes, &mut txn_alice, &mut channel, "")?;
    debug_to_file(&txn_alice, &channel.borrow(), "debug1").unwrap();

    // Alice moves b back -> a
    txn_alice.move_file("b", "a")?;
    repo_alice.rename("b", "a")?;
    let alice3 = record_all(&mut repo_alice, &changes, &mut txn_alice, &mut channel, "")?;
    debug_to_file(&txn_alice, &channel.borrow(), "debug2").unwrap();

    // Bob deletes in parallel to the move + moveback
    let mut repo_bob = working_copy::memory::Memory::new();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();
    let mut channel_bob = txn_bob.open_or_create_channel("main")?;
    txn_bob
        .apply_change(&changes, &mut channel_bob, &alice1)
        .unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    repo_bob.remove_path("a")?;
    let bob1 = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "")?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1").unwrap();
    txn_bob
        .apply_change(&changes, &mut channel_bob, &alice2)
        .unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob2").unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel,
        "",
        true,
        None,
    )?;
    debug!("APPLYING {:?}", alice3);
    txn_bob
        .apply_change(&changes, &mut channel_bob, &alice3)
        .unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob3").unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel,
        "",
        true,
        None,
    )?;

    if resolve_by_deleting {
        debug!("Bob records a solution");
        let bob2 = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "")?;
        debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob4").unwrap();

        // Alice applies Bob's patch.
        txn_alice
            .apply_change(&changes, &mut channel, &bob1)
            .unwrap();
        debug_to_file(&txn_alice, &channel.borrow(), "debug_alice2").unwrap();

        let conflicts = output::output_repository_no_pending(
            &mut repo_alice,
            &changes,
            &mut txn_alice,
            &mut channel,
            "",
            true,
            None,
        )?;
        debug!("conflicts = {:?}", conflicts);
        assert!(!conflicts.is_empty());

        // Alice applies Bob's resolution
        txn_alice
            .apply_change(&changes, &mut channel, &bob2)
            .unwrap();
        debug_to_file(&txn_alice, &channel.borrow(), "debug_alice3").unwrap();
        let conflicts = output::output_repository_no_pending(
            &mut repo_alice,
            &changes,
            &mut txn_alice,
            &mut channel,
            "",
            true,
            None,
        )?;
        debug!("conflicts = {:?}", conflicts);
        assert!(conflicts.is_empty());

        // Testing Bob's tree by outputting
        let conflicts = output::output_repository_no_pending(
            &mut repo_bob,
            &changes,
            &mut txn_bob,
            &mut channel_bob,
            "",
            true,
            None,
        )?;
        debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob4").unwrap();
        debug!("conflicts = {:?}", conflicts);
        assert!(conflicts.is_empty());
    } else {
        output::output_repository_no_pending(
            &mut repo_bob,
            &changes,
            &mut txn_bob,
            &mut channel_bob,
            "",
            true,
            None,
        )?;

        debug!("Bob records a solution");
        let bob2 = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "")?;

        // Alice applies Bob's patch.
        txn_alice
            .apply_change(&changes, &mut channel, &bob1)
            .unwrap();
        debug_to_file(&txn_alice, &channel.borrow(), "debug_alice2").unwrap();

        let conflicts = output::output_repository_no_pending(
            &mut repo_alice,
            &changes,
            &mut txn_alice,
            &mut channel,
            "",
            true,
            None,
        )?;
        debug!("conflicts = {:?}", conflicts);
        assert_eq!(conflicts.len(), 1);
        match conflicts[0] {
            Conflict::ZombieFile { ref path } => assert_eq!(path, "a"),
            ref c => panic!("unexpected conflict {:#?}", c),
        }

        // Alice applies Bob's resolution
        txn_alice
            .apply_change(&changes, &mut channel, &bob2)
            .unwrap();
        let conflicts = output::output_repository_no_pending(
            &mut repo_alice,
            &changes,
            &mut txn_alice,
            &mut channel,
            "",
            true,
            None,
        )?;
        debug!("conflicts = {:?}", conflicts);
        assert!(conflicts.is_empty());

        // Testing Bob's tree by outputting
        let conflicts = output::output_repository_no_pending(
            &mut repo_bob,
            &changes,
            &mut txn_bob,
            &mut channel_bob,
            "",
            true,
            None,
        )?;
        debug!("conflicts = {:?}", conflicts);
        assert!(conflicts.is_empty());
    }
    Ok(())
}

// Move a file into a directory, and delete the former parent in the same change.
#[test]
fn move_delete_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("dir/file", b"a\nb\nc\nd\ne\nf\n".to_vec());
    repo_alice.add_file("dir/file2", b"a\nb\nc\nd\ne\nf\n".to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    txn_alice.add_file("dir/file")?;
    txn_alice.add_file("dir/file2")?;

    let mut channel = txn_alice.open_or_create_channel("main")?;
    let alice0 = record_all(&mut repo_alice, &changes, &mut txn_alice, &mut channel, "")?;
    debug!("alice0 = {:?}", alice0);
    debug_to_file(&txn_alice, &channel.borrow(), "debug0").unwrap();
    repo_alice.add_dir("dir2");
    repo_alice.rename("dir/file", "dir2/file")?;
    repo_alice.rename("dir/file2", "dir2/file2")?;
    repo_alice.remove_path("dir")?;
    txn_alice.move_file("dir/file", "dir2/file")?;
    txn_alice.move_file("dir/file2", "dir2/file2")?;

    let alice1 = record_all(&mut repo_alice, &changes, &mut txn_alice, &mut channel, "")?;
    debug!("alice1 = {:?}", alice1);
    debug_to_file(&txn_alice, &channel.borrow(), "debug1").unwrap();
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_alice, &channel.borrow(), "debug2").unwrap();

    repo_alice.rename("dir2/file", "dir/file").unwrap_or(());
    repo_alice.rename("dir2/file2", "dir/file2").unwrap_or(());
    txn_alice.move_file("dir2/file", "dir/file").unwrap_or(());
    txn_alice.move_file("dir2/file2", "dir/file2").unwrap_or(());
    repo_alice.remove_path("dir2")?;

    let mut state = Builder::new();
    debug!("recording in dir");
    state.record(
        &mut txn_alice,
        Algorithm::default(),
        &mut channel.borrow_mut(),
        &mut repo_alice,
        &changes,
        "dir",
    )?;
    debug!("recording in dir2");
    state.record(
        &mut txn_alice,
        Algorithm::default(),
        &mut channel.borrow_mut(),
        &mut repo_alice,
        &changes,
        "dir2",
    )?;

    let rec = state.finish();
    let changes_ = rec
        .actions
        .into_iter()
        .map(|rec| rec.globalize(&txn_alice).unwrap())
        .collect();
    let alice2 = crate::change::Change::make_change(
        &txn_alice,
        &channel,
        changes_,
        rec.contents,
        crate::change::ChangeHeader {
            message: "test".to_string(),
            authors: vec![],
            description: None,
            timestamp: Utc::now(),
        },
        Vec::new(),
    )
    .unwrap();
    let h_alice2 = changes.save_change(&alice2)?;
    apply::apply_local_change(
        &mut txn_alice,
        &mut channel,
        &alice2,
        &h_alice2,
        &rec.updatables,
    )?;

    debug!("done {:?}", h_alice2);
    debug_to_file(&txn_alice, &channel.borrow(), "debug3").unwrap();

    let (alive, reachable) = check_alive(&txn_alice, &channel.borrow().graph);
    if !alive.is_empty() {
        panic!("alive: {:?}", alive);
    }
    if !reachable.is_empty() {
        panic!("reachable: {:?}", reachable);
    }

    let mut files = repo_alice.list_files();
    files.sort();
    assert_eq!(files, &["dir", "dir/file", "dir/file2"]);

    Ok(())
}

#[test]
fn file_becomes_dir_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("filedir", b"a\nb\nc\nd\ne\nf\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    txn.write().unwrap().add_file("filedir").unwrap();

    let mut channel = txn.write().unwrap().open_or_create_channel("main").unwrap();
    record_all(&mut repo, &changes, &mut txn, &mut channel, "").unwrap();

    repo.remove_path("filedir").unwrap();
    repo.add_file("filedir/file", b"a\nb\nc\nd\ne\nf\n".to_vec());
    txn.write().unwrap().add_file("filedir/file").unwrap();
    record_all(&mut repo, &changes, &mut txn, &mut channel, "").unwrap();
    debug_to_file(&txn, &channel.borrow(), "debug").unwrap();

    Ok(())
}

#[test]
fn record_deleted_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    {
        let mut txn = env.mut_txn_begin().unwrap();
        txn.write().unwrap().add_file("dir/file")?;
        let mut channel = txn.write().unwrap().open_or_create_channel("main")?;
        record_all_output(&mut repo, &changes, &mut txn, &mut channel, "")?;
        debug_to_file(&txn, &channel.borrow(), "debug").unwrap();
        let files: Vec<_> = crate::fs::iter_working_copy(&txn, Inode::ROOT)
            .map(|n| n.unwrap().1)
            .collect();
        assert!(files.is_empty());
    }
    Ok(())
}

#[test]
fn record_prefix() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    {
        let mut txn = env.mut_txn_begin().unwrap();
        let mut channel = txn.write().unwrap().open_or_create_channel("main")?;
        record_all_output(&mut repo, &changes, &mut txn, &mut channel, "")?;
        debug_to_file(&txn, &channel.borrow(), "debug").unwrap();
        let files: Vec<_> = crate::fs::iter_working_copy(&txn, Inode::ROOT)
            .map(|n| n.unwrap().1)
            .collect();
        assert!(files.is_empty());
    }
    Ok(())
}

#[test]
fn record_not_in_repo() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel = txn.write().unwrap().open_or_create_channel("main")?;
    assert!(record_all_output(&mut repo, &changes, &mut txn, &mut channel, "dir").is_err());
    Ok(())
}

#[test]
fn record_not_modified() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel = txn.write().unwrap().open_or_create_channel("main")?;

    repo.add_file("file", b"a\nb\nc\nd\ne\nf\n".to_vec());
    txn.write().unwrap().add_file("file")?;
    record_all_output(&mut repo, &changes, &mut txn, &mut channel, "")?;
    std::thread::sleep(std::time::Duration::from_secs(1));
    record_all_output(&mut repo, &changes, &mut txn, &mut channel, "")?;
    Ok(())
}
