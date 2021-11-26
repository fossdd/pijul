use crate::working_copy::WorkingCopyRead;

use super::*;
use std::io::Write;

/// Add a simple file and clone.
#[test]
fn add_file_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("dir/file", b"a\nb\nc\nd\ne\nf\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;

    {
        let txn = env.arc_txn_begin().unwrap();
        txn.write().add_file("dir/file", 0).unwrap();

        let channel = txn.write().open_or_create_channel("main").unwrap();
        record_all(&repo, &changes, &txn, &channel, "").unwrap();

        // Adding the inode another time.
        assert!(txn.write().add_file("dir/file", 0).is_err());
        txn.commit().unwrap()
    }
    {
        let txn = env.txn_begin()?;
        let files: Vec<_> = crate::fs::iter_working_copy(&txn, Inode::ROOT)
            .map(|n| n.unwrap().1)
            .collect();
        assert_eq!(files, vec!["dir", "dir/file"]);

        let channel_ = txn.load_channel("main").unwrap().unwrap();
        let channel = channel_.read();
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

        let mut it = crate::fs::iter_basenames(&txn, &changes, &channel.graph, key).unwrap();
        let (_, _, name) = it.next().unwrap().unwrap();
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

    let repo = working_copy::memory::Memory::new();
    repo.add_file("dir/file", b"a\nb\nc\nd\ne\nf\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn = env.mut_txn_begin().unwrap();
    txn.add_dir("dir/file", 0)?;
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

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon().unwrap();
    {
        let txn = env.arc_txn_begin().unwrap();
        txn.write().add_file("dir/file", 0).unwrap();
        let channel = txn.write().open_or_create_channel("main").unwrap();

        repo.add_file("dir/file", b"a\nb\nc\nd\ne\nf\n".to_vec());
        record_all_output(&repo, changes.clone(), &txn, &channel, "").unwrap();
        let files: Vec<_> = crate::fs::iter_working_copy(&*txn.read(), Inode::ROOT)
            .map(|f| f.unwrap().1)
            .collect();
        assert_eq!(files, vec!["dir", "dir/file"]);

        repo.remove_path("dir/file", false).unwrap();
        txn.write().remove_file("dir").unwrap();

        let files: Vec<_> = crate::fs::iter_working_copy(&*txn.read(), Inode::ROOT)
            .map(|n| n.unwrap().1)
            .collect();
        debug!("files = {:?}", files);
        assert!(files.is_empty());

        record_all_output(&repo, changes, &txn, &channel, "").unwrap();

        let files: Vec<_> = crate::fs::iter_working_copy(&*txn.read(), Inode::ROOT)
            .map(|n| n.unwrap().1)
            .collect();
        debug!("files = {:?}", files);
        assert!(files.is_empty());

        // Test deletions without recording.
        txn.write().add_file("dir2/file", 0).unwrap();
        txn.write().remove_file("dir2").unwrap();
        assert!(
            crate::fs::iter_working_copy(&*txn.read(), Inode::ROOT).all(|f| f.unwrap().1 != "dir2")
        );
        assert!(txn.write().remove_file("dir2").is_err());
        txn.commit().unwrap()
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

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let txn = env.arc_txn_begin().unwrap();
    txn.write().add_file("a/b/c/d/e", 0)?;
    let channel = txn.write().open_or_create_channel("main")?;

    repo.add_file("a/b/c/d/e", b"a\nb\nc\nd\ne\nf\n".to_vec());
    record_all_output(&repo, changes.clone(), &txn, &channel, "")?;
    let files: Vec<_> = crate::fs::iter_working_copy(&*txn.read(), Inode::ROOT)
        .map(|f| f.unwrap().1)
        .collect();
    assert_eq!(files, vec!["a", "a/b", "a/b/c", "a/b/c/d", "a/b/c/d/e"]);

    repo.remove_path("a/b/c", true)?;
    debug!("Recording the deletion");
    record_all_output(&repo, changes.clone(), &txn, &channel, "")?;

    let repo2 = working_copy::memory::Memory::new();
    output::output_repository_no_pending(&repo2, &changes, &txn, &channel, "", true, None, 1, 0)?;
    debug!("output done");
    assert_eq!(repo2.list_files(), vec!["a", "a/b"]);

    let files: Vec<_> = crate::fs::iter_working_copy(&*txn.read(), Inode::ROOT)
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

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let txn = env.arc_txn_begin().unwrap();
    txn.write().add_file("dir/file", 0)?;
    let channel = txn.write().open_or_create_channel("main")?;

    repo.add_file("dir/file", b"a\nb\nc\nd\ne\nf\n".to_vec());
    record_all_output(&repo, changes.clone(), &txn, &channel, "").unwrap();
    repo.write_file("dir/file")
        .unwrap()
        .write_all(b"a\nb\nc\n")
        .unwrap();
    record_all_output(&repo, changes.clone(), &txn, &channel, "").unwrap();
    let mut file = Vec::new();
    repo.read_file("dir/file", &mut file).unwrap();
    assert_eq!(std::str::from_utf8(&file), Ok("a\nb\nc\n"));
    txn.commit().unwrap();
    Ok(())
}

/// Just delete a few lines of a file.
#[test]
fn del_nonzombie_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let txn = env.arc_txn_begin().unwrap();
    txn.write().add_file("dir/file", 0)?;
    let channel = txn.write().open_or_create_channel("main")?;

    repo.add_file("dir/file", b"a\nb\nc\nd\ne\nf\n".to_vec());
    record_all_output(&repo, changes.clone(), &txn, &channel, "")?;

    repo.write_file("dir/file")?.write_all(b"a\nb\nc\ne\nf\n")?;
    record_all_output(&repo, changes.clone(), &txn, &channel, "")?;
    repo.write_file("dir/file")?.write_all(b"a\nb\nc\nf\n")?;
    record_all_output(&repo, changes.clone(), &txn, &channel, "")?;
    repo.write_file("dir/file")?.write_all(b"a\nb\nc\n")?;
    record_all_output(&repo, changes.clone(), &txn, &channel, "")?;
    let mut file = Vec::new();
    repo.read_file("dir/file", &mut file).unwrap();
    assert_eq!(std::str::from_utf8(&file), Ok("a\nb\nc\n"));
    txn.commit().unwrap();
    Ok(())
}

/// Are permissions properly recorded?
#[test]
fn permissions_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", b"a\nb\nc\nd\ne\nf\n".to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;

    let txn_alice = env_alice.arc_txn_begin().unwrap();
    txn_alice.write().add_file("file", 0)?;

    let channel = txn_alice.write().open_or_create_channel("main")?;
    let alice0 = record_all(&repo_alice, &changes, &txn_alice, &channel, "")?;

    repo_alice.set_permissions("file", 0o755)?;
    let alice1 = record_all(&repo_alice, &changes, &txn_alice, &channel, "")?;

    let repo_bob = working_copy::memory::Memory::new();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let txn_bob = env_bob.arc_txn_begin().unwrap();
    let channel = (&mut *txn_bob.write()).open_or_create_channel("main")?;
    apply::apply_change(
        &changes,
        &mut *txn_bob.write(),
        &mut *channel.write(),
        &alice0,
    )?;
    output::output_repository_no_pending(
        &repo_bob, &changes, &txn_bob, &channel, "", true, None, 1, 0,
    )?;
    let bob_perm = repo_bob.file_metadata("file")?;
    assert_eq!(bob_perm.0, 0);

    apply::apply_change(
        &changes,
        &mut *txn_bob.write(),
        &mut *channel.write(),
        &alice1,
    )?;
    output::output_repository_no_pending(
        &repo_bob, &changes, &txn_bob, &channel, "", true, None, 1, 0,
    )?;
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

    let repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", b"a\nb\nc\nd\ne\nf\n".to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;

    let txn_alice = env_alice.arc_txn_begin().unwrap();
    txn_alice.write().add_file("file", 0)?;

    let channel = (&mut *txn_alice.write()).open_or_create_channel("main")?;
    let alice0 = record_all(&repo_alice, &changes, &txn_alice, &channel, "")?;
    debug!("alice0 = {:?}", alice0);
    txn_alice.write().add_dir("dir", 0)?;
    txn_alice.write().move_file("file", "dir/file2", 0)?;

    repo_alice.add_dir("dir");
    repo_alice.rename("file", "dir/file2")?;
    debug_tree(&*txn_alice.read(), "debug_tree")?;
    let alice1 = record_all(&repo_alice, &changes, &txn_alice, &channel, "")?;
    debug!("alice1 = {:?}", alice1);
    debug_tree(&*txn_alice.read(), "debug_tree")?;
    debug_inodes(&*txn_alice.read());
    debug!("{:?}", repo_alice);

    repo_alice.remove_path("dir/file2", false)?;
    debug!("{:?}", repo_alice);
    let alice2 = record_all(&repo_alice, &changes, &txn_alice, &channel, "")?;
    txn_alice.commit().unwrap();

    let repo_bob = working_copy::memory::Memory::new();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let txn_bob = env_bob.arc_txn_begin().unwrap();
    let channel = (&mut *txn_bob.write()).open_or_create_channel("main")?;
    apply::apply_change(
        &changes,
        &mut *txn_bob.write(),
        &mut *channel.write(),
        &alice0,
    )?;
    output::output_repository_no_pending(
        &repo_bob, &changes, &txn_bob, &channel, "", true, None, 1, 0,
    )?;
    assert_eq!(repo_bob.list_files(), &["file"]);

    apply::apply_change(
        &changes,
        &mut *txn_bob.write(),
        &mut *channel.write(),
        &alice1,
    )?;
    output::output_repository_no_pending(
        &repo_bob, &changes, &txn_bob, &channel, "", true, None, 1, 0,
    )?;
    let mut files = repo_bob.list_files();
    files.sort();
    assert_eq!(files, &["dir", "dir/file2"]);

    apply::apply_change(
        &changes,
        &mut *txn_bob.write(),
        &mut *channel.write(),
        &alice2,
    )?;
    output::output_repository_no_pending(
        &repo_bob, &changes, &txn_bob, &channel, "", true, None, 1, 0,
    )?;
    assert_eq!(repo_bob.list_files(), &["dir"]);

    Ok(())
}

/// Overwrite a file with a move.
#[test]
fn move_file_existing_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file", b"a\nb\nc\nd\ne\nf\n".to_vec());
    repo_alice.add_file("file2", b"a\nb\nc\nd\ne\nf\n".to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;

    let txn_alice = env_alice.arc_txn_begin().unwrap();
    txn_alice.write().add_file("file", 0)?;
    txn_alice.write().add_file("file2", 0)?;

    let channel = (&mut *txn_alice.write()).open_or_create_channel("main")?;
    record_all(&repo_alice, &changes, &txn_alice, &channel, "")?;
    txn_alice.write().move_file("file", "file2", 0)?;
    repo_alice.rename("file", "file2")?;
    record_all(&repo_alice, &changes, &txn_alice, &channel, "")?;
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

    let repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("a", b"a\nb\nc\nd\ne\nf\n".to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;

    let txn_alice = env_alice.arc_txn_begin().unwrap();
    txn_alice.write().add_file("a", 0)?;

    let channel = txn_alice.write().open_or_create_channel("main")?;
    let alice1 = record_all(&repo_alice, &changes, &txn_alice, &channel, "")?;
    // Alice moves a -> b
    txn_alice.write().move_file("a", "b", 0)?;
    repo_alice.rename("a", "b")?;
    let alice2 = record_all(&repo_alice, &changes, &txn_alice, &channel, "")?;

    // Alice moves b back -> a
    txn_alice.write().move_file("b", "a", 0)?;
    repo_alice.rename("b", "a")?;
    let alice3 = record_all(&repo_alice, &changes, &txn_alice, &channel, "")?;

    // Bob deletes in parallel to the move + moveback
    let repo_bob = working_copy::memory::Memory::new();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let txn_bob = env_bob.arc_txn_begin().unwrap();
    let channel_bob = (&mut *txn_bob.write()).open_or_create_channel("main")?;
    txn_bob
        .write()
        .apply_change(&changes, &mut *channel_bob.write(), &alice1)
        .unwrap();
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
    repo_bob.remove_path("a", false)?;
    let bob1 = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "")?;
    (&mut *txn_bob.write())
        .apply_change(&changes, &mut *channel_bob.write(), &alice2)
        .unwrap();

    output::output_repository_no_pending(
        &repo_bob, &changes, &txn_bob, &channel, "", true, None, 1, 0,
    )?;
    debug!("APPLYING {:?}", alice3);
    txn_bob
        .write()
        .apply_change(&changes, &mut *channel_bob.write(), &alice3)
        .unwrap();
    output::output_repository_no_pending(
        &repo_bob, &changes, &txn_bob, &channel, "", true, None, 1, 0,
    )?;

    if resolve_by_deleting {
        debug!("Bob records a solution");
        let bob2 = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "")?;

        // Alice applies Bob's patch.
        txn_alice
            .write()
            .apply_change(&changes, &mut *channel.write(), &bob1)
            .unwrap();

        let conflicts = output::output_repository_no_pending(
            &repo_alice,
            &changes,
            &txn_alice,
            &channel,
            "",
            true,
            None,
            1,
            0,
        )?;
        debug!("conflicts = {:?}", conflicts);
        assert!(!conflicts.is_empty());

        // Alice applies Bob's resolution
        txn_alice
            .write()
            .apply_change(&changes, &mut *channel.write(), &bob2)
            .unwrap();
        let conflicts = output::output_repository_no_pending(
            &repo_alice,
            &changes,
            &txn_alice,
            &channel,
            "",
            true,
            None,
            1,
            0,
        )?;
        debug!("conflicts = {:?}", conflicts);
        assert!(conflicts.is_empty());

        // Testing Bob's tree by outputting
        let conflicts = output::output_repository_no_pending(
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
        debug!("conflicts = {:?}", conflicts);
        assert!(conflicts.is_empty());
    } else {
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

        debug!("Bob records a solution");
        let bob2 = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "")?;

        // Alice applies Bob's patch.
        txn_alice
            .write()
            .apply_change(&changes, &mut *channel.write(), &bob1)
            .unwrap();

        let conflicts = output::output_repository_no_pending(
            &repo_alice,
            &changes,
            &txn_alice,
            &channel,
            "",
            true,
            None,
            1,
            0,
        )?;
        debug!("conflicts = {:?}", conflicts);
        assert_eq!(conflicts.len(), 1);
        match conflicts[0] {
            Conflict::ZombieFile { ref path } => assert_eq!(path, "a"),
            ref c => panic!("unexpected conflict {:#?}", c),
        }

        // Alice applies Bob's resolution
        txn_alice
            .write()
            .apply_change(&changes, &mut *channel.write(), &bob2)
            .unwrap();
        let conflicts = output::output_repository_no_pending(
            &repo_alice,
            &changes,
            &txn_alice,
            &channel,
            "",
            true,
            None,
            1,
            0,
        )?;
        debug!("conflicts = {:?}", conflicts);
        assert!(conflicts.is_empty());

        // Testing Bob's tree by outputting
        let conflicts = output::output_repository_no_pending(
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
        debug!("conflicts = {:?}", conflicts);
        assert!(conflicts.is_empty());
    }
    Ok(())
}

// Move a file into a directory, and delete the former parent in the same change.
#[test]
fn move_delete_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("dir/file", b"a\nb\nc\nd\ne\nf\n".to_vec());
    repo_alice.add_file("dir/file2", b"a\nb\nc\nd\ne\nf\n".to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;

    let txn_alice = env_alice.arc_txn_begin().unwrap();
    txn_alice.write().add_file("dir/file", 0)?;
    txn_alice.write().add_file("dir/file2", 0)?;

    let channel = (&mut *txn_alice.write()).open_or_create_channel("main")?;
    let alice0 = record_all(&repo_alice, &changes, &txn_alice, &channel, "")?;
    debug!("alice0 = {:?}", alice0);
    repo_alice.add_dir("dir2");
    repo_alice.rename("dir/file", "dir2/file")?;
    repo_alice.rename("dir/file2", "dir2/file2")?;
    repo_alice.remove_path("dir", true)?;
    txn_alice.write().move_file("dir/file", "dir2/file", 0)?;
    txn_alice.write().move_file("dir/file2", "dir2/file2", 0)?;

    let alice1 = record_all(&repo_alice, &changes, &txn_alice, &channel, "")?;
    debug!("alice1 = {:?}", alice1);
    output::output_repository_no_pending(
        &repo_alice,
        &changes,
        &txn_alice,
        &channel,
        "",
        true,
        None,
        1,
        0,
    )?;

    repo_alice.rename("dir2/file", "dir/file").unwrap_or(());
    repo_alice.rename("dir2/file2", "dir/file2").unwrap_or(());
    txn_alice
        .write()
        .move_file("dir2/file", "dir/file", 0)
        .unwrap_or(());
    txn_alice
        .write()
        .move_file("dir2/file2", "dir/file2", 0)
        .unwrap_or(());
    repo_alice.remove_path("dir2", true)?;

    let mut state = Builder::new();
    debug!("recording in dir");
    state.record(
        txn_alice.clone(),
        Algorithm::default(),
        &crate::DEFAULT_SEPARATOR,
        channel.clone(),
        &repo_alice,
        &changes,
        "dir",
        1,
    )?;
    debug!("recording in dir2");
    state.record(
        txn_alice.clone(),
        Algorithm::default(),
        &crate::DEFAULT_SEPARATOR,
        channel.clone(),
        &repo_alice,
        &changes,
        "dir2",
        1,
    )?;

    let rec = state.finish();
    let changes_ = rec
        .actions
        .into_iter()
        .map(|rec| rec.globalize(&*txn_alice.read()).unwrap())
        .collect();
    let mut alice2 = crate::change::Change::make_change(
        &*txn_alice.read(),
        &channel,
        changes_,
        std::mem::take(&mut rec.contents.lock()),
        crate::change::ChangeHeader {
            message: "test".to_string(),
            authors: vec![],
            description: None,
            timestamp: Utc::now(),
        },
        Vec::new(),
    )
    .unwrap();
    let h_alice2 = changes.save_change(&mut alice2, |_, _| Ok::<_, anyhow::Error>(()))?;
    apply::apply_local_change(
        &mut *txn_alice.write(),
        &channel,
        &alice2,
        &h_alice2,
        &rec.updatables,
    )?;

    debug!("done {:?}", h_alice2);

    let (alive, reachable) = check_alive(&*txn_alice.read(), &channel.read().graph);
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

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("filedir", b"a\nb\nc\nd\ne\nf\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let txn = env.arc_txn_begin().unwrap();
    txn.write().add_file("filedir", 0).unwrap();

    let channel = (&mut *txn.write()).open_or_create_channel("main").unwrap();
    record_all(&repo, &changes, &txn, &channel, "").unwrap();

    repo.remove_path("filedir", true).unwrap();
    repo.add_file("filedir/file", b"a\nb\nc\nd\ne\nf\n".to_vec());
    txn.write().add_file("filedir/file", 0).unwrap();
    record_all(&repo, &changes, &txn, &channel, "").unwrap();

    Ok(())
}

#[test]
fn record_deleted_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    {
        let txn = env.arc_txn_begin().unwrap();
        txn.write().add_file("dir/file", 0)?;
        let channel = (&mut *txn.write()).open_or_create_channel("main")?;
        record_all_output(&repo, changes.clone(), &txn, &channel, "")?;
        let files: Vec<_> = crate::fs::iter_working_copy(&*txn.read(), Inode::ROOT)
            .map(|n| n.unwrap().1)
            .collect();
        assert!(files.is_empty());
    }
    Ok(())
}

#[test]
fn record_prefix() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    {
        let txn = env.arc_txn_begin().unwrap();
        let channel = txn.write().open_or_create_channel("main")?;
        record_all_output(&repo, changes, &txn, &channel, "")?;
        let files: Vec<_> = crate::fs::iter_working_copy(&*txn.read(), Inode::ROOT)
            .map(|n| n.unwrap().1)
            .collect();
        assert!(files.is_empty());
    }
    Ok(())
}

#[test]
fn record_not_in_repo() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let txn = env.arc_txn_begin().unwrap();
    let channel = (&mut *txn.write()).open_or_create_channel("main")?;
    assert!(record_all_output(&repo, changes, &txn, &channel, "dir").is_err());
    Ok(())
}

#[test]
fn record_not_modified() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let txn = env.arc_txn_begin().unwrap();
    let channel = (&mut *txn.write()).open_or_create_channel("main")?;

    repo.add_file("file", b"a\nb\nc\nd\ne\nf\n".to_vec());
    txn.write().add_file("file", 0)?;
    record_all_output(&repo, changes.clone(), &txn, &channel, "")?;
    std::thread::sleep(std::time::Duration::from_secs(1));
    record_all_output(&repo, changes, &txn, &channel, "")?;
    Ok(())
}

/// Add a simple file, to test submodules.
#[test]
fn add_file2_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());
    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("a/b/c/file", b"a\nb\nc\nd\ne\nf\n".to_vec());
    let env = pristine::sanakirja::Pristine::new_anon()?;
    {
        let txn = env.arc_txn_begin().unwrap();
        txn.write().add_file("a/b/c/file", 0).unwrap();
        let channel = txn.write().open_or_create_channel("main").unwrap();
        record_all(&repo, &changes, &txn, &channel, "").unwrap();
        txn.commit().unwrap()
    }

    let repo2 = working_copy::memory::Memory::new();
    repo2.add_file("a/b/c/file", b"w\nx\ny\nz\n".to_vec());
    let change = {
        let env2 = pristine::sanakirja::Pristine::new_anon()?;
        let txn = env2.arc_txn_begin().unwrap();
        txn.write().add_file("a/b/c/file", 0).unwrap();

        let channel = txn.write().open_or_create_channel("other").unwrap();
        let change = record_all(&repo, &changes, &txn, &channel, "").unwrap();
        txn.commit().unwrap();
        change
    };

    let txn = env.arc_txn_begin().unwrap();
    let channel = txn.write().open_or_create_channel("main").unwrap();
    apply::apply_change(&changes, &mut *txn.write(), &mut *channel.write(), &change)?;
    output::output_repository_no_pending(&repo, &changes, &txn, &channel, "", true, None, 1, 0)?;
    {
        let txn_ = txn.write();
        let mut f = std::fs::File::create("add_file2.dot")?;
        crate::pristine::debug(&*txn_, &txn_.graph(&*channel.read()), &mut f)?;
    }

    // Check that there's a name conflict.
    assert_eq!(repo.list_files().len(), 8);
    Ok(())
}
