use super::*;
use crate::working_copy::{WorkingCopy, WorkingCopyRead};
use std::io::Write;

/// Add a file, write to it, then fork the branch and unrecord once on
/// one side.
#[test]
fn test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("dir/file", b"a\nb\nc\nd\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let txn = env.arc_txn_begin().unwrap();
    txn.write().add_file("dir/file", 0)?;

    let channel = txn.write().open_or_create_channel("main")?;
    let _h0 = record_all(&repo, &changes, &txn, &channel, "")?;

    use std::io::Write;
    repo.write_file("dir/file", Inode::ROOT)?
        .write_all(b"a\nx\nb\nd\n")?;

    let h1 = record_all(&repo, &changes, &txn, &channel, "")?;

    let _channel2 = txn.write().fork(&channel, "main2")?;
    crate::unrecord::unrecord(&mut *txn.write(), &channel, &changes, &h1, 0)?;
    let conflicts = output::output_repository_no_pending(
        &repo, &changes, &txn, &channel, "", true, None, 1, 0,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }
    let mut buf = Vec::new();
    repo.read_file("dir/file", &mut buf)?;
    assert_eq!(std::str::from_utf8(&buf), Ok("a\nb\nc\nd\n"));

    txn.commit()?;

    Ok(())
}

#[test]
fn replace() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("dir/file", b"a\nb\nc\nd\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let txn = env.arc_txn_begin().unwrap();
    txn.write().add_file("dir/file", 0)?;

    let channel = txn.write().open_or_create_channel("main")?;
    let _h0 = record_all(&repo, &changes, &txn, &channel, "")?;

    repo.write_file("dir/file", Inode::ROOT)?
        .write_all(b"a\nx\ny\nd\n")?;

    let h1 = record_all(&repo, &changes, &txn, &channel, "")?;

    let _channel2 = txn.write().fork(&channel, "main2")?;
    crate::unrecord::unrecord(&mut *txn.write(), &channel, &changes, &h1, 0)?;
    let conflicts = output::output_repository_no_pending(
        &repo, &changes, &txn, &channel, "", true, None, 1, 0,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }
    let mut buf = Vec::new();
    repo.read_file("dir/file", &mut buf)?;
    assert_eq!(std::str::from_utf8(&buf), Ok("a\nb\nc\nd\n"));

    txn.commit()?;

    Ok(())
}

#[test]
fn file_move() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("file", b"a\nb\nc\nd\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let txn = env.arc_txn_begin().unwrap();
    txn.write().add_file("file", 0)?;

    let channel = txn.write().open_or_create_channel("main")?;
    let _h0 = record_all(&repo, &changes, &txn, &channel, "")?;

    repo.rename("file", "dir/file")?;
    txn.write().move_file("file", "dir/file", 0)?;
    debug!("recording the move");
    let h1 = record_all(&repo, &changes, &txn, &channel, "")?;

    debug!("unrecording the move");
    crate::unrecord::unrecord(&mut *txn.write(), &channel, &changes, &h1, 0)?;

    assert_eq!(
        crate::fs::iter_working_copy(&*txn.read(), Inode::ROOT)
            .map(|n| n.unwrap().1)
            .collect::<Vec<_>>(),
        vec!["dir", "dir/file"]
    );
    assert_eq!(repo.list_files(), vec!["dir", "dir/file"]);

    output::output_repository_no_pending(&repo, &changes, &txn, &channel, "", true, None, 1, 0)?;
    assert_eq!(
        crate::fs::iter_working_copy(&*txn.read(), Inode::ROOT)
            .map(|n| n.unwrap().1)
            .collect::<Vec<_>>(),
        vec!["file"]
    );

    // Checking that unrecord doesn't delete `dir`, and moves `file`
    // back to the root.
    let mut files = repo.list_files();
    files.sort();
    assert_eq!(files, vec!["dir", "file"]);

    txn.commit()?;

    Ok(())
}

#[test]
fn reconnect_lines() -> Result<(), anyhow::Error> {
    reconnect_(false)
}

#[test]
fn reconnect_files() -> Result<(), anyhow::Error> {
    reconnect_(true)
}

fn reconnect_(delete_file: bool) -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let repo2 = working_copy::memory::Memory::new();
    let repo3 = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("file", b"a\nb\nc\nd\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let env2 = pristine::sanakirja::Pristine::new_anon()?;
    let env3 = pristine::sanakirja::Pristine::new_anon()?;

    let txn = env.arc_txn_begin().unwrap();
    let txn2 = env2.arc_txn_begin().unwrap();
    let txn3 = env3.arc_txn_begin().unwrap();
    txn.write().add_file("file", 0)?;

    let channel = txn.write().open_or_create_channel("main")?;
    let h0 = record_all(&repo, &changes, &txn, &channel, "")?;

    let channel2 = txn2.write().open_or_create_channel("main")?;
    let channel3 = txn3.write().open_or_create_channel("main")?;

    apply::apply_change_arc(&changes, &txn2, &channel2, &h0)?;
    output::output_repository_no_pending(&repo2, &changes, &txn2, &channel2, "", true, None, 1, 0)?;
    apply::apply_change_arc(&changes, &txn3, &channel3, &h0)?;
    output::output_repository_no_pending(&repo3, &changes, &txn3, &channel3, "", true, None, 1, 0)?;

    // This test removes a line (in h1), then replaces it with another
    // one (in h2), removes the pseudo-edges (output, below), and then
    // unrecords h2 to delete the connection. Test: do the
    // pseudo-edges reappear?

    ///////////
    if delete_file {
        repo.remove_path("file", false)?;
    } else {
        repo.write_file("file", Inode::ROOT)?.write_all(b"a\nd\n")?;
    }
    record_all_output(&repo, changes.clone(), &txn, &channel, "")?;

    ///////////
    repo2
        .write_file("file", Inode::ROOT)?
        .write_all(b"a\nb\nx\nc\nd\n")?;
    let h2 = record_all(&repo2, &changes, &txn2, &channel2, "")?;

    repo2
        .write_file("file", Inode::ROOT)?
        .write_all(b"a\nb\nx\nc\ny\nd\n")?;
    let h3 = record_all(&repo2, &changes, &txn2, &channel2, "")?;

    ///////////
    apply::apply_change_arc(&changes, &txn, &channel, &h2)?;
    apply::apply_change_arc(&changes, &txn, &channel, &h3)?;
    output::output_repository_no_pending(&repo, &changes, &txn, &channel, "", true, None, 1, 0)?;

    crate::unrecord::unrecord(&mut *txn.write(), &channel, &changes, &h2, 0)?;

    Ok(())
}

#[test]
fn zombie_file_test() -> Result<(), anyhow::Error> {
    zombie_(None)
}

#[test]
fn zombie_lines_test() -> Result<(), anyhow::Error> {
    zombie_(Some(b"d\n"))
}

fn zombie_(file: Option<&[u8]>) -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let repo2 = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("file", b"a\nb\nc\nd\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let env2 = pristine::sanakirja::Pristine::new_anon()?;

    let txn = env.arc_txn_begin().unwrap();
    let txn2 = env2.arc_txn_begin().unwrap();
    txn.write().add_file("file", 0)?;

    let channel = txn.write().open_or_create_channel("main")?;
    let h0 = record_all(&repo, &changes, &txn, &channel, "")?;
    let channel2 = txn2.write().open_or_create_channel("main")?;

    apply::apply_change_arc(&changes, &txn2, &channel2, &h0)?;
    output::output_repository_no_pending(&repo2, &changes, &txn2, &channel2, "", true, None, 1, 0)?;

    ///////////
    if let Some(file) = file {
        repo.write_file("file", Inode::ROOT)?.write_all(file)?;
    } else {
        repo.remove_path("file", false)?;
    }
    let h1 = record_all_output(&repo, changes.clone(), &txn, &channel, "")?;

    ///////////

    repo2
        .write_file("file", Inode::ROOT)?
        .write_all(b"a\nb\nx\nc\nd\n")?;
    let h2 = record_all_output(&repo2, changes.clone(), &txn2, &channel2, "")?;

    ///////////
    apply::apply_change_arc(&changes, &txn, &channel, &h2)?;

    debug!("unrecording");
    crate::unrecord::unrecord(&mut *txn.write(), &channel, &changes, &h2, 0)?;
    let conflicts = output::output_repository_no_pending(
        &repo, &changes, &txn, &channel, "", true, None, 1, 0,
    )?;
    let mut buf = Vec::new();
    if let Some(f) = file {
        if !conflicts.is_empty() {
            panic!("conflicts = {:#?}", conflicts)
        }
        repo.read_file("file", &mut buf)?;
        assert_eq!(&buf[..], f);
    } else {
        if conflicts.len() != 1 {
            panic!("conflicts = {:#?}", conflicts)
        }
        match conflicts.iter().next().unwrap() {
            Conflict::ZombieFile { ref path } => assert_eq!(path, "file"),
            ref c => panic!("c = {:#?}", c),
        }
    }

    let (alive_, reachable_) = check_alive(&*txn.read(), &channel.read());
    if !alive_.is_empty() {
        panic!("alive: {:?}", alive_);
    }
    if !reachable_.is_empty() {
        panic!("reachable: {:?}", reachable_);
    }

    txn.commit()?;

    // Applying the symmetric.
    apply::apply_change_arc(&changes, &txn2, &channel2, &h1)?;

    debug!("unrecording h1 = {:?}", h1);
    crate::unrecord::unrecord(&mut *txn2.write(), &channel2, &changes, &h1, 0)?;
    let conflicts = output::output_repository_no_pending(
        &repo, &changes, &txn2, &channel, "", true, None, 1, 0,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts)
    }
    Ok(())
}

// Should fail: we're resurrecting a file in a directory that doesn't
// exist any more.
#[test]
fn zombie_dir() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("a/b/c/d", b"a\nb\nc\nd\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let txn = env.arc_txn_begin().unwrap();
    txn.write().add_file("a/b/c/d", 0)?;

    let channel = txn.write().open_or_create_channel("main")?;
    record_all(&repo, &changes, &txn, &channel, "")?;

    repo.remove_path("a/b/c/d", false)?;
    let h1 = record_all_output(&repo, changes.clone(), &txn, &channel, "")?;

    repo.remove_path("a/b", true)?;
    let _h2 = record_all_output(&repo, changes.clone(), &txn, &channel, "")?;
    output::output_repository_no_pending(&repo, &changes, &txn, &channel, "", true, None, 1, 0)?;
    let files = repo.list_files();
    assert_eq!(files, &["a"]);
    debug!("files={:?}", files);

    crate::unrecord::unrecord(&mut *txn.write(), &channel, &changes, &h1, 0)?;

    let conflicts = output::output_repository_no_pending(
        &repo, &changes, &txn, &channel, "", true, None, 1, 0,
    )?
    .into_iter()
    .collect::<Vec<_>>();

    match conflicts[0] {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "a/b"),
        ref c => panic!("c = {:?}", c),
    }
    match conflicts[1] {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "a/b/c"),
        ref c => panic!("c = {:?}", c),
    }

    let files = repo.list_files();
    debug!("files={:?}", files);
    assert_eq!(files, &["a", "a/b", "a/b/c", "a/b/c/d"]);

    let (alive_, reachable_) = check_alive(&*txn.read(), &channel.read());
    if !alive_.is_empty() {
        panic!("alive: {:?}", alive_);
    }
    if !reachable_.is_empty() {
        panic!("reachable: {:?}", reachable_);
    }

    txn.commit()?;

    Ok(())
}

#[test]
fn nodep() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("dir/file", b"a\nb\nc\nd\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let txn = env.arc_txn_begin().unwrap();
    txn.write().add_file("dir/file", 0)?;
    debug_inodes(&*txn.read());

    let channel = txn.write().open_or_create_channel("main")?;
    let h0 = record_all(&repo, &changes, &txn, &channel, "")?;

    repo.write_file("dir/file", Inode::ROOT)?
        .write_all(b"a\nx\nb\nd\n")?;

    let h1 = record_all(&repo, &changes, &txn, &channel, "")?;
    debug_inodes(&*txn.read());

    match crate::unrecord::unrecord(&mut *txn.write(), &channel, &changes, &h0, 0) {
        Err(crate::unrecord::UnrecordError::ChangeIsDependedUpon { .. }) => {}
        _ => panic!("Should not be able to unrecord"),
    }

    debug_inodes(&*txn.read());
    let channel2 = txn.write().open_or_create_channel("main2")?;
    match crate::unrecord::unrecord(&mut *txn.write(), &channel2, &changes, &h0, 0) {
        Err(crate::unrecord::UnrecordError::ChangeNotInChannel { .. }) => {}
        _ => panic!("Should not be able to unrecord"),
    }

    for p in txn.read().log(&*channel.read(), 0).unwrap() {
        debug!("p = {:?}", p);
    }

    debug_inodes(&*txn.read());
    crate::unrecord::unrecord(&mut *txn.write(), &channel, &changes, &h1, 0)?;

    for p in txn.read().log(&*channel.read(), 0).unwrap() {
        debug!("p = {:?}", p);
    }

    debug_inodes(&*txn.read());
    crate::unrecord::unrecord(&mut *txn.write(), &channel, &changes, &h0, 0)?;

    output::output_repository_no_pending(&repo, &changes, &txn, &channel, "", true, None, 1, 0)?;

    // Checking that unrecord doesn't delete files on the filesystem,
    // but updates the tree/revtree tables.
    let mut files = repo.list_files();
    files.sort();
    assert_eq!(files, &["dir", "dir/file"]);
    assert!(crate::fs::iter_working_copy(&*txn.read(), Inode::ROOT)
        .next()
        .is_none());
    txn.commit()?;

    Ok(())
}

#[test]
fn file_del() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let txn = env.arc_txn_begin().unwrap();

    let channel = txn.write().open_or_create_channel("main")?;

    repo.add_file("file", b"blabla".to_vec());
    txn.write().add_file("file", 0)?;
    let h0 = record_all(&repo, &changes, &txn, &channel, "")?;

    repo.remove_path("file", false)?;
    let h = record_all(&repo, &changes, &txn, &channel, "")?;

    debug!("unrecord h");
    // Unrecording the deletion.
    crate::unrecord::unrecord(&mut *txn.write(), &channel, &changes, &h, 0)?;
    let conflicts = output::output_repository_no_pending(
        &repo, &changes, &txn, &channel, "", true, None, 1, 0,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }
    assert_eq!(repo.list_files(), vec!["file"]);

    // Unrecording the initial change.
    debug!("unrecord h0");
    crate::unrecord::unrecord(&mut *txn.write(), &channel, &changes, &h0, 0)?;
    let conflicts = output::output_repository_no_pending(
        &repo, &changes, &txn, &channel, "", true, None, 1, 0,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }
    let files = repo.list_files();
    // Unrecording a file addition shouldn't delete the file.
    assert_eq!(files, &["file"]);
    txn.commit()?;
    Ok(())
}

/// Unrecording a change that edits the file around a conflict marker.
#[test]
fn self_context() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let txn = env.arc_txn_begin().unwrap();

    let mut channel = txn.write().open_or_create_channel("main")?;

    repo.add_file("file", b"a\nb\n".to_vec());
    txn.write().add_file("file", 0)?;
    record_all(&repo, &changes, &txn, &channel, "")?;

    let channel2 = txn.write().fork(&channel, "main2")?;

    repo.write_file("file", Inode::ROOT)?
        .write_all(b"a\nx\nb\n")?;
    record_all(&repo, &changes, &txn, &channel, "")?;
    repo.write_file("file", Inode::ROOT)?
        .write_all(b"a\ny\nb\n")?;
    let b = record_all(&repo, &changes, &txn, &channel2, "")?;

    apply::apply_change_arc(&changes, &txn, &channel, &b)?;
    let conflicts = output::output_repository_no_pending(
        &repo, &changes, &txn, &channel, "", true, None, 1, 0,
    )?;
    debug!("conflicts = {:#?}", conflicts);
    let mut buf = Vec::new();
    repo.read_file("file", &mut buf)?;
    debug!("buf = {:?}", std::str::from_utf8(&buf));
    assert_eq!(conflicts.len(), 1);
    match conflicts.iter().next().unwrap() {
        Conflict::Order { .. } => {}
        ref c => panic!("c = {:?}", c),
    }

    let mut buf = Vec::new();
    repo.read_file("file", &mut buf)?;
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    {
        let mut w = repo.write_file("file", Inode::ROOT)?;
        for l in conflict.iter() {
            if l.starts_with(">>>") {
                writeln!(w, "bla\n{}\nbli", l)?
            } else {
                writeln!(w, "{}", l)?
            }
        }
    }
    let c = record_all(&repo, &changes, &txn, &channel, "")?;

    crate::unrecord::unrecord(&mut *txn.write(), &mut channel, &changes, &c, 0)?;

    let conflicts = output::output_repository_no_pending(
        &repo, &changes, &txn, &channel, "", true, None, 1, 0,
    )?;
    debug!("conflicts = {:#?}", conflicts);
    assert_eq!(conflicts.len(), 1);
    match conflicts.iter().next().unwrap() {
        Conflict::Order { .. } => {}
        ref c => panic!("c = {:?}", c),
    }

    let mut buf = Vec::new();
    repo.read_file("file", &mut buf)?;
    let mut conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    conflict.sort();
    assert_eq!(
        conflict,
        vec![
            "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",
            "================================",
            ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
            "a",
            "b",
            "x",
            "y"
        ]
    );
    txn.commit()?;

    Ok(())
}

#[test]
fn rollback_lines() -> Result<(), anyhow::Error> {
    rollback_(false)
}

#[test]
fn rollback_file() -> Result<(), anyhow::Error> {
    rollback_(true)
}

fn rollback_(delete_file: bool) -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let txn = env.arc_txn_begin().unwrap();

    let channel = txn.write().open_or_create_channel("main")?;

    // Write a-b-c
    repo.add_file("file", b"a\nb\nc\n".to_vec());
    txn.write().add_file("file", 0)?;
    record_all(&repo, &changes, &txn, &channel, "")?;

    // Delete -b-
    if delete_file {
        repo.remove_path("file", false)?
    } else {
        repo.write_file("file", Inode::ROOT)?.write_all(b"a\nd\n")?;
    }
    let h_del = record_all(&repo, &changes, &txn, &channel, "")?;

    // Rollback the deletion of -b-
    let p_del = changes.get_change(&h_del)?;
    debug!("p_del = {:#?}", p_del);
    let mut p_inv = p_del.inverse(
        &h_del,
        crate::change::ChangeHeader {
            authors: vec![],
            message: "rollback".to_string(),
            description: None,
            timestamp: chrono::Utc::now(),
        },
        Vec::new(),
    );
    let h_inv = changes.save_change(&mut p_inv, |_, _| Ok::<_, anyhow::Error>(()))?;
    apply::apply_change_arc(&changes, &txn, &channel, &h_inv)?;
    let conflicts = output::output_repository_no_pending(
        &repo, &changes, &txn, &channel, "", true, None, 1, 0,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts)
    }
    let mut buf = Vec::new();
    repo.read_file("file", &mut buf)?;
    assert_eq!(std::str::from_utf8(&buf), Ok("a\nb\nc\n"));

    // Unrecord the rollback
    crate::unrecord::unrecord(&mut *txn.write(), &channel, &changes, &h_inv, 0)?;
    let conflicts = output::output_repository_no_pending(
        &repo, &changes, &txn, &channel, "", true, None, 1, 0,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts)
    }
    let mut buf = Vec::new();
    repo.read_file("file", &mut buf).unwrap();
    if delete_file {
        assert_eq!(std::str::from_utf8(&buf), Ok("a\nb\nc\n"));
    } else {
        assert_eq!(std::str::from_utf8(&buf), Ok("a\nd\n"));
    }

    txn.commit()?;

    Ok(())
}

/// Delete a line twice on two different channels, merge and unrecord
/// only one of them. Does the deleted edge reappear? It shouldn't.
#[test]
fn double_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let txn = env.arc_txn_begin().unwrap();
    let channel = txn.write().open_or_create_channel("main")?;
    let channel2 = txn.write().open_or_create_channel("main2")?;

    repo.add_file("file", b"blabla\nblibli\nblublu\n".to_vec());
    txn.write().add_file("file", 0)?;
    let h0 = record_all(&repo, &changes, &txn, &channel, "")?;
    debug!("h0 = {:?}", h0);

    apply::apply_change_arc(&changes, &txn, &channel2, &h0)?;

    // First deletion
    {
        let mut w = repo.write_file("file", Inode::ROOT)?;
        writeln!(w, "blabla\nblublu")?;
    }
    let h1 = record_all(&repo, &changes, &txn, &channel, "")?;
    debug!("h1 = {:?}", h1);

    // Second deletion
    let h2 = record_all(&repo, &changes, &txn, &channel2, "")?;
    debug!("h2 = {:?}", h2);

    // Both deletions together.
    debug!("applying");
    apply::apply_change_arc(&changes, &txn, &channel, &h2)?;

    debug!("unrecord h");
    crate::unrecord::unrecord(&mut *txn.write(), &channel, &changes, &h2, 0)?;

    let conflicts = output::output_repository_no_pending(
        &repo, &changes, &txn, &channel, "", true, None, 1, 0,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }

    txn.commit()?;

    Ok(())
}

/// Same as `double` above, but with a (slightly) more convoluted change
/// dependency graph made by rolling the change back a few times.
#[test]
fn double_convoluted() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let txn = env.arc_txn_begin().unwrap();
    let mut channel = txn.write().open_or_create_channel("main")?;
    let mut channel2 = txn.write().open_or_create_channel("main2")?;

    repo.add_file("file", b"blabla\nblibli\nblublu\n".to_vec());
    txn.write().add_file("file", 0)?;
    let h0 = record_all(&repo, &changes, &txn, &channel, "")?;
    debug!("h0 = {:?}", h0);

    apply::apply_change_arc(&changes, &txn, &channel2, &h0)?;

    // First deletion
    {
        let mut w = repo.write_file("file", Inode::ROOT)?;
        write!(w, "blabla\nblibli\n")?;
    }
    let h1 = record_all(&repo, &changes, &txn, &channel, "")?;
    debug!("h1 = {:?}", h1);

    // Second deletion
    {
        let mut w = repo.write_file("file", Inode::ROOT)?;
        writeln!(w, "blabla")?;
    }
    let h2 = record_all(&repo, &changes, &txn, &channel2, "")?;
    debug!("h2 = {:?}", h2);

    // Both deletions together, then unrecord on ~channel~.
    debug!("applying");
    apply::apply_change_arc(&changes, &txn, &channel, &h2)?;

    debug!("unrecord h");
    crate::unrecord::unrecord(&mut *txn.write(), &mut channel, &changes, &h2, 0)?;

    let conflicts = output::output_repository_no_pending(
        &repo, &changes, &txn, &channel, "", true, None, 1, 0,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }

    // Same on ~channel2~, but with a few extra layers of rollbacks in between.
    debug!("rolling back");
    apply::apply_change_arc(&changes, &txn, &channel2, &h1)?;
    let rollback = |h| {
        let p = changes.get_change(&h).unwrap();
        let mut p_inv = p.inverse(
            &h,
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
        h_inv
    };
    let mut h = h2;
    for _i in 0..6 {
        let r = rollback(h);
        apply::apply_change_arc(&changes, &txn, &channel2, &r).unwrap();
        h = r
    }
    crate::unrecord::unrecord(&mut *txn.write(), &mut channel2, &changes, &h1, 0)?;

    let conflicts = output::output_repository_no_pending(
        &repo, &changes, &txn, &channel, "", true, None, 1, 0,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts)
    }

    txn.commit()?;

    Ok(())
}

/// Delete the same file on two different channels, merge, unrecord each patch on the same channel. What happens to tree/revtree?
#[test]
fn double_file() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let txn = env.arc_txn_begin().unwrap();
    let channel = txn.write().open_or_create_channel("main")?;
    let channel2 = txn.write().open_or_create_channel("main2")?;

    repo.add_file("file", b"blabla\nblibli\nblublu\n".to_vec());
    txn.write().add_file("file", 0)?;
    let h0 = record_all(&repo, &changes, &txn, &channel, "")?;
    debug!("h0 = {:?}", h0);

    apply::apply_change_arc(&changes, &txn, &channel2, &h0)?;

    // First deletion
    repo.remove_path("file", false)?;
    let h1 = record_all(&repo, &changes, &txn, &channel, "")?;
    debug!("h1 = {:?}", h1);
    // Second deletion
    let h2 = record_all(&repo, &changes, &txn, &channel2, "")?;
    debug!("h2 = {:?}", h2);

    // Both deletions together.
    debug!("applying");
    apply::apply_change_arc(&changes, &txn, &channel, &h2)?;

    crate::unrecord::unrecord(&mut *txn.write(), &channel, &changes, &h1, 0)?;
    crate::unrecord::unrecord(&mut *txn.write(), &channel, &changes, &h2, 0)?;

    let txn = txn.read();
    let mut inodes = txn.iter_inodes().unwrap();
    assert!(inodes.next().is_some());
    assert!(inodes.next().is_none());
    Ok(())
}
