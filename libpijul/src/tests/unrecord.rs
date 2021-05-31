use super::*;
use crate::working_copy::WorkingCopy;

/// Add a file, write to it, then fork the branch and unrecord once on
/// one side.
#[test]
fn test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("dir/file", b"a\nb\nc\nd\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn = env.mut_txn_begin().unwrap();
    txn.add_file("dir/file")?;

    let mut channel = txn.open_or_create_channel("main")?;
    let _h0 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;

    repo.write_file::<_, std::io::Error, _>("dir/file", |w| {
        w.write_all(b"a\nx\nb\nd\n")?;
        Ok(())
    })?;

    let h1 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;

    let channel2 = txn.fork(&channel, "main2")?;
    crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &h1)?;
    let conflicts = output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }
    let mut buf = Vec::new();
    repo.read_file("dir/file", &mut buf)?;
    assert_eq!(std::str::from_utf8(&buf), Ok("a\nb\nc\nd\n"));

    debug_to_file(&txn, &channel.borrow(), "debug_un")?;
    debug_to_file(&txn, &channel2.borrow(), "debug_un2")?;
    txn.commit()?;

    Ok(())
}

#[test]
fn replace() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("dir/file", b"a\nb\nc\nd\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn = env.mut_txn_begin().unwrap();
    txn.add_file("dir/file")?;

    let mut channel = txn.open_or_create_channel("main")?;
    let _h0 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;

    repo.write_file::<_, std::io::Error, _>("dir/file", |w| {
        w.write_all(b"a\nx\ny\nd\n")?;
        Ok(())
    })?;

    let h1 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;

    let channel2 = txn.fork(&channel, "main2")?;
    debug_to_file(&txn, &channel.borrow(), "debug_un0")?;
    crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &h1)?;
    debug_to_file(&txn, &channel.borrow(), "debug_un1")?;
    let conflicts = output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }
    let mut buf = Vec::new();
    repo.read_file("dir/file", &mut buf)?;
    assert_eq!(std::str::from_utf8(&buf), Ok("a\nb\nc\nd\n"));

    debug_to_file(&txn, &channel.borrow(), "debug_un")?;
    debug_to_file(&txn, &channel2.borrow(), "debug_un2")?;
    txn.commit()?;

    Ok(())
}

#[test]
fn file_move() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("file", b"a\nb\nc\nd\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn = env.mut_txn_begin().unwrap();
    txn.add_file("file")?;

    let mut channel = txn.open_or_create_channel("main")?;
    let _h0 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;

    repo.rename("file", "dir/file")?;
    txn.move_file("file", "dir/file")?;
    debug!("recording the move");
    let h1 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;

    debug_to_file(&txn, &channel.borrow(), "debug_un")?;
    debug!("unrecording the move");
    crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &h1)?;

    debug_to_file(&txn, &channel.borrow(), "debug_un2")?;
    assert_eq!(
        crate::fs::iter_working_copy(&txn, Inode::ROOT)
            .map(|n| n.unwrap().1)
            .collect::<Vec<_>>(),
        vec!["dir", "dir/file"]
    );
    assert_eq!(repo.list_files(), vec!["dir", "dir/file"]);

    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )?;
    assert_eq!(
        crate::fs::iter_working_copy(&txn, Inode::ROOT)
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

    let mut repo = working_copy::memory::Memory::new();
    let mut repo2 = working_copy::memory::Memory::new();
    let mut repo3 = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("file", b"a\nb\nc\nd\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let env2 = pristine::sanakirja::Pristine::new_anon()?;
    let env3 = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn = env.mut_txn_begin().unwrap();
    let mut txn2 = env2.mut_txn_begin().unwrap();
    let mut txn3 = env3.mut_txn_begin().unwrap();
    txn.add_file("file")?;

    let mut channel = txn.open_or_create_channel("main")?;
    let h0 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;

    let mut channel2 = txn2.open_or_create_channel("main")?;
    let mut channel3 = txn3.open_or_create_channel("main")?;

    apply::apply_change(&changes, &mut txn2, &mut channel2, &h0)?;
    output::output_repository_no_pending(
        &mut repo2,
        &changes,
        &mut txn2,
        &mut channel2,
        "",
        true,
        None,
    )?;
    apply::apply_change(&changes, &mut txn3, &mut channel3, &h0)?;
    output::output_repository_no_pending(
        &mut repo3,
        &changes,
        &mut txn3,
        &mut channel3,
        "",
        true,
        None,
    )?;

    // This test removes a line (in h1), then replaces it with another
    // one (in h2), removes the pseudo-edges (output, below), and then
    // unrecords h2 to delete the connection. Test: do the
    // pseudo-edges reappear?

    ///////////
    if delete_file {
        repo.remove_path("file")?;
    } else {
        repo.write_file::<_, std::io::Error, _>("file", |w| {
            w.write_all(b"a\nd\n")?;
            Ok(())
        })?;
    }
    record_all_output(&mut repo, &changes, &mut txn, &mut channel, "")?;

    ///////////
    repo2.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(b"a\nb\nx\nc\nd\n")?;
        Ok(())
    })?;
    let h2 = record_all(&mut repo2, &changes, &mut txn2, &mut channel2, "")?;

    repo2.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(b"a\nb\nx\nc\ny\nd\n")?;
        Ok(())
    })?;
    let h3 = record_all(&mut repo2, &changes, &mut txn2, &mut channel2, "")?;

    ///////////
    apply::apply_change(&changes, &mut txn, &mut channel, &h2)?;
    apply::apply_change(&changes, &mut txn, &mut channel, &h3)?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )?;

    debug_to_file(&txn, &channel.borrow(), "debug_un")?;

    crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &h2)?;

    debug_to_file(&txn, &channel.borrow(), "debug_un2")?;
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

    let mut repo = working_copy::memory::Memory::new();
    let mut repo2 = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("file", b"a\nb\nc\nd\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let env2 = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn = env.mut_txn_begin().unwrap();
    let mut txn2 = env2.mut_txn_begin().unwrap();
    txn.add_file("file")?;

    let mut channel = txn.open_or_create_channel("main")?;
    let h0 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;
    let mut channel2 = txn2.open_or_create_channel("main")?;

    apply::apply_change(&changes, &mut txn2, &mut channel2, &h0)?;
    output::output_repository_no_pending(
        &mut repo2,
        &changes,
        &mut txn2,
        &mut channel2,
        "",
        true,
        None,
    )?;

    ///////////
    if let Some(file) = file {
        repo.write_file::<_, std::io::Error, _>("file", |w| {
            w.write_all(file)?;
            Ok(())
        })?;
    } else {
        repo.remove_path("file")?;
    }
    let h1 = record_all_output(&mut repo, &changes, &mut txn, &mut channel, "")?;
    debug_to_file(&txn, &channel.borrow(), "debug_a")?;

    ///////////

    repo2.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(b"a\nb\nx\nc\nd\n")?;
        Ok(())
    })?;
    let h2 = record_all_output(&mut repo2, &changes, &mut txn2, &mut channel2, "")?;
    debug_to_file(&txn2, &channel2.borrow(), "debug_b")?;

    ///////////
    apply::apply_change(&changes, &mut txn, &mut channel, &h2)?;

    debug_to_file(&txn, &channel.borrow(), "debug_un")?;
    debug!("unrecording");
    crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &h2)?;
    let conflicts = output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel.borrow(), "debug_un2")?;
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
        match conflicts[0] {
            Conflict::ZombieFile { ref path } => assert_eq!(path, "file"),
            ref c => panic!("c = {:#?}", c),
        }
    }

    let (alive_, reachable_) = check_alive(&txn, &channel.borrow().graph);
    if !alive_.is_empty() {
        panic!("alive: {:?}", alive_);
    }
    if !reachable_.is_empty() {
        panic!("reachable: {:?}", reachable_);
    }

    txn.commit()?;

    // Applying the symmetric.
    apply::apply_change(&changes, &mut txn2, &mut channel2, &h1)?;
    debug_to_file(&txn2, &channel2.borrow(), "debug_un3")?;

    debug!("unrecording h1 = {:?}", h1);
    crate::unrecord::unrecord(&mut txn2, &mut channel2, &changes, &h1)?;
    debug_to_file(&txn2, &channel2.borrow(), "debug_un4")?;
    let conflicts = output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn2,
        &mut channel,
        "",
        true,
        None,
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

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("a/b/c/d", b"a\nb\nc\nd\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    txn.add_file("a/b/c/d")?;

    let mut channel = txn.open_or_create_channel("main")?;
    record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;

    repo.remove_path("a/b/c/d")?;
    let h1 = record_all_output(&mut repo, &changes, &mut txn, &mut channel, "")?;

    repo.remove_path("a/b")?;
    let _h2 = record_all_output(&mut repo, &changes, &mut txn, &mut channel, "")?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )?;
    let files = repo.list_files();
    assert_eq!(files, &["a"]);
    debug!("files={:?}", files);

    debug_to_file(&txn, &channel.borrow(), "debug_un")?;
    crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &h1)?;

    debug_to_file(&txn, &channel.borrow(), "debug_un2")?;

    let conflicts = output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )?;

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

    let (alive_, reachable_) = check_alive(&txn, &channel.borrow().graph);
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

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("dir/file", b"a\nb\nc\nd\n".to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn = env.mut_txn_begin().unwrap();
    txn.add_file("dir/file")?;
    debug_inodes(&txn);

    let mut channel = txn.open_or_create_channel("main")?;
    let h0 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;

    repo.write_file::<_, std::io::Error, _>("dir/file", |w| {
        w.write_all(b"a\nx\nb\nd\n")?;
        Ok(())
    })?;

    let h1 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;
    debug_inodes(&txn);

    match crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &h0) {
        Err(crate::unrecord::UnrecordError::ChangeIsDependedUpon { .. }) => {}
        _ => panic!("Should not be able to unrecord"),
    }

    debug_inodes(&txn);
    let mut channel2 = txn.open_or_create_channel("main2")?;
    match crate::unrecord::unrecord(&mut txn, &mut channel2, &changes, &h0) {
        Err(crate::unrecord::UnrecordError::ChangeNotInChannel { .. }) => {}
        _ => panic!("Should not be able to unrecord"),
    }

    for p in txn.log(&channel.borrow(), 0).unwrap() {
        debug!("p = {:?}", p);
    }

    debug_inodes(&txn);
    debug_to_file(&txn, &channel.borrow(), "debug")?;
    crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &h1)?;

    for p in txn.log(&channel.borrow(), 0).unwrap() {
        debug!("p = {:?}", p);
    }

    debug_inodes(&txn);
    debug_to_file(&txn, &channel.borrow(), "debug2")?;
    crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &h0)?;
    debug_to_file(&txn, &channel.borrow(), "debug3")?;

    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )?;

    // Checking that unrecord doesn't delete files on the filesystem,
    // but updates the tree/revtree tables.
    let mut files = repo.list_files();
    files.sort();
    assert_eq!(files, &["dir", "dir/file"]);
    assert!(crate::fs::iter_working_copy(&txn, Inode::ROOT)
        .next()
        .is_none());
    txn.commit()?;

    Ok(())
}

#[test]
fn file_del() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn = env.mut_txn_begin().unwrap();

    let mut channel = txn.open_or_create_channel("main")?;

    repo.add_file("file", b"blabla".to_vec());
    txn.add_file("file")?;
    let h0 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;

    repo.remove_path("file")?;
    let h = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;

    debug_to_file(&txn, &channel.borrow(), "debug")?;
    debug!("unrecord h");
    // Unrecording the deletion.
    crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &h)?;
    debug_to_file(&txn, &channel.borrow(), "debug2")?;
    let conflicts = output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }
    assert_eq!(repo.list_files(), vec!["file"]);

    // Unrecording the initial change.
    debug!("unrecord h0");
    crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &h0)?;
    debug_to_file(&txn, &channel.borrow(), "debug3")?;
    let conflicts = output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
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

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn = env.mut_txn_begin().unwrap();

    let mut channel = txn.open_or_create_channel("main")?;

    repo.add_file("file", b"a\nb\n".to_vec());
    txn.add_file("file")?;
    record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;

    let mut channel2 = txn.fork(&channel, "main2")?;

    repo.write_file::<_, std::io::Error, _>("file", |w| Ok(w.write_all(b"a\nx\nb\n")?))?;
    record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;
    repo.write_file::<_, std::io::Error, _>("file", |w| Ok(w.write_all(b"a\ny\nb\n")?))?;
    let b = record_all(&mut repo, &changes, &mut txn, &mut channel2, "")?;

    apply::apply_change(&changes, &mut txn, &mut channel, &b)?;
    debug_to_file(&txn, &channel.borrow(), "debug")?;
    let conflicts = output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )?;
    debug!("conflicts = {:#?}", conflicts);
    assert_eq!(conflicts.len(), 1);
    match conflicts[0] {
        Conflict::Order { .. } => {}
        ref c => panic!("c = {:?}", c),
    }

    let mut buf = Vec::new();
    repo.read_file("file", &mut buf)?;
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    repo.write_file::<_, std::io::Error, _>("file", |w| {
        for l in conflict.iter() {
            if l.starts_with(">>>") {
                writeln!(w, "bla\n{}\nbli", l)?
            } else {
                writeln!(w, "{}", l)?
            }
        }
        Ok(())
    })?;
    let c = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;
    debug_to_file(&txn, &channel.borrow(), "debug2")?;

    crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &c)?;
    debug_to_file(&txn, &channel.borrow(), "debug3")?;

    let conflicts = output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )?;
    debug!("conflicts = {:#?}", conflicts);
    assert_eq!(conflicts.len(), 1);
    match conflicts[0] {
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

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn = env.mut_txn_begin().unwrap();

    let mut channel = txn.open_or_create_channel("main")?;

    // Write a-b-c
    repo.add_file("file", b"a\nb\nc\n".to_vec());
    txn.add_file("file")?;
    record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;

    // Delete -b-
    if delete_file {
        repo.remove_path("file")?
    } else {
        repo.write_file::<_, std::io::Error, _>("file", |w| {
            w.write_all(b"a\nd\n")?;
            Ok(())
        })?;
    }
    let h_del = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;

    // Rollback the deletion of -b-
    let p_del = changes.get_change(&h_del)?;
    debug!("p_del = {:#?}", p_del);
    let p_inv = p_del.inverse(
        &h_del,
        crate::change::ChangeHeader {
            authors: vec![],
            message: "rollback".to_string(),
            description: None,
            timestamp: chrono::Utc::now(),
        },
        Vec::new(),
    );
    let h_inv = changes.save_change(&p_inv)?;
    apply::apply_change(&changes, &mut txn, &mut channel, &h_inv)?;
    let conflicts = output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel.borrow(), "debug")?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts)
    }
    let mut buf = Vec::new();
    repo.read_file("file", &mut buf)?;
    assert_eq!(std::str::from_utf8(&buf), Ok("a\nb\nc\n"));

    // Unrecord the rollback
    crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &h_inv)?;
    let conflicts = output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn, &channel.borrow(), "debug2")?;
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

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel = txn.open_or_create_channel("main")?;
    let mut channel2 = txn.open_or_create_channel("main2")?;

    repo.add_file("file", b"blabla\nblibli\nblublu\n".to_vec());
    txn.add_file("file")?;
    let h0 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;
    debug!("h0 = {:?}", h0);

    apply::apply_change(&changes, &mut txn, &mut channel2, &h0)?;

    // First deletion
    repo.write_file::<_, std::io::Error, _>("file", |w| {
        writeln!(w, "blabla\nblublu")?;
        Ok(())
    })?;
    let h1 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;
    debug!("h1 = {:?}", h1);

    debug_to_file(&txn, &channel.borrow(), "debug0")?;

    // Second deletion
    let h2 = record_all(&mut repo, &changes, &mut txn, &mut channel2, "")?;
    debug!("h2 = {:?}", h2);

    // Both deletions together.
    debug!("applying");
    apply::apply_change(&changes, &mut txn, &mut channel, &h2)?;

    debug_to_file(&txn, &channel.borrow(), "debug1a")?;
    debug_to_file(&txn, &channel2.borrow(), "debug1b")?;
    debug!("unrecord h");
    crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &h2)?;
    debug_to_file(&txn, &channel.borrow(), "debug2")?;

    let conflicts = output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
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

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel = txn.open_or_create_channel("main")?;
    let mut channel2 = txn.open_or_create_channel("main2")?;

    repo.add_file("file", b"blabla\nblibli\nblublu\n".to_vec());
    txn.add_file("file")?;
    let h0 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;
    debug!("h0 = {:?}", h0);

    apply::apply_change(&changes, &mut txn, &mut channel2, &h0)?;

    // First deletion
    repo.write_file::<_, std::io::Error, _>("file", |w| {
        write!(w, "blabla\nblibli\n")?;
        Ok(())
    })?;
    let h1 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;
    debug!("h1 = {:?}", h1);

    debug_to_file(&txn, &channel.borrow(), "debug0")?;

    // Second deletion
    repo.write_file::<_, std::io::Error, _>("file", |w| {
        writeln!(w, "blabla")?;
        Ok(())
    })?;
    let h2 = record_all(&mut repo, &changes, &mut txn, &mut channel2, "")?;
    debug!("h2 = {:?}", h2);

    // Both deletions together, then unrecord on ~channel~.
    debug!("applying");
    apply::apply_change(&changes, &mut txn, &mut channel, &h2)?;

    debug_to_file(&txn, &channel.borrow(), "debug1a")?;
    debug_to_file(&txn, &channel2.borrow(), "debug1b")?;
    debug!("unrecord h");
    crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &h2)?;
    debug_to_file(&txn, &channel.borrow(), "debug2")?;

    let conflicts = output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }

    // Same on ~channel2~, but with a few extra layers of rollbacks in between.
    debug!("rolling back");
    apply::apply_change(&changes, &mut txn, &mut channel2, &h1)?;
    let rollback = |h| {
        let p = changes.get_change(&h).unwrap();
        let p_inv = p.inverse(
            &h,
            crate::change::ChangeHeader {
                authors: vec![],
                message: "rollback".to_string(),
                description: None,
                timestamp: chrono::Utc::now(),
            },
            Vec::new(),
        );
        let h_inv = changes.save_change(&p_inv).unwrap();
        h_inv
    };
    let mut h = h2;
    for i in 0..6 {
        let r = rollback(h);
        apply::apply_change(&changes, &mut txn, &mut channel2, &r).unwrap();
        debug_to_file(&txn, &channel2.borrow(), format!("debug_{}", i))?;
        h = r
    }
    crate::unrecord::unrecord(&mut txn, &mut channel2, &changes, &h1)?;
    debug_to_file(&txn, &channel2.borrow(), "debug_final")?;

    let conflicts = output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
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

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel = txn.open_or_create_channel("main")?;
    let mut channel2 = txn.open_or_create_channel("main2")?;

    repo.add_file("file", b"blabla\nblibli\nblublu\n".to_vec());
    txn.add_file("file")?;
    let h0 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;
    debug!("h0 = {:?}", h0);

    apply::apply_change(&changes, &mut txn, &mut channel2, &h0)?;

    // First deletion
    repo.remove_path("file")?;
    let h1 = record_all(&mut repo, &changes, &mut txn, &mut channel, "")?;
    debug!("h1 = {:?}", h1);
    // Second deletion
    let h2 = record_all(&mut repo, &changes, &mut txn, &mut channel2, "")?;
    debug!("h2 = {:?}", h2);

    // Both deletions together.
    debug!("applying");
    apply::apply_change(&changes, &mut txn, &mut channel, &h2)?;

    crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &h1)?;
    crate::unrecord::unrecord(&mut txn, &mut channel, &changes, &h2)?;

    let mut inodes = txn.iter_inodes().unwrap();
    assert!(inodes.next().is_some());
    assert!(inodes.next().is_none());
    Ok(())
}
