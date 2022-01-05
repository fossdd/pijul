use super::*;
use crate::working_copy::{WorkingCopy, WorkingCopyRead};
use std::io::Write;

/// Rename conflict
#[test]
fn same_file_test() -> Result<(), anyhow::Error> {
    same_file_("file", "alice", "bob")
}

/// Rename conflict
#[test]
fn same_file_dirs_test() -> Result<(), anyhow::Error> {
    same_file_("file", "alice/file", "bob/file")
}

fn same_file_(file: &str, alice: &str, bob: &str) -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";

    let repo_alice = working_copy::memory::Memory::new();
    let repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file(file, contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let txn_alice = env_alice.arc_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let txn_bob = env_bob.arc_txn_begin().unwrap();

    let channel_alice = txn_alice.write().open_or_create_channel("alice").unwrap();

    txn_alice.write().add_file(file, 0).unwrap();
    let init_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    // Bob clones
    let channel_bob = txn_bob.write().open_or_create_channel("bob").unwrap();
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
    )?;

    // Alice renames "file" to "alice"
    repo_alice.rename(file, alice)?;
    txn_alice.write().move_file(file, alice, 0)?;
    debug!("repo_bob = {:?}", repo_alice.list_files());
    let alice_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "").unwrap();

    // Bob renames "file" to "bob"
    repo_bob.rename(file, bob)?;
    txn_bob.write().move_file(file, bob, 0)?;
    debug!("repo_bob = {:?}", repo_bob.list_files());
    let bob_h = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "").unwrap();

    // Alice applies Bob's change
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &bob_h)?;
    let conflicts = output::output_repository_no_pending(
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
    match conflicts.iter().next().unwrap() {
        Conflict::MultipleNames { .. } => {}
        ref c => panic!("{:#?}", c),
    }

    // Bob applies Alice's change
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &alice_h)?;
    let conflicts = output::output_repository_no_pending(
        &repo_bob,
        &changes,
        &txn_bob,
        &channel_bob,
        alice,
        true,
        None,
        1,
        0,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }
    let conflicts = output::output_repository_no_pending(
        &repo_bob,
        &changes,
        &txn_bob,
        &channel_bob,
        bob,
        true,
        None,
        1,
        0,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }

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
    match conflicts.iter().next().unwrap() {
        Conflict::MultipleNames { .. } => {}
        ref c => panic!("{:#?}", c),
    }

    // Bob solves.
    {
        let txn_ = txn_bob.write();
        let mut f = std::fs::File::create("/tmp/conflict0")?;
        crate::pristine::debug(&*txn_, &txn_.graph(&*channel_bob.read()), &mut f)?;
    }
    info!("recording resolution");
    let bob_solution = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "").unwrap();
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
    if !conflicts.is_empty() {
        let txn_ = txn_bob.write();
        let mut f = std::fs::File::create("/tmp/conflict1")?;
        crate::pristine::debug(&*txn_, &txn_.graph(&*channel_bob.read()), &mut f)?;
        panic!("conflicts = {:#?}", conflicts);
    }

    // Alice applies Bob's solution.
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &bob_solution)?;
    let conflicts = output::output_repository_no_pending(
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
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }

    debug!("repo_alice = {:?}", repo_alice.list_files());
    debug!("repo_bob = {:?}", repo_bob.list_files());
    debug_tree(&*txn_bob.read(), "debug_tree")?;
    Ok(())
}

/// Alice and Bob move two different files to the same name.
#[test]
fn same_name_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";

    let repo_alice = working_copy::memory::Memory::new();
    let repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file1", contents.to_vec());
    repo_alice.add_file("file2", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let txn_alice = env_alice.arc_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let txn_bob = env_bob.arc_txn_begin().unwrap();

    let channel_alice = txn_alice.write().open_or_create_channel("alice")?;

    txn_alice.write().add_file("file1", 0)?;
    txn_alice.write().add_file("file2", 0)?;
    info!("recording file additions");
    debug!("working_copy = {:?}", repo_alice);
    debug_tree(&*txn_alice.read(), "debug_tree")?;
    let init_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    // Bob clones
    let channel_bob = txn_bob.write().open_or_create_channel("bob").unwrap();
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
    )?;

    // Alice renames "file1" to "file"
    repo_alice.rename("file1", "file")?;
    txn_alice.write().move_file("file1", "file", 0)?;

    debug!("repo_bob = {:?}", repo_alice.list_files());
    let alice_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "").unwrap();

    // Bob renames "file2" to "file"
    repo_bob.rename("file2", "file")?;
    txn_bob.write().move_file("file2", "file", 0)?;
    debug!("repo_bob = {:?}", repo_bob.list_files());
    let bob_h = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "").unwrap();

    // Alice applies Bob's change
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

    // Bob applies Alice's change
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &alice_h)?;
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

    assert!(!conflicts.is_empty());

    let mut files_alice = repo_alice.list_files();
    debug!("repo_alice = {:?}", files_alice);
    assert_eq!(files_alice.len(), 2);
    files_alice.sort();
    assert_eq!(files_alice[0], "file");
    assert!(files_alice[1].starts_with("file."));

    // Alice solves it.
    txn_alice.write().move_file(&files_alice[1], "a1", 0)?;
    repo_alice.rename(&files_alice[0], "file")?;
    repo_alice.rename(&files_alice[1], "a1")?;
    let solution_alice = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "").unwrap();

    let mut files_bob = repo_bob.list_files();
    debug!("repo_bob = {:?}", files_bob);
    assert_eq!(files_bob.len(), 2);
    files_bob.sort();
    assert_eq!(files_bob[0], "file");
    assert!(files_bob[1].starts_with("file."));

    // Bob applies Alice's solution and checks that it does solve his problem.
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &solution_alice)?;
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
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }
    let mut files_bob = repo_bob.list_files();
    files_bob.sort();
    assert_eq!(files_bob, vec!["a1", "file"]);
    Ok(())
}

#[test]
fn file_conflicts_same_name_and_two_names() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";

    let repo_alice = working_copy::memory::Memory::new();
    let mut repo_bob = working_copy::memory::Memory::new();
    let repo_charlie = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file1", contents.to_vec());
    repo_alice.add_file("file2", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let txn_alice = env_alice.arc_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.arc_txn_begin().unwrap();

    let channel_alice = txn_alice.write().open_or_create_channel("alice")?;

    txn_alice.write().add_file("file1", 0)?;
    txn_alice.write().add_file("file2", 0)?;
    info!("recording file additions");
    debug!("working_copy = {:?}", repo_alice);
    debug_tree(&*txn_alice.read(), "debug_tree")?;
    let init_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    // Bob clones and renames "file2" to "file"
    let mut channel_bob = txn_bob.write().open_or_create_channel("bob").unwrap();
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
    )?;
    repo_bob.rename("file2", "file")?;
    txn_bob.write().move_file("file2", "file", 0)?;
    debug!("repo_bob = {:?}", repo_bob.list_files());
    let bob_h = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "").unwrap();

    // Alice renames "file1" to "file"
    repo_alice.rename("file1", "file")?;
    txn_alice.write().move_file("file1", "file", 0)?;

    debug!("repo_bob = {:?}", repo_alice.list_files());
    let alice_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "").unwrap();

    // Charlie clones, moves "file1" to "file3" and applies both
    // Alice's and Bob's change.
    let env_charlie = pristine::sanakirja::Pristine::new_anon()?;
    let txn_charlie = env_charlie.arc_txn_begin().unwrap();
    let channel_charlie = txn_charlie
        .write()
        .open_or_create_channel("charlie")
        .unwrap();
    apply::apply_change_arc(&changes, &txn_charlie, &channel_charlie, &init_h).unwrap();
    output::output_repository_no_pending(
        &repo_charlie,
        &changes,
        &txn_charlie,
        &channel_charlie,
        "",
        true,
        None,
        1,
        0,
    )?;
    repo_charlie.rename("file1", "file3")?;
    txn_charlie.write().move_file("file1", "file3", 0)?;
    let charlie_h =
        record_all(&repo_charlie, &changes, &txn_charlie, &channel_charlie, "").unwrap();

    apply::apply_change_arc(&changes, &txn_charlie, &channel_charlie, &bob_h)?;
    apply::apply_change_arc(&changes, &txn_charlie, &channel_charlie, &alice_h)?;

    {
        let txn_ = txn_charlie.write();
        let mut f = std::fs::File::create("/tmp/charlie0")?;
        crate::pristine::debug(&*txn_, &txn_.graph(&*channel_charlie.read()), &mut f)?;
    }
    output::output_repository_no_pending(
        &repo_charlie,
        &changes,
        &txn_charlie,
        &channel_charlie,
        "",
        true,
        None,
        1,
        0,
    )?;
    let mut files_charlie = repo_charlie.list_files();
    files_charlie.sort();
    // Two files with the same name (file), one of which also has another name (file3). This means that we don't know which one of the two names crate::output will pick, between "file3" and the conflicting name.
    // This depends on which file gets output first.
    assert_eq!(files_charlie[0], "file");
    assert!(files_charlie[1] == "file3" || files_charlie[1].starts_with("file."));
    debug!("files_charlie {:?}", files_charlie);

    repo_charlie.rename(&files_charlie[1], "file3")?;
    txn_charlie
        .write()
        .move_file(&files_charlie[1], "file3", 0)?;
    let _charlie_solution =
        record_all(&repo_charlie, &changes, &txn_charlie, &channel_charlie, "").unwrap();

    {
        let txn_ = txn_charlie.write();
        let mut f = std::fs::File::create("/tmp/charlie1")?;
        crate::pristine::debug(&*txn_, &txn_.graph(&*channel_charlie.read()), &mut f)?;
    }

    output::output_repository_no_pending(
        &repo_charlie,
        &changes,
        &txn_charlie,
        &channel_charlie,
        "",
        true,
        None,
        1,
        0,
    )?;
    let mut files_charlie = repo_charlie.list_files();
    files_charlie.sort();
    assert_eq!(files_charlie, &["file", "file3"]);

    // Alice applies Bob's change and Charlie's change.
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &bob_h)?;
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &charlie_h)?;
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
    let mut files_alice = repo_alice.list_files();
    files_alice.sort();
    debug!("files_alice {:?}", files_alice);
    repo_alice.remove_path(&files_alice[1], false).unwrap();
    let _alice_solution =
        record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "").unwrap();

    // Bob applies Alice's change and Charlie's change
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &alice_h)?;
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &charlie_h)?;
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
    let files_bob = repo_bob.list_files();
    debug!("files_bob {:?}", files_bob);
    repo_bob.remove_path(&files_bob[1], false).unwrap();
    let _bob_solution =
        record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
    Ok(())
}

#[test]
fn zombie_file_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";
    let contents2 = b"a\nb\nc\nx\nd\ne\nf\n";

    let repo_alice = working_copy::memory::Memory::new();
    let repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("a/b/c/file", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let txn_alice = env_alice.arc_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let txn_bob = env_bob.arc_txn_begin().unwrap();

    let channel_alice = txn_alice.write().open_or_create_channel("alice").unwrap();

    txn_alice.write().add_file("a/b/c/file", 0).unwrap();
    let init_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    // Bob clones
    let channel_bob = txn_bob.write().open_or_create_channel("bob").unwrap();
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
    )?;

    // Alice deletes "file"
    repo_alice.remove_path("a/b", true)?;
    let alice_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "").unwrap();

    // Bob edits "file"
    repo_bob
        .write_file("a/b/c/file", Inode::ROOT)
        .unwrap()
        .write_all(contents2)?;
    let bob_h = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "").unwrap();

    // Alice applies Bob's change
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &bob_h)?;
    debug!("alice2");
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
    let files_alice = repo_alice.list_files();
    assert_eq!(files_alice, vec!["a", "a/b", "a/b/c", "a/b/c/file"]);
    for x in txn_alice
        .read()
        .iter_tree(
            &OwnedPathId {
                parent_inode: Inode::ROOT,
                basename: crate::small_string::SmallString::new(),
            },
            None,
        )
        .unwrap()
    {
        debug!("x = {:?}", x);
    }
    debug!("recording a solution");
    let alice_solution = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "").unwrap();

    // Bob applies Alice's change
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &alice_h)?;
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
    debug!("repo_alice = {:?}", repo_alice.list_files());
    debug!("repo_bob = {:?}", repo_bob.list_files());
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &alice_solution)?;
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
    let files_bob = repo_bob.list_files();
    assert_eq!(files_bob, vec!["a", "a/b", "a/b/c", "a/b/c/file"]);
    Ok(())
}

#[test]
fn rename_zombie_file() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";
    let contents2 = b"a\nb\nc\nx\nd\ne\nf\n";

    let mut repo_alice = working_copy::memory::Memory::new();
    let repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("a/b/c/file", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.arc_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let txn_bob = env_bob.arc_txn_begin().unwrap();

    let mut channel_alice = txn_alice.write().open_or_create_channel("alice").unwrap();

    txn_alice.write().add_file("a/b/c/file", 0).unwrap();
    let init_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    // Bob clones
    let channel_bob = txn_bob.write().open_or_create_channel("bob").unwrap();
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
    )?;

    // Alice deletes "file"
    repo_alice.remove_path("a/b", true)?;
    let alice_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )
    .unwrap();

    // Bob renames "file"
    repo_bob.rename("a/b/c/file", "a/b/c/file2")?;
    repo_bob
        .write_file("a/b/c/file2", Inode::ROOT)
        .unwrap()
        .write_all(contents2)?;
    txn_bob.write().move_file("a/b/c/file", "a/b/c/file2", 0)?;
    let bob_h = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "").unwrap();

    // Alice applies Bob's change
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &bob_h)?;

    debug!("alice2");
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
    let files_alice = repo_alice.list_files();
    debug!("Alice records {:?}", files_alice);
    repo_alice.rename("a/b/c/file", "a/b/c/file2").unwrap_or(());
    // repo_alice.remove_path("a/b/c/file", false).unwrap_or(());
    // repo_alice.remove_path("a/b/c/file2", false).unwrap_or(());

    txn_alice
        .write()
        .move_file("a/b/c/file", "a/b/c/file2", 0)
        .unwrap_or(());
    let alice_solution = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "").unwrap();
    debug!("Alice recorded {:?}", alice_solution);

    // Bob applies Alice's change
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &alice_h)?;
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
    debug!("repo_alice = {:?}", repo_alice.list_files());
    debug!("repo_bob = {:?}", repo_bob.list_files());
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &alice_solution)?;
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
    let files_bob = repo_bob.list_files();
    assert!(["a", "a/b", "a/b/c", "a/b/c/file2"]
        .iter()
        .all(|n| files_bob.iter().any(|m| m == n)));
    Ok(())
}

#[test]
fn rename_zombie_dir() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";
    let contents2 = b"a\nb\nc\nx\nd\ne\nf\n";

    let mut repo_alice = working_copy::memory::Memory::new();
    let repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("a/b/c/file", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.arc_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let txn_bob = env_bob.arc_txn_begin().unwrap();

    let mut channel_alice = txn_alice.write().open_or_create_channel("alice").unwrap();

    txn_alice.write().add_file("a/b/c/file", 0).unwrap();
    let init_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;

    // Bob clones
    let channel_bob = txn_bob.write().open_or_create_channel("bob").unwrap();
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
    )?;

    // Alice deletes "file"
    repo_alice.remove_path("a/b", true)?;
    let alice_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )
    .unwrap();

    // Bob renames "file"
    repo_bob.rename("a/b/c", "a/b/d")?;
    repo_bob
        .write_file("a/b/d/file", Inode::ROOT)
        .unwrap()
        .write_all(contents2)?;
    txn_bob.write().move_file("a/b/c", "a/b/d", 0)?;
    let bob_h = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "").unwrap();

    // Alice applies Bob's change
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &bob_h)?;
    debug!("alice2");
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
    let files_alice = repo_alice.list_files();
    if files_alice.iter().any(|x| x == "a/b/d/file") {
        let _ = txn_alice.write().add_file("a/b/d/file", 0);
    } else {
        assert!(files_alice.iter().any(|x| x == "a/b/c/file"));
        txn_alice.write().move_file("a/b/c", "a/b/d", 0).unwrap();
        repo_alice.rename("a/b/c", "a/b/d").unwrap();
    }
    debug!("Alice records");
    let alice_solution = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "").unwrap();
    debug!("Alice recorded {:?}", alice_solution);

    // Bob applies Alice's change
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &alice_h)?;
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
    debug!("repo_alice = {:?}", repo_alice.list_files());
    debug!("repo_bob = {:?}", repo_bob.list_files());

    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &alice_solution)?;
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
    let files_bob = repo_bob.list_files();
    debug!("files_bob = {:?}", files_bob);
    assert!(["a", "a/b", "a/b/d", "a/b/d/file"]
        .iter()
        .all(|n| files_bob.iter().any(|m| m == n)));
    Ok(())
}

#[test]
fn double_zombie_file() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";
    let contents2 = b"a\nb\nc\nx\nd\ne\nf\n";
    let contents3 = b"a\nby\n\nc\nd\ne\nf\n";

    let repo_alice = working_copy::memory::Memory::new();
    let repo_bob = working_copy::memory::Memory::new();
    let repo_charlie = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let txn_alice = env_alice.arc_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let txn_bob = env_bob.arc_txn_begin().unwrap();
    let env_charlie = pristine::sanakirja::Pristine::new_anon()?;
    let txn_charlie = env_charlie.arc_txn_begin().unwrap();

    let channel_alice = txn_alice.write().open_or_create_channel("alice").unwrap();

    repo_alice.add_file("a/b/c/file", contents.to_vec());
    txn_alice.write().add_file("a/b/c/file", 0).unwrap();
    let init_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    // Bob and Charlie clone
    let channel_bob = txn_bob.write().open_or_create_channel("bob").unwrap();
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
    )?;

    let channel_charlie = txn_charlie
        .write()
        .open_or_create_channel("charlie")
        .unwrap();
    apply::apply_change_arc(&changes, &txn_charlie, &channel_charlie, &init_h).unwrap();
    let conflicts = output::output_repository_no_pending(
        &repo_charlie,
        &changes,
        &txn_charlie,
        &channel_charlie,
        "",
        true,
        None,
        1,
        0,
    )?;
    if !conflicts.is_empty() {
        panic!("charlie has conflicts: {:?}", conflicts);
    }

    // Alice deletes "file"
    repo_alice.remove_path("a/b", true)?;
    let alice_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "").unwrap();

    // Bob edits "file"
    repo_bob
        .write_file("a/b/c/file", Inode::ROOT)
        .unwrap()
        .write_all(contents2)?;
    let bob_h = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "").unwrap();

    // Charlie edits "file"
    repo_charlie
        .write_file("a/b/c/file", Inode::ROOT)
        .unwrap()
        .write_all(contents3)?;
    let charlie_h =
        record_all(&repo_charlie, &changes, &txn_charlie, &channel_charlie, "").unwrap();

    // Alice applies Bob's and Charlie's changes
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &bob_h)?;
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &charlie_h)?;
    debug!("alice2");
    let conflicts = output::output_repository_no_pending(
        &repo_alice,
        &changes,
        &txn_alice,
        &channel_alice,
        "",
        true,
        None,
        1,
        0,
    )?
    .into_iter()
    .collect::<Vec<_>>();
    let files_alice = repo_alice.list_files();
    assert_eq!(files_alice, vec!["a", "a/b", "a/b/c", "a/b/c/file"]);
    let expected = [
        Conflict::ZombieFile {
            path: "a/b".to_string(),
        },
        Conflict::ZombieFile {
            path: "a/b/c".to_string(),
        },
        Conflict::ZombieFile {
            path: "a/b/c/file".to_string(),
        },
        Conflict::Zombie {
            path: "a/b/c/file".to_string(),
            line: 1,
        },
    ];
    assert_eq!(&conflicts[..], &expected[..]);
    let mut buf = Vec::new();
    repo_alice.read_file("a/b/c/file", &mut buf)?;
    // Alice removes conflict markers.
    {
        let mut w = repo_alice.write_file("a/b/c/file", Inode::ROOT).unwrap();
        for l in std::str::from_utf8(&buf).unwrap().lines() {
            if l.len() < 10 {
                writeln!(w, "{}", l)?
            }
        }
    }

    let alice_solution = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    // Bob applies
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &alice_h)?;
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &charlie_h)?;
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
    )?
    .into_iter()
    .collect::<Vec<_>>();

    assert_eq!(&conflicts[..], &expected[..]);

    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &alice_solution)?;
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
    if !conflicts.is_empty() {
        panic!("bob has conflicts: {:?}", conflicts);
    }

    // Charlie applies
    apply::apply_change_arc(&changes, &txn_charlie, &channel_charlie, &bob_h)?;
    debug!("charlie applies Alice's change");
    apply::apply_change_arc(&changes, &txn_charlie, &channel_charlie, &alice_h)?;
    let conflicts = output::output_repository_no_pending(
        &repo_charlie,
        &changes,
        &txn_charlie,
        &channel_charlie,
        "",
        true,
        None,
        1,
        0,
    )?
    .into_iter()
    .collect::<Vec<_>>();
    assert_eq!(&conflicts[..], &expected[..]);
    debug!("charlie applies Alice's solution");
    apply::apply_change_arc(&changes, &txn_charlie, &channel_charlie, &alice_solution)?;
    let conflicts = output::output_repository_no_pending(
        &repo_charlie,
        &changes,
        &txn_charlie,
        &channel_charlie,
        "",
        true,
        None,
        1,
        0,
    )?;
    if !conflicts.is_empty() {
        panic!("charlie has conflicts: {:?}", conflicts);
    }

    Ok(())
}

#[test]
fn zombie_file_post_resolve() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";

    let repo_alice = working_copy::memory::Memory::new();
    let repo_bob = working_copy::memory::Memory::new();
    let repo_charlie = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let txn_alice = env_alice.arc_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let txn_bob = env_bob.arc_txn_begin().unwrap();
    let env_charlie = pristine::sanakirja::Pristine::new_anon()?;
    let txn_charlie = env_charlie.arc_txn_begin().unwrap();

    let channel_alice = txn_alice.write().open_or_create_channel("alice").unwrap();

    repo_alice.add_file("a/b/c/file", contents.to_vec());
    txn_alice.write().add_file("a/b/c/file", 0).unwrap();
    let init_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    repo_alice.rename("a/b/c/file", "a/b/c/alice")?;
    txn_alice
        .write()
        .move_file("a/b/c/file", "a/b/c/alice", 0)?;
    let alice_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    // Bob clones
    let channel_bob = txn_bob.write().open_or_create_channel("bob").unwrap();
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
    )?;

    // Bob deletes "file"
    repo_bob.remove_path("a/b/c/file", false)?;
    let bob_h = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "").unwrap();
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &alice_h).unwrap();
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
    debug!("conflicts = {:#?}", conflicts);
    assert_eq!(conflicts.len(), 1);
    match conflicts.iter().next().unwrap() {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "a/b/c/alice"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }

    debug!("Bob resolves");
    let bob_resolution = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "").unwrap();
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
    if !conflicts.is_empty() {
        panic!("Bob has conflicts: {:?}", conflicts);
    }

    // Alice applies Bob's patch and solution.
    debug!("Alice applies Bob's resolution");
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &bob_h).unwrap();
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &bob_resolution).unwrap();
    let conflicts = output::output_repository_no_pending(
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
    if !conflicts.is_empty() {
        panic!("Alice has conflicts: {:?}", conflicts);
    }

    // Charlie applies Alice's move and deletes (i.e. does the same as Bob).
    let channel_charlie = txn_charlie
        .write()
        .open_or_create_channel("charlie")
        .unwrap();
    apply::apply_change_arc(&changes, &txn_charlie, &channel_charlie, &init_h).unwrap();
    apply::apply_change_arc(&changes, &txn_charlie, &channel_charlie, &alice_h).unwrap();
    let conflicts = output::output_repository_no_pending(
        &repo_charlie,
        &changes,
        &txn_charlie,
        &channel_charlie,
        "",
        true,
        None,
        1,
        0,
    )?;
    if !conflicts.is_empty() {
        panic!("charlie has conflicts: {:?}", conflicts);
    }

    debug!("Charlie applies Alice's move and deletes");
    repo_charlie.remove_path("a/b/c/alice", false)?;
    let charlie_h =
        record_all(&repo_charlie, &changes, &txn_charlie, &channel_charlie, "").unwrap();

    //
    debug!("Charlie applies Bob's deletion");
    apply::apply_change_arc(&changes, &txn_charlie, &channel_charlie, &bob_h).unwrap();
    debug!("Charlie applies Bob's resolution");
    apply::apply_change_arc(&changes, &txn_charlie, &channel_charlie, &bob_resolution).unwrap();
    let conflicts = output::output_repository_no_pending(
        &repo_charlie,
        &changes,
        &txn_charlie,
        &channel_charlie,
        "",
        true,
        None,
        1,
        0,
    )?;
    assert_eq!(conflicts.len(), 1);
    match conflicts.iter().next().unwrap() {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "a/b/c/alice"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }

    //
    debug!("Alice applies Charlie's deletion");
    apply::apply_change_arc(&changes, &txn_alice, &channel_alice, &charlie_h).unwrap();
    let conflicts = output::output_repository_no_pending(
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
    assert_eq!(conflicts.len(), 1);
    match conflicts.iter().next().unwrap() {
        Conflict::ZombieFile { ref path } => assert!(path == "a/b/c/file" || path == "a/b/c/alice"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }

    //
    apply::apply_change_arc(&changes, &txn_bob, &channel_bob, &charlie_h).unwrap();

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
    assert_eq!(conflicts.len(), 1);
    match conflicts.iter().next().unwrap() {
        Conflict::ZombieFile { ref path } => assert!(path == "a/b/c/file" || path == "a/b/c/alice"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }

    Ok(())
}

#[test]
fn move_vs_delete_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo_alice = working_copy::memory::Memory::new();
    let repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let txn_alice = env_alice.arc_txn_begin().unwrap();
    let channel_alice = txn_alice.write().open_or_create_channel("main")?;
    repo_alice.add_file("file", b"a\n".to_vec());
    txn_alice.write().add_file("file", 0)?;
    let init = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let txn_bob = env_bob.arc_txn_begin().unwrap();
    let channel_bob = txn_bob.write().open_or_create_channel("main")?;
    txn_bob
        .write()
        .apply_change(&changes, &mut *channel_bob.write(), &init)
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

    // Alice moves "file"
    repo_alice.rename("file", "alice/file").unwrap_or(());
    txn_alice
        .write()
        .move_file("file", "alice/file", 0)
        .unwrap_or(());
    let alice_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    // Bob deletes "file"
    repo_bob.remove_path("file", false).unwrap_or(());
    let bob_h = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "")?;

    // Bob applies Alice's change
    debug!("Bob applies Alice's change");
    txn_bob
        .write()
        .apply_change(&changes, &mut *channel_bob.write(), &alice_h)
        .unwrap();
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
    debug!("conflicts = {:#?}", conflicts);
    assert_eq!(conflicts.len(), 1);
    match conflicts.iter().next().unwrap() {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "alice/file"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }
    let files = repo_bob.list_files();
    if files.iter().any(|f| f == "alice/file") {
        repo_bob.remove_path("bob", false).unwrap()
    } else {
        repo_bob.remove_path("alice", false).unwrap()
    }
    let resolution = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "")?;
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
    if !conflicts.is_empty() {
        panic!("Bob has conflicts: {:?}", conflicts);
    }

    // Alice applies Bob's change
    debug!("Alice applies Bob's change");
    txn_alice
        .write()
        .apply_change(&changes, &mut *channel_alice.write(), &bob_h)
        .unwrap();
    let conflicts = output::output_repository_no_pending(
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
    assert_eq!(conflicts.len(), 1);
    match conflicts.iter().next().unwrap() {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "alice/file"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }
    debug!("Alice applies Bob's resolution");
    txn_alice
        .write()
        .apply_change(&changes, &mut *channel_alice.write(), &resolution)
        .unwrap();
    let conflicts = output::output_repository_no_pending(
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
    if !conflicts.is_empty() {
        panic!("Alice has conflicts: {:?}", conflicts);
    }

    Ok(())
}

// Delete the context of an edit inside a file, then delete the file,
// and see if the edit has its context fixed.
#[test]
fn delete_zombie_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo_alice = working_copy::memory::Memory::new();
    let repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let txn_alice = env_alice.arc_txn_begin().unwrap();
    let channel_alice = txn_alice.write().open_or_create_channel("main")?;
    repo_alice.add_file("file", b"a\nb\nc\nd\n".to_vec());
    txn_alice.write().add_file("file", 0)?;
    let init = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let txn_bob = env_bob.arc_txn_begin().unwrap();
    let channel_bob = txn_bob.write().open_or_create_channel("main")?;
    txn_bob
        .write()
        .apply_change(&changes, &mut *channel_bob.write(), &init)
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

    // Alice adds a zombie line.
    repo_alice
        .write_file("file", Inode::ROOT)
        .unwrap()
        .write_all(b"a\nb\nx\nc\nd\n")?;
    record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    // Bob deletes the context of Alice's new line, and then deletes
    // "file".
    repo_bob
        .write_file("file", Inode::ROOT)
        .unwrap()
        .write_all(b"a\nd\n")?;
    let bob_h1 = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "")?;
    repo_bob.remove_path("file", false).unwrap_or(());
    let bob_h2 = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "")?;

    // Alice applies Bob's changes.
    debug!("Alice applies Bob's change");
    txn_alice
        .write()
        .apply_change(&changes, &mut *channel_alice.write(), &bob_h1)
        .unwrap();
    debug!("Applying bob_h2");
    txn_alice
        .write()
        .apply_change(&changes, &mut *channel_alice.write(), &bob_h2)
        .unwrap();

    let (alive, reachable) = check_alive(&*txn_alice.read(), &channel_alice.read());
    if !alive.is_empty() {
        panic!("alive (bob0): {:?}", alive);
    }
    if !reachable.is_empty() {
        panic!("reachable (bob0): {:?}", reachable);
    }

    crate::unrecord::unrecord(
        &mut *txn_alice.write(),
        &channel_alice,
        &changes,
        &bob_h2,
        0,
    )
    .unwrap();
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
    let mut buf = Vec::new();
    repo_alice.read_file("file", &mut buf)?;
    debug!("file = {:?}", std::str::from_utf8(&buf));
    Ok(())
}

#[test]
fn move_into_deleted_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo_alice = working_copy::memory::Memory::new();
    let mut repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let txn_alice = env_alice.arc_txn_begin().unwrap();
    let channel_alice = txn_alice.write().open_or_create_channel("main")?;
    repo_alice.add_file("file", b"a\n".to_vec());
    repo_alice.add_dir("dir");
    txn_alice.write().add_file("file", 0)?;
    txn_alice.write().add_dir("dir", 0)?;
    let init = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.arc_txn_begin().unwrap();
    let mut channel_bob = txn_bob.write().open_or_create_channel("main")?;
    txn_bob
        .write()
        .apply_change(&changes, &mut *channel_bob.write(), &init)
        .unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
        1,
        0,
    )?;

    // Alice moves "file"
    repo_alice.rename("file", "dir/file").unwrap_or(());
    txn_alice
        .write()
        .move_file("file", "dir/file", 0)
        .unwrap_or(());
    let alice_h = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    // Bob deletes "dir"
    repo_bob.remove_path("dir", true).unwrap_or(());
    let bob_h = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "")?;

    // Bob applies Alice's change
    debug!("Bob applies Alice's change");
    txn_bob
        .write()
        .apply_change(&changes, &mut *channel_bob.write(), &alice_h)
        .unwrap();
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
    debug!("conflicts = {:#?}", conflicts);
    assert_eq!(conflicts.len(), 1);
    match conflicts.iter().next().unwrap() {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "dir"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }
    let resolution = record_all(&repo_bob, &changes, &txn_bob, &channel_bob, "")?;
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
    if !conflicts.is_empty() {
        panic!("Bob has conflicts: {:?}", conflicts);
    }

    // Alice applies Bob's change
    debug!("Alice applies Bob's change");
    txn_alice
        .write()
        .apply_change(&changes, &mut *channel_alice.write(), &bob_h)
        .unwrap();
    let conflicts = output::output_repository_no_pending(
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
    assert_eq!(conflicts.len(), 1);
    match conflicts.iter().next().unwrap() {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "dir"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }
    debug!("Alice applies Bob's resolution");
    txn_alice
        .write()
        .apply_change(&changes, &mut *channel_alice.write(), &resolution)
        .unwrap();
    let conflicts = output::output_repository_no_pending(
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
    if !conflicts.is_empty() {
        panic!("Alice has conflicts: {:?}", conflicts);
    }

    Ok(())
}

#[test]
fn move_back_noundel_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo_alice = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let txn_alice = env_alice.arc_txn_begin().unwrap();
    let channel_alice = txn_alice.write().open_or_create_channel("main")?;
    repo_alice.add_file("a", b"a\n".to_vec());
    txn_alice.write().add_file("a", 0)?;
    let _init = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    repo_alice.rename("a", "b").unwrap_or(());
    txn_alice.write().move_file("a", "b", 0)?;
    let _mv = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    repo_alice.rename("b", "a").unwrap_or(());
    txn_alice.write().move_file("b", "a", 0)?;

    info!("MOVE BACK");
    {
        let txn_ = txn_alice.write();
        let mut f = std::fs::File::create("/tmp/moveback")?;
        crate::pristine::debug(&*txn_, &txn_.graph(&*channel_alice.read()), &mut f)?;
    }
    let back = record_all(&repo_alice, &changes, &txn_alice, &channel_alice, "")?;

    let back = changes.get_change(&back).unwrap();
    match back.hashed.changes[0] {
        crate::change::Hunk::FileMove { .. } => {}
        ref x => {
            panic!("{:#?}", x);
        }
    }
    Ok(())
}
