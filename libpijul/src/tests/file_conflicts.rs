use super::*;

use crate::working_copy::WorkingCopy;

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

    let mut repo_alice = working_copy::memory::Memory::new();
    let mut repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file(file, contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();

    let mut channel_alice = txn_alice.open_or_create_channel("alice").unwrap();

    txn_alice.add_file(file).unwrap();
    let init_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice0")?;

    // Bob clones
    let mut channel_bob = txn_bob.open_or_create_channel("bob").unwrap();
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &init_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob0")?;

    // Alice renames "file" to "alice"
    repo_alice.rename(file, alice)?;
    txn_alice.move_file(file, alice)?;
    debug!("repo_bob = {:?}", repo_alice.list_files());
    let alice_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )
    .unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1")?;

    // Bob renames "file" to "bob"
    repo_bob.rename(file, bob)?;
    txn_bob.move_file(file, bob)?;
    debug!("repo_bob = {:?}", repo_bob.list_files());
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1")?;

    // Alice applies Bob's change
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &bob_h)?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1")?;
    let conflicts = output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    match conflicts[0] {
        Conflict::MultipleNames { .. } => {}
        ref c => panic!("{:#?}", c),
    }

    // Bob applies Alice's change
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &alice_h)?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1")?;
    let conflicts = output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        alice,
        true,
        None,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }
    let conflicts = output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        bob,
        true,
        None,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }

    let conflicts = output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    match conflicts[0] {
        Conflict::MultipleNames { .. } => {}
        ref c => panic!("{:#?}", c),
    }

    // Bob solves.
    let bob_solution =
        record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
    let conflicts = output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }

    // Alice applies Bob's solution.
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &bob_solution)?;
    let conflicts = output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }

    debug!("repo_alice = {:?}", repo_alice.list_files());
    debug!("repo_bob = {:?}", repo_bob.list_files());
    debug_tree(&txn_bob, "debug_tree")?;
    Ok(())
}

/// Alice and Bob move two different files to the same name.
#[test]
fn same_name_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";

    let mut repo_alice = working_copy::memory::Memory::new();
    let mut repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file1", contents.to_vec());
    repo_alice.add_file("file2", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();

    let mut channel_alice = txn_alice.open_or_create_channel("alice")?;

    txn_alice.add_file("file1")?;
    txn_alice.add_file("file2")?;
    info!("recording file additions");
    debug!("working_copy = {:?}", repo_alice);
    debug_tree(&txn_alice, "debug_tree")?;
    let init_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice0")?;

    // Bob clones
    let mut channel_bob = txn_bob.open_or_create_channel("bob").unwrap();
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &init_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob0")?;

    // Alice renames "file1" to "file"
    repo_alice.rename("file1", "file")?;
    txn_alice.move_file("file1", "file")?;

    debug!("repo_bob = {:?}", repo_alice.list_files());
    let alice_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )
    .unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1")?;

    // Bob renames "file2" to "file"
    repo_bob.rename("file2", "file")?;
    txn_bob.move_file("file2", "file")?;
    debug!("repo_bob = {:?}", repo_bob.list_files());
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1")?;

    // Alice applies Bob's change
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &bob_h)?;
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

    // Bob applies Alice's change
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &alice_h)?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1")?;
    let conflicts = output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;

    assert!(!conflicts.is_empty());

    let mut files_alice = repo_alice.list_files();
    debug!("repo_alice = {:?}", files_alice);
    assert_eq!(files_alice.len(), 2);
    files_alice.sort();
    assert_eq!(files_alice[0], "file");
    assert!(files_alice[1].starts_with("file."));

    // Alice solves it.
    txn_alice.move_file(&files_alice[1], "a1")?;
    repo_alice.rename(&files_alice[0], "file")?;
    repo_alice.rename(&files_alice[1], "a1")?;
    let solution_alice = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )
    .unwrap();

    let mut files_bob = repo_bob.list_files();
    debug!("repo_bob = {:?}", files_bob);
    assert_eq!(files_bob.len(), 2);
    files_bob.sort();
    assert_eq!(files_bob[0], "file");
    assert!(files_bob[1].starts_with("file."));

    // Bob applies Alice's solution and checks that it does solve his problem.
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &solution_alice)?;
    let conflicts = output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    if !conflicts.is_empty() {
        panic!("conflicts = {:#?}", conflicts);
    }
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob2")?;
    let mut files_bob = repo_bob.list_files();
    files_bob.sort();
    assert_eq!(files_bob, vec!["a1", "file"]);
    Ok(())
}

#[test]
fn file_conflicts_same_name_and_two_names() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";

    let mut repo_alice = working_copy::memory::Memory::new();
    let mut repo_bob = working_copy::memory::Memory::new();
    let mut repo_charlie = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("file1", contents.to_vec());
    repo_alice.add_file("file2", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();

    let mut channel_alice = txn_alice.open_or_create_channel("alice")?;

    txn_alice.add_file("file1")?;
    txn_alice.add_file("file2")?;
    info!("recording file additions");
    debug!("working_copy = {:?}", repo_alice);
    debug_tree(&txn_alice, "debug_tree")?;
    let init_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice0")?;

    // Bob clones and renames "file2" to "file"
    let mut channel_bob = txn_bob.open_or_create_channel("bob").unwrap();
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &init_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob0")?;
    repo_bob.rename("file2", "file")?;
    txn_bob.move_file("file2", "file")?;
    debug!("repo_bob = {:?}", repo_bob.list_files());
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1")?;

    // Alice renames "file1" to "file"
    repo_alice.rename("file1", "file")?;
    txn_alice.move_file("file1", "file")?;

    debug!("repo_bob = {:?}", repo_alice.list_files());
    let alice_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )
    .unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1")?;

    // Charlie clones, moves "file1" to "file3" and applies both
    // Alice's and Bob's change.
    let env_charlie = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_charlie = env_charlie.mut_txn_begin().unwrap();
    let mut channel_charlie = txn_charlie.open_or_create_channel("charlie").unwrap();
    apply::apply_change(&changes, &mut txn_charlie, &mut channel_charlie, &init_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_charlie,
        &changes,
        &mut txn_charlie,
        &mut channel_charlie,
        "",
        true,
        None,
    )?;
    repo_charlie.rename("file1", "file3")?;
    txn_charlie.move_file("file1", "file3")?;
    let charlie_h = record_all(
        &mut repo_charlie,
        &changes,
        &mut txn_charlie,
        &mut channel_charlie,
        "",
    )
    .unwrap();
    debug_to_file(&txn_charlie, &channel_charlie.borrow(), "debug_charlie1")?;

    apply::apply_change(&changes, &mut txn_charlie, &mut channel_charlie, &bob_h)?;
    apply::apply_change(&changes, &mut txn_charlie, &mut channel_charlie, &alice_h)?;
    output::output_repository_no_pending(
        &mut repo_charlie,
        &changes,
        &mut txn_charlie,
        &mut channel_charlie,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_charlie, &channel_charlie.borrow(), "debug_charlie2")?;
    let mut files_charlie = repo_charlie.list_files();
    files_charlie.sort();
    // Two files with the same name (file), one of which also has another name (file3). This means that we don't know which one of the two names crate::output will pick, between "file3" and the conflicting name.
    // This depends on which file gets output first.
    assert_eq!(files_charlie[0], "file");
    assert!(files_charlie[1] == "file3" || files_charlie[1].starts_with("file."));
    debug!("files_charlie {:?}", files_charlie);

    repo_charlie.rename(&files_charlie[1], "file3")?;
    txn_charlie.move_file(&files_charlie[1], "file3")?;
    let _charlie_solution = record_all(
        &mut repo_charlie,
        &changes,
        &mut txn_charlie,
        &mut channel_charlie,
        "",
    )
    .unwrap();
    debug_to_file(&txn_charlie, &channel_charlie.borrow(), "debug_charlie3")?;
    output::output_repository_no_pending(
        &mut repo_charlie,
        &changes,
        &mut txn_charlie,
        &mut channel_charlie,
        "",
        true,
        None,
    )?;
    let mut files_charlie = repo_charlie.list_files();
    files_charlie.sort();
    assert_eq!(files_charlie, &["file", "file3"]);

    // Alice applies Bob's change and Charlie's change.
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &bob_h)?;
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &charlie_h)?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice2")?;
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut files_alice = repo_alice.list_files();
    files_alice.sort();
    debug!("files_alice {:?}", files_alice);
    repo_alice.remove_path(&files_alice[1]).unwrap();
    let _alice_solution = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )
    .unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice3")?;

    // Bob applies Alice's change and Charlie's change
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &alice_h)?;
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &charlie_h)?;
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
    let files_bob = repo_bob.list_files();
    debug!("files_bob {:?}", files_bob);
    repo_bob.remove_path(&files_bob[1]).unwrap();
    let _bob_solution =
        record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob3")?;
    Ok(())
}

#[test]
fn zombie_file_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";
    let contents2 = b"a\nb\nc\nx\nd\ne\nf\n";

    let mut repo_alice = working_copy::memory::Memory::new();
    let mut repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("a/b/c/file", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();

    let mut channel_alice = txn_alice.open_or_create_channel("alice").unwrap();

    txn_alice.add_file("a/b/c/file").unwrap();
    let init_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice0")?;

    // Bob clones
    let mut channel_bob = txn_bob.open_or_create_channel("bob").unwrap();
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &init_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob0")?;

    // Alice deletes "file"
    repo_alice.remove_path("a/b")?;
    let alice_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )
    .unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1")?;

    // Bob edits "file"
    repo_bob.write_file::<_, std::io::Error, _>("a/b/c/file", |w| {
        w.write_all(contents2)?;
        Ok(())
    })?;
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1")?;

    // Alice applies Bob's change
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &bob_h)?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1")?;
    debug!("alice2");
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
    let files_alice = repo_alice.list_files();
    assert_eq!(files_alice, vec!["a", "a/b", "a/b/c", "a/b/c/file"]);
    for x in txn_alice
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
    let alice_solution = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )
    .unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice3")?;

    // Bob applies Alice's change
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &alice_h)?;
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
    debug!("repo_alice = {:?}", repo_alice.list_files());
    debug!("repo_bob = {:?}", repo_bob.list_files());
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &alice_solution)?;
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
    let mut repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("a/b/c/file", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();

    let mut channel_alice = txn_alice.open_or_create_channel("alice").unwrap();

    txn_alice.add_file("a/b/c/file").unwrap();
    let init_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice0")?;

    // Bob clones
    let mut channel_bob = txn_bob.open_or_create_channel("bob").unwrap();
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &init_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob0")?;

    // Alice deletes "file"
    repo_alice.remove_path("a/b")?;
    let alice_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )
    .unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1")?;

    // Bob renames "file"
    repo_bob.rename("a/b/c/file", "a/b/c/file2")?;
    repo_bob.write_file::<_, std::io::Error, _>("a/b/c/file2", |w| {
        w.write_all(contents2)?;
        Ok(())
    })?;
    txn_bob.move_file("a/b/c/file", "a/b/c/file2")?;
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1")?;

    // Alice applies Bob's change
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &bob_h)?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice2")?;

    debug!("alice2");
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice3")?;
    let files_alice = repo_alice.list_files();
    debug!("Alice records {:?}", files_alice);
    repo_alice.rename("a/b/c/file", "a/b/c/file2").unwrap_or(());
    // repo_alice.remove_path("a/b/c/file").unwrap_or(());
    // repo_alice.remove_path("a/b/c/file2").unwrap_or(());

    txn_alice
        .move_file("a/b/c/file", "a/b/c/file2")
        .unwrap_or(());
    let alice_solution = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )
    .unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice4")?;
    debug!("Alice recorded {:?}", alice_solution);

    // Bob applies Alice's change
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &alice_h)?;
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
    debug!("repo_alice = {:?}", repo_alice.list_files());
    debug!("repo_bob = {:?}", repo_bob.list_files());
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &alice_solution)?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob3")?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
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
    let mut repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("a/b/c/file", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();

    let mut channel_alice = txn_alice.open_or_create_channel("alice").unwrap();

    txn_alice.add_file("a/b/c/file").unwrap();
    let init_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice0")?;

    // Bob clones
    let mut channel_bob = txn_bob.open_or_create_channel("bob").unwrap();
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &init_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob0")?;

    // Alice deletes "file"
    repo_alice.remove_path("a/b")?;
    let alice_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )
    .unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1")?;

    // Bob renames "file"
    repo_bob.rename("a/b/c", "a/b/d")?;
    repo_bob.write_file::<_, std::io::Error, _>("a/b/d/file", |w| {
        w.write_all(contents2)?;
        Ok(())
    })?;
    txn_bob.move_file("a/b/c", "a/b/d")?;
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1")?;

    // Alice applies Bob's change
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &bob_h)?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice2")?;
    debug!("alice2");
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice3")?;
    let files_alice = repo_alice.list_files();
    if files_alice.iter().any(|x| x == "a/b/d/file") {
        txn_alice.add_file("a/b/d/file").unwrap_or(());
    } else {
        assert!(files_alice.iter().any(|x| x == "a/b/c/file"));
        txn_alice.move_file("a/b/c", "a/b/d").unwrap();
        repo_alice.rename("a/b/c", "a/b/d").unwrap();
    }
    debug!("Alice records");
    let alice_solution = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )
    .unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice4")?;
    debug!("Alice recorded {:?}", alice_solution);

    // Bob applies Alice's change
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &alice_h)?;
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
    debug!("repo_alice = {:?}", repo_alice.list_files());
    debug!("repo_bob = {:?}", repo_bob.list_files());

    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &alice_solution)?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob3")?;
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
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

    let mut repo_alice = working_copy::memory::Memory::new();
    let mut repo_bob = working_copy::memory::Memory::new();
    let mut repo_charlie = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();
    let env_charlie = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_charlie = env_charlie.mut_txn_begin().unwrap();

    let mut channel_alice = txn_alice.open_or_create_channel("alice").unwrap();

    repo_alice.add_file("a/b/c/file", contents.to_vec());
    txn_alice.add_file("a/b/c/file").unwrap();
    let init_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice0")?;

    // Bob and Charlie clone
    let mut channel_bob = txn_bob.open_or_create_channel("bob").unwrap();
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &init_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob0")?;

    let mut channel_charlie = txn_charlie.open_or_create_channel("charlie").unwrap();
    apply::apply_change(&changes, &mut txn_charlie, &mut channel_charlie, &init_h).unwrap();
    let conflicts = output::output_repository_no_pending(
        &mut repo_charlie,
        &changes,
        &mut txn_charlie,
        &mut channel_charlie,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_charlie, &channel_charlie.borrow(), "debug_charlie0")?;
    if !conflicts.is_empty() {
        panic!("charlie has conflicts: {:?}", conflicts);
    }

    // Alice deletes "file"
    repo_alice.remove_path("a/b")?;
    let alice_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )
    .unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1")?;

    // Bob edits "file"
    repo_bob.write_file::<_, std::io::Error, _>("a/b/c/file", |w| {
        w.write_all(contents2)?;
        Ok(())
    })?;
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1")?;

    // Charlie edits "file"
    repo_charlie.write_file::<_, std::io::Error, _>("a/b/c/file", |w| {
        w.write_all(contents3)?;
        Ok(())
    })?;
    let charlie_h = record_all(
        &mut repo_charlie,
        &changes,
        &mut txn_charlie,
        &mut channel_charlie,
        "",
    )
    .unwrap();
    debug_to_file(&txn_charlie, &channel_charlie.borrow(), "debug_charlie1")?;

    // Alice applies Bob's and Charlie's changes
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &bob_h)?;
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &charlie_h)?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1")?;
    debug!("alice2");
    let conflicts = output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice2")?;
    let files_alice = repo_alice.list_files();
    assert_eq!(files_alice, vec!["a", "a/b", "a/b/c", "a/b/c/file"]);
    assert_eq!(conflicts.len(), 5);
    match conflicts[0] {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "a/b"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }
    let mut buf = Vec::new();
    repo_alice.read_file("a/b/c/file", &mut buf)?;
    // Alice removes conflict markers.
    repo_alice.write_file::<_, std::io::Error, _>("a/b/c/file", |w| {
        for l in std::str::from_utf8(&buf).unwrap().lines() {
            if l.len() < 10 {
                writeln!(w, "{}", l)?
            }
        }
        Ok(())
    })?;

    let alice_solution = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice3")?;

    // Bob applies
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &alice_h)?;
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &charlie_h)?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob2")?;
    let conflicts = output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob2")?;
    assert_eq!(conflicts.len(), 5);
    match conflicts[0] {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "a/b"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }

    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &alice_solution)?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob3")?;
    let conflicts = output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    if !conflicts.is_empty() {
        panic!("bob has conflicts: {:?}", conflicts);
    }

    // Charlie applies
    apply::apply_change(&changes, &mut txn_charlie, &mut channel_charlie, &bob_h)?;
    debug_to_file(&txn_charlie, &channel_charlie.borrow(), "debug_charlie2")?;
    debug!("charlie applies Alice's change");
    apply::apply_change(&changes, &mut txn_charlie, &mut channel_charlie, &alice_h)?;
    debug_to_file(&txn_charlie, &channel_charlie.borrow(), "debug_charlie3")?;
    let conflicts = output::output_repository_no_pending(
        &mut repo_charlie,
        &changes,
        &mut txn_charlie,
        &mut channel_charlie,
        "",
        true,
        None,
    )?;
    assert_eq!(conflicts.len(), 5);
    match conflicts[0] {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "a/b"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }
    debug!("charlie applies Alice's solution");
    apply::apply_change(
        &changes,
        &mut txn_charlie,
        &mut channel_charlie,
        &alice_solution,
    )?;
    debug_to_file(&txn_charlie, &channel_charlie.borrow(), "debug_charlie4")?;
    let conflicts = output::output_repository_no_pending(
        &mut repo_charlie,
        &changes,
        &mut txn_charlie,
        &mut channel_charlie,
        "",
        true,
        None,
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

    let mut repo_alice = working_copy::memory::Memory::new();
    let mut repo_bob = working_copy::memory::Memory::new();
    let mut repo_charlie = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();
    let env_charlie = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_charlie = env_charlie.mut_txn_begin().unwrap();

    let mut channel_alice = txn_alice.open_or_create_channel("alice").unwrap();

    repo_alice.add_file("a/b/c/file", contents.to_vec());
    txn_alice.add_file("a/b/c/file").unwrap();
    let init_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice0")?;

    repo_alice.rename("a/b/c/file", "a/b/c/alice")?;
    txn_alice.move_file("a/b/c/file", "a/b/c/alice")?;
    let alice_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1")?;

    // Bob clones
    let mut channel_bob = txn_bob.open_or_create_channel("bob").unwrap();
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &init_h).unwrap();
    output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob0")?;

    // Bob deletes "file"
    repo_bob.remove_path("a/b/c/file")?;
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &alice_h).unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1")?;
    let conflicts = output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug!("conflicts = {:#?}", conflicts);
    assert_eq!(conflicts.len(), 1);
    match conflicts[0] {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "a/b/c/alice"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }

    debug!("Bob resolves");
    let bob_resolution =
        record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "").unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob2")?;
    let conflicts = output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    if !conflicts.is_empty() {
        panic!("Bob has conflicts: {:?}", conflicts);
    }

    // Alice applies Bob's patch and solution.
    debug!("Alice applies Bob's resolution");
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &bob_h).unwrap();
    apply::apply_change(
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        &bob_resolution,
    )
    .unwrap();
    let conflicts = output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice2")?;
    if !conflicts.is_empty() {
        panic!("Alice has conflicts: {:?}", conflicts);
    }

    // Charlie applies Alice's move and deletes (i.e. does the same as Bob).
    let mut channel_charlie = txn_charlie.open_or_create_channel("charlie").unwrap();
    apply::apply_change(&changes, &mut txn_charlie, &mut channel_charlie, &init_h).unwrap();
    apply::apply_change(&changes, &mut txn_charlie, &mut channel_charlie, &alice_h).unwrap();
    let conflicts = output::output_repository_no_pending(
        &mut repo_charlie,
        &changes,
        &mut txn_charlie,
        &mut channel_charlie,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_charlie, &channel_charlie.borrow(), "debug_charlie0")?;
    if !conflicts.is_empty() {
        panic!("charlie has conflicts: {:?}", conflicts);
    }

    debug!("Charlie applies Alice's move and deletes");
    repo_charlie.remove_path("a/b/c/alice")?;
    let charlie_h = record_all(
        &mut repo_charlie,
        &changes,
        &mut txn_charlie,
        &mut channel_charlie,
        "",
    )
    .unwrap();
    debug_to_file(&txn_charlie, &channel_charlie.borrow(), "debug_charlie1")?;

    //
    debug!("Charlie applies Bob's deletion");
    apply::apply_change(&changes, &mut txn_charlie, &mut channel_charlie, &bob_h).unwrap();
    debug_to_file(&txn_charlie, &channel_charlie.borrow(), "debug_charlie2")?;
    debug!("Charlie applies Bob's resolution");
    apply::apply_change(
        &changes,
        &mut txn_charlie,
        &mut channel_charlie,
        &bob_resolution,
    )
    .unwrap();
    debug_to_file(&txn_charlie, &channel_charlie.borrow(), "debug_charlie3")?;
    let conflicts = output::output_repository_no_pending(
        &mut repo_charlie,
        &changes,
        &mut txn_charlie,
        &mut channel_charlie,
        "",
        true,
        None,
    )?;
    assert_eq!(conflicts.len(), 1);
    match conflicts[0] {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "a/b/c/alice"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }

    //
    debug!("Alice applies Charlie's deletion");
    apply::apply_change(&changes, &mut txn_alice, &mut channel_alice, &charlie_h).unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice3")?;
    let conflicts = output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    assert_eq!(conflicts.len(), 1);
    match conflicts[0] {
        Conflict::ZombieFile { ref path } => assert!(path == "a/b/c/file" || path == "a/b/c/alice"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }

    //
    apply::apply_change(&changes, &mut txn_bob, &mut channel_bob, &charlie_h).unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob3")?;

    let conflicts = output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    assert_eq!(conflicts.len(), 1);
    match conflicts[0] {
        Conflict::ZombieFile { ref path } => assert!(path == "a/b/c/file" || path == "a/b/c/alice"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }

    Ok(())
}

#[test]
fn move_vs_delete_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo_alice = working_copy::memory::Memory::new();
    let mut repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let mut channel_alice = txn_alice.open_or_create_channel("main")?;
    repo_alice.add_file("file", b"a\n".to_vec());
    txn_alice.add_file("file")?;
    let init = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;

    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();
    let mut channel_bob = txn_bob.open_or_create_channel("main")?;
    txn_bob
        .apply_change(&changes, &mut channel_bob, &init)
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

    // Alice moves "file"
    repo_alice.rename("file", "alice/file").unwrap_or(());
    txn_alice.move_file("file", "alice/file").unwrap_or(());
    let alice_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1").unwrap();

    // Bob deletes "file"
    repo_bob.remove_path("file").unwrap_or(());
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "")?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob0").unwrap();

    // Bob applies Alice's change
    debug!("Bob applies Alice's change");
    txn_bob
        .apply_change(&changes, &mut channel_bob, &alice_h)
        .unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1").unwrap();
    let conflicts = output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug!("conflicts = {:#?}", conflicts);
    assert_eq!(conflicts.len(), 1);
    match conflicts[0] {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "alice/file"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }
    let files = repo_bob.list_files();
    if files.iter().any(|f| f == "alice/file") {
        repo_bob.remove_path("bob").unwrap()
    } else {
        repo_bob.remove_path("alice").unwrap()
    }
    let resolution = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "")?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob2").unwrap();
    let conflicts = output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    if !conflicts.is_empty() {
        panic!("Bob has conflicts: {:?}", conflicts);
    }

    // Alice applies Bob's change
    debug!("Alice applies Bob's change");
    txn_alice
        .apply_change(&changes, &mut channel_alice, &bob_h)
        .unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice2").unwrap();
    let conflicts = output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    assert_eq!(conflicts.len(), 1);
    match conflicts[0] {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "alice/file"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }
    debug!("Alice applies Bob's resolution");
    txn_alice
        .apply_change(&changes, &mut channel_alice, &resolution)
        .unwrap();
    let conflicts = output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice3").unwrap();
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

    let mut repo_alice = working_copy::memory::Memory::new();
    let mut repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let mut channel_alice = txn_alice.open_or_create_channel("main")?;
    repo_alice.add_file("file", b"a\nb\nc\nd\n".to_vec());
    txn_alice.add_file("file")?;
    let init = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;

    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();
    let mut channel_bob = txn_bob.open_or_create_channel("main")?;
    txn_bob
        .apply_change(&changes, &mut channel_bob, &init)
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

    // Alice adds a zombie line.
    repo_alice.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(b"a\nb\nx\nc\nd\n")?;
        Ok(())
    })?;
    record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1").unwrap();

    // Bob deletes the context of Alice's new line, and then deletes
    // "file".
    repo_bob.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(b"a\nd\n")?;
        Ok(())
    })?;
    let bob_h1 = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "")?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob0").unwrap();
    repo_bob.remove_path("file").unwrap_or(());
    let bob_h2 = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "")?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1").unwrap();

    // Alice applies Bob's changes.
    debug!("Alice applies Bob's change");
    txn_alice
        .apply_change(&changes, &mut channel_alice, &bob_h1)
        .unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice2").unwrap();
    debug!("Applying bob_h2");
    txn_alice
        .apply_change(&changes, &mut channel_alice, &bob_h2)
        .unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice3").unwrap();

    let (alive, reachable) = check_alive(&txn_alice, &channel_alice.borrow().graph);
    if !alive.is_empty() {
        panic!("alive (bob0): {:?}", alive);
    }
    if !reachable.is_empty() {
        panic!("reachable (bob0): {:?}", reachable);
    }

    crate::unrecord::unrecord(&mut txn_alice, &mut channel_alice, &changes, &bob_h2).unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice4").unwrap();
    output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    let mut buf = Vec::new();
    repo_alice.read_file("file", &mut buf)?;
    debug!("file = {:?}", std::str::from_utf8(&buf));
    Ok(())
}

#[test]
fn move_into_deleted_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo_alice = working_copy::memory::Memory::new();
    let mut repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let mut channel_alice = txn_alice.open_or_create_channel("main")?;
    repo_alice.add_file("file", b"a\n".to_vec());
    repo_alice.add_dir("dir");
    txn_alice.add_file("file")?;
    txn_alice.add_dir("dir")?;
    let init = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;

    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();
    let mut channel_bob = txn_bob.open_or_create_channel("main")?;
    txn_bob
        .apply_change(&changes, &mut channel_bob, &init)
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

    // Alice moves "file"
    repo_alice.rename("file", "dir/file").unwrap_or(());
    txn_alice.move_file("file", "dir/file").unwrap_or(());
    let alice_h = record_all(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice1").unwrap();

    // Bob deletes "dir"
    repo_bob.remove_path("dir").unwrap_or(());
    let bob_h = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "")?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob0").unwrap();

    // Bob applies Alice's change
    debug!("Bob applies Alice's change");
    txn_bob
        .apply_change(&changes, &mut channel_bob, &alice_h)
        .unwrap();
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob1").unwrap();
    let conflicts = output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    debug!("conflicts = {:#?}", conflicts);
    assert_eq!(conflicts.len(), 1);
    match conflicts[0] {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "dir"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }
    let resolution = record_all(&mut repo_bob, &changes, &mut txn_bob, &mut channel_bob, "")?;
    debug_to_file(&txn_bob, &channel_bob.borrow(), "debug_bob2").unwrap();
    let conflicts = output::output_repository_no_pending(
        &mut repo_bob,
        &changes,
        &mut txn_bob,
        &mut channel_bob,
        "",
        true,
        None,
    )?;
    if !conflicts.is_empty() {
        panic!("Bob has conflicts: {:?}", conflicts);
    }

    // Alice applies Bob's change
    debug!("Alice applies Bob's change");
    txn_alice
        .apply_change(&changes, &mut channel_alice, &bob_h)
        .unwrap();
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice2").unwrap();
    let conflicts = output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    assert_eq!(conflicts.len(), 1);
    match conflicts[0] {
        Conflict::ZombieFile { ref path } => assert_eq!(path, "dir"),
        ref c => panic!("unexpected conflict {:#?}", c),
    }
    debug!("Alice applies Bob's resolution");
    txn_alice
        .apply_change(&changes, &mut channel_alice, &resolution)
        .unwrap();
    let conflicts = output::output_repository_no_pending(
        &mut repo_alice,
        &changes,
        &mut txn_alice,
        &mut channel_alice,
        "",
        true,
        None,
    )?;
    debug_to_file(&txn_alice, &channel_alice.borrow(), "debug_alice3").unwrap();
    if !conflicts.is_empty() {
        panic!("Alice has conflicts: {:?}", conflicts);
    }

    Ok(())
}
