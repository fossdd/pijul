use super::*;

use crate::working_copy::WorkingCopy;

#[test]
fn remove_file() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";

    let repo_alice = working_copy::memory::Memory::new();
    let repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("a/b/c/d", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let txn_alice = env_alice.arc_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let txn_bob = env_bob.arc_txn_begin().unwrap();
    let channel_alice = txn_alice.write().open_or_create_channel("alice").unwrap();

    txn_alice.write().add_file("a/b/c/d", 0).unwrap();
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

    // Bob removes a/b and records
    repo_bob.remove_path("a/b/c", true)?;
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
    Ok(())
}
