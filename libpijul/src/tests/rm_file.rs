use super::*;

use crate::working_copy::WorkingCopy;

#[test]
fn remove_file() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";

    let mut repo_alice = working_copy::memory::Memory::new();
    let mut repo_bob = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo_alice.add_file("a/b/c/d", contents.to_vec());

    let env_alice = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_alice = env_alice.mut_txn_begin().unwrap();
    let env_bob = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn_bob = env_bob.mut_txn_begin().unwrap();
    let mut channel_alice = txn_alice.open_or_create_channel("alice").unwrap();

    txn_alice.add_file("a/b/c/d").unwrap();
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

    // Bob removes a/b and records
    repo_bob.remove_path("a/b/c")?;
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
    Ok(())
}
