use super::*;
use crate::pristine::GraphIter;
use crate::working_copy::WorkingCopy;
use std::io::Write;

// Avoiding quadratic reconnects when possible.
#[test]
fn quadratic_pseudo_edges() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"TxnTX\n";

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let txn = env.arc_txn_begin().unwrap();
    let channel = txn.write().open_or_create_channel("main").unwrap();

    txn.write().add_file("file", 0)?;
    record_all(&mut repo, &changes, &txn, &channel, "").unwrap();
    let n = 100;
    for i in 0..=n {
        let mut w = repo.write_file("file", Inode::ROOT).unwrap();
        for j in 0..i {
            writeln!(w, "{}", j)?;
        }
        w.write_all(&contents[..])?;
        for j in (0..i).rev() {
            writeln!(w, "{}", j)?;
        }
        record_all(&repo, &changes, &txn, &channel, "").unwrap();
    }
    {
        let mut w = repo.write_file("file", Inode::ROOT).unwrap();
        for j in 0..n {
            writeln!(w, "{}", j)?;
        }
        for j in (0..n).rev() {
            writeln!(w, "{}", j)?;
        }
    }
    record_all(&repo, &changes, &txn, &channel, "").unwrap();
    // Test that not too many edges have been inserted.
    {
        let graph = channel.read();
        let mut m = 0;
        let mut cursor = txn.read().graph_cursor(&*graph, None).unwrap();
        while let Some(Ok(_)) = txn.read().next_graph(&*graph, &mut cursor) {
            m += 1
        }
        let m0 = n * 8 + 10;
        if m > m0 {
            panic!("{} > {}", m, m0)
        }
    }
    txn.commit().unwrap();
    Ok(())
}

// Avoiding linear context repairs when possible.
use crate::MutTxnTExt;

#[test]
fn linear_context_repair() {
    env_logger::try_init().unwrap_or(());

    let contents = b"TxnTX\nZZZZZ\n";

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon().unwrap();
    let mut txn = env.arc_txn_begin().unwrap();
    let channel = txn.write().open_or_create_channel("main").unwrap();

    txn.write().add_file("file", 0).unwrap();
    record_all(&mut repo, &changes, &txn, &channel, "").unwrap();
    let n = 20;
    for i in 0..=n {
        let mut w = repo.write_file("file", Inode::ROOT).unwrap();
        for j in 0..i {
            writeln!(w, "{}", j).unwrap();
        }
        w.write_all(&contents[..]).unwrap();
        for j in (0..i).rev() {
            writeln!(w, "{}", j).unwrap();
        }
        record_all(&repo, &changes, &txn, &channel, "").unwrap();
    }
    let mut channel2 = txn.write().fork(&channel, "fork").unwrap();
    {
        let mut w = repo.write_file("file", Inode::ROOT).unwrap();
        for j in 0..n {
            writeln!(w, "{}", j).unwrap();
        }
        w.write_all(b"TxnTX\nYYYYY\nZZZZZ\n").unwrap();
        for j in (0..n).rev() {
            writeln!(w, "{}", j).unwrap();
        }
    }

    let p1 = record_all(&mut repo, &changes, &mut txn, &mut channel2, "").unwrap();

    ::sanakirja::debug::debug(
        &txn.read().txn,
        &[&channel.read().graph, &channel2.read().graph],
        "debug_sanakirja",
        true,
    );

    {
        let mut w = repo.write_file("file", Inode::ROOT).unwrap();
        for j in 0..n {
            writeln!(w, "{}", j).unwrap();
        }
        for j in (0..n).rev() {
            writeln!(w, "{}", j).unwrap();
        }
    }
    let p2 = record_all(&repo, &changes, &txn, &channel, "").unwrap();

    debug!("Applying P1");
    txn.write()
        .apply_change(&changes, &mut *channel.write(), &p1)
        .unwrap();
    debug!("Applying P2");
    txn.write()
        .apply_change(&changes, &mut *channel2.write(), &p2)
        .unwrap();

    // Test that not too many edges have been inserted.
    {
        let graph = &channel.read();
        let mut m = 0;
        let mut cursor = txn.read().graph_cursor(graph, None).unwrap();
        while let Some(Ok(_)) = txn.read().next_graph(graph, &mut cursor) {
            m += 1
        }
        debug!("m (channel, alice) = {:?}", m);
        let original_edges = 8 * n + 27;
        if m > original_edges {
            panic!("{}: {} > {}", n, m, original_edges)
        }
    }
    {
        let graph = &channel2.read();
        let mut m = 0;
        let mut cursor = txn.read().graph_cursor(graph, None).unwrap();
        while let Some(Ok(_)) = txn.read().next_graph(graph, &mut cursor) {
            m += 1
        }
        debug!("m (channel2, bob) = {:?}", m);
        let original_edges = 8 * n + 27;
        if m > original_edges {
            panic!("{}: {} > {}", n, m, original_edges)
        }
    }
    txn.commit().unwrap();
}
