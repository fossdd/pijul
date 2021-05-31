use super::*;
use crate::working_copy::WorkingCopy;

// Avoiding quadratic reconnects when possible.
#[test]
fn quadratic_pseudo_edges() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"TxnTX\n";

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel = txn.open_or_create_channel("main").unwrap();

    txn.add_file("file")?;
    record_all(&mut repo, &changes, &mut txn, &mut channel, "").unwrap();
    debug_to_file(&txn, &channel.borrow(), "debug").unwrap();
    let n = 100;
    for i in 0..=n {
        repo.write_file::<_, std::io::Error, _>("file", |w| {
            for j in 0..i {
                writeln!(w, "{}", j)?;
            }
            w.write_all(&contents[..])?;
            for j in (0..i).rev() {
                writeln!(w, "{}", j)?;
            }
            Ok(())
        })
        .unwrap();
        record_all(&mut repo, &changes, &mut txn, &mut channel, "").unwrap();
        debug_to_file(&txn, &channel.borrow(), &format!("debug{}", i)).unwrap();
    }
    repo.write_file::<_, std::io::Error, _>("file", |w| {
        for j in 0..n {
            writeln!(w, "{}", j)?;
        }
        for j in (0..n).rev() {
            writeln!(w, "{}", j)?;
        }
        Ok(())
    })
    .unwrap();
    record_all(&mut repo, &changes, &mut txn, &mut channel, "").unwrap();
    debug_to_file(&txn, &channel.borrow(), "debug_final").unwrap();
    // Test that not too many edges have been inserted.
    {
        let channel = channel.borrow();
        let mut m = 0;
        let mut it = txn.iter_graph(&channel.graph, None).unwrap();
        while let Some(Ok(_)) = txn.next_graph(&channel.graph, &mut it) {
            m += 1
        }
        let m0 = n * 8 + 6;
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
    let mut txn = env.mut_txn_begin().unwrap();
    let mut channel = txn.open_or_create_channel("main").unwrap();

    txn.add_file("file").unwrap();
    record_all(&mut repo, &changes, &mut txn, &mut channel, "").unwrap();
    debug_to_file(&txn, &channel.borrow(), "debug").unwrap();
    let n = 20;
    for i in 0..=n {
        repo.write_file::<_, std::io::Error, _>("file", |w| {
            for j in 0..i {
                writeln!(w, "{}", j).unwrap();
            }
            w.write_all(&contents[..]).unwrap();
            for j in (0..i).rev() {
                writeln!(w, "{}", j).unwrap();
            }
            Ok(())
        })
        .unwrap();
        record_all(&mut repo, &changes, &mut txn, &mut channel, "").unwrap();
        debug_to_file(&txn, &channel.borrow(), &format!("debug{}", i)).unwrap();
    }
    let mut channel2 = txn.fork(&channel, "fork").unwrap();
    repo.write_file::<_, std::io::Error, _>("file", |w| {
        for j in 0..n {
            writeln!(w, "{}", j).unwrap();
        }
        w.write_all(b"TxnTX\nYYYYY\nZZZZZ\n").unwrap();
        for j in (0..n).rev() {
            writeln!(w, "{}", j).unwrap();
        }
        Ok(())
    })
    .unwrap();

    let p1 = record_all(&mut repo, &changes, &mut txn, &mut channel2, "").unwrap();

    ::sanakirja::debug::debug(
        &txn.txn,
        &[txn.graph(&channel.borrow()), txn.graph(&channel2.borrow())],
        "debug_sanakirja",
        true,
    );

    debug_to_file(&txn, &channel2.borrow(), "debug_bob0").unwrap();
    repo.write_file::<_, std::io::Error, _>("file", |w| {
        for j in 0..n {
            writeln!(w, "{}", j).unwrap();
        }
        for j in (0..n).rev() {
            writeln!(w, "{}", j).unwrap();
        }
        Ok(())
    })
    .unwrap();
    let p2 = record_all(&mut repo, &changes, &mut txn, &mut channel, "").unwrap();

    debug_to_file(&txn, &channel.borrow(), "debug_alice0").unwrap();
    debug!("Applying P1");
    txn.apply_change(&changes, &mut channel, &p1).unwrap();
    debug_to_file(&txn, &channel.borrow(), "debug_alice").unwrap();
    debug!("Applying P2");
    txn.apply_change(&changes, &mut channel2, &p2).unwrap();
    debug_to_file(&txn, &channel2.borrow(), "debug_bob").unwrap();

    // Test that not too many edges have been inserted.
    {
        let channel = channel.borrow();
        let mut m = 0;
        let mut it = txn.iter_graph(&channel.graph, None).unwrap();
        while let Some(Ok(_)) = txn.next_graph(&channel.graph, &mut it) {
            m += 1
        }
        debug!("m (channel, alice) = {:?}", m);
        let original_edges = 8 * n + 27;
        if m > original_edges {
            panic!("{}: {} > {}", n, m, original_edges)
        }
    }
    {
        let channel = channel2.borrow();
        let mut m = 0;
        let mut it = txn.iter_graph(&channel.graph, None).unwrap();
        while let Some(Ok(_)) = txn.next_graph(&channel.graph, &mut it) {
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
