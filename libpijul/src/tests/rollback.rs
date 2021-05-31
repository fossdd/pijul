use super::*;
use crate::working_copy::WorkingCopy;

#[test]
fn rollback_conflict_resolution_simple() {
    env_logger::try_init().unwrap_or(());

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon().unwrap();

    let mut txn = env.mut_txn_begin().unwrap();

    let mut channela = txn.open_or_create_channel("main").unwrap();

    // Create a simple conflict between axb and ayb
    repo.add_file("file", b"a\nb\n".to_vec());
    txn.add_file("file").unwrap();
    record_all(&mut repo, &changes, &mut txn, &mut channela, "").unwrap();

    let mut channelb = txn.fork(&channela, "other").unwrap();

    repo.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(b"a\nx\nb\n").unwrap();
        Ok(())
    })
    .unwrap();
    let ha = record_all(&mut repo, &changes, &mut txn, &mut channela, "").unwrap();

    repo.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(b"a\ny\nb\n").unwrap();
        Ok(())
    })
    .unwrap();
    let hb = record_all(&mut repo, &changes, &mut txn, &mut channelb, "").unwrap();

    apply::apply_change(&changes, &mut txn, &mut channelb, &ha).unwrap();
    apply::apply_change(&changes, &mut txn, &mut channela, &hb).unwrap();

    debug_to_file(&txn, &channela.borrow(), "debuga").unwrap();
    debug_to_file(&txn, &channelb.borrow(), "debugb").unwrap();

    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channela,
        "",
        true,
        None,
    )
    .unwrap();
    let mut buf = Vec::new();
    repo.read_file("file", &mut buf).unwrap();
    debug!("{}", std::str::from_utf8(&buf).unwrap());

    // Solve the conflict.
    let conflict: Vec<_> = std::str::from_utf8(&buf).unwrap().lines().collect();
    repo.write_file::<_, std::io::Error, _>("file", |w| {
        for l in conflict.iter().filter(|l| l.len() == 1) {
            writeln!(w, "{}", l).unwrap()
        }
        Ok(())
    })
    .unwrap();

    buf.clear();
    repo.read_file("file", &mut buf).unwrap();
    debug!("{}", std::str::from_utf8(&buf).unwrap());
    let resb = record_all(&mut repo, &changes, &mut txn, &mut channela, "").unwrap();
    debug_to_file(&txn, &channela.borrow(), "debugres").unwrap();

    let p_inv = changes.get_change(&resb).unwrap().inverse(
        &resb,
        crate::change::ChangeHeader {
            authors: vec![],
            message: "rollback".to_string(),
            description: None,
            timestamp: chrono::Utc::now(),
        },
        Vec::new(),
    );
    let h_inv = changes.save_change(&p_inv).unwrap();
    apply::apply_change(&changes, &mut txn, &mut channela, &h_inv).unwrap();
    debug_to_file(&txn, &channela.borrow(), "debug").unwrap();
}

#[test]
fn rollback_conflict_resolution_swap() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();

    let env = pristine::sanakirja::Pristine::new_anon()?;

    let mut txn = env.mut_txn_begin().unwrap();

    let mut channela = txn.open_or_create_channel("main")?;

    // Create a simple conflict between axb and ayb
    repo.add_file("file", b"a\nb\n".to_vec());
    txn.add_file("file")?;
    record_all(&mut repo, &changes, &mut txn, &mut channela, "")?;

    let mut channelb = txn.fork(&channela, "other")?;

    repo.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(b"a\nx\nb\n")?;
        Ok(())
    })?;
    let ha = record_all(&mut repo, &changes, &mut txn, &mut channela, "")?;

    repo.write_file::<_, std::io::Error, _>("file", |w| {
        w.write_all(b"a\ny\nb\n")?;
        Ok(())
    })?;
    let hb = record_all(&mut repo, &changes, &mut txn, &mut channelb, "")?;

    apply::apply_change(&changes, &mut txn, &mut channelb, &ha)?;
    apply::apply_change(&changes, &mut txn, &mut channela, &hb)?;

    debug_to_file(&txn, &channela.borrow(), "debuga")?;
    debug_to_file(&txn, &channelb.borrow(), "debugb")?;

    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channela,
        "",
        true,
        None,
    )?;
    let mut buf = Vec::new();
    repo.read_file("file", &mut buf)?;
    debug!("{}", std::str::from_utf8(&buf).unwrap());

    // Solve the conflict.
    let conflict: Vec<_> = std::str::from_utf8(&buf)?.lines().collect();
    repo.write_file::<_, std::io::Error, _>("file", |w| {
        for l in conflict.iter().filter(|l| l.len() == 1) {
            writeln!(w, "{}", l)?
        }
        Ok(())
    })?;

    buf.clear();
    repo.read_file("file", &mut buf)?;
    debug!("{}", std::str::from_utf8(&buf).unwrap());
    let resb = record_all(&mut repo, &changes, &mut txn, &mut channelb, "")?;
    debug_to_file(&txn, &channelb.borrow(), "debugres")?;

    let p_inv = changes.get_change(&resb).unwrap().inverse(
        &resb,
        crate::change::ChangeHeader {
            authors: vec![],
            message: "rollback".to_string(),
            description: None,
            timestamp: chrono::Utc::now(),
        },
        Vec::new(),
    );
    let h_inv = changes.save_change(&p_inv)?;
    apply::apply_change(&changes, &mut txn, &mut channelb, &h_inv)?;
    debug_to_file(&txn, &channelb.borrow(), "debug")?;

    Ok(())
}
