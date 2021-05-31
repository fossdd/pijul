use super::*;
use crate::working_copy::WorkingCopy;

#[test]
fn clone_simple() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";
    let contents2 = b"a\nb\n\nc\nd\nx\nf\n";

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut recorded_changes = Vec::new();
    let mut txn = env.mut_txn_begin().unwrap();
    {
        let mut channel = txn.open_or_create_channel("main").unwrap();

        txn.add_file("file")?;
        recorded_changes.push(record_all(&mut repo, &changes, &mut txn, &mut channel, "").unwrap());
        debug_to_file(&txn, &channel.borrow(), "debug0").unwrap();
        repo.write_file::<_, std::io::Error, _>("file", |w| {
            w.write_all(contents2).unwrap();
            Ok(())
        })
        .unwrap();
        recorded_changes.push(record_all(&mut repo, &changes, &mut txn, &mut channel, "").unwrap());
        debug_to_file(&txn, &channel.borrow(), "debug1").unwrap();
    }
    txn.commit().unwrap();

    let mut channel_changes = Vec::new();
    {
        let txn = env.txn_begin()?;
        for channel in txn.iter_channels("").unwrap() {
            let channel = channel.unwrap();
            for x in txn.log(&channel.borrow(), 0).unwrap() {
                let (_, (i, _)) = x.unwrap();
                channel_changes.push(i.into())
            }
        }
    }
    info!("{:?}", channel_changes);
    assert_eq!(channel_changes, recorded_changes);
    let mut repo2 = working_copy::memory::Memory::new();
    let env2 = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn2 = env2.mut_txn_begin().unwrap();
    {
        let mut channel = txn2.open_or_create_channel("main2").unwrap();
        for h in channel_changes.iter() {
            info!("applying {:?}", h);
            apply::apply_change(&changes, &mut txn2, &mut channel, h).unwrap();
            debug_to_file(&txn2, &channel.borrow(), "debug2").unwrap();
            output::output_repository_no_pending(
                &mut repo2,
                &changes,
                &mut txn2,
                &mut channel,
                "",
                true,
                None,
            )
            .unwrap();
        }
        assert_eq!(repo2.list_files(), vec!["file".to_string()]);
        let mut file = Vec::new();
        repo2.read_file("file", &mut file).unwrap();
        assert_eq!(file, contents2);
    }
    txn2.commit().unwrap();
    Ok(())
}

#[test]
fn clone_prefixes() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("a/b/c/d", contents.to_vec());
    repo.add_file("e/f/g/h", contents.to_vec());
    repo.add_file("i/j/k/l", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn = env.mut_txn_begin().unwrap();
    let h = {
        let mut channel = txn.open_or_create_channel("main").unwrap();
        txn.add_file("a/b/c/d")?;
        txn.add_file("e/f/g/h")?;
        txn.add_file("i/j/k/l")?;
        record_all(&mut repo, &changes, &mut txn, &mut channel, "")?
    };
    let h2 = {
        let mut channel = txn.open_or_create_channel("main").unwrap();
        repo.write_file::<_, std::io::Error, _>("a/b/c/d", |w| {
            w.write_all(b"edits\n")?;
            Ok(())
        })?;
        repo.write_file::<_, std::io::Error, _>("e/f/g/h", |w| {
            w.write_all(b"edits\n")?;
            Ok(())
        })?;
        record_all(&mut repo, &changes, &mut txn, &mut channel, "a/b/c/d")?
    };

    txn.commit().unwrap();

    // Cloning
    debug!("Cloning");
    let mut repo2 = working_copy::memory::Memory::new();
    let env2 = pristine::sanakirja::Pristine::new_anon()?;
    let mut txn2 = env2.mut_txn_begin().unwrap();
    {
        let mut channel = txn2.open_or_create_channel("main2").unwrap();
        apply::apply_change(&changes, &mut txn2, &mut channel, &h).unwrap();
        output::output_repository_no_pending(
            &mut repo2,
            &changes,
            &mut txn2,
            &mut channel,
            "e/f",
            true,
            None,
        )?;
        assert_eq!(
            repo2.list_files(),
            ["e", "e/f", "e/f/g", "e/f/g/h"]
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
        );

        apply::apply_change(&changes, &mut txn2, &mut channel, &h2).unwrap();
        output::output_repository_no_pending(
            &mut repo2,
            &changes,
            &mut txn2,
            &mut channel,
            "",
            true,
            None,
        )?;
        let mut buf = Vec::new();
        repo2.read_file("a/b/c/d", &mut buf)?;
        assert_eq!(buf, b"edits\n");
        buf.clear();
        repo2.read_file("e/f/g/h", &mut buf)?;
        assert_eq!(buf, contents);
    }
    txn2.commit().unwrap();
    let mut txn2 = env2.mut_txn_begin().unwrap();
    txn2.open_or_create_channel("main2").unwrap();
    Ok(())
}
