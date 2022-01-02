use super::*;
use crate::working_copy::{WorkingCopy, WorkingCopyRead};
use std::io::Write;

#[test]
fn clone_simple() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";
    let contents2 = b"a\nb\n\nc\nd\nx\nf\n";

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("file", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let mut recorded_changes = Vec::new();
    let txn = env.arc_txn_begin().unwrap();
    {
        let channel = txn.write().open_or_create_channel("main").unwrap();

        txn.write().add_file("file", 0)?;
        recorded_changes.push(record_all(&repo, &changes, &txn, &channel, "").unwrap());
        repo.write_file("file", Inode::ROOT)
            .unwrap()
            .write_all(contents2)
            .unwrap();
        recorded_changes.push(record_all(&repo, &changes, &txn, &channel, "").unwrap());
    }
    txn.commit().unwrap();

    let mut channel_changes = Vec::new();
    {
        let txn = env.txn_begin()?;
        for channel in txn.channels("")? {
            for x in txn.log(&channel.read(), 0).unwrap() {
                let (_, (i, _)) = x.unwrap();
                channel_changes.push(i.into())
            }
        }
    }
    info!("{:?}", channel_changes);
    assert_eq!(channel_changes, recorded_changes);
    let repo2 = working_copy::memory::Memory::new();
    let env2 = pristine::sanakirja::Pristine::new_anon()?;
    let txn2 = env2.arc_txn_begin().unwrap();
    {
        let channel = txn2.write().open_or_create_channel("main2").unwrap();
        for h in channel_changes.iter() {
            info!("applying {:?}", h);
            apply::apply_change(&changes, &mut *txn2.write(), &mut *channel.write(), h).unwrap();
            output::output_repository_no_pending(
                &repo2, &changes, &txn2, &channel, "", true, None, 1, 0,
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
    let txn = env.arc_txn_begin().unwrap();
    let h = {
        let channel = txn.write().open_or_create_channel("main").unwrap();
        txn.write().add_file("a/b/c/d", 0)?;
        txn.write().add_file("e/f/g/h", 0)?;
        txn.write().add_file("i/j/k/l", 0)?;
        record_all(&repo, &changes, &txn, &channel, "")?
    };
    let h2 = {
        let channel = txn.write().open_or_create_channel("main").unwrap();
        repo.write_file("a/b/c/d", Inode::ROOT)
            .unwrap()
            .write_all(b"edits\n")?;
        repo.write_file("e/f/g/h", Inode::ROOT)
            .unwrap()
            .write_all(b"edits\n")?;
        record_all(&mut repo, &changes, &txn, &channel, "a/b/c/d")?
    };

    txn.commit().unwrap();

    // Cloning
    debug!("Cloning");
    let repo2 = working_copy::memory::Memory::new();
    let env2 = pristine::sanakirja::Pristine::new_anon()?;
    let txn2 = env2.arc_txn_begin().unwrap();
    {
        let channel = txn2.write().open_or_create_channel("main2").unwrap();
        apply::apply_change(&changes, &mut *txn2.write(), &mut *channel.write(), &h).unwrap();
        output::output_repository_no_pending(
            &repo2, &changes, &txn2, &channel, "e/f", true, None, 1, 0,
        )?;
        assert_eq!(
            repo2.list_files(),
            ["e", "e/f", "e/f/g", "e/f/g/h"]
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
        );

        apply::apply_change(&changes, &mut *txn2.write(), &mut *channel.write(), &h2).unwrap();
        output::output_repository_no_pending(
            &repo2, &changes, &txn2, &channel, "", true, None, 1, 0,
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
