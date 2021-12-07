use super::*;
use crate::working_copy::{WorkingCopy, WorkingCopyRead};
use std::io::Write;

#[test]
fn partial_clone() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let contents = b"a\nb\nc\nd\ne\nf\n";

    let mut repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    repo.add_file("a/b/c", contents.to_vec());
    repo.add_file("d/e/f", contents.to_vec());
    repo.add_file("g/h/i", contents.to_vec());

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let txn = env.arc_txn_begin().unwrap();
    {
        let channel = txn.write().open_or_create_channel("main").unwrap();
        txn.write().add_file("a/b/c", 0)?;
        record_all(&mut repo, &changes, &txn, &channel, "")?;
        txn.write().add_file("d/e/f", 0)?;
        let hd = record_all(&repo, &changes, &txn, &channel, "")?;
        txn.write().add_file("g/h/i", 0)?;
        let hg = record_all(&repo, &changes, &txn, &channel, "")?;

        repo.rename("g/h/i", "d/e/ff")?;
        txn.write().move_file("g/h/i", "d/e/ff", 0)?;
        let hmove = record_all(&repo, &changes, &txn, &channel, "")?;

        let inode = crate::fs::find_inode(&*txn.read(), "d")?;
        let key = *txn.read().get_inodes(&inode, None).unwrap().unwrap();
        let changes: Vec<_> = txn
            .read()
            .log_for_path(&*channel.read(), key, 0)
            .unwrap()
            .map(|x| x.unwrap())
            .collect();
        let check = vec![hd, hg, hmove];
        assert_eq!(changes, check)
    }
    txn.commit().unwrap();
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
    let mut txn = env.arc_txn_begin().unwrap();
    let h = {
        let mut channel = txn.write().open_or_create_channel("main").unwrap();
        txn.write().add_file("a/b/c/d", 0)?;
        txn.write().add_file("e/f/g/h", 0)?;
        txn.write().add_file("i/j/k/l", 0)?;
        record_all(&mut repo, &changes, &mut txn, &mut channel, "")?
    };
    let h2 = {
        let channel = txn.write().open_or_create_channel("main").unwrap();
        repo.write_file("a/b/c/d", Inode::ROOT)
            .unwrap()
            .write_all(b"edits\n")?;
        repo.write_file("e/f/g/h", Inode::ROOT)
            .unwrap()
            .write_all(b"edits\n")?;
        record_all(&repo, &changes, &txn, &channel, "a/b/c/d")?
    };

    txn.commit().unwrap();

    // Cloning
    debug!("Cloning");
    let repo2 = working_copy::memory::Memory::new();
    let env2 = pristine::sanakirja::Pristine::new_anon()?;
    let txn2 = env2.arc_txn_begin().unwrap();
    {
        let channel = txn2.write().open_or_create_channel("main2").unwrap();
        apply::apply_change_arc(&changes, &txn2, &channel, &h).unwrap();
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

        apply::apply_change_arc(&changes, &txn2, &channel, &h2).unwrap();
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
