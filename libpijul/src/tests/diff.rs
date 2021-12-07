use super::*;
use crate::alive::retrieve;
use rand::distributions::Alphanumeric;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use std::io::Write;

#[test]
fn bin_diff_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let repo = working_copy::memory::Memory::new();
    let changes = changestore::memory::Memory::new();
    let mut contents = String::new();
    repo.add_file("file", contents.clone().into());
    let env = pristine::sanakirja::Pristine::new_anon()?;
    let id = {
        let txn = env.arc_txn_begin().unwrap();
        txn.write().add_file("file", 0).unwrap();
        let channel = txn.write().open_or_create_channel("main").unwrap();
        let h = record_all(&repo, &changes, &txn, &channel, "").unwrap();
        let id = *txn.read().get_internal(&h.into()).unwrap().unwrap();
        txn.commit().unwrap();
        id
    };
    let mut rng = ChaCha20Rng::seed_from_u64(1234);
    for i in 0..1000 {
        contents.extend(
            (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(80)
                .map(char::from),
        );
        contents.push('\n');
        repo.write_file("file", Inode::ROOT)
            .unwrap()
            .write_all(contents.as_bytes())
            .unwrap();
        if i % 10 == 0 {
            let txn = env.arc_txn_begin().unwrap();
            let channel = txn.write().open_or_create_channel("main").unwrap();
            record_all(&repo, &changes, &txn, &channel, "").unwrap();
            txn.commit().unwrap();
        }
    }
    {
        let txn = env.arc_txn_begin().unwrap();
        let channel = txn.write().open_or_create_channel("main").unwrap();
        record_all(&repo, &changes, &txn, &channel, "").unwrap();
        txn.commit().unwrap();
    }
    let len = contents.len();
    unsafe {
        let c = contents.as_bytes_mut();
        if c[len / 2] == b'y' {
            c[len / 2] = b'x'
        } else {
            c[len / 2] = b'y'
        }
    }
    {
        let txn = env.arc_txn_begin().unwrap();
        let channel = txn.write().open_or_create_channel("main").unwrap();
        debug_to_file(&*txn.read(), &channel, "debug").unwrap();
        let mut rec = crate::record::Builder::new();
        let rec = rec.recorded();
        let vertex = Position {
            change: id,
            pos: ChangePosition(1u64.into()),
        };
        let mut ret = retrieve(&*txn.read(), txn.read().graph(&*channel.read()), vertex)?;
        rec.lock().diff(
            &changes,
            &*txn.read(),
            &*channel.read(),
            crate::record::Algorithm::Myers,
            String::new(),
            Inode::ROOT,
            vertex.to_option(),
            &mut ret,
            contents.as_bytes(),
            &None,
            &crate::DEFAULT_SEPARATOR,
        )?;
        debug!("{:#?}", rec.lock().actions);
        record_all(&repo, &changes, &txn, &channel, "").unwrap();
        debug_to_file(&*txn.read(), &channel, "debug").unwrap();
        // txn.commit().unwrap()
    }
    Ok(())
}
