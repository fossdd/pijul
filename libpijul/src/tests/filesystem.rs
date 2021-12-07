use super::*;
use std::io::Write;

const MAX_FILES: usize = 10;

#[test]
fn filesystem() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let r = tempfile::tempdir()?;
    let repo = working_copy::filesystem::FileSystem::from_root(r.path());

    let f = tempfile::tempdir()?;
    let changes = changestore::filesystem::FileSystem::from_root(f.path(), MAX_FILES);

    repo.write_file("dir/file", Inode::ROOT)
        .unwrap()
        .write_all(&b"a\nb\nc\nd\ne\nf\n"[..])
        .unwrap();

    let f = tempfile::tempdir()?;
    let env = pristine::sanakirja::Pristine::new(f.path().join("pristine"))?;
    let txn = env.arc_txn_begin().unwrap();
    txn.write().add_file("dir/file", 0).unwrap();

    let channel = txn.write().open_or_create_channel("main").unwrap();
    let p = record_all(&repo, &changes, &txn, &channel, "").unwrap();
    let channel = txn.write().open_or_create_channel("main2").unwrap();
    info!("applying");
    apply::apply_change_arc(&changes, &txn, &channel, &p)?;
    output::output_repository_no_pending(&repo, &changes, &txn, &channel, "", true, None, 1, 0)
        .unwrap();

    txn.commit().unwrap();

    repo.rename("dir/file", "dir/file.old")?;
    repo.remove_path("dir/file.old", false)?;
    repo.remove_path("dir", true)?;
    Ok(())
}

#[test]
fn symlink() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let r = tempfile::tempdir()?;
    let repo = working_copy::filesystem::FileSystem::from_root(r.path());

    let f = tempfile::tempdir()?;
    let changes = changestore::filesystem::FileSystem::from_root(f.path(), MAX_FILES);

    repo.write_file("dir/file", Inode::ROOT)
        .unwrap()
        .write_all(&b"a\nb\nc\nd\ne\nf\n"[..])
        .unwrap();
    std::os::unix::fs::symlink(&r.path().join("dir/file"), &r.path().join("dir/link")).unwrap();

    let f = tempfile::tempdir()?;
    std::fs::create_dir_all(f.path())?;
    let env = pristine::sanakirja::Pristine::new(f.path().join("pristine"))?;
    let txn = env.arc_txn_begin().unwrap();
    txn.write().add_file("dir/file", 0).unwrap();
    txn.write().add_file("dir/link", 0).unwrap();

    let channel = txn.write().open_or_create_channel("main").unwrap();
    let p = record_all(&repo, &changes, &txn, &channel, "").unwrap();
    info!("applying");
    let channel = txn.write().open_or_create_channel("main2").unwrap();
    apply::apply_change_arc(&changes, &txn, &channel, &p)?;
    output::output_repository_no_pending(&repo, &changes, &txn, &channel, "", true, None, 1, 0)
        .unwrap();

    txn.commit().unwrap();

    repo.rename("dir/file", "dir/file.old")?;
    repo.remove_path("dir/file.old", false)?;
    repo.remove_path("dir", true)?;
    Ok(())
}

#[test]
fn record_dead_symlink() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let r = tempfile::tempdir()?;
    let repo = working_copy::filesystem::FileSystem::from_root(r.path());

    let f = tempfile::tempdir()?;
    let changes = changestore::filesystem::FileSystem::from_root(f.path(), MAX_FILES);

    std::fs::create_dir_all(&r.path().join("dir")).unwrap();
    std::os::unix::fs::symlink("../file", &r.path().join("dir/link")).unwrap();

    let f = tempfile::tempdir()?;
    std::fs::create_dir_all(f.path())?;
    let env = pristine::sanakirja::Pristine::new(f.path().join("pristine"))?;
    let txn = env.arc_txn_begin().unwrap();
    txn.write().add_file("dir/link", 0).unwrap();

    let channel = txn.write().open_or_create_channel("main").unwrap();
    let p = record_all(&repo, &changes, &txn, &channel, "").unwrap();
    info!("applying");
    let channel = txn.write().open_or_create_channel("main2").unwrap();
    apply::apply_change_arc(&changes, &txn, &channel, &p)?;
    output::output_repository_no_pending(&repo, &changes, &txn, &channel, "", true, None, 1, 0)
        .unwrap();

    txn.commit().unwrap();
    Ok(())
}

#[test]
fn overwrite_dead_symlink() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let r = tempfile::tempdir()?;
    let repo = working_copy::filesystem::FileSystem::from_root(r.path());

    let f = tempfile::tempdir()?;
    let changes = changestore::filesystem::FileSystem::from_root(f.path(), MAX_FILES);

    repo.write_file("dir/file", Inode::ROOT)
        .unwrap()
        .write_all(&b"a\nb\nc\nd\ne\nf\n"[..])
        .unwrap();

    let f = tempfile::tempdir()?;
    std::fs::create_dir_all(f.path())?;
    let env = pristine::sanakirja::Pristine::new(f.path().join("pristine"))?;
    let txn = env.arc_txn_begin().unwrap();
    txn.write().add_file("dir/file", 0).unwrap();

    let channel = txn.write().open_or_create_channel("main").unwrap();
    let p = record_all(&repo, &changes, &txn, &channel, "").unwrap();
    info!("applying");
    let channel = txn.write().open_or_create_channel("main2").unwrap();

    // Substitute dir/file with a dead symlink
    std::fs::remove_file(&r.path().join("dir/file")).unwrap();
    std::os::unix::fs::symlink("a/b/c/d/file", &r.path().join("dir/file")).unwrap();
    debug!("meta = {:?}", std::fs::metadata("dir/file"));
    // And output.
    apply::apply_change_arc(&changes, &txn, &channel, &p)?;
    output::output_repository_no_pending(&repo, &changes, &txn, &channel, "", true, None, 1, 0)
        .unwrap();

    txn.commit().unwrap();
    Ok(())
}
