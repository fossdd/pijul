use super::*;

#[test]
fn filesystem() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let r = tempfile::tempdir()?;
    let mut repo = working_copy::filesystem::FileSystem::from_root(r.path());

    let f = tempfile::tempdir()?;
    let changes = changestore::filesystem::FileSystem::from_root(f.path());

    repo.write_file::<_, std::io::Error, _>("dir/file", |f| {
        Ok(f.write_all(&b"a\nb\nc\nd\ne\nf\n"[..])?)
    })?;

    let f = tempfile::tempdir()?;
    let env = pristine::sanakirja::Pristine::new(f.path().join("pristine"))?;
    let mut txn = env.mut_txn_begin().unwrap();
    txn.add_file("dir/file").unwrap();

    let mut channel = txn.open_or_create_channel("main").unwrap();
    let p = record_all(&mut repo, &changes, &mut txn, &mut channel, "").unwrap();
    let mut channel = txn.open_or_create_channel("main2").unwrap();
    info!("applying");
    apply::apply_change(&changes, &mut txn, &mut channel, &p)?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )
    .unwrap();

    txn.commit().unwrap();

    repo.rename("dir/file", "dir/file.old")?;
    repo.remove_path("dir/file.old")?;
    repo.remove_path("dir")?;
    Ok(())
}

#[test]
fn symlink() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let r = tempfile::tempdir()?;
    let mut repo = working_copy::filesystem::FileSystem::from_root(r.path());

    let f = tempfile::tempdir()?;
    let changes = changestore::filesystem::FileSystem::from_root(f.path());

    repo.write_file::<_, std::io::Error, _>("dir/file", |f| {
        Ok(f.write_all(&b"a\nb\nc\nd\ne\nf\n"[..])?)
    })?;
    std::os::unix::fs::symlink(&r.path().join("dir/file"), &r.path().join("dir/link")).unwrap();

    let f = tempfile::tempdir()?;
    std::fs::create_dir_all(f.path())?;
    let env = pristine::sanakirja::Pristine::new(f.path().join("pristine"))?;
    let mut txn = env.mut_txn_begin().unwrap();
    txn.add_file("dir/file").unwrap();
    txn.add_file("dir/link").unwrap();

    let mut channel = txn.open_or_create_channel("main").unwrap();
    let p = record_all(&mut repo, &changes, &mut txn, &mut channel, "").unwrap();
    info!("applying");
    let mut channel = txn.open_or_create_channel("main2").unwrap();
    apply::apply_change(&changes, &mut txn, &mut channel, &p)?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )
    .unwrap();

    txn.commit().unwrap();

    repo.rename("dir/file", "dir/file.old")?;
    repo.remove_path("dir/file.old")?;
    repo.remove_path("dir")?;
    Ok(())
}

#[test]
fn record_dead_symlink() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let r = tempfile::tempdir()?;
    let mut repo = working_copy::filesystem::FileSystem::from_root(r.path());

    let f = tempfile::tempdir()?;
    let changes = changestore::filesystem::FileSystem::from_root(f.path());

    std::fs::create_dir_all(&r.path().join("dir")).unwrap();
    std::os::unix::fs::symlink("../file", &r.path().join("dir/link")).unwrap();

    let f = tempfile::tempdir()?;
    std::fs::create_dir_all(f.path())?;
    let env = pristine::sanakirja::Pristine::new(f.path().join("pristine"))?;
    let mut txn = env.mut_txn_begin().unwrap();
    txn.add_file("dir/link").unwrap();

    let mut channel = txn.open_or_create_channel("main").unwrap();
    let p = record_all(&mut repo, &changes, &mut txn, &mut channel, "").unwrap();
    info!("applying");
    let mut channel = txn.open_or_create_channel("main2").unwrap();
    apply::apply_change(&changes, &mut txn, &mut channel, &p)?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )
    .unwrap();

    txn.commit().unwrap();
    Ok(())
}

#[test]
fn overwrite_dead_symlink() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let r = tempfile::tempdir()?;
    let mut repo = working_copy::filesystem::FileSystem::from_root(r.path());

    let f = tempfile::tempdir()?;
    let changes = changestore::filesystem::FileSystem::from_root(f.path());

    repo.write_file::<_, std::io::Error, _>("dir/file", |f| {
        Ok(f.write_all(&b"a\nb\nc\nd\ne\nf\n"[..])?)
    })?;

    let f = tempfile::tempdir()?;
    std::fs::create_dir_all(f.path())?;
    let env = pristine::sanakirja::Pristine::new(f.path().join("pristine"))?;
    let mut txn = env.mut_txn_begin().unwrap();
    txn.add_file("dir/file").unwrap();

    let mut channel = txn.open_or_create_channel("main").unwrap();
    let p = record_all(&mut repo, &changes, &mut txn, &mut channel, "").unwrap();
    info!("applying");
    let mut channel = txn.open_or_create_channel("main2").unwrap();

    // Substitute dir/file with a dead symlink
    std::fs::remove_file(&r.path().join("dir/file")).unwrap();
    std::os::unix::fs::symlink("a/b/c/d/file", &r.path().join("dir/file")).unwrap();
    debug!("meta = {:?}", std::fs::metadata("dir/file"));
    // And output.
    apply::apply_change(&changes, &mut txn, &mut channel, &p)?;
    output::output_repository_no_pending(
        &mut repo,
        &changes,
        &mut txn,
        &mut channel,
        "",
        true,
        None,
    )
    .unwrap();

    txn.commit().unwrap();
    Ok(())
}
