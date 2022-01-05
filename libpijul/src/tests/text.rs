use super::*;
use crate::working_copy::WorkingCopy;

#[test]
fn add_non_utf8_file_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut buf = Vec::new();
    use std::io::Read;
    let mut fh = std::fs::File::open("src/tests/data/1252.1")?;
    fh.read_to_end(&mut buf)?;
    let repo = working_copy::memory::Memory::new();
    repo.add_file("file", buf);

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let txn = env.arc_txn_begin().unwrap();
    let channel = txn.write().open_or_create_channel("main")?;
    txn.write().add_file("file", 0)?;

    let store = changestore::memory::Memory::new();
    let (h, change) = record_all_change(&repo, &store, &txn, &channel, "")?;

    let mut v = Vec::new();
    change.write(&store, Some(h), true, &mut v).unwrap();

    let lines: Vec<&str> = std::str::from_utf8(&v)
        .unwrap()
        .lines()
        .filter(|l| l.starts_with("+"))
        .collect();
    assert_eq!(
        vec![
            "+ French / Français (Windows CP 1252)",
            "+ € abcde ‚ xys ƒ uvw „ !bla …... † XA>TH ‡, Salut"
        ],
        lines
    );

    Ok(())
}

/// Change a non-utf-8 text file.
#[test]
fn change_non_utf8_file_test() -> Result<(), anyhow::Error> {
    env_logger::try_init().unwrap_or(());

    let mut buf = Vec::new();
    use std::io::Read;
    let mut fh = std::fs::File::open("src/tests/data/8859-1.1")?;
    fh.read_to_end(&mut buf)?;
    let repo = working_copy::memory::Memory::new();
    repo.add_file("file", buf);

    let env = pristine::sanakirja::Pristine::new_anon()?;
    let txn = env.arc_txn_begin().unwrap();
    let channel = txn.write().open_or_create_channel("main")?;
    txn.write().add_file("file", 0)?;

    let store = changestore::memory::Memory::new();
    record_all(&repo, &store, &txn, &channel, "")?;

    let mut buf = Vec::new();
    {
        use std::io::Read;
        let mut fh = std::fs::File::open("src/tests/data/8859-1.2")?;
        fh.read_to_end(&mut buf)?;
    }
    use std::io::Write;
    repo.write_file("file", Inode::ROOT)
        .unwrap()
        .write_all(&buf)
        .unwrap();
    let (h1, change1) = record_all_change(&repo, &store, &txn, &channel, "")?;

    // only one line was changed
    let mut v = Vec::new();
    change1.write(&store, Some(h1), true, &mut v).unwrap();
    let lines: Vec<&str> = std::str::from_utf8(&v)
        .unwrap()
        .lines()
        .filter(|l| l.starts_with(|c| c == '-' || c == '+'))
        .collect();
    assert_eq!(
        vec![
            "- French / Français (ISO Latin-1 / ISO 8859-1)",
            "+ Français / French (ISO Latin-1 / ISO 8859-1)"
        ],
        lines
    );

    Ok(())
}
