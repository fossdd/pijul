use crate::fs::*;
use crate::patch::*;
use crate::pristine::*;
use crate::record::*;
use crate::*;

fn hash_mismatch(patch: &Patch3) -> Result<(), anyhow::Error> {
env_logger::try_init().unwrap_or(());
use crate::patch::*;
let mut buf = tempfile::NamedTempFile::new()?;
let mut h = patch.serialize(&mut buf)?;
match h {
crate::pristine::Hash::Blake3(ref mut h) => h[0] = h[0].wrapping_add(1),
_ => unreachable!(),
}
match Patch3::deserialize(buf.path().to_str().unwrap(), &h) {
Err(e) => {
let e = e.downcast();
if let Ok(Error::PatchHashMismatch { .. }) = e {
} else {
unreachable!()
}
}
_ => unreachable!(),
}

let mut f = PatchFile::open(buf.path().to_str().unwrap())?;
assert_eq!(f.read_header()?, patch.header);
assert_eq!(f.read_dependencies()?, patch.dependencies);
assert_eq!(f.read_metadata()?, &patch.metadata[..]);
assert_eq!(f.read_changes()?, patch.changes);
Ok(())
}

#[test]
fn hash_mism() -> Result<(), anyhow::Error> {
env_logger::try_init().unwrap_or(());

let contents = b"a\nb\nc\nd\ne\nf\n";
let mut repo = working_copy::memory::Memory::new();
let patches = patchstore::memory::Memory::new();
repo.add_file("file", contents.to_vec());
repo.add_file("file2", contents.to_vec());

let mut env = pristine::sanakirja::Pristine::new_anon()?;
let mut txn = env.mut_txn_begin();
let branch = txn.open_or_create_branch("main")?;
let mut branch = branch.borrow_mut();
add_file(&mut txn, "file")?;
add_file(&mut txn, "file2")?;

let mut state = Builder::new();
state
.record(
&mut txn,
Algorithm::Myers,
&mut branch,
&mut repo,
&patches,
"",
)
.unwrap();
let rec = state.finish();
let changes: Vec<_> = rec.actions
.into_iter()
.flat_map(|x| x.globalize(&txn).into_iter())
.collect();
info!("changes = {:?}", changes);
let patch0 = crate::patch::Patch3::make_patch(
&txn,
&branch,
changes,
rec.contents,
crate::patch::PatchHeader {
name: "test".to_string(),
authors: vec![],
description: None,
timestamp: chrono::Utc::now(),
},
Vec::new(),
);

apply::apply_local_patch(&patches, &mut txn, &mut branch, &patch0, &rec.updatables)?;

hash_mismatch(&patch0)?;


Ok(())
}
