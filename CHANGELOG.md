# Changelog

## Unreleased

## 1.0.0-beta.2

### Fixed

- Fixing a bug with name conflicts, where files could end up with 0 alive name.
- Fixing a few panics/unwraps
- Fixing a bug where a zombie file could be deleted by `pijul unrecord`, but its contents would stay zombie.
- CVE-2022-24713

### New features

- Better documentation for `pijul key`.
- `pijul pull` does not open $EDITOR anymore when given a list of changes.

## 1.0.0-beta.1

### Fixed

- Fixed a failed assertion in the patch text format.
- Fixed a "merged vertices" bug when moving files and editing them in the same patch, where the new name was "glued" to the new lines inside the file, causing confusion.
- Fixed a performance issue on Windows, where canonicalizing paths can cause a significant slowdown (1ms for each file).
