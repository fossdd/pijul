# Changelog

## 1.0.0-beta.1

### Fixed

- Fixed a failed assertion in the patch text format.
- Fixed a "merged vertices" bug when moving files and editing them in the same patch, where the new name was "glued" to the new lines inside the file, causing confusion.
- Fixed a performance issue on Windows, where canonicalizing paths can cause a significant slowdown (1ms for each file).
