//! Treating strings as paths. For portability reasons, paths must
//! internally be treated as strings, and converted to paths only by
//! the backend, if required (in-memory backends will typically not
//! need that conversion).

/// Returns the parent of the path, if it exists. This function tries
/// to replicate the behaviour of `std::path::Path::parent`, but with
/// `&str` instead of `Path`.
///
/// ```ignore
/// use libpijul::path::parent;
/// assert_eq!(parent("/foo/bar"), Some("/foo"));
/// assert_eq!(parent("foo"), Some(""));
/// assert_eq!(parent("/"), None);
/// assert_eq!(parent(""), None);
/// ```
pub fn parent(mut path: &str) -> Option<&str> {
    loop {
        if path == "/" || path.is_empty() {
            return None;
        } else if let Some(i) = path.rfind('/') {
            let (a, b) = path.split_at(i);
            if b == "/" {
                path = a
            } else {
                return Some(a);
            }
        } else {
            return Some("");
        }
    }
}

/// Returns the file name of the path. if it exists. This function
/// tries to replicate the behaviour of `std::path::Path::file_name`,
/// but with `&str` instead of `Path`.
///
/// Like the original, returns `None` if the path terminates in `..`.
///
/// ```ignore
/// use libpijul::path::file_name;
/// assert_eq!(file_name("/usr/bin/"), Some("bin"));
/// assert_eq!(file_name("tmp/foo.txt"), Some("foo.txt"));
/// assert_eq!(file_name("foo.txt/."), Some("foo.txt"));
/// assert_eq!(file_name("foo.txt/.//"), Some("foo.txt"));
/// assert_eq!(file_name("foo.txt/.."), None);
/// assert_eq!(file_name("/"), None);
/// ```
pub fn file_name(mut path: &str) -> Option<&str> {
    if path == "/" || path.is_empty() {
        None
    } else {
        while let Some(i) = path.rfind('/') {
            let (_, f) = path.split_at(i + 1);
            if f == ".." {
                return None;
            } else if f.is_empty() || f == "." {
                path = path.split_at(i).0
            } else {
                return Some(f);
            }
        }
        Some(path)
    }
}

#[test]
fn test_file_name() {
    assert_eq!(file_name("/usr/bin/"), Some("bin"));
    assert_eq!(file_name("tmp/foo.txt"), Some("foo.txt"));
    assert_eq!(file_name("foo.txt/."), Some("foo.txt"));
    assert_eq!(file_name("foo.txt/.//"), Some("foo.txt"));
    assert_eq!(file_name("foo.txt/.."), None);
    assert_eq!(file_name("/"), None);
}

/// Returns an iterator of the non-empty components of a path,
/// delimited by `/`. Note that `.` and `..` are treated as
/// components.
#[cfg(not(windows))]
pub fn components(path: &str) -> Components {
    Components(path.split(&['/'][..]))
}

#[cfg(windows)]
pub fn components(path: &str) -> Components {
    Components(path.split(&['/', '\\'][..]))
}

#[derive(Clone)]
pub struct Components<'a>(std::str::Split<'a, &'static [char]>);

impl<'a> std::fmt::Debug for Components<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "Components {{ .. }}")
    }
}

impl<'a> Iterator for Components<'a> {
    type Item = &'a str;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(n) = self.0.next() {
                if !n.is_empty() {
                    return Some(n);
                }
            } else {
                return None;
            }
        }
    }
}

/// Push a path component on an existing path. Only works if `extra`
/// is a relative path.
/// ```ignore
/// use libpijul::path::push;
/// let mut s = "a".to_string();
/// push(&mut s, "b");
/// assert_eq!(s, "a/b");
/// push(&mut s, "c");
/// assert_eq!(s, "a/b/c");
/// ```
pub fn push(path: &mut String, extra: &str) {
    assert!(!extra.starts_with('/')); // Make sure the extra path is relative.
    if !path.ends_with('/') && !path.is_empty() {
        path.push('/');
    }
    path.push_str(extra)
}

/// Pop the last component off an existing path.
/// ```ignore
/// use libpijul::path::pop;
/// let mut s = "a/b/c".to_string();
/// pop(&mut s);
/// assert_eq!(s, "a/b");
/// pop(&mut s);
/// assert_eq!(s, "a");
/// pop(&mut s);
/// assert_eq!(s, "");
/// ```
pub fn pop(path: &mut String) {
    if let Some(i) = path.rfind('/') {
        path.truncate(i)
    } else {
        path.clear()
    }
}
