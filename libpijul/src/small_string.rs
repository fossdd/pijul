pub const MAX_LENGTH: usize = 255;

/// A string of length at most 255, with a more compact on-disk
/// encoding.
#[repr(packed)]
pub struct SmallString {
    pub len: u8,
    pub str: [u8; MAX_LENGTH],
}

/// A borrowed version of `SmallStr`.
pub struct SmallStr {
    len: u8,
    _str: [u8],
}

impl std::hash::Hash for SmallStr {
    fn hash<H: std::hash::Hasher>(&self, hasher: &mut H) {
        self.as_bytes().hash(hasher)
    }
}

impl Clone for SmallString {
    fn clone(&self) -> Self {
        Self::from_str(self.as_str())
    }
}

impl std::fmt::Debug for SmallString {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        use std::ops::Deref;
        self.deref().fmt(fmt)
    }
}

impl PartialEq for SmallStr {
    fn eq(&self, x: &SmallStr) -> bool {
        self.as_str().eq(x.as_str())
    }
}

impl std::ops::Deref for SmallString {
    type Target = SmallStr;
    fn deref(&self) -> &Self::Target {
        let len = self.len as usize;
        unsafe {
            std::mem::transmute(std::slice::from_raw_parts(
                self as *const Self as *const u8,
                1 + len,
            ))
        }
    }
}

impl AsRef<SmallStr> for SmallString {
    fn as_ref(&self) -> &SmallStr {
        let len = self.len as usize;
        unsafe {
            std::mem::transmute(std::slice::from_raw_parts(
                self as *const Self as *const u8,
                1 + len,
            ))
        }
    }
}

impl AsMut<SmallStr> for SmallString {
    fn as_mut(&mut self) -> &mut SmallStr {
        let len = self.len as usize;
        unsafe {
            std::mem::transmute(std::slice::from_raw_parts_mut(
                self as *mut Self as *mut u8,
                1 + len,
            ))
        }
    }
}

impl std::ops::DerefMut for SmallString {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let len = self.len as usize;
        unsafe {
            std::mem::transmute(std::slice::from_raw_parts_mut(
                self as *mut Self as *mut u8,
                1 + len,
            ))
        }
    }
}

#[test]
fn eq() {
    let s0 = SmallString::from_str("blabla");
    let s1 = SmallString::from_str("blabla");
    assert_eq!(s0, s1);

    assert_eq!(s0, s1);

    assert_eq!(s0, s1);
    assert_eq!(s0, s0);
    assert_eq!(s1, s1);
}

#[test]
fn debug() {
    let s = SmallString::from_str("blabla");
    assert_eq!(format!("{:?}", s), "\"blabla\"");
}

impl Eq for SmallStr {}

impl PartialEq for SmallString {
    fn eq(&self, x: &SmallString) -> bool {
        self.as_str().eq(x.as_str())
    }
}
impl Eq for SmallString {}

impl std::hash::Hash for SmallString {
    fn hash<H: std::hash::Hasher>(&self, x: &mut H) {
        self.as_str().hash(x)
    }
}

impl PartialOrd for SmallStr {
    fn partial_cmp(&self, x: &SmallStr) -> Option<std::cmp::Ordering> {
        self.as_str().partial_cmp(x.as_str())
    }
}
impl Ord for SmallStr {
    fn cmp(&self, x: &SmallStr) -> std::cmp::Ordering {
        self.as_str().cmp(x.as_str())
    }
}

impl PartialOrd for SmallString {
    fn partial_cmp(&self, x: &SmallString) -> Option<std::cmp::Ordering> {
        self.as_str().partial_cmp(x.as_str())
    }
}
impl Ord for SmallString {
    fn cmp(&self, x: &SmallString) -> std::cmp::Ordering {
        self.as_str().cmp(x.as_str())
    }
}

#[test]
fn ord() {
    let s0 = SmallString::from_str("1234");
    let s1 = SmallString::from_str("5678");
    assert!(s0 < s1);
    assert!(s0 < s1);
    assert_eq!(s0.cmp(&s1), std::cmp::Ordering::Less);
}

impl std::fmt::Debug for SmallStr {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.as_str().fmt(fmt)
    }
}

impl Default for SmallString {
    fn default() -> Self {
        Self {
            len: 0,
            str: [0; MAX_LENGTH],
        }
    }
}

impl SmallString {
    pub fn new() -> Self {
        Self::default()
    }
    /// ```ignore
    /// use libpijul::small_string::*;
    /// let mut s = SmallString::from_str("blah!");
    /// assert_eq!(s.len(), s.as_str().len());
    /// ```
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// ```ignore
    /// use libpijul::small_string::*;
    /// let mut s = SmallString::from_str("blah");
    /// s.clear();
    /// assert_eq!(s.as_str(), "");
    /// assert!(s.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn from_str(s: &str) -> Self {
        let mut b = SmallString {
            len: s.len() as u8,
            str: [0; MAX_LENGTH],
        };
        b.clone_from_str(s);
        b
    }
    pub fn clone_from_str(&mut self, s: &str) {
        self.len = s.len() as u8;
        (&mut self.str[..s.len()]).copy_from_slice(s.as_bytes());
    }

    /// ```ignore
    /// use libpijul::small_string::*;
    /// let mut s = SmallString::from_str("blah");
    /// s.clear();
    /// assert!(s.is_empty());
    /// ```
    pub fn clear(&mut self) {
        self.len = 0;
    }
    pub fn push_str(&mut self, s: &str) {
        let l = self.len as usize;
        assert!(l + s.len() <= 0xff);
        (&mut self.str[l..l + s.len()]).copy_from_slice(s.as_bytes());
        self.len += s.len() as u8;
    }

    pub fn as_str(&self) -> &str {
        use std::ops::Deref;
        self.deref().as_str()
    }

    pub fn as_bytes(&self) -> &[u8] {
        use std::ops::Deref;
        self.deref().as_bytes()
    }
}
/*
impl SmallStr {
    pub const EMPTY: &'static SmallStr = &SmallStr {
        len: 0,
        str: [][..]
    };
}
*/
impl SmallStr {
    /// ```ignore
    /// use libpijul::small_string::*;
    /// let mut s = SmallString::from_str("");
    /// assert!(s.as_small_str().is_empty());
    /// s.push_str("blah");
    /// assert!(!s.as_small_str().is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// ```ignore
    /// use libpijul::small_string::*;
    /// let mut s = SmallString::from_str("blah");
    /// assert_eq!(s.as_small_str().len(), "blah".len())
    /// ```
    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    pub fn as_bytes(&self) -> &[u8] {
        let s: &[u8] = unsafe { std::mem::transmute(self) };
        &s[1..]
    }

    pub fn to_owned(&self) -> SmallString {
        SmallString::from_str(self.as_str())
    }
}

/// Faster than running doc tests.
#[test]
fn all_doc_tests() {
    {
        let s = SmallString::from_str("blah!");
        assert_eq!(s.len(), s.as_str().len());
    }
    {
        let mut s = SmallString::from_str("blah");
        s.clear();
        assert_eq!(s.as_str(), "");
        assert!(s.is_empty());
    }
    {
        let mut s = SmallString::from_str("blah");
        s.clear();
        assert!(s.is_empty());
    }
    {
        let mut s = SmallString::from_str("");
        assert!(s.is_empty());
        s.push_str("blah");
        assert!(!s.is_empty());
    }
    {
        let s = SmallString::from_str("blah");
        assert_eq!(s.len(), "blah".len())
    }
}

impl sanakirja::UnsizedStorable for SmallStr {
    const ALIGN: usize = 1;

    fn size(&self) -> usize {
        1 + self.len as usize
    }
    unsafe fn write_to_page(&self, p: *mut u8) {
        std::ptr::copy(&self.len, p, 1 + self.len as usize);
        debug!(
            "writing {:?}",
            std::slice::from_raw_parts(p, 1 + self.len as usize)
        );
    }
    unsafe fn from_raw_ptr<'a, T>(_: &T, p: *const u8) -> &'a Self {
        smallstr_from_raw_ptr(p)
    }
    unsafe fn onpage_size(p: *const u8) -> usize {
        let len = *p as usize;
        debug!(
            "onpage_size {:?}",
            std::slice::from_raw_parts(p, 1 + len as usize)
        );
        1 + len
    }
}

impl sanakirja::Storable for SmallStr {
    fn compare<T>(&self, _: &T, x: &Self) -> std::cmp::Ordering {
        self.cmp(x)
    }
    type PageReferences = std::iter::Empty<u64>;
    fn page_references(&self) -> Self::PageReferences {
        std::iter::empty()
    }
}

impl ::sanakirja::debug::Check for SmallStr {}

unsafe fn smallstr_from_raw_ptr<'a>(p: *const u8) -> &'a SmallStr {
    let len = *p as usize;
    std::mem::transmute(std::slice::from_raw_parts(p, 1 + len as usize))
}

#[test]
fn smallstr_repr() {
    use sanakirja::UnsizedStorable;
    let o = SmallString::from_str("blablabla");
    let mut x = vec![0u8; 200];
    unsafe {
        o.write_to_page(x.as_mut_ptr());
        let p = smallstr_from_raw_ptr(x.as_ptr());
        assert_eq!(p.as_str(), "blablabla")
    }
}
