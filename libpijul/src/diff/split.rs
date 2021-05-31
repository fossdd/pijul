use crate::HashSet;

pub struct LineSplit<'a> {
    buf: &'a [u8],
    missing_eol: Option<&'a HashSet<usize>>,
    current: usize,
}

impl super::vertex_buffer::Diff {
    pub fn lines(&self) -> LineSplit {
        LineSplit {
            buf: &self.contents_a,
            missing_eol: Some(&self.missing_eol),
            current: 0,
        }
    }
}

impl<'a> std::convert::From<&'a [u8]> for LineSplit<'a> {
    fn from(buf: &'a [u8]) -> LineSplit<'a> {
        LineSplit {
            buf,
            missing_eol: None,
            current: 0,
        }
    }
}

impl<'a> Iterator for LineSplit<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.buf.len() {
            return None;
        }
        let current = self.current;
        while self.current < self.buf.len() && self.buf[self.current] != b'\n' {
            self.current += 1
        }
        if self.current < self.buf.len() {
            self.current += 1
        }
        let mut last = self.current;
        if let Some(miss) = self.missing_eol {
            if miss.contains(&(self.current - 1)) {
                last -= 1
            }
        }
        Some(&self.buf[current..last])
    }
}
