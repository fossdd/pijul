use crate::HashSet;

pub struct LineSplit<'a> {
    buf: &'a [u8],
    missing_eol: Option<&'a HashSet<usize>>,
    current: usize,
    m: regex::bytes::Matches<'a, 'a>,
}

impl super::vertex_buffer::Diff {
    pub fn lines<'a>(&'a self, r: &'a regex::bytes::Regex) -> LineSplit<'a> {
        LineSplit {
            buf: &self.contents_a,
            missing_eol: Some(&self.missing_eol),
            current: 0,
            m: r.find_iter(&self.contents_a),
        }
    }
}

impl<'a> std::convert::From<&'a [u8]> for LineSplit<'a> {
    fn from(buf: &'a [u8]) -> LineSplit<'a> {
        LineSplit {
            buf,
            missing_eol: None,
            current: 0,
            m: super::DEFAULT_SEPARATOR.find_iter(buf),
        }
    }
}

impl<'a> LineSplit<'a> {
    pub fn from_bytes_with_sep(buf: &'a [u8], sep: &'a regex::bytes::Regex) -> LineSplit<'a> {
        LineSplit {
            buf,
            missing_eol: None,
            current: 0,
            m: sep.find_iter(buf),
        }
    }
}

impl<'a> Iterator for LineSplit<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(m) = self.m.next() {
            let start = self.current;
            let next = m.end();
            self.current = next;

            let mut last = next;
            if let Some(miss) = self.missing_eol {
                if miss.contains(&(self.current - 1)) {
                    last -= 1
                }
            }
            Some(&self.buf[start..last])
        } else if self.current < self.buf.len() {
            let cur = self.current;
            self.current = self.buf.len();
            Some(&self.buf[cur..])
        } else {
            None
        }
    }
}
