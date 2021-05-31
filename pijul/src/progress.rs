use log::*;
use std::borrow::Cow;
use std::io::Write;
use std::sync::{Arc, Mutex};

lazy_static::lazy_static! {
    pub static ref PROGRESS: crate::progress::Cursors = crate::progress::Cursors::new();
}

pub struct Cursors {
    pub inner: Arc<Mutex<InnerCursors>>,
    t: Mutex<Option<std::thread::JoinHandle<()>>>,
}

pub struct InnerCursors {
    drawn: usize,
    cursors: Vec<Cursor>,
    n_post: usize,
    n_pre: usize,
    w: usize,
    stop: bool,
}

impl std::ops::Index<usize> for InnerCursors {
    type Output = Cursor;
    fn index(&self, i: usize) -> &Self::Output {
        self.cursors.index(i)
    }
}

impl std::ops::IndexMut<usize> for InnerCursors {
    fn index_mut(&mut self, i: usize) -> &mut Self::Output {
        self.cursors.index_mut(i)
    }
}

impl Cursors {
    pub fn new() -> Self {
        let inner = Arc::new(Mutex::new(InnerCursors {
            drawn: 0,
            cursors: Vec::new(),
            n_post: 0,
            n_pre: 0,
            stop: false,
            w: 0,
        }));
        let cursors = Cursors {
            inner,
            t: Mutex::new(None),
        };
        cursors.restart();
        cursors
    }

    fn restart(&self) {
        debug!("restart");
        let mut t = self.t.lock().unwrap();
        if t.is_some() {
            return;
        }
        let inner_ = self.inner.clone();
        *t = Some(std::thread::spawn(move || loop {
            {
                let mut inner = if let Ok(inner) = inner_.lock() {
                    inner
                } else {
                    break;
                };
                if inner.stop {
                    inner.render().unwrap();
                    break;
                } else {
                    inner.render().unwrap();
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }));
    }

    pub fn stop(&self) {
        debug!("stop");
        if let Ok(mut n) = self.inner.lock() {
            n.stop = true
        }
    }

    pub fn join(&self) {
        debug!("join");
        self.stop();
        let mut t = self.t.lock().unwrap();
        if let Some(t) = t.take() {
            t.join().unwrap();
        }
    }

    pub fn borrow_mut(
        &self,
    ) -> Result<
        std::sync::MutexGuard<InnerCursors>,
        std::sync::PoisonError<std::sync::MutexGuard<'_, InnerCursors>>,
    > {
        debug!("borrow_mut");
        self.restart();
        let mut m = self.inner.lock()?;
        m.stop = false;
        m.n_pre = 0;
        Ok(m)
    }
}

#[allow(dead_code)]
pub enum Cursor {
    Static {
        pre: Cow<'static, str>,
    },
    Bar {
        pre: Cow<'static, str>,
        n: usize,
        i: usize,
    },
    Spin {
        pre: Cow<'static, str>,
        i: usize,
    },
}

impl Cursor {
    fn pre(&self) -> &str {
        match self {
            Cursor::Static { pre } => pre,
            Cursor::Bar { pre, .. } => pre,
            Cursor::Spin { pre, .. } => pre,
        }
    }
    fn n(&self) -> usize {
        match self {
            Cursor::Bar { n, .. } => {
                let mut n = *n;
                let mut r = 6;
                while n > 0 {
                    n /= 10;
                    r += 2
                }
                r
            }
            _ => 0,
        }
    }

    pub fn incr(&mut self) {
        match self {
            Cursor::Bar { i, .. } => *i += 1,
            _ => {}
        }
    }

    pub fn incr_len(&mut self) {
        match self {
            Cursor::Bar { n, .. } => *n += 1,
            _ => {}
        }
    }

    fn render<W: std::io::Write>(
        &mut self,
        stdout: &mut W,
        npre: usize,
        npost: usize,
        w: usize,
    ) -> Result<(), std::io::Error> {
        match self {
            Cursor::Static { pre } => {
                for _ in 0..npre - pre.chars().count() {
                    stdout.write_all(b" ")?;
                }
                stdout.write_all(pre.as_bytes())?;

                // Fil the rest of the line with spaces.
                for _ in 0..w - npre {
                    stdout.write_all(b" ")?;
                }
                Ok(())
            }
            Cursor::Bar { pre, i, n } => {
                for _ in 0..npre - pre.chars().count() {
                    stdout.write_all(b" ")?;
                }

                // Comupte the appropriate width for the bar.
                let w_digits = {
                    let mut n = *n;
                    let mut nd = if n == 0 { 1 } else { 0 } + if *i == 0 { 1 } else { 0 };
                    while n > 0 {
                        n /= 10;
                        nd += 1
                    }
                    let mut n = *i;
                    while n > 0 {
                        n /= 10;
                        nd += 1
                    }
                    nd
                };
                let w = w - npre - npost - w_digits;

                // Output the bar.
                write!(stdout, "{} [", pre)?;

                let wb = (w as usize).min(50);
                if *n <= 1 {
                    for _ in 0..wb as usize {
                        if *i == 1 {
                            write!(stdout, "=")?;
                        } else {
                            write!(stdout, " ")?
                        }
                    }
                } else {
                    let k = (wb as usize * *i) / *n;
                    for j in 0..wb as usize {
                        if j < k {
                            write!(stdout, "=")?;
                        } else if j == k {
                            write!(stdout, ">")?;
                        } else {
                            write!(stdout, " ")?
                        }
                    }
                }
                write!(stdout, "] {}/{}", *i, *n)?;

                let nw = w + npost - wb - 6;
                for _ in 0..nw {
                    stdout.write_all(b" ")?;
                }
                Ok(())
            }
            Cursor::Spin { pre, i } => {
                for _ in 0..npre - pre.chars().count() {
                    stdout.write_all(b" ")?;
                }
                stdout.write_all(pre.as_bytes())?;
                stdout.write_all(b" ")?;
                const SYM: [&str; 8] = ["←", "↖", "↑", "↗", "→", "↘", "↓", "↙"];
                stdout.write_all(SYM[*i].as_bytes())?;
                *i = (*i + 1) % SYM.len();
                // Fill the rest of the line with spaces.
                for _ in 0..w - npre - 2 {
                    stdout.write_all(b" ")?;
                }
                Ok(())
            }
        }
    }
}

impl InnerCursors {
    pub fn push(&mut self, c: Cursor) -> usize {
        let r = self.cursors.len();
        self.cursors.push(c);
        r
    }

    fn render(&mut self) -> Result<(), std::io::Error> {
        use terminal_size::*;
        let mut stdout = std::io::stdout();
        if let Some((Width(w), _)) = terminal_size() {
            if self.n_pre == 0 {
                self.n_post = 0;
                for c in self.cursors.iter() {
                    let n_pre = c.pre().chars().count();
                    self.n_pre = self.n_pre.max(n_pre);
                    self.n_post = self.n_post.max(c.n());
                }
            }
            let w = w as usize;
            for _ in 0..self.drawn {
                stdout.write_all(b"\x1B[F")?;
            }
            self.w = w;
            for c in self.cursors.iter_mut() {
                c.render(&mut stdout, self.n_pre, self.n_post, w)?;
                // Clear the end of the line and move to the next one.
                stdout.write_all(b"\x1B[K\n")?;
            }
            self.drawn = self.cursors.len();
            // Erase the terminal after the cursor.
            stdout.write_all(b"\x1B[J")?;
            stdout.flush()?;
        }
        Ok(())
    }
}
