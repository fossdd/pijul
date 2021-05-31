pub(crate) struct Vector2<A> {
    v: Vec<A>,
    bounds: Vec<usize>,
}

impl<A> Vector2<A> {
    pub(crate) fn new() -> Self {
        Vector2 {
            v: Vec::new(),
            bounds: vec![0],
        }
    }
    pub(crate) fn len(&self) -> usize {
        self.bounds.len() - 1
    }
    pub(crate) fn with_capacities(total: usize, n: usize) -> Self {
        let mut bounds = Vec::with_capacity(n);
        bounds.push(0);
        Vector2 {
            v: Vec::with_capacity(total),
            bounds,
        }
    }
    pub(crate) fn push_to_last(&mut self, a: A) {
        assert!(self.bounds.len() > 1);
        *self.bounds.last_mut().unwrap() += 1;
        self.v.push(a)
    }
    pub(crate) fn push(&mut self) {
        self.bounds.push(self.v.len())
    }
    pub(crate) fn last_mut(&mut self) -> Option<&mut [A]> {
        if self.bounds.len() >= 2 {
            let i = self.bounds.len() - 2;
            Some(&mut self.v[self.bounds[i]..self.bounds[i + 1]])
        } else {
            None
        }
    }
}

impl<A> std::ops::Index<usize> for Vector2<A> {
    type Output = [A];
    fn index(&self, i: usize) -> &[A] {
        &self.v[self.bounds[i]..self.bounds[i + 1]]
    }
}

impl<A> std::ops::IndexMut<usize> for Vector2<A> {
    fn index_mut(&mut self, i: usize) -> &mut [A] {
        &mut self.v[self.bounds[i]..self.bounds[i + 1]]
    }
}

impl<A: std::fmt::Debug> std::fmt::Debug for Vector2<A> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "[")?;
        for i in 0..self.bounds.len() - 1 {
            if i > 0 {
                write!(fmt, ", ")?
            }
            write!(fmt, "{:?}", &self[i])?
        }
        write!(fmt, "]")?;
        Ok(())
    }
}

#[test]
fn test_v2() {
    let mut v: Vector2<usize> = Vector2::new();
    v.push();
    v.push_to_last(0);
    v.push_to_last(1);
    v.push_to_last(2);
    v.push();
    v.push_to_last(4);
    v.push_to_last(5);
    v.push_to_last(6);
    assert_eq!(&v[0], &[0, 1, 2][..]);
    assert_eq!(&v[1], &[4, 5, 6][..]);
}

#[test]
#[should_panic]
fn test_v2_() {
    let w: Vector2<usize> = Vector2::new();
    println!("{:?}", &w[0]);
}
