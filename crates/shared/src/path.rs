//! `HandlePath` and related types.

use hashbrown::Equivalent;
use serde::{
    de::{Error as DeError, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

use std::{
    borrow::Cow,
    convert::Infallible,
    fmt,
    hash::{Hash, Hasher},
    iter,
    str::FromStr,
};

const PATH_SEP: char = '/';

/// Inner part of `HandlePath`.
#[derive(Debug, Clone, Copy)]
enum Inner<'a> {
    Empty,
    /// Linked list cell, with the suffix containing 1 or more segments.
    Link {
        head: &'a Self,
        tail: &'a str,
    },
}

/// Path to a [`Handle`](crate::interface::Handle) in a [map](crate::interface::HandleMap).
///
/// Conceptually, a path is hierarchical like a path in a file system; consists of zero or more
/// string segments. A path can be represented as a string with the segments separated by `/`.
/// Hierarchical nature of paths is used to compose simplest workflow handles (channel senders
/// and receivers) into composable high-level components.
///
/// # Examples
///
/// ```
/// # use hashbrown::HashMap;
/// # use tardigrade_shared::handle::{HandlePath, HandlePathBuf};
/// const PATH: HandlePath<'_> = HandlePath::new("some/test").join("path");
/// assert_eq!(PATH.to_string(), "some/test/path");
/// assert_eq!(PATH.segments().collect::<Vec<_>>(), ["some", "test", "path"]);
///
/// let path_buf: HandlePathBuf = PATH.to_owned();
/// assert_eq!(path_buf.as_ref(), PATH);
/// let other_path_buf: HandlePathBuf = "other/path".parse()?;
///
/// // One of key path properties is ability to use as keys in hash maps:
/// let mut map = HashMap::<_, _>::from_iter([
///     (path_buf, 555),
///     (other_path_buf.clone(), 777),
/// ]);
/// assert_eq!(map[&PATH], 555);
/// assert_eq!(map[&other_path_buf], 777);
/// # Ok::<_, Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Copy)]
pub struct HandlePath<'a> {
    inner: Inner<'a>,
}

impl fmt::Display for HandlePath<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let segments: Vec<_> = self.segments().collect();
        for (i, segment) in segments.iter().enumerate() {
            formatter.write_str(segment)?;
            if i + 1 < segments.len() {
                write!(formatter, "{PATH_SEP}")?;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for HandlePath<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl PartialEq for HandlePath<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.segments().eq(other.segments())
    }
}

impl Eq for HandlePath<'_> {}

impl Hash for HandlePath<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for segment in self.segments() {
            str::hash(segment, state);
        }
    }
}

impl HandlePath<'static> {
    /// Empty path, i.e., a path with zero segments.
    pub const EMPTY: Self = Self {
        inner: Inner::Empty,
    };
}

impl<'a> HandlePath<'a> {
    /// Creates a path from the supplied string.
    pub const fn new(s: &'a str) -> Self {
        let inner = if s.is_empty() {
            Inner::Empty
        } else {
            Inner::Link {
                head: &Inner::Empty,
                tail: s,
            }
        };
        Self { inner }
    }

    /// Appends the `suffix` to this path and returns the resulting path.
    #[must_use]
    pub const fn join(&'a self, suffix: &'a str) -> Self {
        if suffix.is_empty() {
            *self
        } else {
            Self {
                inner: Inner::Link {
                    head: &self.inner,
                    tail: suffix,
                },
            }
        }
    }

    /// Converts this path to the owned form.
    pub fn to_owned(self) -> HandlePathBuf {
        HandlePathBuf::from(self)
    }

    #[doc(hidden)] // sort of low-level
    pub fn to_cow_string(self) -> Cow<'a, str> {
        match self.inner {
            Inner::Empty => Cow::Borrowed(""),

            Inner::Link {
                head: Inner::Empty,
                tail,
            } => Cow::Borrowed(tail),

            Inner::Link { .. } => Cow::Owned(self.to_string()),
        }
    }

    /// Checks whether this path is empty (contains no segments).
    pub fn is_empty(self) -> bool {
        matches!(self.inner, Inner::Empty)
    }

    /// Iterates over the segments in this path.
    pub fn segments(self) -> impl Iterator<Item = &'a str> {
        let ancestors = iter::successors(Some(self.inner), |&inner| match inner {
            Inner::Empty => None,
            Inner::Link { head, .. } => Some(*head),
        });
        let mut ancestors: Vec<_> = ancestors.collect();
        ancestors.reverse();

        ancestors.into_iter().flat_map(|inner| match inner {
            Inner::Empty => Either::Left(iter::empty()),
            Inner::Link { tail, .. } => Either::Right(tail.split(PATH_SEP)),
        })
    }
}

impl<'a> From<&'a str> for HandlePath<'a> {
    fn from(path: &'a str) -> Self {
        Self::new(path)
    }
}

impl<'a> From<&'a HandlePathBuf> for HandlePath<'a> {
    fn from(path: &'a HandlePathBuf) -> Self {
        path.as_ref()
    }
}

// Ad-hoc impl of `Iterator` for a union of two types.
#[derive(Debug)]
enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> Iterator for Either<L, R>
where
    L: Iterator,
    R: Iterator<Item = L::Item>,
{
    type Item = L::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Left(left) => left.next(),
            Self::Right(right) => right.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Left(left) => left.size_hint(),
            Self::Right(right) => right.size_hint(),
        }
    }
}

/// Owned version of a [`HandlePath`].
///
/// See [`HandlePath`] docs for an overview and examples of usage.
#[derive(Clone, Eq)]
pub struct HandlePathBuf {
    segments: String,
}

impl HandlePathBuf {
    /// Borrows a [`HandlePath`] from this path.
    pub fn as_ref(&self) -> HandlePath<'_> {
        HandlePath::new(&self.segments)
    }

    /// Checks whether this path is empty (contains no segments).
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Iterates over the segments in this path.
    pub fn segments(&self) -> impl Iterator<Item = &str> + '_ {
        self.segments.split(PATH_SEP)
    }

    /// Extends this path with the specified suffix.
    pub fn extend(&mut self, suffix: Self) {
        if self.segments.is_empty() {
            self.segments = suffix.segments;
        } else if suffix.segments.is_empty() {
            // Do nothing
        } else {
            self.segments.push(PATH_SEP);
            self.segments.push_str(&suffix.segments);
        }
    }
}

impl fmt::Display for HandlePathBuf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.segments)
    }
}

impl fmt::Debug for HandlePathBuf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

/// Parses a path from its string presentation (e.g., `some::compound::path` for a 3-segment path).
/// This conversion is infallible; it simply delegates to [`From`]`<&str>`.
impl FromStr for HandlePathBuf {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(s))
    }
}

impl PartialEq for HandlePathBuf {
    fn eq(&self, other: &Self) -> bool {
        self.segments == other.segments
    }
}

impl Hash for HandlePathBuf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for segment in self.segments() {
            str::hash(segment, state);
        }
    }
}

/// Parses a path from its string presentation (e.g., `some::compound::path` for a 3-segment path).
impl From<&str> for HandlePathBuf {
    fn from(s: &str) -> Self {
        Self {
            segments: s.to_owned(),
        }
    }
}

impl From<HandlePath<'_>> for HandlePathBuf {
    fn from(path: HandlePath<'_>) -> Self {
        let segments = path.to_cow_string().into_owned();
        Self { segments }
    }
}

impl Equivalent<HandlePathBuf> for HandlePath<'_> {
    fn equivalent(&self, key: &HandlePathBuf) -> bool {
        let buf_segments = key.segments();
        buf_segments.eq(self.segments())
    }
}

impl Serialize for HandlePathBuf {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for HandlePathBuf {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StrVisitor;

        impl Visitor<'_> for StrVisitor {
            type Value = HandlePathBuf;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(
                    formatter,
                    "string path with segments separated by {PATH_SEP}"
                )
            }

            fn visit_str<E: DeError>(self, value: &str) -> Result<Self::Value, E> {
                Ok(HandlePathBuf::from_str(value).unwrap())
            }
        }

        deserializer.deserialize_str(StrVisitor)
    }
}

#[cfg(test)]
mod tests {
    use hashbrown::HashMap;

    use std::collections::hash_map::DefaultHasher;

    use super::*;

    #[test]
    fn creating_paths() {
        let path = HandlePath::new("first/second/third");
        let segments: Vec<_> = path.segments().collect();
        assert_eq!(segments, ["first", "second", "third"]);

        let prefix = HandlePath::new("first");
        let same_path = prefix.join("second/third");
        let segments: Vec<_> = same_path.segments().collect();
        assert_eq!(segments, ["first", "second", "third"]);
        assert_eq!(path, same_path);
        assert_ne!(path, prefix);
    }

    #[test]
    fn empty_paths() {
        let path = HandlePath::new("");
        assert_eq!(path, HandlePath::EMPTY);
        assert!(path.is_empty());
        assert!(path.segments().next().is_none());

        let path = path.join("test");
        assert_eq!(path, HandlePath::new("test"));
        assert!(!path.is_empty());
        assert_eq!(path.segments().collect::<Vec<_>>(), ["test"]);

        let same_path = path.join("");
        assert_eq!(same_path, path);
        assert_eq!(path.segments().collect::<Vec<_>>(), ["test"]);
    }

    #[test]
    fn path_cow_string_produces_correct_results() {
        let path = HandlePath::new("first");
        assert_eq!(path.to_cow_string(), "first");
        let path = path.join("second");
        assert_eq!(path.to_cow_string(), "first/second");
    }

    #[test]
    fn hash_and_equiv_implementations_match() {
        const PATH: HandlePath<'_> = HandlePath::new("first").join("second").join("3");

        let path_buf = HandlePathBuf::from(PATH);
        let path_hash = {
            let mut hasher = DefaultHasher::default();
            PATH.hash(&mut hasher);
            hasher.finish()
        };
        let path_buf_hash = {
            let mut hasher = DefaultHasher::default();
            path_buf.hash(&mut hasher);
            hasher.finish()
        };
        assert_eq!(path_buf_hash, path_hash);
        assert!(PATH.equivalent(&path_buf));
        assert_eq!(path_buf.to_string(), PATH.to_string());

        let borrowed = path_buf.as_ref();
        assert_eq!(borrowed, PATH);
        let borrowed_hash = {
            let mut hasher = DefaultHasher::default();
            borrowed.hash(&mut hasher);
            hasher.finish()
        };
        assert_eq!(borrowed_hash, path_hash);
        assert_eq!(borrowed.to_string(), PATH.to_string());
    }

    #[test]
    fn non_static_segments() {
        let segments = [1, 2, 3].map(|i| i.to_string());
        let path = HandlePath::EMPTY.join(&segments[0]);
        let path = path.join(&segments[1]);
        let path = path.join(&segments[2]);
        assert_eq!(path.to_string(), "1/2/3");
    }

    #[test]
    fn handle_map_operations() {
        let mut handle_map = HashMap::new();
        handle_map.insert("test".into(), 42);
        let path = HandlePath::new("test");
        let path = path.join("more");
        handle_map.insert(path.to_owned(), 23);

        //assert_eq!(handle_map["test"], 42);
        assert_eq!(handle_map[&HandlePath::new("test")], 42);
        assert_eq!(handle_map[&path], 23);
    }
}
