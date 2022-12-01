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

const PATH_SEP: &str = "::";

#[derive(Debug, Clone, Copy)]
enum Inner<'a> {
    Link(&'a Self, &'a str),
    Slice(&'a [String]),
}

impl Inner<'static> {
    const EMPTY: Self = Self::Slice(&[]);
}

/// Path to a [`Handle`](crate::interface::Handle) in a [map](crate::interface::HandleMap).
///
/// Conceptually, a path consists of zero or more string segments. A path can be represented
/// as a string, with the segments separated by `::`.
///
/// # Examples
///
/// ```
/// # use hashbrown::HashMap;
/// # use tardigrade_shared::interface::{HandlePath, HandlePathBuf};
/// const PATH: HandlePath<'_> = HandlePath::simple("test").join("path");
/// assert_eq!(PATH.to_string(), "test::join");
/// let path_buf: HandlePathBuf = PATH.to_owned();
/// assert_eq!(path_buf.as_ref(), PATH);
/// let other_path_buf: HandlePathBuf = "other::path".parse()?;
///
/// // One of key path properties is ability to use as keys in hash maps:
/// let mut map =
///     HashMap::from_iter([(path_buf, 555), (other_path_buf, 777)]);
/// assert_eq!(map[&PATH], 555);
/// assert_eq!(map[&other_path_buf], 777);
///
/// // Note that `str` keys can be used for indexing as well:
/// map.insert("third_path".into(), 42);
/// assert_eq!(map["third_path"], 42);
/// assert!(map.contains_key(&HandlePath::simple("third_path")));
/// # Ok::<_, Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Copy)]
pub struct HandlePath<'a> {
    inner: Inner<'a>,
}

impl fmt::Display for HandlePath<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut segments: Vec<_> = self.rev_segments().collect();
        segments.reverse();
        for (i, segment) in segments.iter().enumerate() {
            formatter.write_str(segment)?;
            if i + 1 < segments.len() {
                formatter.write_str(PATH_SEP)?;
            }
        }
        Ok(())
    }
}

impl PartialEq for HandlePath<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.rev_segments().eq(other.rev_segments())
    }
}

impl Eq for HandlePath<'_> {}

impl Hash for HandlePath<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for segment in self.rev_segments() {
            str::hash(segment, state);
        }
    }
}

impl HandlePath<'static> {
    /// Empty path, i.e., a path with zero segments.
    pub const EMPTY: Self = Self {
        inner: Inner::EMPTY,
    };
}

impl<'a> HandlePath<'a> {
    /// Creates a simple path from a single segment.
    pub const fn simple(segment: &'a str) -> Self {
        Self {
            inner: Inner::Link(&Inner::EMPTY, segment),
        }
    }

    /// Appends the `suffix` to this path and returns the resulting path.
    #[must_use]
    pub const fn join(&'a self, suffix: &'a str) -> Self {
        Self {
            inner: Inner::Link(&self.inner, suffix),
        }
    }

    /// Converts this path to the owned form.
    pub fn to_owned(self) -> HandlePathBuf {
        HandlePathBuf::from(self)
    }

    #[doc(hidden)] // sort of low-level
    pub fn to_cow_string(self) -> Cow<'a, str> {
        match self.inner {
            Inner::Slice([]) => Cow::Borrowed(""),
            Inner::Link(Inner::Slice([]), segment) => Cow::Borrowed(segment),
            Inner::Slice([segment]) => Cow::Borrowed(segment),
            _ => Cow::Owned(self.to_string()),
        }
    }

    fn rev_segments(self) -> impl Iterator<Item = &'a str> {
        let ancestors = iter::successors(Some(self.inner), |&inner| match inner {
            Inner::Link(head, _) => Some(*head),
            Inner::Slice(_) => None,
        });
        ancestors.flat_map(|inner| match inner {
            Inner::Link(_, tail) => Either::Left(iter::once(tail)),
            Inner::Slice(slice) => Either::Right(slice.iter().rev().map(String::as_str)),
        })
    }
}

/// Creates a path consisting of a single provided segment.
impl<'a> From<&'a str> for HandlePath<'a> {
    fn from(segment: &'a str) -> Self {
        Self::simple(segment)
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
/// See [`HandlePath`] docs for examples of usage.
#[derive(Debug, Clone, Eq)]
pub struct HandlePathBuf {
    segments: Vec<String>,
}

impl HandlePathBuf {
    /// Borrows a [`HandlePath`] from this path.
    pub fn as_ref(&self) -> HandlePath<'_> {
        HandlePath {
            inner: Inner::Slice(&self.segments),
        }
    }
}

impl fmt::Display for HandlePathBuf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, segment) in self.segments.iter().enumerate() {
            formatter.write_str(segment)?;
            if i + 1 < self.segments.len() {
                formatter.write_str(PATH_SEP)?;
            }
        }
        Ok(())
    }
}

/// Parses a path from its string presentation (e.g., `some::compound::path` for a 3-segment path).
impl FromStr for HandlePathBuf {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let segments: Vec<_> = s.split(PATH_SEP).map(String::from).collect();
        Ok(Self { segments })
    }
}

impl PartialEq for HandlePathBuf {
    fn eq(&self, other: &Self) -> bool {
        self.segments == other.segments
    }
}

impl Hash for HandlePathBuf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for segment in self.segments.iter().rev() {
            str::hash(segment, state);
        }
    }
}

/// Creates a path consisting of a single provided segment.
impl From<String> for HandlePathBuf {
    fn from(segment: String) -> Self {
        Self {
            segments: vec![segment],
        }
    }
}

/// Creates a path consisting of a single provided segment.
impl From<&str> for HandlePathBuf {
    fn from(segment: &str) -> Self {
        Self {
            segments: vec![segment.to_owned()],
        }
    }
}

impl From<HandlePath<'_>> for HandlePathBuf {
    fn from(path: HandlePath<'_>) -> Self {
        if let Inner::Slice(segments) = path.inner {
            Self {
                segments: segments.to_vec(),
            }
        } else {
            let mut segments: Vec<_> = path.rev_segments().map(String::from).collect();
            segments.reverse();
            Self { segments }
        }
    }
}

impl Equivalent<HandlePathBuf> for HandlePath<'_> {
    fn equivalent(&self, key: &HandlePathBuf) -> bool {
        let buf_segments = key.segments.iter().rev();
        buf_segments.eq(self.rev_segments())
    }
}

impl Equivalent<HandlePathBuf> for str {
    fn equivalent(&self, key: &HandlePathBuf) -> bool {
        key.segments.len() == 1 && key.segments[0] == *self
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

/// Newtype for indexing channel receivers, e.g., in an [`Interface`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReceiverAt<T>(pub T);

impl<T: fmt::Display> fmt::Display for ReceiverAt<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "channel receiver `{}`", self.0)
    }
}

/// Newtype for indexing channel senders, e.g., in an [`Interface`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SenderAt<T>(pub T);

impl<T: fmt::Display> fmt::Display for SenderAt<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "channel sender `{}`", self.0)
    }
}

#[cfg(test)]
mod tests {
    use hashbrown::HashMap;

    use std::collections::hash_map::DefaultHasher;

    use super::*;

    #[test]
    fn path_cow_string_produces_correct_results() {
        let path = HandlePath::simple("first");
        assert_eq!(path.to_cow_string(), "first");
        let path = path.join("second");
        assert_eq!(path.to_cow_string(), "first::second");
    }

    #[test]
    fn hash_and_equiv_implementations_match() {
        const PATH: HandlePath<'_> = HandlePath::simple("first").join("second").join("3");

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
        assert_eq!(path.to_string(), "1::2::3");
    }

    #[test]
    fn handle_map_operations() {
        let mut handle_map = HashMap::new();
        handle_map.insert("test".into(), 42);
        let path = HandlePath::simple("test");
        let path = path.join("more");
        handle_map.insert(path.to_owned(), 23);

        assert_eq!(handle_map["test"], 42);
        assert_eq!(handle_map[&HandlePath::simple("test")], 42);
        assert_eq!(handle_map[&path], 23);
    }
}