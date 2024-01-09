use core::fmt::Display;
use serde::{de, ser::SerializeStruct, Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{DeserializeAs, SerializeAs};
use std::{
    convert::{TryFrom, TryInto},
    fmt::{Debug, LowerHex},
    str::FromStr,
};

/// Wrapper type for a FixedString type in Clickhouse
/// Uses custom serializing handling in SerializeStruct impl for RowBinarySerializer
/// Forgoes the LEB128 encoding and just encodes the raw byte string
/// For deserializing the type FixedString(n) with a `query()`, wrap toString(...) around the value
/// For example:
///
/// CREATE TABLE test (
///     t1 String,
///     t2 FixedString(50)
/// ) ...
///
/// query("SELECT t1, toString(t2) FROM test;").fetch...
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FixedString {
    pub string: String,
}

impl FixedString {
    pub fn new(string: String) -> Self {
        FixedString { string }
    }
}

impl From<String> for FixedString {
    fn from(value: String) -> Self {
        FixedString { string: value }
    }
}

impl From<&str> for FixedString {
    fn from(value: &str) -> Self {
        FixedString {
            string: value.to_string(),
        }
    }
}

impl Display for FixedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.string)
    }
}

impl Serialize for FixedString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FixedString", 1)?;
        state.serialize_field("FixedString", &self.string)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for FixedString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        Ok(FixedString::new(s))
    }
}

impl<T> SerializeAs<T> for FixedString
where
    T: Debug,
{
    fn serialize_as<S>(source: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FixedString", 1)?;
        state.serialize_field("FixedString", &format!("{:?}", source))?;
        state.end()
    }
}

impl<'de, T> DeserializeAs<'de, T> for FixedString
where
    T: Debug + FromStr,
    T::Err: Display,
{
    fn deserialize_as<D>(deserializer: D) -> Result<T, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let obj = serde_json::Value::deserialize(deserializer).map_err(de::Error::custom)?;
        let fixed_str = obj
            .get("FixedString")
            .ok_or_else(|| de::Error::custom("no FixedString field"))?;

        fixed_str
            .as_str()
            .unwrap()
            .parse()
            .map_err(de::Error::custom)
    }
}
