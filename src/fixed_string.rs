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
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FixedString {
    pub string: String,
}

impl FixedString {
    pub fn new(string: String) -> Self {
        FixedString { string }
    }
}

impl<T> From<T> for FixedString
where
    String: From<T>,
{
    fn from(value: T) -> Self {
        let string: String = value.into();
        FixedString { string }
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
        let obj = serde_json::Value::deserialize(deserializer).map_err(de::Error::custom)?;

        if obj.is_object() {
            let fixed_str = obj
                .get("FixedString")
                .ok_or_else(|| de::Error::custom("no FixedString field"))?;

            Ok(FixedString::new(fixed_str.as_str().unwrap().to_string()))
        } else {
            Ok(FixedString::new(obj.as_str().unwrap().to_string()))
        }
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

        if obj.is_object() {
            let fixed_str = obj
                .get("FixedString")
                .ok_or_else(|| de::Error::custom("no FixedString field"))?;

            fixed_str
                .as_str()
                .unwrap()
                .parse()
                .map_err(de::Error::custom)
        } else {
            obj.as_str().unwrap().parse().map_err(de::Error::custom)
        }
    }
}
