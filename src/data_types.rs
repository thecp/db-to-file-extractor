use serde::{Serialize, Serializer};
use sqlx::types::Uuid;
use tiberius::{
    numeric::Decimal,
    time::chrono::{self, Utc},
};

#[derive(Debug)]
pub enum DataType {
    String(Option<String>),
    Int(Option<i32>),
    BigInt(Option<i64>),
    Float(Option<f32>),
    Double(Option<f64>),
    Decimal(Option<Decimal>),
    Bool(Option<bool>),
    Uuid(Option<Uuid>),
    DateTimeUtc(Option<chrono::DateTime<Utc>>),
    DateTime(Option<chrono::NaiveDateTime>),
    Date(Option<chrono::NaiveDate>),
    Time(Option<chrono::NaiveTime>),
}

impl Serialize for DataType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // TODO use if let guards which are experimental at the moment
        match self {
            Self::String(str) => {
                if let Some(str) = str {
                    serializer.serialize_str(str)
                } else {
                    serializer.serialize_none()
                }
            }
            Self::Int(int) => {
                if let Some(int) = int {
                    serializer.serialize_i32(*int)
                } else {
                    serializer.serialize_none()
                }
            }
            Self::BigInt(int) => {
                if let Some(int) = int {
                    serializer.serialize_i64(*int)
                } else {
                    serializer.serialize_none()
                }
            }
            Self::Float(float) => {
                if let Some(float) = float {
                    serializer.serialize_f32(*float)
                } else {
                    serializer.serialize_none()
                }
            }
            Self::Double(double) => {
                if let Some(double) = double {
                    serializer.serialize_f64(*double)
                } else {
                    serializer.serialize_none()
                }
            }
            Self::Decimal(decimal) => {
                if let Some(decimal) = decimal {
                    serializer.serialize_f64(decimal.to_string().parse().unwrap())
                } else {
                    serializer.serialize_none()
                }
            }
            Self::Bool(b) => {
                if let Some(b) = b {
                    serializer.serialize_bool(*b)
                } else {
                    serializer.serialize_none()
                }
            }
            Self::Uuid(uuid) => {
                if let Some(uuid) = uuid {
                    serializer.serialize_str(&uuid.to_string())
                } else {
                    serializer.serialize_none()
                }
            }
            Self::DateTimeUtc(datetime) => {
                if let Some(datetime) = datetime {
                    serializer.serialize_str(&datetime.to_string())
                } else {
                    serializer.serialize_none()
                }
            }
            Self::DateTime(datetime) => {
                if let Some(datetime) = datetime {
                    serializer.serialize_str(&datetime.to_string())
                } else {
                    serializer.serialize_none()
                }
            }
            Self::Date(date) => {
                if let Some(date) = date {
                    serializer.serialize_str(&date.to_string())
                } else {
                    serializer.serialize_none()
                }
            }
            Self::Time(time) => {
                if let Some(time) = time {
                    serializer.serialize_str(&time.to_string())
                } else {
                    serializer.serialize_none()
                }
            }
        }
    }
}
