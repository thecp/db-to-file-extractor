use serde::{Deserialize, Serialize, Serializer};
use sqlx::types::Uuid;
use tiberius::{time::chrono::{self, Utc}, AuthMethod, numeric::Decimal};

#[derive(Deserialize, Debug)]
pub struct Config {
    pub tables: Vec<TableConfig>,
    pub database: DatabaseConfig,
}

#[derive(Deserialize, Debug)]
pub struct DatabaseConfig {
    user: String,
    password: String,
    server: String,
    database: String,
    #[serde(alias = "type")]
    pub database_type: DatabaseType,
}

impl Config {
    pub fn connection_string(&self) -> String {
        match self.database.database_type {
            DatabaseType::MySQL => {
                format!(
                    "mysql://{}:{}@{}/{}",
                    self.database.user,
                    self.database.password,
                    self.database.server,
                    self.database.database
                )
            },
            _ => unimplemented!()
        }
    }

    pub fn mssql_config(&self) -> tiberius::Config {
        let mut db_config = tiberius::Config::new();
     
        db_config.host(&self.database.server);
        db_config.port(1433);
        db_config.authentication(AuthMethod::sql_server(&self.database.user, &self.database.password));
        db_config.trust_cert(); // on production, it is not a good idea to do this

        db_config
    }
}

#[derive(Deserialize, Debug)]
pub struct TableConfig {
    pub name: String,
    pub columns: Vec<String>,
    #[serde(alias = "where")]
    pub where_clause: Option<String>,
}

impl TableConfig {
    pub fn get_where_clause(&self) -> &str {
        match &self.where_clause {
            Some(w) => &w,
            None => "1=1"
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseType {
    MySQL,
    MsSQL,
}

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
    Time(Option<chrono::NaiveTime>)
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
            },
            Self::Date(date) => {
                if let Some(date) = date {
                    serializer.serialize_str(&date.to_string())
                } else {
                    serializer.serialize_none()
                }
            },
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
