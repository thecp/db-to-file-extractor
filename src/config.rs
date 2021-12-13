use serde::{Deserialize, Serialize, Serializer};

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
    database_type: DatabaseType,
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
            DatabaseType::MsSQL => {
                format!(
                    "jdbc:sqlserver://Server={}; Database={}; User Id={}; Password={};",
                    self.database.server,
                    self.database.database,
                    self.database.user,
                    self.database.password
                )
            }
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct TableConfig {
    pub name: String,
    pub columns: Vec<ColumnConfig>,
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
pub struct ColumnConfig {
    pub name: String,
    #[serde(alias = "type")]
    pub data_type: DataTypeInput,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum DatabaseType {
    MySQL,
    MsSQL,
}

#[derive(Deserialize, Debug)]
pub enum DataType {
    String(String),
    Integer(i64),
}

#[derive(Deserialize, Debug)]
pub enum DataTypeInput {
    String,
    Integer,
}

impl Serialize for DataType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::String(str) => serializer.serialize_str(str),
            Self::Integer(int) => serializer.serialize_i64(*int),
        }
    }
}
