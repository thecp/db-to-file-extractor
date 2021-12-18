use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug, JsonSchema, Serialize)]
pub struct Config {
    pub tables: Vec<TableConfig>,
    pub database: DatabaseConfig,
}

#[derive(Deserialize, Debug, JsonSchema, Serialize)]
pub struct DatabaseConfig {
    pub user: String,
    pub password: String,
    pub server: String,
    pub database: String,
    #[serde(alias = "type")]
    pub database_type: DatabaseType,
}

#[derive(Deserialize, Debug, JsonSchema, Serialize)]
pub struct TableConfig {
    pub name: String,
    pub columns: Vec<String>,
    #[serde(alias = "where")]
    pub where_clause: Option<String>,
}

#[derive(Deserialize, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseType {
    MySQL,
    MsSQL,
}
