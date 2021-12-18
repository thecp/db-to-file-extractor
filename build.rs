// TODO move config into separate crate
#[path = "src/config.rs"]
mod config;

use std::{
    fs::File,
    io::{BufWriter, Write},
};

use crate::config::{Config, DatabaseConfig, DatabaseType, TableConfig};
use schemars::schema_for_value;

fn main() {
    let database = DatabaseConfig {
        database_type: DatabaseType::MsSQL,
        server: "localhost".to_string(),
        database: "test_database".to_string(),
        user: "testuser".to_string(),
        password: "passw0rd!".to_string(),
    };

    let columns = vec![];

    let tables = vec![TableConfig {
        columns,
        name: "some_table".to_string(),
        where_clause: Some("where 1=1".to_string()),
    }];

    let config = Config { database, tables };

    let schema = schema_for_value!(config);

    let file = File::create("schema.json").expect("Unable to create file");
    let mut file = BufWriter::new(file);
    file.write_all(serde_json::to_string_pretty(&schema).unwrap().as_bytes())
        .expect("Unable to write data");
    file.flush().unwrap();
}
