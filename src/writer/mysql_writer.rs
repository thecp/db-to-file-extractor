use futures::TryStreamExt;
use sqlx::mysql::{MySqlPoolOptions, MySqlRow};
use sqlx::Row;
use sqlx::{MySql, Pool};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use crate::config::{Config, DataType, DataTypeInput};
use crate::DatabaseWriter;
use crate::sql::sql_to_string;

pub struct MySqlWriter<'a> {
    config: &'a Config,
    pools: Pool<MySql>,
    dir: PathBuf,
}

impl<'a> DatabaseWriter for MySqlWriter<'a> {
    fn database_to_sql(&self) -> anyhow::Result<()> {
        tokio::runtime::Runtime::new()?
            .block_on(self.database_to_sql_())
            .map_err(anyhow::Error::from)
    }

    fn database_to_json(&self) -> anyhow::Result<()> {
        tokio::runtime::Runtime::new()?
            .block_on(self.database_to_json_())
            .map_err(anyhow::Error::from)
    }
}

impl<'a> MySqlWriter<'a> {
    pub fn new(config: &'a Config, dir: PathBuf) -> anyhow::Result<MySqlWriter<'_>> {
        let pools = tokio::runtime::Runtime::new()?.block_on(get_connection_pool(config))?;
        Ok(MySqlWriter { config, dir, pools })
    }

    async fn database_to_sql_(&self) -> Result<(), sqlx::Error> {
        for table in &self.config.tables {
            let table_str = table
                .columns
                .iter()
                .map(|col| col.name.clone())
                .collect::<Vec<String>>()
                .join(",");
            let sql = format!(
                "select {} from {} where {}",
                table_str,
                table.name,
                table.get_where_clause()
            );
            let mut rows = sqlx::query(&sql).fetch(&self.pools);

            let file = File::create(self.dir.join(format!("{}.sql", table.name)))
                .expect("Unable to create file");
            let mut file = BufWriter::new(file);
            file.write(format!("INSERT INTO {} ({}) VALUES ", &table.name, table_str).as_bytes())
                .expect("Unable to write data");

            let mut initial = true;

            while let Some(row) = rows.try_next().await? {
                if !initial {
                    file.write(",".as_bytes()).expect("Unable to write data");
                } else {
                    initial = false;
                }

                let mut values = Vec::new();
                for column in &table.columns {
                    values.push(mysql_value(&column.data_type, &row, &*column.name)?);
                }
                file.write(sql_to_string(&values).unwrap().as_bytes())
                    .expect("Unable to write data");
            }

            file.write(";".as_bytes()).expect("Unable to write data");
            file.flush()?;
        }

        Ok(())
    }

    async fn database_to_json_(&self) -> Result<(), sqlx::Error> {
        for table in &self.config.tables {
            let table_str = table
                .columns
                .iter()
                .map(|col| col.name.clone())
                .collect::<Vec<String>>()
                .join(",");
            let sql = format!(
                "select {} from {} where {}",
                table_str,
                table.name,
                table.get_where_clause()
            );
            let mut rows = sqlx::query(&sql).fetch(&self.pools);

            let file = File::create(self.dir.join(format!("/tmp/{}.json", table.name)))
                .expect("Unable to create file");
            let mut file = BufWriter::new(file);
            file.write("[".as_bytes()).expect("Unable to write data");

            let mut initial = true;

            while let Some(row) = rows.try_next().await? {
                if !initial {
                    file.write(",".as_bytes()).expect("Unable to write data");
                } else {
                    initial = false;
                }

                let mut key_value_map = HashMap::new();
                for column in &table.columns {
                    key_value_map.insert(
                        &*column.name,
                        mysql_value(&column.data_type, &row, &*column.name)?,
                    );
                }
                file.write(serde_json::to_string(&key_value_map).unwrap().as_bytes())
                    .expect("Unable to write data");
            }

            file.write("]".as_bytes()).expect("Unable to write data");
            file.flush()?;
        }

        Ok(())
    }
}

fn mysql_value(
    data_type: &DataTypeInput,
    row: &MySqlRow,
    column_name: &str,
) -> Result<DataType, sqlx::Error> {
    match data_type {
        DataTypeInput::String => Ok(DataType::String(row.try_get(&column_name)?)),
        DataTypeInput::Int => Ok(DataType::Int(row.try_get(&column_name)?)),
        DataTypeInput::BigInt => Ok(DataType::BigInt(row.try_get(&column_name)?)),
        DataTypeInput::Float => Ok(DataType::Float(row.try_get(&column_name)?)),
        DataTypeInput::Double => Ok(DataType::Double(row.try_get(&column_name)?)),
        DataTypeInput::Decimal => unimplemented!(),
        DataTypeInput::Bool => Ok(DataType::Bool(row.try_get(&column_name)?)),
        DataTypeInput::Uuid => Ok(DataType::Uuid(row.try_get(&column_name)?)),
        DataTypeInput::DateTimeUtc => unimplemented!(),
        DataTypeInput::DateTime => unimplemented!(),
        DataTypeInput::Date => unimplemented!(),
        DataTypeInput::Time => unimplemented!(),
    }
}

async fn get_connection_pool(config: &Config) -> Result<Pool<MySql>, sqlx::Error> {
    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect(&config.connection_string())
        .await?;

    Ok(pool)
}
