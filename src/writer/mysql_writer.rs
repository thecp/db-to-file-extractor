use futures::TryStreamExt;
use regex::Regex;
use sqlx::mysql::{MySqlPoolOptions, MySqlRow};
use sqlx::Row;
use sqlx::{MySql, Pool};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use crate::config::Config;
use crate::data_types::DataType;
use crate::sql::sql_to_string;
use crate::DatabaseWriter;

pub struct MySqlWriter<'a> {
    config: &'a Config,
    pools: Pool<MySql>,
    dir: PathBuf,
}

use async_trait::async_trait;

#[async_trait]
impl<'a> DatabaseWriter for MySqlWriter<'a> {
    async fn database_to_sql(&self) -> anyhow::Result<()> {
        self.database_to_sql_().await.map_err(anyhow::Error::from)
    }

    async fn database_to_json(&self) -> anyhow::Result<()> {
        self.database_to_json_().await.map_err(anyhow::Error::from)
    }
}

impl<'a> MySqlWriter<'a> {
    pub async fn new(config: &'a Config, dir: PathBuf) -> anyhow::Result<MySqlWriter<'a>> {
        let pools = get_connection_pool(config).await?;
        Ok(MySqlWriter { config, dir, pools })
    }

    async fn database_to_sql_(&self) -> Result<(), sqlx::Error> {
        for table in &self.config.tables {
            let schema = self.get_schema_for_table(&table.name).await?;

            let table_str = table.columns.join(",");
            let sql = format!(
                "select {} from {} where {}",
                table_str,
                table.name,
                table.where_clause.as_ref().unwrap_or(&"1=1".to_string())
            );
            let mut rows = sqlx::query(&sql).fetch(&self.pools);

            let file = File::create(self.dir.join(format!("{}.sql", table.name)))
                .expect("Unable to create file");
            let mut file = BufWriter::new(file);
            file.write_all(
                format!("INSERT INTO {} ({}) VALUES ", &table.name, table_str).as_bytes(),
            )
            .expect("Unable to write data");

            let mut initial = true;

            while let Some(row) = rows.try_next().await? {
                if !initial {
                    file.write_all(",".as_bytes()).expect("Unable to write data");
                } else {
                    initial = false;
                }

                let mut values = Vec::new();
                for column in &table.columns {
                    values.push(mysql_value(schema.get(column).unwrap(), &row, column)?);
                }
                file.write_all(sql_to_string(&values).unwrap().as_bytes())
                    .expect("Unable to write data");
            }

            file.write_all(";".as_bytes()).expect("Unable to write data");
            file.flush()?;
        }

        Ok(())
    }

    async fn database_to_json_(&self) -> Result<(), sqlx::Error> {
        for table in &self.config.tables {
            let schema = self.get_schema_for_table(&table.name).await?;

            let table_str = table.columns.join(",");
            let sql = format!(
                "select {} from {} where {}",
                table_str,
                table.name,
                table.where_clause.as_ref().unwrap_or(&"1=1".to_string())
            );
            let mut rows = sqlx::query(&sql).fetch(&self.pools);

            let file = File::create(self.dir.join(format!("/tmp/{}.json", table.name)))
                .expect("Unable to create file");
            let mut file = BufWriter::new(file);
            file.write_all("[".as_bytes()).expect("Unable to write data");

            let mut initial = true;

            while let Some(row) = rows.try_next().await? {
                if !initial {
                    file.write_all(",".as_bytes()).expect("Unable to write data");
                } else {
                    initial = false;
                }

                let mut key_value_map = HashMap::new();
                for column in &table.columns {
                    key_value_map.insert(
                        column,
                        mysql_value(schema.get(column).unwrap(), &row, column)?,
                    );
                }
                file.write_all(serde_json::to_string(&key_value_map).unwrap().as_bytes())
                    .expect("Unable to write data");
            }

            file.write_all("]".as_bytes()).expect("Unable to write data");
            file.flush()?;
        }

        Ok(())
    }

    async fn get_schema_for_table<'b>(
        &self,
        table_name: &'b str,
    ) -> Result<HashMap<String, String>, sqlx::Error> {
        let sql = format!("DESCRIBE {}", table_name);
        let mut rows = sqlx::query(&sql).fetch(&self.pools);

        let mut schema = HashMap::new();
        while let Some(row) = rows.try_next().await? {
            let column_name: &str = row.try_get("Field")?;
            let data_type: &str = row.try_get("Type")?;
            schema.insert(column_name.to_string(), data_type.to_string());
        }

        Ok(schema)
    }
}

fn mysql_value(
    data_type: &str,
    row: &MySqlRow,
    column_name: &str,
) -> Result<DataType, sqlx::Error> {
    let (t, _) = data_type_regex(data_type);
    let t = t.as_str();
    match t {
        "varchar" | "char" => Ok(DataType::String(row.try_get(&column_name)?)),
        "int" => Ok(DataType::Int(row.try_get(&column_name)?)),
        "bigint" => Ok(DataType::BigInt(row.try_get(&column_name)?)),
        "float" => Ok(DataType::Float(row.try_get(&column_name)?)),
        "double" => Ok(DataType::Double(row.try_get(&column_name)?)),
        "boolean" => Ok(DataType::Bool(row.try_get(&column_name)?)),
        "datetime" => unimplemented!(),
        "date" => unimplemented!(),
        "time" => unimplemented!(),
        _ => unimplemented!(),
    }
}

async fn get_connection_pool(config: &Config) -> Result<Pool<MySql>, sqlx::Error> {
    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect(&config.connection_string())
        .await?;

    Ok(pool)
}

fn data_type_regex(data_type: &str) -> (String, Option<i32>) {
    let reg = Regex::new(r"([a-z]+)(?:\((\d)*\))*").unwrap();
    let matches = reg.captures(data_type).unwrap();
    let t = &matches[1];

    (
        t.to_owned(),
        matches
            .get(2)
            .map(|elem| elem.as_str().parse::<i32>().unwrap()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_regex() {
        assert_eq!(("int".to_owned(), None), data_type_regex("int"));
        assert_eq!(
            ("varchar".to_owned(), Some(5)),
            data_type_regex("varchar(5)")
        );
    }
}
