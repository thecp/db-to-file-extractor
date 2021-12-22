use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use tiberius::Client;
use tiberius::Row;
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::config::Config;
use crate::data_types::DataType;
use crate::sql::sql_to_string;
use crate::DatabaseWriter;

pub struct MssqlWriter<'a> {
    config: &'a Config,
    dir: PathBuf,
}

use async_trait::async_trait;

#[async_trait]
impl<'a> DatabaseWriter for MssqlWriter<'a> {
    async fn database_to_sql(&self) -> anyhow::Result<()> {
        self.database_to_sql_().await
    }

    async fn database_to_json(&self) -> anyhow::Result<()> {
        self.database_to_json_().await
    }
}

impl<'a> MssqlWriter<'a> {
    pub async fn new(config: &'a Config, dir: PathBuf) -> anyhow::Result<MssqlWriter<'a>> {
        Ok(MssqlWriter { config, dir })
    }

    async fn database_to_sql_(&self) -> anyhow::Result<()> {
        let mut client = self.new_client().await?;

        for table in &self.config.tables {
            let schema = self.get_schema_for_table(&table.name).await?;

            let table_str = table.columns.join(",");
            let sql = format!(
                "select {} from {}.dbo.{} where {}",
                table_str,
                self.config.database.database,
                table.name,
                table.where_clause.as_ref().unwrap_or(&"1=1".to_string())
            );

            let stream = client.query(sql, &[]).await?;

            let file = File::create(self.dir.join(format!("{}.sql", table.name)))
                .expect("Unable to create file");
            let mut file = BufWriter::new(file);
            file.write_all(
                format!("INSERT INTO {} ({}) VALUES ", &table.name, table_str).as_bytes(),
            )
            .expect("Unable to write data");

            let rows = stream.into_first_result().await?;
            let mut initial = true;

            for row in rows {
                if !initial {
                    file.write_all(",".as_bytes())
                        .expect("Unable to write data");
                } else {
                    initial = false;
                }

                let mut values = Vec::new();
                for (idx, column) in table.columns.iter().enumerate() {
                    values.push(mssql_value(schema.get(column).unwrap(), &row, idx)?);
                }
                file.write_all(sql_to_string(&values).unwrap().as_bytes())
                    .expect("Unable to write data");
            }

            file.write_all(";".as_bytes())
                .expect("Unable to write data");
            file.flush()?;
        }

        Ok(())
    }

    async fn database_to_json_(&self) -> anyhow::Result<()> {
        let mut client = self.new_client().await?;

        for table in &self.config.tables {
            let schema = self.get_schema_for_table(&table.name).await?;

            let table_str = table.columns.join(",");
            let sql = format!(
                "select {} from {}.dbo.{} where {}",
                table_str,
                self.config.database.database,
                table.name,
                table.where_clause.as_ref().unwrap_or(&"1=1".to_string())
            );

            let stream = client.query(sql, &[]).await?;

            let file = File::create(self.dir.join(format!("{}.json", table.name)))
                .expect("Unable to create file");
            let mut file = BufWriter::new(file);

            let rows = stream.into_first_result().await?;
            let rows: Vec<HashMap<&str, DataType>> = rows
                .into_iter()
                .map(|row| {
                    let mut key_value_map = HashMap::new();
                    for (idx, column) in table.columns.iter().enumerate() {
                        key_value_map.insert(
                            (*column).as_str(),
                            mssql_value(schema.get(column).unwrap(), &row, idx).unwrap(),
                        );
                    }
                    key_value_map
                })
                .collect();

            file.write_all(serde_json::to_string_pretty(&rows).unwrap().as_bytes())?;
            file.flush()?;
        }

        Ok(())
    }

    async fn new_client(&self) -> anyhow::Result<Client<Compat<TcpStream>>> {
        let db_config = self.config.mssql_config();

        let tcp = TcpStream::connect(db_config.get_addr()).await?;
        tcp.set_nodelay(true)?;

        Ok(Client::connect(db_config, tcp.compat_write()).await?)
    }

    async fn get_schema_for_table<'b>(
        &self,
        table_name: &'b str,
    ) -> anyhow::Result<HashMap<String, String>> {
        let sql = format!(
            "SELECT COLUMN_NAME, DATA_TYPE FROM {}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=@P1",
            self.config.database.database
        );
        let mut client = self.new_client().await?;

        let stream = client.query(sql, &[&table_name.to_string()]).await?;

        let mut schema = HashMap::new();
        let rows = stream.into_first_result().await?;
        for row in rows {
            let column_name: &str = row.try_get(0)?.unwrap();
            let data_type: &str = row.try_get(1)?.unwrap();
            schema.insert(column_name.to_string(), data_type.to_string());
        }

        Ok(schema)
    }
}

fn mssql_value(data_type: &str, row: &Row, column_idx: usize) -> anyhow::Result<DataType> {
    match data_type {
        "varchar" | "nvarchar" => {
            let t: Option<&str> = row.try_get(column_idx)?;
            Ok(DataType::String(t.map(|s| s.to_string())))
        }
        "int" => Ok(DataType::Int(row.try_get(column_idx)?)),
        "bigint" => Ok(DataType::BigInt(row.try_get(column_idx)?)),
        "float" => Ok(DataType::Float(row.try_get(column_idx)?)),
        "decimal" => Ok(DataType::Decimal(row.try_get(column_idx)?)),
        "smallint" => Ok(DataType::Bool(row.try_get(column_idx)?)),
        "uniqueidentifier" => Ok(DataType::Uuid(row.try_get(column_idx)?)),
        "datetime" => Ok(DataType::DateTime(row.try_get(column_idx)?)),
        "datetimeoffset" => Ok(DataType::DateTime(row.try_get(column_idx)?)),
        "date" => Ok(DataType::Date(row.try_get(column_idx)?)),
        "time" => Ok(DataType::Time(row.try_get(column_idx)?)),
        _ => unimplemented!(),
    }
}
