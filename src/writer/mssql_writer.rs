use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use tiberius::Client;
use tiberius::Row;
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::config::{Config, DataType, DataTypeInput};
use crate::DatabaseWriter;
use crate::sql::sql_to_string;

pub struct MssqlWriter<'a> {
    config: &'a Config,
    dir: PathBuf,
}

impl<'a> DatabaseWriter for MssqlWriter<'a> {
    fn database_to_sql(&self) -> anyhow::Result<()> {
        tokio::runtime::Runtime::new()?.block_on(self.database_to_sql_())
    }

    fn database_to_json(&self) -> anyhow::Result<()> {
        tokio::runtime::Runtime::new()?.block_on(self.database_to_json_())
    }
}

impl<'a> MssqlWriter<'a> {
    pub fn new(config: &'a Config, dir: PathBuf) -> Self {
        Self { config, dir }
    }

    async fn database_to_sql_(&self) -> anyhow::Result<()> {
        let db_config = self.config.mssql_config();

        let tcp = TcpStream::connect(db_config.get_addr()).await?;
        tcp.set_nodelay(true)?;

        let mut client = Client::connect(db_config, tcp.compat_write()).await?;

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

            let stream = client.query(sql, &[]).await?;

            let file = File::create(self.dir.join(format!("{}.sql", table.name)))
                .expect("Unable to create file");
            let mut file = BufWriter::new(file);
            file.write(format!("INSERT INTO {} ({}) VALUES ", &table.name, table_str).as_bytes())
                .expect("Unable to write data");

            let rows = stream.into_first_result().await?;
            let mut initial = true;

            for row in rows {
                if !initial {
                    file.write(",".as_bytes()).expect("Unable to write data");
                } else {
                    initial = false;
                }

                let mut values = Vec::new();
                for (idx, column) in table.columns.iter().enumerate() {
                    values.push(mssql_value(&column.data_type, &row, idx)?);
                }
                file.write(sql_to_string(&values).unwrap().as_bytes())
                    .expect("Unable to write data");
            }

            file.write(";".as_bytes()).expect("Unable to write data");
            file.flush()?;
        }

        Ok(())
    }

    async fn database_to_json_(&self) -> anyhow::Result<()> {
        let db_config = self.config.mssql_config();

        let tcp = TcpStream::connect(db_config.get_addr()).await?;
        tcp.set_nodelay(true)?;

        let mut client = Client::connect(db_config, tcp.compat_write()).await?;

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

            let stream = client.query(sql, &[]).await?;

            let file = File::create(self.dir.join(format!("{}.sql", table.name)))
                .expect("Unable to create file");
            let mut file = BufWriter::new(file);

            let rows = stream.into_first_result().await?;
            let rows: Vec<HashMap<&str, DataType>> = rows.into_iter().map(|row| {
                let mut key_value_map = HashMap::new();
                for (idx, column) in table.columns.iter().enumerate() {
                    key_value_map.insert(&*column.name, mssql_value(&column.data_type, &row, idx).unwrap());
                }
                key_value_map
            }).collect();

            file.write(serde_json::to_string_pretty(&rows).unwrap().as_bytes())?;
            file.flush()?;
        }

        Ok(())
    }
}

fn mssql_value(
    data_type: &DataTypeInput,
    row: &Row,
    column_idx: usize,
) -> anyhow::Result<DataType> {
    match data_type {
        DataTypeInput::String => {
            let t: Option<&str> = row.try_get(column_idx)?;
            Ok(DataType::String(t.map(|s| s.to_string())))
        }
        DataTypeInput::Int => Ok(DataType::Int(row.try_get(column_idx)?)),
        DataTypeInput::BigInt => Ok(DataType::BigInt(row.try_get(column_idx)?)),
        DataTypeInput::Float => Ok(DataType::Float(row.try_get(column_idx)?)),
        DataTypeInput::Double => Ok(DataType::Double(row.try_get(column_idx)?)),
        DataTypeInput::Decimal => Ok(DataType::Decimal(row.try_get(column_idx)?)),
        DataTypeInput::Bool => Ok(DataType::Bool(row.try_get(column_idx)?)),
        DataTypeInput::Uuid => Ok(DataType::Uuid(row.try_get(column_idx)?)),
        DataTypeInput::DateTimeUtc => Ok(DataType::DateTimeUtc(row.try_get(column_idx)?)),
        DataTypeInput::DateTime => Ok(DataType::DateTime(row.try_get(column_idx)?)),
        DataTypeInput::Date => Ok(DataType::Date(row.try_get(column_idx)?)),
        DataTypeInput::Time => Ok(DataType::Time(row.try_get(column_idx)?)),
    }
}
