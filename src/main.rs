pub mod config;

use anyhow::bail;
use sqlx::AnyPool;
use sqlx::{Row};
use sqlx::any::AnyPoolOptions;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::str::FromStr;
use structopt::StructOpt;
use futures::TryStreamExt;
use config::{Config, DataType, DataTypeInput};

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let opt = Opt::from_args();

    let contents =
        fs::read_to_string("config.json").expect("Something went wrong reading the file");

    let config: Config = serde_json::from_str(&contents).unwrap();

    let pool = get_connection_pool(&config).await?;

    match opt._type {
        OutputType::JSON => database_to_json(pool, config).await?,
        OutputType::SQL => database_to_sql(pool, config).await?,
    }

    Ok(())
}

async fn database_to_sql(pool: AnyPool, config: Config) -> Result<(), sqlx::Error> {
    for table in &config.tables {
        let table_str = table.columns.iter().map(|col| col.name.clone()).collect::<Vec<String>>().join(",");
        let sql = format!("select {} from {} where {}", table_str, table.name, table.get_where_clause());
        let mut rows = sqlx::query(&sql)
            .fetch(&pool);

        let file = File::create(format!("/tmp/{}.sql", table.name)).expect("Unable to create file");
        let mut file = BufWriter::new(file);
        file.write(format!("INSERT INTO {} ({}) VALUES ", &table.name, table_str).as_bytes()).expect("Unable to write data");

        let mut initial = true;

        while let Some(row) = rows.try_next().await? {
            if !initial {
                file.write(",".as_bytes()).expect("Unable to write data");
            } else {
                initial = false;
            }

            let mut values = Vec::new();
            for column in &table.columns {
                match column.data_type {
                    DataTypeInput::String => {
                        let value: String = row.try_get(&*column.name)?;
                        values.push(DataType::String(value));
                    },
                    DataTypeInput::Integer => {
                        let value: i64 = row.try_get(&*column.name)?;
                        values.push(DataType::Integer(value));
                    }
                }
            }
            file.write(sql_to_string(&values).unwrap().as_bytes()).expect("Unable to write data");
        }

        file.write(";".as_bytes()).expect("Unable to write data");
        file.flush()?;
    }

    Ok(())
}

async fn database_to_json(pool: AnyPool, config: Config) -> Result<(), sqlx::Error> {
    for table in &config.tables {
        let mut sql = table.columns.iter().map(|col| col.name.clone()).collect::<Vec<String>>().join(",");
        sql = format!("select {} from {} where {}", sql, table.name, table.get_where_clause());
        let mut rows = sqlx::query(&sql)
            .fetch(&pool);

        let file = File::create(format!("/tmp/{}.json", table.name)).expect("Unable to create file");
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
                match column.data_type {
                    DataTypeInput::String => {
                        let value: String = row.try_get(&*column.name)?;
                        key_value_map.insert(&*column.name, DataType::String(value));
                    },
                    DataTypeInput::Integer => {
                        let value: i64 = row.try_get(&*column.name)?;
                        key_value_map.insert(&*column.name, DataType::Integer(value));
                    }
                }
            }
            file.write(serde_json::to_string(&key_value_map).unwrap().as_bytes()).expect("Unable to write data");
        }

        file.write("]".as_bytes()).expect("Unable to write data");
        file.flush()?;
    }

    Ok(())
}

async fn get_connection_pool(config: &Config) -> Result<AnyPool, sqlx::Error> {
    let pool = AnyPoolOptions::new()
        .max_connections(5)
        .connect(&config.connection_string())
        .await?;
    
    Ok(pool)
}

fn sql_to_string(data: &Vec<DataType>) -> anyhow::Result<String> {
    Ok(format!(
        "({})",
        data.iter().map(|e| serde_json::to_string(e).unwrap()).collect::<Vec<String>>().join(",")
    ))
}

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
struct Opt {
    /// Output directory
    #[structopt(parse(from_os_str), default_value = "/tmp", short, long)]
    output: PathBuf,

    /// Output type (json or sql)
    #[structopt(default_value = "json", long)]
    _type: OutputType,
}

#[derive(Debug)]
enum OutputType {
    JSON,
    SQL
}

impl FromStr for OutputType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq("json") {
            Ok(Self::JSON)
        } else if s.eq("sql") {
            Ok(Self::SQL)
        } else {
            bail!("output type can only be json or sql")
        }
    }
}
