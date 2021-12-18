pub mod config;
pub mod data_types;
pub mod writer;
pub mod sql;

use anyhow::bail;
use config::{Config, DatabaseType};
use writer::DatabaseWriter;
use writer::mssql_writer::MssqlWriter;
use writer::mysql_writer::MySqlWriter;
use tiberius::AuthMethod;

use std::fs::read_to_string;
use std::path::PathBuf;
use std::str::FromStr;
use structopt::StructOpt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    let contents =
        read_to_string("config.json").expect("Something went wrong reading the config file");

    let config: Config = serde_json::from_str(&contents).unwrap();

    match config.database.database_type {
        config::DatabaseType::MsSQL => {
            let mssql_writer = MssqlWriter::new(&config, opt.output).await?;
            match opt._type {
                OutputType::Json => mssql_writer.database_to_json().await?,
                OutputType::Sql => mssql_writer.database_to_sql().await?
            }
        },
        config::DatabaseType::MySQL => {
            let mysql_writer = MySqlWriter::new(&config, opt.output).await?;
            match opt._type {
                OutputType::Json => mysql_writer.database_to_json().await?,
                OutputType::Sql => mysql_writer.database_to_sql().await?
            }
        },
    }

    Ok(())
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
    Json,
    Sql,
}

impl FromStr for OutputType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq("json") {
            Ok(Self::Json)
        } else if s.eq("sql") {
            Ok(Self::Sql)
        } else {
            bail!("output type can only be json or sql")
        }
    }
}

// TODO move this somwhere else
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