pub mod config;
pub mod writer;
pub mod sql;

use anyhow::bail;
use config::Config;
use writer::DatabaseWriter;
use writer::mssql_writer::MssqlWriter;
use writer::mysql_writer::MySqlWriter;

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
            let mssql_writer = MssqlWriter::new(&config, opt.output);
            match opt._type {
                OutputType::JSON => mssql_writer.database_to_json(),
                OutputType::SQL => mssql_writer.database_to_sql()
            }
        },
        config::DatabaseType::MySQL => {
            let mysql_writer = MySqlWriter::new(&config, opt.output)?;
            match opt._type {
                OutputType::JSON => mysql_writer.database_to_json(),
                OutputType::SQL => mysql_writer.database_to_sql()
            }
        },
    }
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
    SQL,
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