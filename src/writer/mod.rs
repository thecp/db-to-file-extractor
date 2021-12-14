pub mod mssql_writer;
pub mod mysql_writer;

pub trait DatabaseWriter {
    // TODO consider using async fn traits once stable
    fn database_to_sql(&self) -> anyhow::Result<()>;
    fn database_to_json(&self) -> anyhow::Result<()>;
}
