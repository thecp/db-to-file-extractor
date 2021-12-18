pub mod mssql_writer;
pub mod mysql_writer;

use async_trait::async_trait;

#[async_trait]
pub trait DatabaseWriter {
    // TODO consider using async fn traits once stable
    async fn database_to_sql(&self) -> anyhow::Result<()>;
    async fn database_to_json(&self) -> anyhow::Result<()>;
}
