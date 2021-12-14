use crate::config::DataType;

pub fn sql_to_string(data: &Vec<DataType>) -> anyhow::Result<String> {
    Ok(format!(
        "({})",
        data.iter().map(|e| serde_json::to_string(e).unwrap()).collect::<Vec<String>>().join(",")
    ))
}
