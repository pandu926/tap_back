use anyhow::{Context, Result};
use scylla::{PreparedStatement, Session};
use scylla::transport::errors::QueryError;
use scylla::from_row::FromRow;
use scylla::frame::response::result::Row;
use scylla::cql_to_rust::FromRow as ScyllaFromRow;

/// Helper to fetch a single optional row and deserialize it into T
pub async fn fetch_optional<T, A>(
    session: &Session,
    stmt: &PreparedStatement,
    args: A,
) -> Result<Option<T>>
where
    T: ScyllaFromRow + Send + Sync,
    A: scylla::IntoTypedRows + Send,
{
    let result = session
        .execute_unpaged(stmt, args)
        .await
        .context("Failed to execute statement")?;

    let rows = result
        .into_rows_result()
        .context("Failed to convert query result into rows")?;

    if let Some(row) = rows.into_iter().next() {
        let value: T = row.context("Failed to parse row")?;
        Ok(Some(value))
    } else {
        Ok(None)
    }
}

/// Helper to fetch multiple rows and deserialize into Vec<T>
pub async fn fetch_all<T, A>(
    session: &Session,
    stmt: &PreparedStatement,
    args: A,
) -> Result<Vec<T>>
where
    T: ScyllaFromRow + Send + Sync,
    A: scylla::IntoTypedRows + Send,
{
    let result = session
        .execute_unpaged(stmt, args)
        .await
        .context("Failed to execute statement")?;

    let rows = result
        .into_rows_result()
        .context("Failed to convert query result into rows")?;

    let data = rows
        .rows::<T>()
        .context("Failed to parse rows")?
        .map(|row| row.context("Failed to parse row"))
        .collect::<Result<Vec<_>>>()?;

    Ok(data)
}
