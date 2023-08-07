//! general purpose utilities

use std::path::{Path, PathBuf};

use futures::future;
use polars::export::chrono;
use tokio::task::{JoinError, JoinHandle};

/// Wrapper over `future::try_join_all` that adds better logging for failed task
pub async fn wait_all_tasks<E: std::error::Error>(
    info: &str,
    tasks: &mut Vec<JoinHandle<Result<(), E>>>,
) -> Result<Result<(), E>, JoinError> {
    println!("Waiting for {} {} to finish", tasks.len(), info);
    for res in future::try_join_all(std::mem::take(tasks)).await? {
        if let Err(e) = res.as_ref() {
            println!("{} phase failed due to {}", info, e);
            return Ok(res)
        }
    }
    println!("All {} finished", info);
    Ok(Ok(()))
}

/// Lists file path with `extension` in specified directory `dir`
pub async fn list_files(dir: &Path, extension: &str) -> tokio::io::Result<Vec<PathBuf>> {
    let mut paths = vec![];
    if let Ok(mut read_dir) = tokio::fs::read_dir(dir).await {
        while let Some(entry) = read_dir.next_entry().await? {
            if entry.path().extension().unwrap_or_default() == extension {
                paths.push(entry.path());
            }
        }
    }
    Ok(paths)
}

/// Parse string date specification to `NaiveDate` (e.g. "2021-01-01", "today", "yesterday")
pub fn parse_date(date: &str) -> chrono::ParseResult<chrono::NaiveDate> {
    if date == "today" {
        return Ok(chrono::Utc::now().naive_utc().date())
    }
    if date == "yesterday" {
        return Ok(chrono::Utc::now().naive_utc().date().pred_opt().unwrap())
    }
    chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
}
