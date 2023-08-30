//! general purpose utilities

use polars::export::chrono;

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
