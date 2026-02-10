#' Open an OSF Parquet dataset
#'
#' @param dir Path to directory containing Parquet files.  Defaults `Sys.getenv("PARQUET_DIR")`, which can be set in your `.Renviron` file.
#' @param tbl Name of Parquet file (i.e., OSF database table)
#' @param duck Logical. Should the dataset be converted to a DuckDB dataset using `duckdb::as_duckdb()`?  Defaults to `TRUE`.
#' @export
open_parquet <- function(dir = Sys.getenv("PARQUET_DIR"), tbl, duck = TRUE) {
  if (duck) {
    arrow::open_dataset(file.path(dir, paste0(tbl, ".parquet"))) |>
      arrow::to_duckdb()
  } else {
    arrow::open_dataset(file.path(dir, paste0(tbl, ".parquet")))
  }
}


#' Collect or return lazy dataset
#'
#' @param x A dataset (e.g., Arrow or DuckDB)
#' @param .lazy Logical. Should the dataset be returned in its lazy form (i.e., without collecting into memory)?  Defaults to `TRUE`.
#' @export
collector <- function(x, .lazy = TRUE) {
  if (.lazy) {
    return(x)
  } else {
    return(dplyr::collect(x))
  }
}


#' Generate sequence of monthly cutoff dates for time series analysis
#' @export
set_timespan <- function(
  start = "2023-01-01",
  end = Sys.Date(),
  delta = "month"
) {
  timespan <- c(
    as.character(as.Date(start)),
    as.character(lubridate::floor_date(as.Date(end)))
  )

  seq(as.POSIXct(timespan[1]), as.POSIXct(timespan[2]), by = delta)
}
