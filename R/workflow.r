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
#' @export
collector <- function(x, .lazy = TRUE) {
  if (.lazy) {
    return(x)
  } else {
    return(dplyr::collect(x))
  }
}
