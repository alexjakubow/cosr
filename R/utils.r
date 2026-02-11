# utils.r -----------------------------------------------------------------
#
# INFRASTRUCTURE & UTILITY FUNCTIONS
#
# This file contains foundational utilities for data access, lazy evaluation
# control, and common filter expressions. These are used throughout the
# package but rarely called directly by end users.
#
# Key functions:
#   - open_parquet() - Open OSF Parquet datasets
#   - collector() - Control lazy vs. eager evaluation
#   - set_timespan() - Generate date sequences
#   - expr_valid_regs - Filter expression for valid registrations
# -----------------------------------------------------------------------------

#' @importFrom dplyr across starts_with desc everything select left_join mutate filter arrange summarise group_by
#' @importFrom rlang sym exprs
#' @importFrom arrow open_dataset
#' @importFrom dbplyr sql
NULL


#' Open an OSF Parquet dataset
#'
#' Opens a Parquet file from the OSF database directory and optionally
#' converts it to a DuckDB connection for efficient querying.
#'
#' The Parquet directory path is read from the `PARQUET_DIR` environment
#' variable, which should be set in your `.Renviron` file.
#'
#' @param dir Path to directory containing Parquet files. Defaults to
#'   `Sys.getenv("PARQUET_DIR")`, which can be set in your `.Renviron` file.
#' @param tbl Name of Parquet file (i.e., OSF database table name without
#'   the .parquet extension)
#' @param duck Logical. Should the dataset be converted to a DuckDB dataset
#'   using `arrow::to_duckdb()`? Defaults to `TRUE` for better query performance.
#'
#' @return An Arrow dataset or DuckDB connection
#'
#' @examples
#' \dontrun{
#' # Open OSF abstractnode table
#' nodes <- open_parquet(tbl = "osf_abstractnode")
#'
#' # Open as Arrow dataset (not DuckDB)
#' nodes_arrow <- open_parquet(tbl = "osf_abstractnode", duck = FALSE)
#' }
#'
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
#' Controls whether to return a lazy query (for memory efficiency) or
#' collect results into memory (for immediate use). This is used internally
#' by most query functions to support the `lazy` parameter.
#'
#' @param x A dataset (e.g., Arrow or DuckDB query)
#' @param .lazy Logical. Should the dataset be returned in its lazy form
#'   (i.e., without collecting into memory)? Defaults to `TRUE`.
#'
#' @return Either the lazy query (if `.lazy = TRUE`) or a collected tibble
#'   (if `.lazy = FALSE`)
#'
#' @examples
#' \dontrun{
#' # Return lazy query
#' lazy_result <- collector(my_query, .lazy = TRUE)
#'
#' # Collect into memory
#' df <- collector(my_query, .lazy = FALSE)
#' }
#'
#' @export
collector <- function(x, .lazy = TRUE) {
  if (.lazy) {
    return(x)
  } else {
    return(dplyr::collect(x))
  }
}


#' Generate sequence of monthly cutoff dates for time series analysis
#'
#' Creates a sequence of dates at monthly intervals, used by
#' [event_history_monthly()] to generate time series snapshots.
#'
#' @param start Start date (default: "2023-01-01"). Can be a string or Date object.
#' @param end End date (default: `Sys.Date()`). Can be a string or Date object.
#' @param delta Interval between dates (default: "month"). Passed to `seq.POSIXct()`.
#'
#' @return A POSIXct vector of dates
#'
#' @examples
#' \dontrun{
#' # Monthly dates for 2024
#' months_2024 <- set_timespan(
#'   start = "2024-01-01",
#'   end = "2024-12-31"
#' )
#'
#' # Daily dates
#' days <- set_timespan(
#'   start = "2024-01-01",
#'   end = "2024-01-31",
#'   delta = "day"
#' )
#' }
#'
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


#' Common filter expression for valid registrations
#'
#' Defines "valid" Open Science Registrations (OSRs) as those that are:
#'   - Public (is_public == TRUE)
#'   - Not deprecated (is.na(deleted))
#'   - Have a registration date (!is.na(registered_date))
#'   - Accepted by moderation (moderation_state == "accepted")
#'   - Not spam (spam_status != 2 or is.na(spam_status))
#'
#' This expression can be used with the `sample` parameter in most query
#' functions to filter the base set of registrations.
#'
#' @format An rlang expression list that can be spliced into dplyr::filter()
#'
#' @examples
#' \dontrun{
#' # Use in a query
#' valid_regs <- open_parquet(tbl = "osf_abstractnode") |>
#'   dplyr::filter(type == "osf.registration", !!!expr_valid_regs)
#'
#' # Pass to a function
#' assign_attributes(sample = expr_valid_regs)
#' }
#'
#' @export
expr_valid_regs <- rlang::exprs(
  is_public == TRUE, # public
  is.na(deleted), # non-deprecated
  !is.na(registered_date), # has registration date
  moderation_state == "accepted", # approved by moderation
  (spam_status != 2 | is.na(spam_status)) # authentic (not spam)
)
