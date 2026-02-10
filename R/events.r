#' Generate monthly event history dataset for a specified sample and date range
#'
#' This function generates a monthly time series dataset of event counts (e.g., OSR, LOS) for a specified sample and date range. It applies a user-defined function to compute event counts at each monthly cutoff date, combines the results into a single data frame, and saves it as a Parquet file.
#'
#' @param .start The start date for the time series (default: "2023-01-01").
#' @param .end The end date for the time series (default: `Sys.Date()`)
#' @param .sample The sample of registrations to analyze (e.g., `cosr::expr_valid_regs`).
#' @param .fn The function to apply for computing event counts at each cutoff date (default: `cosr::resource_counts_at_date`).
#' @param .fileout The file path for the output Parquet file (e.g., "data/event_history_monthly.parquet").
#'
#' @return A Parquet file containing the monthly event history dataset
#' @export
event_history_monthly <- function(
  .start = "2023-01-01",
  .end = Sys.Date(),
  .sample = cosr::expr_valid_regs,
  .fn = cosr::resource_counts_at_date,
  .fileout
) {
  # Generate monthly time series of event counts for the specified sample and date range
  MONTHS <- cosr::set_timespan(start = .start, end = .end, delta = "month")
  ts_results <- purrr::map(
    .x = MONTHS,
    .f = ~ .fn(
      sample = .sample,
      cutoff = .x,
      lazy = FALSE
    ),
    .progress = TRUE
  )
  names(ts_results) <- MONTHS

  # Combine results into a single data frame and save as Parquet file
  ts_results |>
    dplyr::bind_rows(.id = "date") |>
    dplyr::arrange(date, node_id) |>
    dplyr::group_by(node_id) |>
    dplyr::mutate(
      los_lag = dplyr::lag(is_los),
      los_change = dplyr::case_when(
        is_los == 1 & (is.na(los_lag) | los_lag == 0) ~ 1,
        is_los == 0 & los_lag == 1 ~ -1,
        TRUE ~ 0
      )
    ) |>
    arrow::write_parquet(.fileout)
}
