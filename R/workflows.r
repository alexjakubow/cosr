# workflows.r -------------------------------------------------------------
#
# HIGH-LEVEL WORKFLOW FUNCTIONS
#
# This file contains complete pipeline functions that most users will call.
# These functions orchestrate multiple steps to produce analytical datasets.
#
# Typical usage:
#   1. assign_attributes() - Generate current attribute memberships
#   2. event_history_monthly() - Generate monthly LOS time series
#   3. los_summary_by_attributes_all() - Summarize by all subgroups
#
# For more control, see queries.r, analysis.r, and compute.r
# -----------------------------------------------------------------------------

#' Assign current attributes (subgroup status) for all OSRs
#'
#' This is typically the FIRST STEP in a workflow. It captures current
#' attributes/subgroup memberships for all Open Science Registrations (OSRs)
#' and caches the results as Parquet files in the `data/` directory.
#'
#' The function processes all 8 supported attributes in parallel and saves
#' them for use in downstream analyses.
#'
#' @param sample A dplyr filter expression to apply to the base table of OSRs.
#'   Defaults to `cosr::expr_valid_regs`, which captures all "valid" registrations.
#'
#' @return Invisible NULL. Results are written to Parquet files in data/:
#'   - data/osr_affiliated.parquet
#'   - data/osr_funder.parquet
#'   - data/osr_funded.parquet
#'   - data/osr_institution.parquet
#'   - data/osr_template.parquet
#'   - data/osr_provider.parquet
#'   - data/osr_subject.parquet
#'   - data/osr_subject_parent.parquet
#'
#' @examples
#' \dontrun{
#' # Generate attributes for all valid registrations
#' assign_attributes()
#'
#' # Generate attributes for a custom sample
#' assign_attributes(sample = rlang::exprs(created >= "2024-01-01"))
#' }
#'
#' @seealso [SUPPORTED_ATTRIBUTES], [get_registration_funder()], [get_registration_institution()]
#' @export
assign_attributes <- function(sample = cosr::expr_valid_regs) {
  PARAMS <- tibble::tribble(
    ~fn                                   , ~outfile                          ,
    cosr::get_registration_affiliated     , "data/osr_affiliated.parquet"     ,
    cosr::get_registration_funder         , "data/osr_funder.parquet"         ,
    cosr::get_registration_funded         , "data/osr_funded.parquet"         ,
    cosr::get_registration_institution    , "data/osr_institution.parquet"    ,
    cosr::get_registration_template       , "data/osr_template.parquet"       ,
    cosr::get_registration_provider       , "data/osr_provider.parquet"       ,
    cosr::get_registration_subject        , "data/osr_subject.parquet"        ,
    cosr::get_registration_subject_parent , "data/osr_subject_parent.parquet"
  )

  purrr::walk2(
    .x = PARAMS$fn,
    .y = PARAMS$outfile,
    .f = ~ {
      .x(sample = sample) |>
        arrow::to_arrow() |>
        arrow::write_parquet(.y)
    },
    .progress = TRUE
  )

  invisible(NULL)
}


#' Generate monthly event history dataset for a specified sample and date range
#'
#' This is typically the SECOND STEP in a workflow. It generates a monthly time
#' series dataset tracking LOS (Lifecycle Open Science) status for registrations
#' over time.
#'
#' The function computes resource counts at each monthly cutoff date and tracks
#' changes in LOS status (gains, losses, no change).
#'
#' @param .start The start date for the time series (default: "2023-01-01").
#' @param .end The end date for the time series (default: `Sys.Date()`)
#' @param .sample The sample of registrations to analyze (default: `cosr::expr_valid_regs`).
#' @param .fn The function to apply for computing event counts at each cutoff date
#'   (default: `cosr::resource_counts_at_date`).
#' @param .fileout The file path for the output Parquet file (e.g., "data/los_ts.parquet").
#'
#' @return Invisible NULL. A Parquet file is written containing monthly snapshots with:
#'   - node_id: Registration identifier
#'   - date: Month of snapshot
#'   - linked_outputs/outcomes: Count of linked resources
#'   - removed_outputs/outcomes: Count of removed resources
#'   - net_outputs/outcomes: Net resource counts
#'   - is_los: Binary LOS indicator
#'   - los_change: Change in LOS status (1 = gained, -1 = lost, 0 = no change)
#'
#' @examples
#' \dontrun{
#' # Generate monthly LOS time series for all valid registrations
#' event_history_monthly(
#'   .start = "2023-01-01",
#'   .end = Sys.Date(),
#'   .fileout = "data/los_ts.parquet"
#' )
#' }
#'
#' @seealso [resource_counts_at_date()], [set_timespan()]
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

  invisible(NULL)
}


#' Compute time series summaries by multiple subgroup variables (complete pipeline)
#'
#' This is typically the THIRD STEP in a workflow. It's a wrapper function that
#' computes summary statistics for all supported attributes and saves them as
#' Parquet files.
#'
#' This function requires that you've already run:
#'   1. assign_attributes() - to generate attribute files
#'   2. event_history_monthly() - to generate time series data
#'
#' @param subgroup_vars Character vector of attribute names to summarize
#'   (default: all supported attributes)
#' @param los_ts_path Path to the LOS time series Parquet file
#'
#' @return Invisible NULL. Results are written to Parquet files:
#'   - data/los_ts_<attribute>.parquet (one per attribute)
#'
#' @examples
#' \dontrun{
#' # After running assign_attributes() and event_history_monthly():
#' los_summary_by_attributes_all(
#'   subgroup_vars = SUPPORTED_ATTRIBUTES,
#'   los_ts_path = "data/los_ts.parquet"
#' )
#'
#' # Or just for specific attributes:
#' los_summary_by_attributes_all(
#'   subgroup_vars = c("funder", "institution"),
#'   los_ts_path = "data/los_ts.parquet"
#' )
#' }
#'
#' @seealso [los_summary_by_attribute()], [SUPPORTED_ATTRIBUTES]
#' @export
los_summary_by_attributes_all <- function(
  subgroup_vars = cosr::SUPPORTED_ATTRIBUTES,
  los_ts_path
) {
  # Checks
  if (!is.character(subgroup_vars) || length(subgroup_vars) < 1) {
    stop("subgroup_vars must be a character vector with at least one element")
  }
  if (!file.exists(los_ts_path)) {
    stop(glue::glue("{los_ts_path} does not exist"))
  }

  # Generate summaries for each subgroup variable
  purrr::walk(
    subgroup_vars,
    ~ {
      result <- cosr::los_summary_by_attribute(
        subgroup_var = as.character(.x),
        los_ts_path = los_ts_path
      )

      # Write to file
      outpath <- glue::glue("data/los_ts_{tolower(.x)}.parquet")
      arrow::write_parquet(result, outpath)
    },
    .progress = TRUE
  )

  invisible(NULL)
}
