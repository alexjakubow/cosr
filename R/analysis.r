# analysis.r --------------------------------------------------------------
#
# ANALYTICAL & SUMMARIZATION FUNCTIONS
#
# This file contains functions for computing summary statistics and
# aggregations on LOS time series data.
#
# These functions are typically used AFTER you've generated time series data
# with event_history_monthly() and attribute data with assign_attributes().
#
# Key functions:
#   - los_summarizer() - Generic summarizer with threshold filtering
#   - los_summary_by_attribute() - Summarize by a single attribute
#   - resource_counts_at_date() - Compute LOS status at a specific date
# -----------------------------------------------------------------------------

#' Generic summarizer for LOS time series data
#'
#' This is a flexible function for computing summary statistics (counts,
#' proportions) for subgroups in your time series data. It can group by
#' any combination of variables and apply an optional threshold filter.
#'
#' @param tbl The input time series dataset (e.g., `data/los_ts.parquet`)
#' @param lazy Whether to return a lazy query (default: TRUE) or collect
#'   results into a data frame (default: FALSE)
#' @param threshold A numeric value between 0 and 1 specifying the minimum
#'   proportion of total OSRs required for a subgroup to be included in the
#'   summary (default: 0, meaning no threshold filter is applied)
#' @param ... One or more unquoted expressions specifying the subgroup
#'   variables to summarize by (e.g., `institution`, `funded`, `provider`)
#'
#' @return An (un)lazy data frame containing summary statistics for each
#'   subgroup, including:
#'   - n_osr: Number of registrations in subgroup
#'   - prop_osr: Proportion of all registrations
#'   - have_resources/outputs/outcomes: Counts of registrations with resources
#'   - prop_resources/outputs/outcomes/los: Proportions
#'
#' @examples
#' \dontrun{
#' # Summarize by institution
#' ts_data <- arrow::open_dataset("data/los_ts.parquet")
#' los_summarizer(ts_data, institution, lazy = FALSE)
#'
#' # Summarize by multiple attributes with threshold
#' los_summarizer(ts_data, institution, date,
#'                threshold = 0.01, lazy = FALSE)
#' }
#'
#' @export
los_summarizer <- function(
  tbl,
  lazy = TRUE,
  threshold = 0,
  ...
) {
  # Checks
  if (!is.numeric(threshold) || threshold < 0 || threshold > 1) {
    stop("threshold must be a non-negative numeric value between 0 and 1")
  }

  # Summarize and compute proportions
  prefiltered_tbl <- tbl |>
    dplyr::summarise(
      .by = c(...),
      n_osr = dplyr::n(),
      have_resources = sum(net_resources > 0, na.rm = TRUE),
      have_outputs = sum(net_outputs > 0, na.rm = TRUE),
      have_outcomes = sum(net_outcomes > 0, na.rm = TRUE),
      are_los = sum(is_los, na.rm = TRUE)
    ) |>
    dplyr::mutate(
      across(
        starts_with(c("have_", "are_")),
        ~ ifelse(is.na(.x), 0, .x)
      ),
      prop_osr = n_osr / sum(n_osr, na.rm = TRUE),
      prop_resources = have_resources / n_osr,
      prop_outputs = have_outputs / n_osr,
      prop_outcomes = have_outcomes / n_osr,
      prop_los = are_los / n_osr
    )

  # Apply threshold filter
  prefiltered_tbl <- prefiltered_tbl |>
    dplyr::filter(prop_osr >= threshold)

  # Final arrangement and return/collect
  prefiltered_tbl |>
    dplyr::arrange(desc(prop_los)) |>
    dplyr::select(c(...), n_osr, prop_los, everything()) |>
    cosr::collector(lazy)
}


#' Compute time series summaries by a single attribute
#'
#' This function loads time series data, joins it with attribute data,
#' and computes summary statistics for each subgroup. It's used internally
#' by [los_summary_by_attributes_all()] but can also be called directly.
#'
#' Unlike [los_summarizer()], this function is designed for programmatic use
#' with string variable names. It groups data by the specified attribute and
#' date, computing counts and proportions for resources, outputs, outcomes,
#' and LOS status.
#'
#' @param subgroup_var The attribute to summarize by as a string
#'   (e.g., "institution", "funder", "funded")
#' @param los_ts_path The file path to the LOS time series dataset
#'   (e.g., "data/los_ts.parquet")
#'
#' @return An in-memory data frame containing summary statistics for each
#'   subgroup defined by the specified attribute variable, grouped by
#'   date and attribute value. Columns include:
#'   - n_osr: Number of registrations in subgroup
#'   - prop_osr: Proportion of all registrations
#'   - have_resources/outputs/outcomes: Counts of registrations with resources
#'   - prop_resources/outputs/outcomes/los: Proportions
#'
#' @examples
#' \dontrun{
#' # Summarize by institution over time
#' inst_summary <- los_summary_by_attribute(
#'   subgroup_var = "institution",
#'   los_ts_path = "data/los_ts.parquet"
#' )
#' }
#'
#' @seealso [los_summarizer()], [los_summary_by_attributes_all()]
#' @export
los_summary_by_attribute <- function(
  subgroup_var,
  los_ts_path = "data/los_ts.parquet"
) {
  # Load time series data
  ts_data <- arrow::open_dataset(los_ts_path)

  if (subgroup_var != "overall") {
    # Load subgroup data
    subgroup_path <- paste0("data/osr_", tolower(subgroup_var), ".parquet")
    subgroup_data <- arrow::open_dataset(subgroup_path)

    # Join with subgroup data
    joined_data <- dplyr::left_join(ts_data, subgroup_data, by = "node_id")

    # Summarize by the subgroup variable and date
    # Using all_of() to select columns by string name - no tidy eval needed!
    result <- joined_data |>
      dplyr::group_by(dplyr::across(dplyr::all_of(c(subgroup_var, "date"))))
  } else {
    # If overall summary, just group by date
    result <- ts_data |>
      dplyr::group_by(date)
  }

  result <- result |>
    dplyr::summarise(
      n_osr = dplyr::n(),
      have_resources = sum(net_resources > 0, na.rm = TRUE),
      have_outputs = sum(net_outputs > 0, na.rm = TRUE),
      have_outcomes = sum(net_outcomes > 0, na.rm = TRUE),
      are_los = sum(is_los, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::starts_with(c("have_", "are_")),
        ~ ifelse(is.na(.x), 0, .x)
      ),
      prop_osr = n_osr / sum(n_osr, na.rm = TRUE),
      prop_resources = have_resources / n_osr,
      prop_outputs = have_outputs / n_osr,
      prop_outcomes = have_outcomes / n_osr,
      prop_los = are_los / n_osr
    ) |>
    dplyr::arrange(dplyr::desc(prop_los)) |>
    dplyr::collect()

  return(result)
}


#' Count resources and compute LOS status at a specific cutoff date
#'
#' This function computes summary statistics for registrations at a specified
#' cutoff date, including counts of linked and removed resources (outputs and
#' outcomes) and binary LOS status.
#'
#' This is typically used as the aggregation function in [event_history_monthly()],
#' but can also be called directly to analyze a single point in time.
#'
#' **LOS Definition:** A registration is "LOS" if it has:
#'   - At least one net output (data, code, materials, supplements)
#'   - At least one net outcome (publication)
#'
#' @param sample The sample of registrations to analyze as an `rlang` expression.
#'   Defaults to `cosr::expr_valid_regs`.
#' @param cutoff The cutoff date for including resource history events
#'   (default: `Sys.Date()`). Only resources linked or removed on or before
#'   this date will be included.
#' @param lazy Whether to return a lazy query (TRUE) or collect results (FALSE).
#'   Default: TRUE
#'
#' @return A data frame (or lazy query) with columns:
#'   - node_id: Registration identifier
#'   - linked_outputs/outcomes: Count of linked resources
#'   - removed_outputs/outcomes: Count of removed resources
#'   - first_linked_output/outcome: Timestamp of first linked resource
#'   - total_resources: Total count of all linked resources
#'   - net_resources/outputs/outcomes: Net counts (linked - removed)
#'   - is_los: Binary indicator (1 if LOS, 0 otherwise)
#'
#' @examples
#' \dontrun{
#' # Get current LOS status
#' current_los <- resource_counts_at_date(
#'   sample = cosr::expr_valid_regs,
#'   cutoff = Sys.Date(),
#'   lazy = FALSE
#' )
#'
#' # Get historical snapshot
#' jan_2024 <- resource_counts_at_date(
#'   cutoff = "2024-01-01",
#'   lazy = FALSE
#' )
#' }
#'
#' @seealso [get_resource_history()], [event_history_monthly()]
#' @export
resource_counts_at_date <- function(
  sample = cosr::expr_valid_regs,
  cutoff = Sys.Date(),
  lazy = TRUE
) {
  get_resource_history(sample, cutoff) |>
    dplyr::summarise(
      .by = "node_id",
      linked_outputs = sum(
        artifact_type %in% c(1, 11, 21, 41) & is.na(artifact_deleted),
        na.rm = TRUE
      ),
      linked_outcomes = sum(
        artifact_type == 31 & is.na(artifact_deleted),
        na.rm = TRUE
      ),
      removed_outputs = sum(
        artifact_type %in% c(1, 11, 21, 41) & !is.na(artifact_deleted),
        na.rm = TRUE
      ),
      removed_outcomes = sum(
        artifact_type == 31 & !is.na(artifact_deleted),
        na.rm = TRUE
      ),
      first_linked_output = min(
        artifact_created[
          artifact_type %in% c(1, 11, 21, 41) & is.na(artifact_deleted)
        ],
        na.rm = TRUE
      ),
      first_linked_outcome = min(
        artifact_created[artifact_type == 31 & is.na(artifact_deleted)],
        na.rm = TRUE
      )
    ) |>
    dplyr::mutate(
      total_resources = linked_outputs + linked_outcomes,
      net_resources = total_resources - (removed_outputs + removed_outcomes),
      net_outputs = linked_outputs - removed_outputs,
      net_outcomes = linked_outcomes - removed_outcomes,
      is_los = ifelse(net_outputs > 0 & net_outcomes > 0, 1, 0)
    ) |>
    collector(lazy)
}
