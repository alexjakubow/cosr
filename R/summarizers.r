#' Generic summarizer for LOS time series data'
#'
#' This function takes a time series dataset (e.g., `data/los_ts.parquet`) and computes summary statistics (e.g., counts, proportions) for each subgroup defined by the specified variables.  It also applies an optional threshold filter to include only subgroups with a minimum share of the total available OSRs in the dataset.
#'
#' @param tbl The input time series dataset (e.g., `data/los_ts.parquet`)
#' @param lazy Whether to return a lazy query (default: TRUE) or collect results into a data frame (default: FALSE)
#' @param threshold A numeric value between 0 and 1 specifying the minimum proportion of total OSRs required for a subgroup to be included in the summary (default: 0, meaning no threshold filter is applied)
#' @param ... One or more unquoted expressions specifying the subgroup variables to summarize by (e.g., `institution`, `funded`, `provider`, `subject`, etc.)
#'
#' @return An (un)lazy data frame containing summary statistics for each subgroup defined by the specified variables, including counts and proportions of OSRs, resources, outputs, outcomes, and LOS.
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


#' Compute time series summaries by attribute
#'
#' This function computes summary statistics for LOS time series data by specified attribute (or subgroup) variables. It loads the time series dataset, joins it with attribute data, and applies the `los_summarizer` function to compute summaries for each subgroup.
#'
#' @param subgroup_var The attribute to summarize by as a string (e.g., "institution", "funder", "funded", "provider", "subject", etc.)
#' @param los_ts_path The file path to the LOS time series dataset (e.g., "data/los_ts.parquet")
#'
#' @return An in-memory data frame containing summary statistics for each subgroup defined by the specified attribute variable
#' @export
los_summary_by_attribute <- function(
  subgroup_var,
  los_ts_path = "data/los_ts.parquet"
) {
  # Load time series data
  ts_data <- open_dataset(los_ts_path)

  # Load subgroup data
  subgroup_path <- paste0("data/osr_", tolower(subgroup_var), ".parquet")
  subgroup_data <- arrow::open_dataset(subgroup_path) |>
    select(node_id, !!sym(subgroup_var))

  # Join with subgroup data and summarize
  left_join(ts_data, subgroup_data, by = "node_id") |>
    cosr::los_summarizer(lazy = FALSE, !!sym(subgroup_var), date)
}


#' Wrapper to compute time series summaries by multiple subgroup variables
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

  # Generate summaries for each subgroup variable and combine results
  purrr::walk(
    subgroup_vars,
    ~ cosr::los_summary_by_attribute(
      subgroup_var = as.character(.x),
      los_ts_path = los_ts_path
    ),
    .progress = TRUE
  )
}
