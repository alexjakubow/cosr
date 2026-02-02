# Transformers -----------------------------------------------------------------
#' @export
count_to_binary <- function(x) {
  dplyr::if_else(x > 1, 1, x)
}

#' @export
los_coarsen <- function(tbl) {
  dplyr::collect(tbl) |>
    dplyr::rename(
      has_plans = n_plans,
      has_outputs = n_outputs,
      has_outcomes = n_outcomes
    ) |>
    dplyr::mutate(
      dplyr::across(dplyr::starts_with("has_"), count_to_binary)
    )
}


# Generic Summarizers ---------------------------------------------------------
#' @export
los_summarizer <- function(tbl, n_threshold = 0, ...) {
  tbl |>
    dplyr::summarise(
      .by = c(...),
      total = dplyr::n(),
      have_plans = sum(has_plans, na.rm = TRUE),
      have_outputs = sum(has_outputs, na.rm = TRUE),
      have_outcomes = sum(has_outcomes, na.rm = TRUE),
      are_los = sum(is_los, na.rm = TRUE)
    ) |>
    dplyr::mutate(
      prop_plans = have_plans / total,
      prop_outputs = have_outputs / total,
      prop_outcomes = have_outcomes / total,
      prop_los = are_los / total
    ) |>
    dplyr::filter(total >= n_threshold) |>
    dplyr::arrange(desc(prop_los)) |>
    dplyr::select(c(...), total, prop_los, everything())
}
