#' @export
registration_badges <- function(
  sample = NULL,
  cutoff = Sys.Date(),
  lazy = TRUE
) {
  cosr::get_badge_history(sample, cutoff, lazy) |>
    dplyr::summarize(
      .by = node_id,
      n_badges = dplyr::n(),
      # Output badges
      n_data = sum(artifact_type == 1, na.rm = TRUE),
      n_materials = sum(artifact_type == 21, na.rm = TRUE),
      n_code = sum(artifact_type == 11, na.rm = TRUE),
      n_supplements = sum(artifact_type == 31, na.rm = TRUE),
      # Outcome badges
      n_papers = sum(artifact_type == 41, na.rm = TRUE),
      # LOS components
      n_plans = max(1, na.rm = TRUE), #restrict valid registrations using `sample` arg
      n_outputs = sum(artifact_type %in% BADGE_TYPES$outputs, na.rm = TRUE),
      n_outcomes = sum(artifact_type %in% BADGE_TYPES$outcomes, na.rm = TRUE)
    ) |>
    dplyr::mutate(
      is_los = n_plans > 0 & n_outputs > 0 & n_outcomes > 0
    ) |>
    collector(lazy)
}


# Helpers ---------------------------------------------------------
#' @export
get_badge_history <- function(
  sample = NULL,
  cutoff = Sys.Date(),
  lazy = TRUE
) {
  # Base table query
  basetable <- cosr::open_parquet(tbl = "osf_abstractnode") |>
    dplyr::filter(
      type == "osf.registration",
      !!!sample,
      created <= as.POSIXct(cutoff),
      (is.na(deleted) | deleted > as.POSIXct(cutoff))
    ) |>
    dplyr::select(node_id = id)

  # Resources and artifacts added
  resources <- cosr::open_parquet(tbl = "osf_nodelog") |>
    dplyr::filter(
      action == "resource_identifier_added",
      created <= as.POSIXct(cutoff)
    ) |>
    dplyr::select(node_id, params)

  artifacts <- cosr::open_parquet(tbl = "osf_outcomeartifact") |>
    dplyr::filter(
      created <= as.POSIXct(cutoff),
      (is.na(deleted) | deleted > as.POSIXct(cutoff)),
      finalized == TRUE
    ) |>
    dplyr::select(
      artifact_id = `_id`,
      artifact_type
    )

  # Link regs to artifact_type via parsed `params` field
  basetable |>
    dplyr::left_join(resources, by = "node_id") |>
    dplyr::mutate(
      params = sql(
        "regexp_extract(params, '[[:digit:]][[:alnum:]]{5,}')"
      )
    ) |>
    dplyr::rename(artifact_id = params) |>
    dplyr::left_join(artifacts, by = "artifact_id") |>
    dplyr::select(node_id, artifact_type) |>
    cosr::collector(lazy)
}

#' @export
BADGE_TYPES <- list(
  outputs = c(1, 11, 21, 31),
  outcomes = c(41)
)
