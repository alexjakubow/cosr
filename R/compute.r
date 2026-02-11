# compute.r ---------------------------------------------------------------
#
# CORE COMPUTATIONAL FUNCTIONS
#
# This file contains low-level computational logic for retrieving and
# processing resource history data. These are building-block functions
# used by higher-level functions in analysis.r and workflows.r.
#
# Most users won't call these directly - use the functions in workflows.r
# or analysis.r instead. These are exposed for advanced users who need
# fine-grained control over resource history queries.
#
# Key functions:
#   - get_resource_history() - Retrieve resource linkage history
# -----------------------------------------------------------------------------

#' Get resource linkage history for registrations
#'
#' This is a low-level function that retrieves the complete history of
#' resources (data, code, materials, papers, supplements) linked to and
#' removed from registrations up until a specified cutoff date.
#'
#' This function combines data from multiple OSF database tables to create
#' a comprehensive dataset of resource history. Most users should use
#' [resource_counts_at_date()] instead, which processes this raw history
#' into summary statistics.
#'
#' **Artifact Types:**
#'   - Type 1, 11, 21, 41: Outputs (data, code, materials, supplements)
#'   - Type 31: Outcomes (preprints, papers)
#'
#' @param sample The sample of registrations to analyze as an `rlang` expression
#'   to be defused and evaluated in the context of the `osf_abstractnode` table.
#'   Defaults to `cosr::expr_valid_regs`.
#' @param cutoff The cutoff date for including resource history events
#'   (default: `Sys.Date()`). Only resources linked or removed on or before
#'   this date will be included in the results.
#' @param lazy Whether to return a lazy query (default: TRUE) or collect
#'   results into a data frame (default: FALSE)
#'
#' @return A data frame (or lazy query) with columns:
#'   - node_id: Registration identifier
#'   - artifact_id: Unique artifact identifier
#'   - artifact_type: Type code (1, 11, 21, 31, 41)
#'   - artifact_created: Timestamp when artifact was linked
#'   - artifact_deleted: Timestamp when artifact was removed (NA if still linked)
#'
#' @examples
#' \dontrun{
#' # Get resource history for all valid registrations
#' history <- get_resource_history(
#'   sample = cosr::expr_valid_regs,
#'   cutoff = Sys.Date(),
#'   lazy = FALSE
#' )
#'
#' # Get history for specific date
#' history_2024 <- get_resource_history(
#'   cutoff = "2024-01-01",
#'   lazy = FALSE
#' )
#' }
#'
#' @seealso [resource_counts_at_date()]
#' @export
get_resource_history <- function(
  sample = cosr::expr_valid_regs,
  cutoff = Sys.Date(),
  lazy = TRUE
) {
  # Base table query
  basetable <- cosr::open_parquet(tbl = "osf_abstractnode") |>
    dplyr::filter(
      type == "osf.registration",
      !!!sample,
      created <= as.POSIXct(cutoff)
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
      artifact_type,
      artifact_created = created,
      artifact_deleted = deleted
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
    cosr::collector(lazy)
}
