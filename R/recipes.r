#' Get resource linkage history
#'
#' This function retrieves the history of resources (e.g., data, code, materials, papers, supplements) linked and removed from registrations up until a specified cutoff date. It combines data from multiple tables to create a comprehensive dataset of resource history for each registration.
#'
#' @param sample The sample of registrations to analyze as an `rlang` expression to be defused and evaluated in the context of the `osf_abstractnode` table.  This defaults to `cosr::expr_valid_regs`.
#' @param cutoff The cutoff date for including resource history events (default: `Sys.Date()`).  Only resources linked or removed on or before this date will be included in the results.
#' @param lazy Whether to return a lazy query (default: TRUE) or collect results into a data frame (default: FALSE)
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
      created <= as.POSIXct(cutoff) #,
      #(is.na(deleted) | deleted > as.POSIXct(cutoff))
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


# Registration Recipes ----------------------------------------------------------
#' Count resources and LOS status at cutoff date
#'
#' This function computes summary statistics for registrations at a specified cutoff date, including counts of linked and removed resources (outputs and outcomes) and LOS status. It can be used to generate time series summaries of LOS by various attributes.
#'
#' @param sample The sample of registrations to analyze as an `rlang` expression to be defused and evaluated in the context of the `osf_abstractnode` table.  This defaults to `cosr::expr_valid_regs`.
#' @param cutoff The cutoff date for including resource history events (default: `Sys.Date()`).  Only resources linked or removed on or before this date will be included in the results.
#' @param lazy Whether to return a lazy query (default: TRUE) or collect results into a data frame (default: FALSE)
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
      ) #,
      # first_los = max(
      #   min(
      #     artifact_created[
      #       artifact_type %in% c(1, 11, 21, 41) & is.na(artifact_deleted)
      #     ],
      #     na.rm = TRUE
      #   ),
      #   min(
      #     artifact_created[artifact_type == 31 & is.na(artifact_deleted)],
      #     na.rm = TRUE
      #   ),
      #   na.rm = TRUE
      # )
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
