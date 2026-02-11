# queries.r ---------------------------------------------------------------
#
# ATTRIBUTE QUERY FUNCTIONS
#
# This file contains functions for extracting specific attributes (metadata)
# for registrations. Each function retrieves one type of attribute.
#
# Most users should use assign_attributes() (in workflows.r) which runs all
# of these queries in batch. Use these individual functions when you need:
#   - A single specific attribute
#   - Custom filtering or sampling
#   - Lazy evaluation for large datasets
#
# Available attributes (see SUPPORTED_ATTRIBUTES):
#   - institution, funder, funded, template, provider,
#   - subject, subject_parent, affiliated, creator
# -----------------------------------------------------------------------------

#' Attributes currently supported for summarization and subgroup analyses
#'
#' This constant defines the 8 attribute types that can be used for
#' subgroup analysis in LOS workflows.
#'
#' @format Character vector of length 8
#' @export
SUPPORTED_ATTRIBUTES <- c(
  "institution",
  "funder",
  "funded",
  "template",
  "provider",
  "subject",
  "subject_parent",
  "affiliated"
)


# Metadata Extraction -----------------------------------------------------

#' Get funder metadata for all OSF resources
#'
#' Extracts and parses funding information from OSF metadata records.
#' This is used internally by [get_registration_funder()] but can also
#' be called directly for custom analyses.
#'
#' @param cache Logical. Should the results be cached? Defaults to `TRUE`.
#'
#' @return If `cache = TRUE`, returns the file path to the cached Parquet file.
#'   If `cache = FALSE`, returns a tibble with funder metadata for all OSF resources.
#'
#' @export
get_funder_metadata <- function(cache = TRUE) {
  funder <- cosr::open_parquet(tbl = "osf_guidmetadatarecord") |>
    dplyr::filter(funding_info != "[]") |>
    dplyr::select(
      record_id = guid_id,
      resource_type_general,
      funding_info
    ) |>
    dplyr::collect() |>
    dplyr::mutate(
      funding_info = purrr::map(
        funding_info,
        ~ jsonlite::fromJSON(.x, flatten = TRUE)
      )
    ) |>
    tidyr::unnest(funding_info) |>
    dplyr::select(
      record_id,
      resource_type_general,
      funder = funder_name,
      funder_identifier
    )
  if (cache) {
    pathout <- "data/funder_metadata.parquet"
    arrow::write_parquet(funder, pathout)
    funder <- pathout
  }
  return(funder)
}


# Query Functions ---------------------------------------------------------

#' Get funder information for registrations
#'
#' Retrieves funder names for each registration by joining metadata records.
#'
#' @param sample Filter expression for sampling registrations (default: NULL)
#' @param simplify Logical. If TRUE, returns binary "Funded"/"Unfunded" status
#'   instead of specific funder names (default: FALSE)
#' @param lazy Logical. Return lazy query (TRUE) or collect results (FALSE)?
#'   Default: TRUE
#'
#' @return A data frame (or lazy query) with columns:
#'   - node_id: Registration identifier
#'   - funder: Funder name (or "Funded"/"Unfunded" if simplified)
#'
#' @examples
#' \dontrun{
#' # Get all funders
#' funders <- get_registration_funder(lazy = FALSE)
#'
#' # Get binary funded status
#' funded_status <- get_registration_funder(simplify = TRUE, lazy = FALSE)
#' }
#'
#' @export
get_registration_funder <- function(
  sample = NULL,
  simplify = FALSE,
  lazy = TRUE
) {
  # Collect funder metadata in memory and then cache as Parquet file
  tbl_funder <- get_funder_metadata(cache = TRUE) |>
    arrow::open_dataset() |>
    arrow::to_duckdb()

  # Query for guid table
  guid <- cosr::open_parquet(tbl = "osf_guid") |>
    dplyr::filter(content_type_id == 30) |>
    dplyr::select(
      record_id = id,
      node_id = object_id,
    )

  # Join funder metadata to guid table
  tbl_funder <- tbl_funder |>
    dplyr::inner_join(guid, by = "record_id")

  # Base table query
  basetable <- cosr::open_parquet(tbl = "osf_abstractnode") |>
    dplyr::filter(type == "osf.registration", !!!sample) |>
    dplyr::select(node_id = id)

  # Simplify Funded status (optional)
  if (simplify) {
    tbl_funder <- tbl_funder |>
      dplyr::mutate(
        funder = dplyr::if_else(
          !is.na(funder),
          "Funded",
          "Unfunded"
        )
      )
  }

  # Join and return/collect
  dplyr::left_join(basetable, tbl_funder, by = "node_id") |>
    dplyr::mutate(
      funder = dplyr::if_else(
        is.na(funder),
        "Unfunded",
        funder
      )
    ) |>
    dplyr::select(node_id, funder) |>
    cosr::collector(lazy)
}


#' Get binary funded status for registrations
#'
#' Convenience wrapper around [get_registration_funder()] that returns
#' simplified "Funded"/"Unfunded" status.
#'
#' @param ... Arguments passed to [get_registration_funder()]
#' @param .simplify Logical. Defaults to TRUE (always simplified)
#'
#' @return A data frame with columns: node_id, funded
#'
#' @export
get_registration_funded <- function(..., .simplify = TRUE) {
  get_registration_funder(..., simplify = .simplify) |>
    dplyr::rename(funded = funder)
}


#' Get institution affiliations for registrations
#'
#' Retrieves institution names for registrations that are affiliated with
#' Open Science Framework Institutions (OSFI).
#'
#' @param sample Filter expression for sampling (default: NULL)
#' @param nodetype Type of node to query (default: "registration")
#' @param simplify Logical. If TRUE, returns binary "OSFI-Affiliate"/"Unaffiliated"
#'   instead of institution names (default: FALSE)
#' @param lazy Logical. Return lazy query? (default: TRUE)
#'
#' @return A data frame with columns: node_id, institution
#'
#' @export
get_registration_institution <- function(
  sample = NULL,
  nodetype = "registration",
  simplify = FALSE,
  lazy = TRUE
) {
  # Query base table
  basetable <- cosr::open_parquet(tbl = "osf_abstractnode") |>
    dplyr::filter(type == paste0("osf.", nodetype), !!!sample) |>
    dplyr::select(node_id = id)

  # Query node affiliations
  affiliations <- cosr::open_parquet(
    tbl = "osf_abstractnode_affiliated_institutions"
  ) |>
    dplyr::select(node_id = abstractnode_id, institution_id)

  # Query institutions and names
  institutions <- cosr::open_parquet(tbl = "osf_institution") |>
    dplyr::select(institution_id = id, institution = name)

  # Combine query
  basetable <- basetable |>
    dplyr::left_join(affiliations, by = "node_id") |>
    dplyr::left_join(institutions, by = "institution_id") |>
    dplyr::select(node_id, institution)

  # Simplify OSFI status (optional)
  if (simplify) {
    basetable <- basetable |>
      dplyr::mutate(
        institution = dplyr::if_else(
          is.na(institution),
          "Unaffiliated",
          "OSFI-Affiliate"
        )
      )
  } else {
    basetable <- basetable |>
      dplyr::mutate(
        institution = dplyr::if_else(
          is.na(institution),
          "Unaffiliated",
          institution
        )
      )
  }
  # Return/Collect
  cosr::collector(basetable, lazy)
}


#' Get binary OSFI affiliation status for registrations
#'
#' Convenience wrapper around [get_registration_institution()] that returns
#' simplified affiliated status.
#'
#' @param ... Arguments passed to [get_registration_institution()]
#' @param .simplify Logical. Defaults to TRUE
#'
#' @return A data frame with columns: node_id, affiliated
#'
#' @export
get_registration_affiliated <- function(
  ...,
  .simplify = TRUE
) {
  get_registration_institution(..., simplify = .simplify) |>
    dplyr::rename(affiliated = institution)
}


#' Get registration template (schema) information
#'
#' Retrieves the registration template/schema name used for each registration.
#'
#' @param sample Filter expression for sampling (default: NULL)
#' @param lazy Logical. Return lazy query? (default: TRUE)
#'
#' @return A data frame with columns: node_id, template
#'
#' @export
get_registration_template <- function(sample = NULL, lazy = TRUE) {
  # Query schema data
  schema <- cosr::open_parquet(tbl = "osf_registrationschema") |>
    dplyr::select(
      schema_id = id,
      template_id = `_id`,
      template = name,
      schema_version,
      schema_created = created,
      schema_active = active,
      schema_visitble = visible
    )

  # Query registration metadata
  regmeta <- cosr::open_parquet(tbl = "osf_abstractnode") |>
    dplyr::filter(
      type == "osf.registration",
      !is.na(registered_meta),
      !!!sample
    ) |>
    dplyr::select(node_id = id, registered_meta)

  # Get template ID from metatdata
  templates <- regmeta |>
    dplyr::mutate(
      template_id = sql(
        "regexp_extract(registered_meta, '[[:digit:]][[:alnum:]]{5,}')"
      )
    ) |>
    dplyr::select(-registered_meta)

  # Collect and return
  templates |>
    dplyr::left_join(schema, by = "template_id") |>
    dplyr::select(node_id, template) |>
    cosr::collector(lazy)
}


#' Get provider information for registrations
#'
#' Retrieves the OSF provider name for each registration.
#'
#' @param sample Filter expression for sampling (default: NULL)
#' @param nodetype Type of node (default: "registration")
#' @param lazy Logical. Return lazy query? (default: TRUE)
#'
#' @return A data frame with columns: node_id, provider
#'
#' @export
get_registration_provider <- function(
  sample = NULL,
  nodetype = "registration",
  lazy = TRUE
) {
  # Query base table
  basetable <- cosr::open_parquet(tbl = "osf_abstractnode") |>
    dplyr::filter(type == paste0("osf.", nodetype), !!!sample) |>
    dplyr::select(node_id = id, provider_id)

  # Query provider
  provider <- cosr::open_parquet(tbl = "osf_abstractprovider") |>
    dplyr::select(provider_id = id, provider = name)

  # Collect and return
  basetable |>
    dplyr::left_join(provider, by = "provider_id") |>
    dplyr::select(node_id, provider) |>
    cosr::collector(lazy)
}


#' Get subject classifications for registrations
#'
#' Retrieves research subject classifications assigned to registrations.
#'
#' @param sample Filter expression for sampling (default: NULL)
#' @param parents_only Logical. Return only parent subjects? (default: FALSE)
#' @param lazy Logical. Return lazy query? (default: TRUE)
#'
#' @return A data frame with columns: node_id, subject
#'
#' @export
get_registration_subject <- function(
  sample = NULL,
  parents_only = FALSE,
  lazy = TRUE
) {
  # Base table query
  basetable <- cosr::open_parquet(tbl = "osf_abstractnode") |>
    dplyr::filter(!!!sample, type == "osf.registration") |>
    dplyr::select(node_id = id)

  # Query node subjects
  node_subjects <- cosr::open_parquet(tbl = "osf_abstractnode_subjects") |>
    dplyr::select(node_id = abstractnode_id, subject_id)

  # Query subjects
  subjects <- cosr::open_parquet(tbl = "osf_subject") |>
    dplyr::select(
      provider_id,
      subject_id = id,
      subject = text,
      parent_id
    )

  # Join query
  basetable <- basetable |>
    dplyr::left_join(node_subjects, by = "node_id") |>
    dplyr::left_join(subjects, by = "subject_id")

  # Optional filter for parent subjects only
  if (parents_only) {
    basetable <- basetable |>
      dplyr::filter(is.na(parent_id))
  }

  # Select relevant columns
  basetable <- basetable |>
    dplyr::select(node_id, subject) |>
    dplyr::mutate(
      subject = dplyr::if_else(
        is.na(subject),
        "Unspecified",
        subject
      )
    )

  # Return/Collect
  cosr::collector(basetable, lazy)
}


#' Get parent subject classifications for registrations
#'
#' Convenience wrapper around [get_registration_subject()] that returns
#' only parent (top-level) subjects.
#'
#' @param ... Arguments passed to [get_registration_subject()]
#' @param .parents_only Logical. Defaults to TRUE
#'
#' @return A data frame with columns: node_id, subject_parent
#'
#' @export
get_registration_subject_parent <- function(..., .parents_only = TRUE) {
  get_registration_subject(..., parents_only = .parents_only) |>
    dplyr::rename(subject_parent = subject)
}


#' Get creator (user) information for registrations
#'
#' Retrieves the creator/author user ID for each registration.
#'
#' @param object_sample Filter expression for registrations (default: NULL)
#' @param creator_sample Filter expression for users (default: NULL)
#' @param nodetype Type of node (default: "registration")
#' @param lazy Logical. Return lazy query? (default: TRUE)
#'
#' @return A data frame with columns: node_id, creator_id
#'
#' @export
get_registration_creator <- function(
  object_sample = NULL,
  creator_sample = NULL,
  nodetype = "registration",
  lazy = TRUE
) {
  # Query base table
  basetable <- cosr::open_parquet(tbl = "osf_abstractnode") |>
    dplyr::filter(type == paste0("osf.", nodetype), !!!object_sample) |>
    dplyr::select(node_id = id, creator_id)

  # Query creator user
  users <- cosr::open_parquet(tbl = "osf_osfuser") |>
    dplyr::filter(!!!creator_sample) |>
    dplyr::select(
      creator_id = id
    )

  # Collect and return
  basetable |>
    dplyr::left_join(users, by = "creator_id") |>
    cosr::collector(lazy)
}
