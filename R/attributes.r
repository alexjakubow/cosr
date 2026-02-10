#' Attributes currently supported for summarization and subgroup analyses
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


# Helper Functions -------------------------------------------------------------
#' Get funder metadata for all OSF resources
#'
#' @param cache Logical. Should the results be cached? Defaults to `TRUE`.
#'
#' @returns If `cache = TRUE`, returns the file path to the cached Parquet file.  If `cache = FALSE`, returns a tibble with funder metadata for all OSF resources.
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


# Query Functions -------------------------------------------------------------
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


#' @export
get_registration_funded <- function(..., .simplify = TRUE) {
  get_registration_funder(..., simplify = .simplify) |>
    dplyr::rename(funded = funder)
}


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


#' @export
get_registration_affiliated <- function(
  ...,
  .simplify = TRUE
) {
  get_registration_institution(..., simplify = .simplify) |>
    dplyr::rename(affiliated = institution)
}


#' @export
get_registration_provider <- function(
  sample = NULL,
  nodetype = "registration",
  lazy = TRUE
) {
  # Querty base table
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


#' @export
get_registration_subject_parent <- function(..., .parents_only = TRUE) {
  get_registration_subject(..., parents_only = .parents_only) |>
    dplyr::rename(subject_parent = subject)
}


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


# Assignment Functions ---------------------------------------------------------
#' Assign current attributes (subgroup status for all OSRs
#'
#' This function is designed to be run once per time period (e.g., monthly) to capture current attributes/ subgroup memberships for all OSRs.  The results are cached as Parquet files in the `data/` directory for use in downstream analyses.
#'
#' @param sample A dplyr filter expression to apply to the base table of OSRs (e.g., `dplyr::filter(created >= "2024-01-01")`).  The default is `cosr::expr_valid_regs`, which captures all "valid" registrations.
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
}
