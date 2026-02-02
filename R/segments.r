# Helper Functions -------------------------------------------------------------
get_funder_metadata <- function() {
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
}


# Query Functions -------------------------------------------------------------
#' @export
get_registration_funder <- function(
  sample = NULL,
  nodetype = "registration",
  simplify = FALSE
) {
  # Funder metadata (in memory)
  tbl_funder <- get_funder_metadata()

  # Query for guid table
  guid <- cosr::open_parquet(tbl = "osf_guid") |>
    dplyr::filter(content_type_id == 30) |>
    dplyr::select(
      record_id = id,
      node_id = object_id,
    )

  # Join funder metadata to guid table
  tbl_funder <- tbl_funder |>
    dplyr::inner_join(dplyr::collect(guid), by = "record_id")

  # Base table query
  basetable <- cosr::open_parquet(tbl = "osf_abstractnode") |>
    dplyr::filter(type == paste0("osf.", nodetype), !!!sample) |>
    dplyr::select(node_id = id)

  # Simplify Funded status (optional)
  if (simplify) {
    tbl_funder <- tbl_funder |>
      dplyr::mutate(
        funder = dplyr::if_else(
          is.na(funder),
          "Funded",
          "Unfunded"
        )
      )
  }

  # Collect and return
  dplyr::inner_join(
    dplyr::collect(basetable),
    tbl_funder,
    by = "node_id"
  )
}


#' @export
get_registration_funded <- function(..., .simplify = TRUE) {
  get_registration_funder(..., simplify = .simplify)
}


#' @export
get_registration_institutions <- function(
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
get_registration_schema <- function(sample = NULL, lazy = TRUE) {
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
get_registration_affiliation_status <- function(
  ...,
  .simplify = TRUE
) {
  get_registration_institutions(..., simplify = .simplify)
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
get_registration_subjects <- function(
  sample = NULL,
  parents_only = TRUE,
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
    dplyr::select(node_id, subject)

  # Return/Collect
  cosr::collector(basetable, lazy)
}


#' @export
get_registration_subjects_detailed <- function(..., .parents_only = FALSE) {
  get_registration_subjects(..., parents_only = .parents_only)
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
