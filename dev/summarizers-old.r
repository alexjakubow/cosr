# Transformers -----------------------------------------------------------------
# #' @export
# count_to_binary <- function(x) {
#   dplyr::if_else(x > 1, 1, x)
# }

# #' @export
# los_coarsen <- function(tbl) {
#   dplyr::collect(tbl) |>
#     dplyr::rename(
#       has_plans = n_plans,
#       has_outputs = n_outputs,
#       has_outcomes = n_outcomes
#     ) |>
#     dplyr::mutate(
#       dplyr::across(dplyr::starts_with("has_"), count_to_binary)
#     )
# }

# Generic Summarizers ---------------------------------------------------------
#' @export
osr_summarizer <- function(tbl, n_threshold = 0, lazy = FALSE, ...) {
  tbl |>
    dplyr::summarise(
      .by = c(...),
      n_osr = dplyr::n(),
    ) |>
    dplyr::mutate(share_osr = n_osr / sum(n_osr, na.rm = TRUE)) |>
    dplyr::arrange(desc(share_osr)) |>
    dplyr::select(c(...), n_osr, share_osr) |>
    cosr::collector(lazy)
}


#' @export
los_summarizer <- function(tbl, n_threshold = 0, lazy = TRUE, ...) {
  tbl |>
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
      prop_resources = have_resources / n_osr,
      prop_outputs = have_outputs / n_osr,
      prop_outcomes = have_outcomes / n_osr,
      prop_los = are_los / n_osr
    ) |>
    dplyr::filter(n_osr >= n_threshold) |>
    dplyr::arrange(desc(prop_los)) |>
    dplyr::select(c(...), n_osr, prop_los, everything()) |>
    cosr::collector(lazy)
}


# #' @export
# los_summarizer <- function(tbl, n_threshold = 0, ...) {
#   tbl |>
#     dplyr::summarise(
#       .by = c(...),
#       total = dplyr::n(),
#       have_plans = sum(has_plans, na.rm = TRUE),
#       have_outputs = sum(has_outputs, na.rm = TRUE),
#       have_outcomes = sum(has_outcomes, na.rm = TRUE),
#       are_los = sum(is_los, na.rm = TRUE)
#     ) |>
#     dplyr::mutate(
#       prop_plans = have_plans / total,
#       prop_outputs = have_outputs / total,
#       prop_outcomes = have_outcomes / total,
#       prop_los = are_los / total
#     ) |>
#     dplyr::filter(total >= n_threshold) |>
#     dplyr::arrange(desc(prop_los)) |>
#     dplyr::select(c(...), total, prop_los, everything())
# }

# Registration Population Summarizers ------------------------------------------
#' @export
registration_summaries <- function(
  .sample = cosr::expr_valid_regs,
  ...
) {
  PARAMS <- tibble::tribble(
    ~fn                                       , ~facet                   , ~label                  ,
    cosr::get_registration_affiliation_status , rlang::expr(institution) , "Affiliation Status"    ,
    cosr::get_registration_funded             , rlang::expr(funder)      , "Funded Status"         ,
    cosr::get_registration_funder             , rlang::expr(funder)      , "Funder"                ,
    cosr::get_registration_institutions       , rlang::expr(institution) , "Institution"           ,
    cosr::get_registration_template           , rlang::expr(template)    , "Registration Template" ,
    cosr::get_registration_provider           , rlang::expr(provider)    , "Registry"              ,
    cosr::get_registration_subjects           , rlang::expr(subject)     , "Subjects"              ,
    cosr::get_registration_subjects_detailed  , rlang::expr(subject)     , "Subjects (Detailed)"
  )

  # Compute datasets
  purrr::map2(
    PARAMS$fn,
    PARAMS$facet,
    ~ .x(sample = .sample) |>
      cosr::osr_summarizer(n_threshold = 0, lazy = FALSE, !!.y),
    .progress = TRUE
  ) |>
    rlang::set_names(PARAMS$label)
}

# # Registration Badge Summarizers ------------------------------------------------
# #' @export
# registration_los_segmentation <- function(
#   .sample = cosr::expr_valid_regs,
#   .method = cosr::registration_badges,
#   ...
# ) {
#   # Classify
#   classified <- dplyr::collect(.method(sample = .sample, ...))

#   # Set segmentation functions (as a tribble so rows stay bundled)
#   PARAMS <- tibble::tribble(
#     ~fn                                       , ~facet                   , ~label                  ,
#     cosr::get_registration_affiliation_status , rlang::expr(institution) , "Affiliation Status"    ,
#     cosr::get_registration_funded             , rlang::expr(funder)      , "Funded Status"         ,
#     cosr::get_registration_funder             , rlang::expr(funder)      , "Funder"                ,
#     cosr::get_registration_institutions       , rlang::expr(institution) , "Institution"           ,
#     cosr::get_registration_schema             , rlang::expr(template)    , "Registration Template" ,
#     cosr::get_registration_provider           , rlang::expr(provider)    , "Registry"              ,
#     cosr::get_registration_subjects           , rlang::expr(subject)     , "Subjects"              ,
#     cosr::get_registration_subjects_detailed  , rlang::expr(subject)     , "Subjects (Detailed)"
#   )

#   # Compute datasets
#   purrr::map2(
#     PARAMS$fn,
#     PARAMS$facet,
#     ~ .x(sample = .sample) |>
#       dplyr::collect() |>
#       dplyr::left_join(classified) |>
#       cosr::los_coarsen() |>
#       cosr::los_summarizer(n_threshold = 0, !!.y),
#     .progress = TRUE
#   ) |>
#     rlang::set_names(PARAMS$label)
# }

# #' TS Summarizers --------------------------------------------------------------
# #' @export
# reg_los_summarizer <- function(tbl, n_threshold = 0, lazy = TRUE, ...) {
#   tbl |>
#     dplyr::summarise(
#       .by = c(...),
#       n_osr = dplyr::n(),
#       have_resources = sum(net_resources > 0, na.rm = TRUE),
#       have_outputs = sum(net_outputs > 0, na.rm = TRUE),
#       have_outcomes = sum(net_outcomes > 0, na.rm = TRUE),
#       are_los = sum(is_los, na.rm = TRUE)
#     ) |>
#     dplyr::mutate(
#       across(
#         starts_with(c("have_", "are_")),
#         ~ ifelse(is.na(.x), 0, .x)
#       ),
#       prop_resources = have_resources / n_osr,
#       prop_outputs = have_outputs / n_osr,
#       prop_outcomes = have_outcomes / n_osr,
#       prop_los = are_los / n_osr
#     ) |>
#     dplyr::filter(n_osr >= n_threshold) |>
#     dplyr::arrange(desc(prop_los)) |>
#     dplyr::select(c(...), n_osr, prop_los, everything()) |>
#     cosr::collector(lazy)
# }

# #' @export
# compute_los_summaries <- function(
#   .sample = cosr::expr_valid_regs,
#   .method = cosr::resource_counts_at_date,
#   .cutoff = Sys.Date(),
#   ...
# ) {
#   # Classify
#   los_status <- .method(sample = .sample, cutoff = .cutoff)

#   # Set segmentation functions (as a tribble so rows stay bundled)
#   PARAMS <- tibble::tribble(
#     ~fn                                       , ~facet                   , ~label                  ,
#     cosr::get_registration_affiliation_status , rlang::expr(institution) , "Affiliation Status"    ,
#     #cosr::get_registration_funded             , rlang::expr(funder)      , "Funded Status"         ,
#     #cosr::get_registration_funder             , rlang::expr(funder)      , "Funder"                ,
#     cosr::get_registration_institutions       , rlang::expr(institution) , "Institution"           ,
#     cosr::get_registration_schema             , rlang::expr(template)    , "Registration Template" ,
#     cosr::get_registration_provider           , rlang::expr(provider)    , "Registry"              ,
#     cosr::get_registration_subjects           , rlang::expr(subject)     , "Subjects"              ,
#     cosr::get_registration_subjects_detailed  , rlang::expr(subject)     , "Subjects (Detailed)"
#   )

#   # Compute datasets
#   purrr::map2(
#     PARAMS$fn,
#     PARAMS$facet,
#     ~ .x(sample = .sample) |>
#       dplyr::left_join(los_status, by = "node_id") |>
#       cosr::reg_los_summarizer(n_threshold = 0, lazy = FALSE, !!.y),
#     .progress = TRUE
#   ) |>
#     rlang::set_names(PARAMS$label)
# }

# # Registration Population Summarizers ------------------------------------------
# #' @export
# registration_summaries <- function(
#   .sample = cosr::expr_valid_regs,
#   ...
# ) {
#   PARAMS <- tibble::tribble(
#     ~fn                                       , ~facet                   , ~label                  ,
#     cosr::get_registration_affiliation_status , rlang::expr(institution) , "Affiliation Status"    ,
#     cosr::get_registration_funded             , rlang::expr(funder)      , "Funded Status"         ,
#     cosr::get_registration_funder             , rlang::expr(funder)      , "Funder"                ,
#     cosr::get_registration_institutions       , rlang::expr(institution) , "Institution"           ,
#     cosr::get_registration_template           , rlang::expr(template)    , "Registration Template" ,
#     cosr::get_registration_provider           , rlang::expr(provider)    , "Registry"              ,
#     cosr::get_registration_subjects           , rlang::expr(subject)     , "Subjects"              ,
#     cosr::get_registration_subjects_detailed  , rlang::expr(subject)     , "Subjects (Detailed)"
#   )

#   # Compute datasets
#   purrr::map2(
#     PARAMS$fn,
#     PARAMS$facet,
#     ~ .x(sample = .sample) |>
#       cosr::osr_summarizer(n_threshold = 0, lazy = FALSE, !!.y),
#     .progress = TRUE
#   ) |>
#     rlang::set_names(PARAMS$label)
# }
