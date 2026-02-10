get_registrations <- function(lazy = TRUE) {
  cosr::open_parquet(tbl = "osf_abstractnode") |>
    dplyr::filter(type == "osf.registration") |>
    dplyr::select(node_id = id, created, deleted) |>
    cosr::collector(lazy)
}


last_logged_action <- function(date = Sys.Date(), actions, date_col = created, lazy = TRUE) {
  # Registration query
  regs <- cosr::open_parquet(tbl = "osf_abstractnode") |>
    dplyr::filter(
      type == "osf.registration",
      (is.na(deleted) | deleted > as.POSIXct(date)),
      created <= as.POSIXct(date)
    ) |>
    dplyr::select(node_id = id)

  # Log query
  log <- cosr::open_parquet(tbl = "osf_nodelog") |>
    dplyr::select(
      node_id = id,
      action,
      {{ date_col }}
    ) |>
    dplyr::filter({{ date_col }} <= date, action %in% actions) |>
    dbplyr::window_order({{ date_col }}) |>
    dplyr::summarise(
      .by = node_id,
      status = last(action)
    )

  # Join and collect
  regs |>
    dplyr::left_join(log, by = "node_id") |>
    cosr::collector(lazy)
}


registration_log <- function(lazy = TRUE) {
  # Registration query
  regs <- cosr::open_parquet(tbl = "osf_abstractnode") |>
    dplyr::filter(type == "osf.registration") |>
    dplyr::select(node_id = id)

  # Log query
  log <- cosr::open_parquet(tbl = "osf_nodelog") |>
    dplyr::select(
      node_id,
      action,
      created
    )

  # Join and collect
  regs |>
    dplyr::left_join(log, by = "node_id") |>
    cosr::collector(lazy) 
}

num_actions <- registration_log() |>
  dplyr::summarise(
    .by = node_id,
    num_actions = dplyr::n(),
    first_action = min(created, na.rm = TRUE),
    last_action = max(created, na.rm = TRUE)
  ) |>
  dplyr::mutate(
    num_actions = dplyr::if_else(num_actions == 1 & is.na(first_action), 0L, num_actions)
  ) |>
  dplyr::collect()

action_summary <- registration_log() |>
  dplyr::summarise(
    .by = action,
    n = dplyr::n()
  ) |>
  dplyr::collect()


# pot_public <- last_logged_action(actions = "made_public")



#' Moderation history query
# If we care exclusively about currently LOS, we filter on to_state == "accepted"
# If we want to treat embargoed as if the are LOS, we select on trigger == "accept_submission"


# #' Last logged action query
# #'
# #' @param tbl Database table connection to query (duckdb)
# #' @param date_col Date column name
# #' @param status_col Status column name
# #' @param date Query date
# #' @param ... Column names to group by
# #' @export
# last_logged_action <- function(tbl, date_col, status_col, date, ...) {
#   tbl |>
#     dplyr::filter({{ date_col }} <= date) |>
#     dbplyr::window_order({{ date_col }}) |>
#     dplyr::summarise(
#       .by = c(...),
#       status = last({{ status_col }})
#     )
# }


#  <- function(
#   tbl = cosr::open_parquet(tbl = "osf_registrationaction"),
#   date = Sys.Date(),
  
# )


# moderation_history <- last_logged_action(
#   tbl = cosr::open_parquet(tbl = "osf_registrationaction"),
#   date_col = created,
#   status_col = to_state,
#   date = Sys.Date(),
#   target_id
# ) |>
#   collect()


# moderation_history <- function(include_embargoed = FALSE, lazy = TRUE) {
#   cosr::open_parquet(tbl = "osf_registrationaction") |>
#     dplyr::filter(
#       if (include_embargoed) {
#         trigger == "accept_submission"
#       } else {
#         to_state == "accepted"
#       }
#     ) |>
#     dplyr::select(
#       node_id,
#       action = trigger,
#       created
#     ) |>
#     cosr::collector(lazy)
# }


# current_moderation_state <- function(lazy = TRUE) {
#   cosr::open_parquet(tbl = "osf_registrationaction") |>
#     dplyr::select(
#       node_id = id,
#       moderation_state
#     ) |>
#     cosr::collector(lazy)
# }