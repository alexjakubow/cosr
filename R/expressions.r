#' Common `rlang` expressions for filtering and summarizing datasets

#' Define valid (OSR) registrations
#' @export
expr_valid_regs <- rlang::exprs(
  is_public == TRUE, #public
  is.na(deleted), #nondeprecated
  !is.na(registered_date),
  moderation_state == "accepted",
  (spam_status != 2 | is.na(spam_status)) #authentic
)
