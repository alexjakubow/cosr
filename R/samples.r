#' The `samples` module contains predefined sample expressions and
#' utility functions for generating time spans.

#' @export
expr_valid_regs <- rlang::exprs(
  is_public == TRUE, #public
  is.na(deleted), #nondeprecated
  moderation_state == "accepted",
  (spam_status != 2L | is.na(spam_status)) #authentic
)


#' @export
set_timespan <- function(
  delta = "month",
  start = "2018-01-01",
  end = Sys.Date()
) {
  timespan <- c(
    as.character(as.Date(start)),
    as.character(lubridate::floor_date(as.Date(end)))
  )

  seq(as.POSIXct(timespan[1]), as.POSIXct(timespan[2]), by = delta)
}
