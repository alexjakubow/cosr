# create_registration_table <- function() {
#   cosr::open_parquet(tbl = "osf_abstractnode", duck = FALSE) |>
#     dplyr::filter(type == "osf.registration")
# }

# create_registration_log_table <- function() {
#   create_registration_table() |>
#     dplyr::select(node_id = id) |>
#     dplyr::left_join(
#       cosr::open_parquet(tbl = "osf_nodelog", duck = FALSE),
#       by = "node_id"
#     )
# }

# save_registration_tables <- function(dir = "~osfdata/derived") {
#   if (!dir.exists(dir)) {
#     dir.create(dir, recursive = TRUE)
#   }
#   create_registration_table() |>
#     arrow::write_parquet(file.path(dir, "registration.parquet"))

#   create_registration_log_table() |>
#     arrow::write_parquet(file.path(dir, "registration_log.parquet"))
# }


get_registrations <- function(lazy = TRUE) {
  cosr::open_parquet(tbl = "osf_abstractnode") |>
    dplyr::filter(type == "osf.registration") |>
    dplyr::select(node_id = id) 
}