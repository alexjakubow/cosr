testthat::test_that("yields lazy duckdb connection", {
  lazy_result <- get_resource_history(
    sample = rlang::exprs(id == 1416738)
  )

  testthat::expect_s3_class(lazy_result, "tbl_duckdb_connection")
})
