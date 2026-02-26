# reporting.r ------------------------------------------------------------------

#' Neat labels for reporting
#' A named vector of labels for reporting metrics. The names correspond to metric identifiers, and the values are human-readable labels that can be used in tables and visualizations.
#' @export
REPORTING_LABELS <- c(
  n_osr = "Open Registrations",
  have_outputs = "Open Registrations with Linked Outputs",
  have_outcomes = "Open Registrations with Linked Outcomes",
  are_los = "Lifecycle Open Science (LOS) Registrations",
  prop_los = "Share of Open Registrations that are LOS"
)


#' Transform summary data into long format for reporting
#'
#' This function takes a summary data frame and transforms it into a long format
#' suitable for reporting. It captures grouping variables, pivots the data to a
#' long format, and calculates changes over time (lag, delta, growth) for each
#' metric.
#'
#' @param tbl A data frame containing summary data with a `date` column and
#'   various metric columns.
#' @param ... Optional grouping variables to include in the transformation. If
#'   provided, the function will group the data by these variables along with
#'   the metric. If not provided, the function will group only by the metric.
#'
#' @return A data frame in long format with columns for `date`, `metric`,
#'   `value`, `moment`, and any specified grouping variables. The `moment`
#'   column distinguishes between:
#'   - `value`: the observed metric value at that date
#'   - `lag`: the value from the previous period
#'   - `delta`: the absolute change from the previous period
#'   - `growth`: the relative change (rate) from the previous period
#'
#' @examples
#' \dontrun{
#' # No grouping
#' summary_longform(los_ts_summary)
#'
#' # One grouping variable
#' summary_longform(los_ts_funded, funded)
#'
#' # Multiple grouping variables
#' summary_longform(los_ts_data, funded, institution)
#' }
#'
#' @seealso [generate_value_types()], [generate_reporting_dataset()]
#' @export
summary_longform <- function(tbl, ...) {
  # Capture grouping variables
  group_vars <- rlang::enquos(...)

  # Determine which columns to pivot (exclude date and grouping vars)
  if (length(group_vars) > 0) {
    group_var_names <- purrr::map_chr(group_vars, rlang::as_name)
    pivot_cols <- setdiff(names(tbl), c("date", group_var_names))
  } else {
    pivot_cols <- setdiff(names(tbl), "date")
  }

  # Build the transformation
  result <- tbl |>
    tidyr::pivot_longer(
      cols = dplyr::all_of(pivot_cols),
      names_to = "metric",
      values_to = "value"
    )

  # Add grouping if specified
  if (length(group_vars) > 0) {
    result <- result |>
      dplyr::group_by(metric, !!!group_vars)
  } else {
    result <- result |>
      dplyr::group_by(metric)
  }

  # Calculate changes over time, then pivot to long form by moment
  result |>
    dplyr::arrange(date) |>
    dplyr::mutate(
      lag = dplyr::lag(value),
      delta = value - lag,
      growth = ifelse(lag == 0, NA, delta / lag)
    ) |>
    tidyr::pivot_longer(
      cols = c(value, lag, delta, growth),
      names_to = "moment",
      values_to = "value"
    )
}


#' Generate value types based on metric names
#'
#' This function identifies metrics that represent counts (e.g., those starting
#' with `n_`, `are_`, `have_`) and proportions (e.g., those starting with
#' `prop_`) and adds a new column `value_type` to indicate the type of value.
#' Metric name prefixes are stripped so that counts and proportions for the
#' same underlying concept share the same `metric` value (e.g., both
#' `have_outputs` and `prop_outputs` become `outputs`).
#'
#' @param tbl A data frame containing a column named `metric` with metric names.
#'
#' @return A data frame with rows filtered to only counts and proportions, with
#'   an additional `value_type` column (`"count"` or `"proportion"`) and metric
#'   name prefixes stripped.
#'
#' @examples
#' \dontrun{
#' summary_longform(los_ts_summary) |>
#'   generate_value_types()
#' }
#'
#' @seealso [summary_longform()], [generate_reporting_dataset()]
#' @export
generate_value_types <- function(tbl) {
  dplyr::bind_rows(
    tbl |>
      dplyr::filter(grepl("^(n_|are_|have_)", metric)) |>
      dplyr::mutate(
        metric = gsub("^(n_|are_|have_)", "", metric),
        value_type = "count"
      ),
    tbl |>
      dplyr::filter(grepl("^prop_", metric)) |>
      dplyr::mutate(
        metric = gsub("^prop_", "", metric),
        value_type = "proportion"
      )
  )
}


#' Generate a complete long-form reporting dataset
#'
#' This is the main reporting workflow function. It generates a tidy, long-form
#' dataset combining aggregate (overall) summaries and per-attribute subgroup
#' summaries into a single data frame suitable for reporting and visualization.
#'
#' For each attribute in `subgroup_vars`, the function reads the corresponding
#' pre-computed Parquet file (e.g., `data/los_ts_institution.parquet`), applies
#' [summary_longform()] with the attribute as a grouping variable, and appends
#' the result to the aggregate summary. An `attribute` column is added to
#' identify the subgroup dimension of each row (`"overall"` for aggregate rows).
#'
#' The result is passed through [generate_value_types()] to add a `value_type`
#' column and normalize metric names.
#'
#' @param subgroup_vars Character vector of attribute names to include.
#'   Defaults to [SUPPORTED_ATTRIBUTES]. Each must have a corresponding
#'   `data/los_ts_<attribute>.parquet` file generated by
#'   [los_summary_by_attributes_all()].
#' @param los_ts_summary_path Path to the aggregate LOS time series summary
#'   Parquet file (default: `"data/los_ts_summary.parquet"`).
#' @param data_dir Directory containing per-attribute Parquet files
#'   (default: `"data"`).
#'
#' @return A tidy long-form data frame with columns:
#'   - `attribute`: the subgroup dimension (`"overall"` or attribute name)
#'   - `subgroup`: the subgroup value (e.g., institution name, `NA` for overall)
#'   - `date`: the snapshot date
#'   - `metric`: the metric name (prefixes stripped, e.g., `"osr"`, `"outputs"`)
#'   - `moment`: one of `"value"`, `"lag"`, `"delta"`, `"growth"`
#'   - `value`: the numeric value
#'   - `value_type`: `"count"` or `"proportion"`
#'
#' @examples
#' \dontrun{
#' # Generate full reporting dataset for all attributes
#' report_df <- generate_reporting_dataset()
#'
#' # Generate for a subset of attributes
#' report_df <- generate_reporting_dataset(
#'   subgroup_vars = c("funded", "institution")
#' )
#'
#' # Save to disk
#' arrow::write_parquet(report_df, "data/reporting_dataset.parquet")
#' }
#'
#' @seealso [summary_longform()], [generate_value_types()],
#'   [los_summary_by_attributes_all()], [SUPPORTED_ATTRIBUTES]
#' @export
generate_reporting_dataset <- function(
  subgroup_vars = cosr::SUPPORTED_ATTRIBUTES,
  los_ts_summary_path = "data/los_ts_summary.parquet",
  data_dir = "data"
) {
  # Checks
  if (!file.exists(los_ts_summary_path)) {
    stop(
      "Aggregate summary file '",
      los_ts_summary_path,
      "' not found.\n",
      "Please run event_history_monthly() first.",
      call. = FALSE
    )
  }

  # Step 1: Aggregate (overall) summary
  message("Processing: overall")
  overall <- arrow::read_parquet(los_ts_summary_path) |>
    summary_longform() |>
    dplyr::mutate(attribute = "overall", subgroup = NA_character_)

  # Step 2: Per-attribute summaries
  attr_results <- purrr::map(subgroup_vars, function(attr) {
    file_path <- file.path(
      data_dir,
      paste0("los_ts_", tolower(attr), ".parquet")
    )

    if (!file.exists(file_path)) {
      warning(
        "File '",
        file_path,
        "' not found. Skipping '",
        attr,
        "'.",
        call. = FALSE
      )
      return(NULL)
    }

    message("Processing: ", attr)

    arrow::read_parquet(file_path) |>
      summary_longform(!!rlang::sym(attr)) |>
      dplyr::rename(subgroup = !!rlang::sym(attr)) |>
      dplyr::mutate(
        attribute = attr,
        subgroup = as.character(subgroup)
      )
  }) |>
    purrr::compact() # Drop any NULLs from skipped attributes

  # Step 3: Combine and add value types
  dplyr::bind_rows(overall, !!!attr_results) |>
    dplyr::relocate(attribute, subgroup, date, metric, moment, value) |>
    generate_value_types()
}


# Lookup: how each moment maps to a subtable header in the Google Sheet
GSHEET_SUBTABLE_LABELS <- c(
  value = "TOTALS",
  delta = "NET CHANGE",
  growth = "% GROWTH"
)


#' Format the overall reporting dataset as a wide annual Google Sheet
#'
#' Builds a named list of data frames — one per year — each structured to match
#' the CSV template ("LOS Indicators on OSF - YYYY.csv"). Each data frame
#' contains three subtables (TOTALS, NET CHANGE, % GROWTH) stacked vertically
#' with two blank separator rows between them. Columns are months 1–12.
#'
#' @param report_df The long-form reporting data frame produced by
#'   [generate_reporting_dataset()]. Only rows where `attribute == "overall"`
#'   are used.
#' @param years Integer vector of years to generate sheets for. Defaults to all
#'   years present in `report_df`.
#' @param pretty_numbers Logical. If `TRUE` (default), formats numbers for
#'   readability: large integers get thousand separators (e.g., "123,456") and
#'   proportions are converted to percentages with 2 decimal places (e.g., "1.23").
#'   If `FALSE`, returns raw numeric values.
#'
#' @return A named list of data frames, one per year (e.g. `list("2024" = ...,
#'   "2025" = ...)`). Each data frame has a leading `Indicator` column and
#'   columns `1`–`12` for each calendar month.
#'
#' @examples
#' \dontrun{
#' report_df <- generate_reporting_dataset()
#' annual    <- format_gsheet_annual(report_df)
#' write_gsheet_report(annual)
#'
#' # With raw numbers (no formatting)
#' annual_raw <- format_gsheet_annual(report_df, pretty_numbers = FALSE)
#' }
#'
#' @seealso [write_gsheet_report()], [generate_reporting_dataset()]
#' @export
format_gsheet_annual <- function(
  report_df,
  years = NULL,
  pretty_numbers = TRUE
) {
  rlang::check_installed("lubridate", reason = "to parse year/month from dates")

  # Work with overall rows only; add year and month columns
  base <- report_df |>
    dplyr::ungroup() |>
    dplyr::filter(attribute == "overall") |>
    dplyr::mutate(
      year = lubridate::year(as.Date(date)),
      month = lubridate::month(as.Date(date))
    )

  if (is.null(years)) {
    years <- sort(unique(base$year))
  }

  # Metric rows in display order, matching the CSV template.
  # Labels are derived from REPORTING_LABELS so they stay in sync.
  metric_rows <- tibble::tribble(
    ~metric    , ~value_type  , ~key            ,
    "osr"      , "count"      , "n_osr"         ,
    "outputs"  , "count"      , "have_outputs"  ,
    "outcomes" , "count"      , "have_outcomes" ,
    "los"      , "count"      , "are_los"       ,
    "los"      , "proportion" , "prop_los"
  ) |>
    dplyr::mutate(label = REPORTING_LABELS[key]) |>
    dplyr::select(-key)

  # Blank separator row (Indicator + 12 empty month columns)
  sep_row <- tibble::tibble(
    Indicator = NA_character_,
    !!!purrr::set_names(as.list(rep(NA_real_, 12)), as.character(1:12))
  )

  purrr::set_names(
    purrr::map(years, function(yr) {
      year_data <- base |> dplyr::filter(year == yr)

      # Build one subtable per moment (value, delta, growth)
      subtables <- purrr::map(names(GSHEET_SUBTABLE_LABELS), function(mom) {
        # Header row: subtable label in Indicator column, month numbers as values
        header_row <- tibble::tibble(
          Indicator = GSHEET_SUBTABLE_LABELS[[mom]],
          !!!purrr::set_names(as.list(as.double(1:12)), as.character(1:12))
        )

        # One data row per metric
        data_rows <- purrr::pmap_dfr(
          metric_rows,
          function(metric, value_type, label) {
            # Pull values for this metric/value_type/moment, one row per month
            vals <- year_data |>
              dplyr::filter(
                .data$metric == .env$metric,
                .data$value_type == .env$value_type,
                .data$moment == .env$mom
              ) |>
              dplyr::select(month, value) |>
              tidyr::pivot_wider(
                names_from = month,
                values_from = value
              )

            # Ensure all 12 month columns exist (fill gaps with NA)
            for (m in setdiff(as.character(1:12), names(vals))) {
              vals[[m]] <- NA_real_
            }
            vals <- dplyr::select(vals, dplyr::all_of(as.character(1:12)))

            dplyr::bind_cols(tibble::tibble(Indicator = label), vals)
          }
        )

        dplyr::bind_rows(header_row, data_rows)
      })

      # Stack subtables with two blank rows between each
      result <- purrr::reduce(subtables, \(acc, tbl) {
        dplyr::bind_rows(acc, sep_row, sep_row, tbl)
      })

      # Apply pretty number formatting if requested
      if (pretty_numbers) {
        # Rows with percentage values (proportion rows in each subtable):
        # Row 6: TOTALS subtable
        # Row 14: NET CHANGE subtable
        # Row 22: % GROWTH subtable
        prop_rows <- c(6, 14, 22)

        result <- result |>
          dplyr::mutate(
            row_num = dplyr::row_number(),
            dplyr::across(
              dplyr::matches("^[0-9]+$"), # Month columns (1-12)
              ~ {
                curr_row <- dplyr::row_number()
                dplyr::case_when(
                  # Skip header rows (month numbers as values)
                  . < 13 & . == as.numeric(dplyr::cur_column()) ~ as.character(
                    .
                  ),
                  # Skip NA values
                  is.na(.) ~ NA_character_,
                  # Format proportion rows as percentages (multiply by 100, 2 decimals)
                  curr_row %in% prop_rows ~ sprintf("%.2f", . * 100),
                  # Otherwise format as integer with commas
                  TRUE ~ format(
                    round(.),
                    big.mark = ",",
                    scientific = FALSE,
                    trim = TRUE
                  )
                )
              }
            )
          ) |>
          dplyr::select(-row_num)
      }

      result
    }),
    as.character(years)
  )
}


#' Write annual LOS indicator reports to a Google Sheet
#'
#' Takes the output of [format_gsheet_annual()] and writes each year as its own
#' tab in a Google Sheet. If the sheet does not yet exist it is created; if it
#' already exists the relevant tabs are overwritten.
#'
#' New year tabs are added to the leftmost position, so the most recent year
#' appears first when the sheet is opened. Existing tabs retain their position.
#'
#' The first column (Indicator labels) is automatically resized to fit content.
#'
#' Authentication is handled by `googlesheets4`. Run
#' `googlesheets4::gs4_auth()` interactively before calling this function for
#' the first time, or supply a service-account token via the `token` argument.
#'
#' @param annual_list Named list of data frames as returned by
#'   [format_gsheet_annual()].
#' @param sheet_name Title of the Google Sheet to create or update
#'   (default: `"LOS Indicators on OSF"`).
#' @param sheet_id Google Sheet ID or URL to update an existing sheet.
#'   If `NULL` (default) a new sheet is created.
#' @param token Optional OAuth token or path to a service-account JSON file
#'   passed to `googlesheets4::gs4_auth()`. If `NULL`, uses the currently
#'   cached credentials.
#'
#' @return The Google Sheet ID (invisibly).
#'
#' @examples
#' \dontrun{
#' # Authenticate (once per session)
#' googlesheets4::gs4_auth()
#'
#' # Generate, format, and write
#' report_df <- generate_reporting_dataset()
#' annual    <- format_gsheet_annual(report_df)
#' write_gsheet_report(annual, sheet_name = "LOS Indicators on OSF")
#'
#' # Update existing sheet by ID
#' write_gsheet_report(annual, sheet_id = "your-sheet-id-here")
#' }
#'
#' @seealso [format_gsheet_annual()], [generate_reporting_dataset()]
#' @export
write_gsheet_report <- function(
  annual_list,
  sheet_name = "LOS Indicators on OSF",
  sheet_id = NULL,
  token = NULL
) {
  rlang::check_installed("googlesheets4", reason = "to write Google Sheets")

  # Authenticate if a token is provided
  if (!is.null(token)) {
    googlesheets4::gs4_auth(token = token)
  }

  # Create or locate the sheet
  if (is.null(sheet_id)) {
    message("Creating new Google Sheet: '", sheet_name, "'")
    ss <- googlesheets4::gs4_create(sheet_name)
  } else {
    message("Using existing Google Sheet: ", sheet_id)
    ss <- googlesheets4::as_sheets_id(sheet_id)
  }

  # Write each year to its own tab
  # Process in reverse order so newest years appear leftmost
  years_reversed <- rev(names(annual_list))

  purrr::walk(years_reversed, function(yr) {
    tbl <- annual_list[[yr]]
    tab_name <- as.character(yr)
    message("  Writing tab: ", tab_name)

    existing_sheets <- googlesheets4::sheet_names(ss)

    if (tab_name %in% existing_sheets) {
      # Update existing tab
      googlesheets4::sheet_write(tbl, ss = ss, sheet = tab_name)
    } else {
      # Add new tab at the beginning (leftmost position)
      googlesheets4::sheet_add(ss, sheet = tab_name)
      googlesheets4::sheet_relocate(ss, sheet = tab_name, .before = 1)
      googlesheets4::sheet_write(tbl, ss = ss, sheet = tab_name)
    }

    # Auto-fit first column only (Indicator names)
    googlesheets4::range_autofit(ss, sheet = tab_name, range = "A:A")
  })

  message("✓ Done! Sheet ID: ", googlesheets4::as_sheets_id(ss))
  invisible(googlesheets4::as_sheets_id(ss))
}
