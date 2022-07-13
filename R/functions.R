list_file <- function(path, reg = "\\.csv$", ...) {
  dir_ls(
    path    = paste0("~/Documents/data/cpd/", path),
    regexp  = reg,
    recurse = TRUE,
    ...
  )
}

write_data <- function(df, data_name = NULL) {

  if (!fs::dir_exists("data/")) {
    fs::dir_create("data/")
  }

  if (is.null(data_name)) {
    stop("Must specify data_name")
  }

  dtcol <- ifelse("dt" %in% colnames(df), "dt", "dt_start")

  arrow::write_dataset(
    dataset = group_by(df, year := year(!!rlang::sym(dtcol))),
    path    = paste0("data/", data_name),
    format  = "parquet"
  )

  message("dataset written: ", data_name)
  d <- data.frame(
    report = paste0("generated dataset: ", data_name, " (", Sys.time(), ")")
  )
  return(d)

}



#
# library(arrow)
# library(data.table)
# library(tidyverse)
# library(lubridate)
# assignment        = "data/assignment/"
# return_all_events = TRUE
# event             = arrow::open_dataset("data/arrest") |> dplyr::collect()
# by                = c("last_name", "first_name", "appointed", "birth", "initial")

#' to implement:
#' overtime -- setDT(assignment)[, dt_overtime := dt_end + (6*60*60)]
#' closest shift?

join <- function(
    event,
    assignment,
    by = c("last_name", "first_name", "appointed"),
    return_all_events = TRUE
) {

  assignment <- setDT(assignment)[
    (present_for_duty), c("aid", "oid", "dt_start", "dt_end", ..by)
  ]

  # set id and by
  id <- "eid"
  by_dt <- c(by, "dt_start <= dt", "dt_end >= dt")

  # join event and assignment data
  k <- assignment[setDT(event), on = by_dt]

  # action sometimes incorrectly recorded around midnight
  unmatched <- copy(event)[
    !k[!is.na(aid)], on = id][
      , dt := fifelse(hour(dt) < 12, dt + days(1), dt - days(1))
    ]
  l <- assignment[unmatched, on = by_dt]

  # bind and reduce
  if ("role" %in% names(event)) {
    id <- c(id, "role")
  }

  success <- unique(rbind(k, l)[!is.na(aid), c("aid", "oid", ..by, ..id)])
  nested  <- success[, list(aid = list(aid)), by = c(by, id, "oid")]

  # return full events data with aid and oid columns
  if (return_all_events) {
    d <- nested[event, on = c(by, id)]
    d[lengths(aid) == 0]$aid <- NA_character_ # list column
    setcolorder(d, neworder = names(event))
    stopifnot(nrow(d) == nrow(event))
  }

  return(d)

}
