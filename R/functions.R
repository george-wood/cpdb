list_file <- function(path, reg = "\\.csv$", ...) {
  dir_ls(
    path    = paste0("~/Documents/data/cpd/", path),
    regexp  = reg,
    recurse = TRUE,
    ...
  )
}

write_data <- function(file, data_name = NULL) {

  if (!fs::dir_exists("data/")) {
    fs::dir_create("data/")
  }

  if (is.null(data_name)) {
    stop("Must specify data_name")
  }

  dtcol <- ifelse("dt" %in% colnames(file), "dt", "dt_start")

  arrow::write_dataset(
    dataset = group_by(file, year := year(!!rlang::sym(dtcol))),
    path = paste0("data/", data_name),
    format = "parquet"
  )

  message("dataset written: ", data_name)
  d <- data.frame(
    report = paste0("generated dataset: ", data_name, " (", Sys.time(), ")")
  )
  return(d)

}



# #
# library(arrow)
# library(data.table)
# library(tidyverse)
# library(lubridate)
# assignment = "data/assignment/"
# return_all_events = TRUE
# event = arrow::open_dataset("data/isr") |> dplyr::collect()
# by = c("last_name",       "first_name",       "appointed",       "birth")
# #


join <- function(
    event,
    assignment = "data/assignment/",
    by = c("last_name", "first_name", "appointed"),
    return_all_events = TRUE
) {

  assignment <-
    open_dataset(assignment) |>
    select(
      "last_name", "first_name", "appointed", "birth", "star",
      "dt_start", "dt_end", "aid", "oid"
    ) |>
    collect()

  # set id and by
  id <- "eid"
  by <- c(by, "dt_start <= dt", "dt_end >= dt")

  # join event and assignment data
  k <- setDT(assignment)[setDT(event), on = by]

  # action sometimes incorrectly recorded around midnight
  unmatched <- copy(event)[
    !k[!is.na(aid)], on = id][
      , dt := fifelse(hour(dt) < 12, dt + days(1), dt - days(1))
    ]
  l <- assignment[unmatched, on = by]

  # bind and reduce
  if ("role" %in% names(event)) {
    id <- c(id, "role")
  }

  d <- event[unique(rbind(k, l)[!is.na(aid), c("aid", "oid", ..id)]), on = id]

  # return fill events data with aid and oid columns
  if (return_all_events) {

    ## option 1, do not nest
    d <- unique(d[, c("aid", "oid", ..id)])[event, on = id]

    ## option 2, nest aid
    # d <- d[, list(aid = list(.SD)), by = setdiff(names(d), "aid")]

    ## option 3, nest aid and oid (not sure this works)
    # d <- d[, list(aid = list(aid), oid = list(oid)),
    #        by = setdiff(names(d), c("aid", "oid"))]

    ## check
    # stopifnot(nrow(d) == nrow(event))
  }

  return(d)

}
