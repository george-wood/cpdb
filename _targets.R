#devtools::install_github("george-wood/cpdata", force = TRUE)
pacman::p_load(targets, fs, data.table, purrr, cpdata)

# options
tar_option_set(
  format   = "parquet",
  packages = c("cpdata", "arrow", "dplyr")
)

# functions
list_file <- function(path, reg = "\\.csv$", ...) {
  dir_ls(
    path    = paste0("~/Documents/data/cpd/", path),
    regexp  = reg,
    recurse = TRUE,
    ...
  )
}

write_data <- function(upstream) {

  if (!fs::dir_exists("data/")) {
    fs::dir_create("data/")
  }

  up <-
    sub(
      x = sub(x = deparse(substitute(upstream)), ".*f[.]", ""),
      "[_|)|,].*", ""
    )

  dtcol <- ifelse("dt" %in% colnames(upstream), "dt", "dt_start")

  arrow::write_dataset(
    dataset = group_by(upstream, year := year(!!rlang::sym(dtcol))),
    path = paste0("data/", up),
    format = "parquet"
  )

  message("dataset written: ", up)
  d <- data.frame(
    report = paste0("generated dataset: ", up, " (", Sys.time(), ")")
  )
  return(d)

}

# targets
list(

  # files
  tar_target(
    f.arrest_report,
    list_file("arrest/p701162"),
    format = "file"
  ),
  tar_target(
    f.arrest_officer,
    list_file("arrest/p708085"),
    format = "file"
  ),
  tar_target(
    f.assignment,
    list_file("assignment/p602033"),
    format = "file"
  ),
  tar_target(
    f.contact,
    list_file("contact_cards/p058306"),
    format = "file"
  ),
  tar_target(
    f.force_report,
    list_file("trr/", reg = "0\\.csv$|p583646.*1\\.csv$"),
    format = "file"
  ),
  tar_target(
    f.force_action,
    list_file("trr/", reg = "2\\.csv$"),
    format = "file"
  ),
  tar_target(
    f.isr,
    list_file("isr/p646845"),
    format = "file"
  ),
  tar_target(
    f.ticket,
    list_file("tickets", reg = "parking"),
    format = "file"
  ),

  # preprocess and write
  tar_target(
    arrest,
    write_data(tidy_arrest(f.arrest_report, f.arrest_officer))
  ),
  tar_target(
    assignment,
    write_data(rbindlist(map(f.assignment, tidy_assignment)))
  ),
  tar_target(
    contact,
    write_data(rbindlist(map(f.contact, tidy_contact)))
  ),
  tar_target(
    force,
    write_data(tidy_force(f.force_report, f.force_action))
  ),
  tar_target(
    isr,
    write_data(tidy_isr(f.isr))
  ),
  tar_target(
    ticket,
    write_data(tidy_ticket(f.ticket, zip = TRUE))
  )


)


