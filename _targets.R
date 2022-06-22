# devtools::install_github("george-wood/cpdata", force = TRUE)
pacman::p_load(targets, fs, data.table, purrr, cpdata)

source("R/functions.R")

# options
tar_option_set(
  format   = "parquet",
  packages = c("cpdata", "arrow", "dplyr", "lubridate")
)

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
    list_file("tickets/", reg = "parking"),
    format = "file"
  ),


  # preprocess and write
  tar_target(
    assignment,
    write_data(
      data_name = "assignment",
      tidy_assignment(f.assignment)
    )
  ),
  tar_target(
    arrest,
    write_data(
      data_name = "arrest",
      tidy_arrest(f.arrest_report, f.arrest_officer) |>
        join(by = c("last_name",
                    "first_name",
                    "appointed",
                    "birth"))
    )
  ),
  tar_target(
    contact,
    write_data(
      data_name = "contact",
      tidy_contact(f.contact) |>
        join(by = c("last_name",
                    "first_name",
                    "birth >= birth_lower",
                    "birth <= birth_upper"))
    )
  ),
  tar_target(
    force,
    write_data(
      data_name = "force",
      tidy_force(f.force_report, f.force_action) |>
        join(by = c("last_name",
                    "first_name",
                    "appointed",
                    "birth >= birth_lower",
                    "birth <= birth_upper"))
    )
  ),
  tar_target(
    isr,
    write_data(
      data_name = "isr",
      tidy_isr(f.isr) |>
        join(by = c("last_name",
                    "first_name",
                    "appointed",
                    "birth"))
    )
  ),
  tar_target(
    ticket,
    write_data(
      data_name = "ticket",
      tidy_ticket(f.ticket, zip = TRUE) |>
        join(by = c("star"))
    )
  )



)


