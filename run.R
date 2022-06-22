library(targets)
tar_make()
tar_visnetwork(targets_only = TRUE)

# development
pacman::p_load(arrow, tidyverse, janitor, lubridate, data.table)

assignment <-
  open_dataset("data/assignment/") |>
  # select(all_of(by), "dt_start", "dt_end", "aid", "oid") |>
  collect()

event <- force <-
  open_dataset("data/force") |>
  collect()

