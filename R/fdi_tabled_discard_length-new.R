#' @name fdi_tabled_discard_length
#' @title Table D NAO OFR discards length generation (FDI process)
#' @description Process for generation and optionally extraction of the FDI table D (NAO OFR discards length).
#' @param year {\link[base]{integer}} expected. By default NULL. Year of interest.
#' @param ocean {\link[base]{character}} expected. By default NULL. Ocean of interest.
#' @param program {\link[base]{character}} expected. By default NULL. Programs of interest.
#' @param tablea_catch_summary {\link[base]{list}} expected. Output "table_a" of the function {\link[acdc]{fdi_tablea_catch_summary}}.
#' @param template_checking {\link[base]{logical}} expected. By default FALSE Checking FDI table generated regarding the official FDI template.
#' @param table_export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @return The process returns a list with the FDI table D inside.
#' @export
#' @importFrom DBI dbGetQuery sqlInterpolate SQL
#' @importFrom dplyr as_tibble tibble mutate relocate rename filter summarise grouip_by case_when select left_join
#' @importFrom tidyr uncount
#' @importFrom lubridate quarter
#' @importFrom codama r_type_checking

fdi_tabled_discard_length <- function(year = NULL,
                                      ocean = NULL,
                                      program = NULL,
                                      flag = NULL,
                                      tablea_catch_summary,
                                      template_checking = FALSE,
                                      table_export_path = NULL) {

  cat(format(x = Sys.time(), format = "%Y-%m-%d %H:%M:%S"),
      " - Start process - FDI table D generation.\n",
      sep = "")

  # global variables assignment ----
  # totwghtlandg <- NULL
  # discards <- NULL
  # no_samples <- NULL
  # country <- NULL
  # year <- NULL
  # domain_discards <- NULL
  # nep_sub_region <- NULL
  # species <- NULL
  # TOTWGHTLANDG <- NULL
  # DISCARDS <- NULL
  # discard_cv <- NULL
  # discard_ci_upper <- NULL
  # discard_ci_lower <- NULL
  # total_trips <- NULL
  # total_sampled_trips <- NULL
  # no_length_measurements <- NULL
  # length_unit <- NULL
  # min_length <- NULL
  # max_length <- NULL
  # no_length <- NULL
  # mean_weight_at_length <- NULL
  # weight_unit <- NULL

  # arguments verifications ----
  # if (codama::r_type_checking(r_object = observe_discard_path,
  #                             type = "character",
  #                             length = 1L,
  #                             output = "logical") != TRUE) {
  #   return(codama::r_type_checking(r_object = observe_discard_path,
  #                                  type = "character",
  #                                  length = 1L,
  #                                  output = "message"))
  # }
  # if (codama::r_type_checking(r_object = template_checking,
  #                             type = "logical",
  #                             output = "logical") != TRUE) {
  #   return(codama::r_type_checking(r_object = template_checking,
  #                                  type = "logical",
  #                                  output = "message"))
  # }
  # if (codama::r_type_checking(r_object = template_year,
  #                             type = "integer",
  #                             output = "logical") != TRUE) {
  #   return(codama::r_type_checking(r_object = template_year,
  #                                  type = "integer",
  #                                  output = "message"))
  # }
  # if (codama::r_type_checking(r_object = table_export_path,
  #                             type = "character",
  #                             length = 1L,
  #                             output = "logical") != TRUE) {
  #   return(codama::r_type_checking(r_object = table_export_path,
  #                                  type = "character",
  #                                  length = 1L,
  #                                  output = "message"))
  # }

  # data extraction ----
  cat(format(x = Sys.time(), format = "%Y-%m-%d %H:%M:%S"),
      " - Start process - Extracting discards samples.\n",
      sep = "")

  # observe_sample_sql_query_initial <- paste(readLines(con = system.file("sql", "fdi", "observe_sample_fdi.sql", package = "acdc")), collapse = "\n")
  observe_sample_sql_query_initial <- paste(readLines("/Users/philippe/Dropbox/r-development/acdc/inst/sql/fdi/observe_sample_fdi.sql"), collapse = "\n")

  observe_con <- postgresql_dbconnection(db_user = config[["databases_configuration"]][["observe_vmot6"]][["login"]],
                                         db_password = config[["databases_configuration"]][["observe_vmot6"]][["password"]],
                                         db_dbname = config[["databases_configuration"]][["observe_vmot6"]][["dbname"]],
                                         db_host = config[["databases_configuration"]][["observe_vmot6"]][["host"]],
                                         db_port = config[["databases_configuration"]][["observe_vmot6"]][["port"]])[[2]]

  observe_sample_sql_query_final <- DBI::sqlInterpolate(conn = observe_con,
                                                        sql  = observe_sample_sql_query_initial,
                                                        start_year = DBI::SQL(paste0(paste0(min(period),collapse = ", "))),
                                                        end_year = DBI::SQL(paste0(paste0(max(period),collapse = ", "))),
                                                        program = DBI::SQL(paste0("'", paste0(program, collapse = "', '"),"'")),
                                                        ocean = DBI::SQL(paste0("'", paste0(ocean, collapse = "', '"),"'")),
                                                        flag = DBI::SQL(paste0("'", paste0(flag, collapse = "', '"),"'")))

  observe_sample <- DBI::dbGetQuery(conn = observe_con,
                                    statement = observe_sample_sql_query_final)

  # dbDisconnect(observe_con)

  cat(format(x = Sys.time(), format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process - Extracting discards samples.\n",
      sep = "")

  # data design ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process - Table D design.\n",
      sep = "")

  observe_sample <- as_tibble(observe_sample) %>%
    tidyr::uncount(weights = count) %>% # one line per measurement
    dplyr::mutate(count = 1) %>% # count is 1 after untable
    dplyr::relocate(count, .before = length_type) %>% # relocate field count
    dplyr::mutate(length = floor(length)) %>% # floor all measurements
    dplyr::mutate(length = as.integer(length)) %>% # numeric to integer
    dplyr::rename(species = fao_code) %>% # rename species column
    dplyr::mutate(species = dplyr::case_when(species %in% c("3DEY") ~ "DIO",
                                             species %in% c("XXX*") ~ "MZZ",
                                             TRUE ~ species)) # rename non-FAO codes

  observe_sample_discard <- observe_sample %>%
    dplyr::filter(is.na(fate_code) == F) %>% # remove samples with not fate
    dplyr::filter(fate_code %in% c(1,2,3,4,5,9,10,11,13,14)) # discards only

  observe_sample_discard <- suppressMessages(furdeb::marine_area_overlay(data = observe_sample_discard,
                                                                         overlay_expected = "fao_area",
                                                                         longitude_name = "longitude",
                                                                         latitude_name = "latitude",
                                                                         fao_area_file_path = fao_area_file_path,
                                                                         fao_overlay_level = "division",
                                                                         auto_selection_fao = TRUE,
                                                                         silent = TRUE)) %>% # add fao areas columns
    dplyr::mutate(sub_region = dplyr::case_when(best_fao_area %in% c("41.1.4") ~ "41.1",
                                                best_fao_area %in% c("47.A.0",
                                                                     "47.A.1") ~ "47.A",
                                                best_fao_area %in% c("47.B.1") ~ "47.B",
                                                best_fao_area %in% c("47.1.1",
                                                                     "47.1.2",
                                                                     "47.1.3",
                                                                     "47.1.4",
                                                                     "47.1.5",
                                                                     "47.1.6") ~ "47.1",
                                                best_fao_area %in% c("47.2.2") ~ "47.2",
                                                best_fao_area %in% c("34.2") ~ "34.2.0",
                                                TRUE ~ best_fao_area)) %>% # find best fao area
    dplyr::mutate(nep_sub_region = as.character(NA)) %>% # add nep sub region
    dplyr::mutate(country = dplyr::case_when(country == "MYT" ~ "FRA",
                                             TRUE ~ country)) %>% # recode MYT as FRA
    dplyr::mutate(quarter = lubridate::quarter(observe_sample_discard$observation_date)) %>% # add quarter
    dplyr::mutate(domain_discards = paste(country,
                                          quarter,
                                          best_fao_area,
                                          paste0("PS-",school_type),
                                          "LPF_NA_NA_NA",
                                          "VL40XX",
                                          species,
                                          "NA",
                                          sep = "_")) # build domain

  observe_sampling_stratum <- observe_sample_discard %>%
    dplyr::group_by(country,
                    year,
                    domain_discards) %>%
    dplyr::summarise(total_sampled_trips = length(unique(trip_id)),
                     no_length_measurements = sum(count),
                     min_length = min(length),
                     max_length = max(length),
                     .groups = "drop") # calculate information in sampling stratum

  observe_sample_discard_agg <- observe_sample_discard %>%
    dplyr::group_by(country,
                    year,
                    domain_discards,
                    nep_sub_region,
                    species,
                    length_type,
                    length) %>%
    dplyr::summarise(no_length = sum(count),
                     mean_weight_at_length = mean(weight),
                     .groups = "drop") # aggregate samples

  observe_sample_discard_agg <- observe_sample_discard_agg %>%
    dplyr::left_join(observe_sampling_stratum,
                     by = c("country",
                            "year",
                            "domain_discards")) # join to get sampling stratum information

  names(observe_sample_discard_agg) <- toupper(names(observe_sample_discard_agg))

  table_d_final <- observe_sample_discard_agg %>%
    dplyr::left_join(tablea_catch_summary[, c("COUNTRY",
                                              "YEAR",
                                              "DOMAIN_DISCARDS",
                                              "NEP_SUB_REGION",
                                              "SPECIES",
                                              "TOTWGHTLANDG",
                                              "DISCARDS")],
                     by = c("COUNTRY",
                            "YEAR",
                            "DOMAIN_DISCARDS",
                            "NEP_SUB_REGION",
                            "SPECIES")) %>% # join table a to get TOTWGHTLANDG and DISCARDS
    dplyr::filter(is.na(TOTWGHTLANDG) == F,
                  is.na(DISCARDS) == F) %>% # remove lines where TOTWGHTLANDG and/or DISCARDS are NA
    dplyr::mutate(DISCARD_CV = "NK",
                  DISCARD_CI_UPPER = "NK",
                  DISCARD_CI_LOWER = "NK",
                  TOTAL_TRIPS = "NK",
                  LENGTH_UNIT = "cm",
                  NO_LENGTH = round(NO_LENGTH/1000, digits = 3),
                  WEIGHT_UNIT = "kg") %>% # complete value in the concerned columns
    dplyr::select(COUNTRY,
                  YEAR,
                  DOMAIN_DISCARDS,
                  NEP_SUB_REGION,
                  SPECIES,
                  TOTWGHTLANDG,
                  DISCARDS,
                  DISCARD_CV,
                  DISCARD_CI_UPPER,
                  DISCARD_CI_LOWER,
                  TOTAL_TRIPS,
                  TOTAL_SAMPLED_TRIPS,
                  NO_LENGTH_MEASUREMENTS,
                  LENGTH_UNIT,
                  MIN_LENGTH,
                  MAX_LENGTH,
                  LENGTH,
                  NO_LENGTH,
                  MEAN_WEIGHT_AT_LENGTH,
                  WEIGHT_UNIT) # select final columns

  cat(format(x = Sys.time(), format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process - Table D design.\n",
      sep = "")

  # template checking ----
  if (template_checking == TRUE) {
    fdi_template_checking(fdi_table = table_d_final,
                          template_year = template_year,
                          table_id = "d")
  } else {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Warning: main table output not checking with official FDI template.\n",
        sep = "")
  }

  # export ----
  if (! is.null(x = table_export_path)) {
    fdi_table_export(fdi_table = table_d_final,
                     export_path = table_export_path,
                     table_id = "d")
  }
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process - FDI table D generation.\n",
      sep = "")
  return(list("fdi_tables" = list("table_d" = table_d_final)))
}
