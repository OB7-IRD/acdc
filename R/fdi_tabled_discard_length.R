#' @name fdi_tabled_discard_length
#' @title Table D NAO OFR discards length generation (FDI process)
#' @description Process for generation and optionally extraction of the FDI table D (NAO OFR discards length).
#' @param observe_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the observe database.
#' @param period {\link[base]{integer}} expected. Year(s) period for data extraction(s).
#' @param ocean {\link[base]{character}} expected. Ocean(s) of interest.
#' @param program {\link[base]{character}} expected. Programs of interest.
#' @param flag {\link[base]{integer}} expected. Flag(s) selection for data extraction(s).
#' @param fao_area_file_path {\link[base]{character}} expected. File path of the FAO area grid. The file format has to be .Rdata or .RData extension.
#' @param tablea_catch_summary {\link[base]{list}} expected. Output "table_a" of the function {\link[acdc]{fdi_tablea_catch_summary}}.
#' @param template_checking {\link[base]{logical}} expected. By default FALSE Checking FDI table generated regarding the official FDI template.
#' @param template_year {\link[base]{integer}} expected. By default NULL. Template year.
#' @param table_export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @return The process returns a list with the FDI table D inside.
#' @importFrom codama r_type_checking file_path_checking
#' @importFrom DBI sqlInterpolate SQL dbGetQuery
#' @importFrom dplyr as_tibble mutate relocate rename filter case_when group_by summarise left_join select
#' @importFrom tidyr uncount
#' @importFrom furdeb marine_area_overlay
#' @importFrom lubridate quarter
#' @importFrom stringr str_replace fixed
#' @export
fdi_tabled_discard_length <- function(observe_con,
                                      period,
                                      ocean,
                                      program,
                                      flag,
                                      fao_area_file_path,
                                      tablea_catch_summary,
                                      template_checking = FALSE,
                                      template_year = NULL,
                                      table_export_path = NULL) {
  cat(format(x = Sys.time(), format = "%Y-%m-%d %H:%M:%S"),
      " - Start process - FDI table D generation.\n",
      sep = "")
  # 1 - global variable assignment  ----
  COUNTRY <- NULL
  DISCARDS <- NULL
  DISCARD_CI_LOWER <- NULL
  DISCARD_CI_UPPER <- NULL
  DISCARD_CV <- NULL
  DOMAIN_DISCARDS <- NULL
  LENGTH <- NULL
  LENGTH_UNIT <- NULL
  MAX_LENGTH <- NULL
  MEAN_WEIGHT_AT_LENGTH <- NULL
  MIN_LENGTH <- NULL
  NEP_SUB_REGION <- NULL
  NO_LENGTH <- NULL
  NO_LENGTH_MEASUREMENTS <- NULL
  SPECIES <- NULL
  TOTAL_SAMPLED_TRIPS <- NULL
  TOTAL_TRIPS <- NULL
  TOTWGHTLANDG <- NULL
  WEIGHT_UNIT <- NULL
  YEAR <- NULL
  best_fao_area <- NULL
  count <- NULL
  country <- NULL
  domain_discards <- NULL
  fao_code <- NULL
  fate_code <- NULL
  length_type <- NULL
  nep_sub_region <- NULL
  school_type <- NULL
  species <- NULL
  tablea <- NULL
  trip_id <- NULL
  weight <- NULL
  year <- NULL
  # 2 - Arguments verifications ----
  if (codama::r_type_checking(r_object = observe_con,
                              type = "PostgreSQLConnection",
                              length = 1L,
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = observe_con,
                                   type = "PostgreSQLConnection",
                                   length = 1L,
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = period,
                              type = "integer",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = period,
                                   type = "integer",
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = ocean,
                              type = "character",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = ocean,
                                   type = "character",
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = program,
                              type = "character",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = program,
                                   type = "character",
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = flag,
                              type = "integer",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = flag,
                                   type = "integer",
                                   output = "message"))
  }
  if (codama::file_path_checking(file_path =  fao_area_file_path,
                                 extension = c("Rdata",
                                               "RData"),
                                 output = "logical") != TRUE) {
    return(codama::file_path_checking(file_path =  fao_area_file_path,
                                      extension = c("Rdata",
                                                    "RData"),
                                      output = "message"))
  }
  if (codama::r_type_checking(r_object = template_checking,
                              type = "logical",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = template_checking,
                                   type = "logical",
                                   output = "message"))
  }
  if ((! is.null(x = template_year))
      && codama::r_type_checking(r_object = template_year,
                                 type = "integer",
                                 output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = template_year,
                                   type = "integer",
                                   output = "message"))
  }
  if ((! is.null(x = table_export_path))
      && codama::r_type_checking(r_object = table_export_path,
                                 type = "character",
                                 length = 1L,
                                 output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = table_export_path,
                                   type = "character",
                                   length = 1L,
                                   output = "message"))
  }
  # 3 - Data extraction ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process - Extracting discards samples.\n",
      sep = "")
  observe_sample_sql_query_initial <- paste(readLines(con = system.file("sql",
                                                                        "fdi",
                                                                        "observe_sample_fdi.sql",
                                                                        package = "acdc")),
                                            collapse = "\n")
  observe_sample_sql_query_final <- DBI::sqlInterpolate(conn = observe_con,
                                                        sql  = observe_sample_sql_query_initial,
                                                        start_year = DBI::SQL(paste0(paste0(min(period),
                                                                                            collapse = ", "))),
                                                        end_year = DBI::SQL(paste0(paste0(max(period),
                                                                                          collapse = ", "))),
                                                        program = DBI::SQL(paste0("'", paste0(program,
                                                                                              collapse = "', '"),
                                                                                  "'")),
                                                        ocean = DBI::SQL(paste0("'", paste0(ocean,
                                                                                            collapse = "', '"),
                                                                                "'")),
                                                        flag = DBI::SQL(paste0("'", paste0(flag,
                                                                                           collapse = "', '"),
                                                                               "'")))
  observe_sample <- DBI::dbGetQuery(conn = observe_con,
                                    statement = observe_sample_sql_query_final)
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process - Extracting discards samples.\n",
      sep = "")
  # 4 - Data design ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process - Table D design.\n",
      sep = "")
  observe_sample <- dplyr::as_tibble(x = observe_sample) %>%
    tidyr::uncount(weights = count) %>% # one line per measurement
    dplyr::mutate(count = 1) %>% # count is 1 after untable
    dplyr::relocate(count,
                    .before = length_type) %>% # relocate field count
    dplyr::mutate(length = floor(x = length)) %>% # floor all measurements
    dplyr::mutate(length = as.integer(x = length)) %>% # numeric to integer
    dplyr::rename(species = fao_code) %>% # rename species column
    dplyr::mutate(species = dplyr::case_when(species %in% c("3DEY") ~ "DIO",
                                             species %in% c("XXX*") ~ "MZZ",
                                             TRUE ~ species)) # rename non-FAO codes
  observe_sample_discard <- observe_sample %>%
    dplyr::filter(is.na(x = fate_code) == FALSE) %>% # remove samples with not fate
    dplyr::filter(fate_code %in% c(1, 2, 3, 4, 5, 9, 10, 11, 13, 14)) # discards only
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
    dplyr::mutate(nep_sub_region = "NA") %>% # add nep sub region
    dplyr::mutate(country = dplyr::case_when(country == "MYT" ~ "FRA",
                                             TRUE ~ country)) %>% # recode MYT as FRA
    dplyr::mutate(quarter = lubridate::quarter(observe_sample_discard$observation_date)) %>% # add quarter
    dplyr::mutate(domain_discards = paste(country,
                                          quarter,
                                          best_fao_area,
                                          paste0("PS-",
                                                 school_type),
                                          "LPF_NA_NA_NA",
                                          "VL40XX",
                                          species,
                                          "NA",
                                          sep = "_")) # build domain
  observe_sampling_stratum <- observe_sample_discard %>%
    dplyr::group_by(country,
                    year,
                    domain_discards) %>%
    dplyr::summarise(total_sampled_trips = length(x = unique(trip_id)),
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
                     .groups = "drop") %>% # aggregate samples
    dplyr::mutate(mean_weight_at_length = dplyr::case_when(
      is.na(x = mean_weight_at_length) ~ "NK",
      TRUE ~ as.character(x = mean_weight_at_length)
    ))
  observe_sample_discard_agg <- observe_sample_discard_agg %>%
    dplyr::left_join(observe_sampling_stratum,
                     by = c("country",
                            "year",
                            "domain_discards")) # join to get sampling stratum information
  names(observe_sample_discard_agg) <- toupper(x = names(observe_sample_discard_agg))
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
    dplyr::filter(is.na(x = TOTWGHTLANDG) == FALSE,
                  is.na(x = DISCARDS) == FALSE) %>% # remove lines where TOTWGHTLANDG and/or DISCARDS are NA
    dplyr::mutate(DISCARD_CV = "NK",
                  DISCARD_CI_UPPER = "NK",
                  DISCARD_CI_LOWER = "NK",
                  TOTAL_TRIPS = "NK",
                  LENGTH_UNIT = "cm",
                  NO_LENGTH = round(NO_LENGTH / 1000,
                                    digits = 3),
                  WEIGHT_UNIT = "kg",
                  NEP_SUB_REGION = "NA") %>% # complete value in the concerned columns
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
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process - Table D design.\n",
      sep = "")
  # 5 - Template checking ----
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

  # 6 - Export ----
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
