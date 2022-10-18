#' @name global_load_fides_data
#' @title Load FIDES data
#' @description Process for Load FIDES data from "official" FIDES files or a public alternative available on https://griffincarpenter.org/reports/european-fishing-quotas-2001-2021/.
#' @param fides_format {\link[base]{character}} expected. Source of FIDES data. You can choose between "unique" for national extraction from FIDES data (one file per country and years), "common" for global FIDES data for all EU countries or "public" to use a public source (see link in the function description).
#' @param reference_period {\link[base]{integer}} expected. Period of reference, in years.
#' @param path {\link[base]{character}} expected. Input path directory where FIDES files are located.
#' @param country {\link[base]{character}} expected. Country(ies) id(s) for data extraction associated. Use 3-alpha country.
#' @return The function return a {\link[tibble]{tibble}}.
#' @export
#' @importFrom dplyr filter mutate relocate case_when tibble inner_join rename rowwise select
#' @importFrom codama r_type_checking
#' @importFrom readxl read_excel
#' @importFrom stringr str_extract str_replace
#' @importFrom utils read.csv
global_load_fides_data <- function(fides_format = "common",
                                   reference_period,
                                   path = NULL,
                                   country = NULL) {
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process for load data from FIDES data.\n",
      sep = "")
  # global variables assignement ----
  year <- NULL
  Amendment.check <- NULL
  Year <- NULL
  Level.Description <- NULL
  stock_group <- NULL
  level_description <- NULL
  adapted_quota_ori <- NULL
  level_code <- NULL
  # global arguments verifications ----
  if (codama::r_type_checking(r_object = fides_format,
                              type = "character",
                              length = as.integer(x = 1),
                              allowed_values = c("unique",
                                                 "common",
                                                 "public"),
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = fides_format,
                                   type = "character",
                                   length = as.integer(x = 1),
                                   allowed_values = c("unique",
                                                      "common",
                                                      "public"),
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = reference_period,
                              type = "integer",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = reference_period,
                                   type = "integer",
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = country,
                              type = "character",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = country,
                                   type = "character",
                                   output = "message"))
  }
  # process ----
  if ((fides_format == "unique")) {
    # specific arguments verifications
    if (codama::r_type_checking(r_object = path,
                                type = "character",
                                length = as.integer(x = 1),
                                output = "logical") != TRUE) {
      return(codama::r_type_checking(r_object = path,
                                     type = "character",
                                     length = as.integer(x = 1),
                                     output = "message"))
    }
    # process
    countries <- c(country,
                   "EU")
    fides_files <- unlist(x = lapply(X = countries,
                                     FUN = function(country_id) {
                                       file.path(path,
                                                 paste0(country_id,
                                                        reference_period,
                                                        ".xls"))
                                     }))
    tac_final = do.call(what = rbind,
                        args = lapply(X = fides_files,
                                      FUN = function(fides_file) {
                                        if (file.exists(fides_file)) {
                                          currrent_fides_file <- readxl::read_excel(path = fides_file,
                                                                                    sheet = 1,
                                                                                    col_names = TRUE) %>%
                                            dplyr::mutate(year = as.integer(x = stringr::str_extract(string = fides_file,
                                                                                                     pattern = "[:digit:]{4}(?=.xls)"))) %>%
                                            dplyr::relocate(year)
                                        } else {
                                          cat(format(x = Sys.time(),
                                                     format = "%Y-%m-%d %H:%M:%S"),
                                              " - Warning: no fides file available in the location\n",
                                              fides_file,
                                              ".\n",
                                              sep = "")
                                        }
                                      }))
  } else if (fides_format == "common") {
    # specific arguments verifications
    if (codama::r_type_checking(r_object = path,
                                type = "character",
                                length = as.integer(x = 1),
                                output = "logical") != TRUE) {
      return(codama::r_type_checking(r_object = path,
                                     type = "character",
                                     length = as.integer(x = 1),
                                     output = "message"))
    }
    # process
    countries <- c(country,
                   "XEU")
    fides_file <- read.table(file = file.path(path,
                                              "export_quota_20220204tl.csv"),
                             dec = ".",
                             sep = ";",
                             header = TRUE)
    tac_final <- dplyr::filter(.data = fides_file,
                               level_code %in% !!countries)
  } else if (fides_format == "public") {
    # second process
    geo <- read.table(file = system.file("referentials",
                                         "geo.def",
                                         package = "acdc"),
                      header = TRUE,
                      sep = ";")
    tac <- utils::read.csv(file = system.file("record_of_european_tacs.csv",
                                              package = "acdc")) %>%
      dplyr::filter(Amendment.check == "Final"
                    & Year %in% reference_period) %>%
      dplyr::mutate(Level = dplyr::case_when(
        Level == "EU" ~ "European union (27 MS)",
        TRUE ~ Level
      ))
    tac_final <- dplyr::tibble(country = tac$Level,
                               stock_group = NA,
                               species = stringr::str_extract(string = tac$Abbreviation,
                                                              pattern = "[:alpha:]*"),
                               area = stringr::str_extract(string = tac$Abbreviation,
                                                           pattern = "(?<=/).*"),
                               sc_type = NA,
                               parent_species = NA,
                               parent_area = tac$TAC.Zone,
                               adapted_quota_ori = tac$Agreed.TAC,
                               margin = NA,
                               catches = as.numeric(x = NA),
                               sc_catches = NA,
                               percent_cons = NA,
                               unit = NA,
                               fishing_stop = NA,
                               year= tac$Year) %>%
      dplyr::inner_join(geo,
                        by = c("country" = "Geopolitical_entity")) %>%
      dplyr::relocate(Level.Description,
                      .before = stock_group) %>%
      dplyr::rename(level_description = Level.Description) %>%
      dplyr::filter(level_description %in% c(!!country,
                                             "EEC")) %>%
      dplyr::rowwise() %>%
      dplyr::mutate(adapted_quota = dplyr::case_when(
        stringr::str_detect(string = adapted_quota_ori,
                            pattern = "^[:digit:]+[:punct:][:digit:]+[:punct:][:digit:]+$") ~ stringr::str_replace(string = adapted_quota_ori,
                                                                                                                   pattern = "[:punct:]",
                                                                                                                   replacement = ""),
        stringr::str_detect(string = adapted_quota_ori,
                            pattern = "^[:digit:]+[:punct:][:digit:]+$") ~ stringr::str_replace(string = adapted_quota_ori,
                                                                                                pattern = "[:punct:]",
                                                                                                replacement = "."),
        stringr::str_detect(string = adapted_quota_ori,
                            pattern = "[^[:digit:]]") ~ NA_character_,
        TRUE ~ adapted_quota_ori
      )) %>%
      dplyr::select(-country,
                    -geo,
                    -adapted_quota_ori)
  } else {
    stop(format(x = Sys.time(),
                format = "%Y-%m-%d %H:%M:%S"),
         " - Error, invalid \"fides_format\" argument.\n",
         sep = "")
  }
  if (nrow(x = tac_final) == 0) {
    stop(format(x = Sys.time(),
                format = "%Y-%m-%d %H:%M:%S"),
         " - Error, no FIDES data available regarding the input arguments.\n",
         sep = "")
  } else {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Successful process for load data from FIDES data.\n",
        sep = "")
  }

  return(tac_final)
}
