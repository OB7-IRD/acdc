#' @name fdi_tabled_discard_length
#' @title Table D NAO OFR discards length generation (FDI process)
#' @description Process for generation and optionally extraction of the FDI table D (NAO OFR discards length).
#' @param observe_discard_path {\link[base]{character}} expected. Directory path of the discards data extractions. Check this input with Philippe Sabarros (philippe.sabarros@ird.fr).
#' @param tablea_catch_summary {\link[base]{list}} expected. Output "table_a" of the function {\link[acdc]{fdi_tablea_catch_summary}}.
#' @param template_checking {\link[base]{logical}} expected. By default FALSE Checking FDI table generated regarding the official FDI template.
#' @param template_year {\link[base]{integer}} expected. By default NULL. Template year.
#' @param table_export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @return The process returns a list with the FDI table D inside.
#' @export
#' @importFrom codama r_type_checking
#' @importFrom dplyr mutate rowwise case_when select inner_join
fdi_tabled_discard_length <- function(observe_discard_path,
                                      tablea_catch_summary,
                                      template_checking = FALSE,
                                      template_year = NULL,
                                      table_export_path = NULL) {
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on FDI table D generation.\n",
      sep = "")
  # global variables assignment ----
  totwghtlandg <- NULL
  discards <- NULL
  no_samples <- NULL
  country <- NULL
  year <- NULL
  domain_discards <- NULL
  nep_sub_region <- NULL
  species <- NULL
  TOTWGHTLANDG <- NULL
  DISCARDS <- NULL
  discard_cv <- NULL
  discard_ci_upper <- NULL
  discard_ci_lower <- NULL
  total_trips <- NULL
  total_sampled_trips <- NULL
  no_length_measurements <- NULL
  length_unit <- NULL
  min_length <- NULL
  max_length <- NULL
  no_length <- NULL
  mean_weight_at_length <- NULL
  weight_unit <- NULL
  # arguments verifications ----
  if (codama::r_type_checking(r_object = observe_discard_path,
                              type = "character",
                              length = 1L,
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = observe_discard_path,
                                   type = "character",
                                   length = 1L,
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
  # observers discards data extraction ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on discards data.\n",
      sep = "")
  observe_discard <- lapply(X = list.files(path = observe_discard_path),
                            FUN = function(file_name) {
                              read.csv2(file.path(observe_discard_path,
                                                  file_name),
                                        stringsAsFactors = FALSE)
                            })
  observe_discard <- do.call("rbind",
                             observe_discard) %>%
    dplyr::mutate(nep_sub_region = "NA",
                  country = dplyr::case_when(
                    country == "MYT" ~ "FRA",
                    TRUE ~ country
                  )) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      mean_weight_at_length = dplyr::case_when(
        is.na(x = mean_weight_at_length) ~ "NK",
        TRUE ~ as.character(x = mean_weight_at_length)
      ))
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on discards data.\n",
      sep = "")
  # final design ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start table D design.\n",
      sep = "")
  tabled_final <- observe_discard %>%
    dplyr::select(-totwghtlandg,
                  -discards) %>%
    dplyr::inner_join(tablea_catch_summary[, c("COUNTRY",
                                               "YEAR",
                                               "DOMAIN_DISCARDS",
                                               "NEP_SUB_REGION",
                                               "SPECIES",
                                               "TOTWGHTLANDG",
                                               "DISCARDS")],
                      by = c("country" = "COUNTRY",
                             "year" = "YEAR",
                             "domain_discards" = "DOMAIN_DISCARDS",
                             "nep_sub_region" = "NEP_SUB_REGION",
                             "species" = "SPECIES")) %>%
    dplyr::select(country,
                  year,
                  domain_discards,
                  nep_sub_region,
                  species,
                  TOTWGHTLANDG,
                  DISCARDS,
                  discard_cv,
                  discard_ci_upper,
                  discard_ci_lower,
                  total_trips,
                  total_sampled_trips,
                  no_length_measurements,
                  length_unit,
                  min_length,
                  max_length,
                  length,
                  no_length,
                  mean_weight_at_length,
                  weight_unit)
  names(tabled_final) <- toupper(names(tabled_final))
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful table D design.\n",
      sep = "")
  # template checking ----
  if (template_checking == TRUE) {
    fdi_template_checking(fdi_table = tabled_final,
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
    fdi_table_export(fdi_table = tabled_final,
                     export_path = table_export_path,
                     table_id = "d")
  }
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on FDI table D generation.\n",
      sep = "")
  return(list("fdi_tables" = list("table_d" = tabled_final)))
}
