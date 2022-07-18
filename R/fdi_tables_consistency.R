#' @name fdi_tables_consistency
#' @title FDI tables consistency (FDI process)
#' @description Process for cross-checking of the FDI tables.
#' @param tablea Output "table_a" of the function {\link[acdc]{fdi_tablea_catch_summary}}
#' @param tabled Output "table_d" of the function {\link[acdc]{fdi_tabled_discard_length}}
#' @param tablef Output "table_f" of the function {\link[acdc]{fdi_tablef_landings_length}}
#' @param tableg Output "table_g" of the function {\link[acdc]{fdi_tableg_effort}}
#' @param tableh Output "table_h" of the function {\link[acdc]{fdi_tableh_landings_rectangle}}
#' @param tablei Output "table_i" of the function {\link[acdc]{fdi_tablei_effort_rectangle}}
#' @param tablea_bycatch_retained Output "bycatch_retained" of the function {\link[acdc]{fdi_tablea_catch_summary}}.
#' @return The process returns a list with 3 verifications data.frame
#' @export
#' @importFrom dplyr group_by summarise full_join mutate
fdi_tables_consistency <- function(tablea,
                                   tabled,
                                   tablef,
                                   tableg,
                                   tableh,
                                   tablei,
                                   tablea_bycatch_retained) {
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on FDI table consistency checking.\n",
      sep = "")
  # global variables assignement ----
  COUNTRY <- NULL
  YEAR <- NULL
  TOTWGHTLANDG <- NULL
  DISCARDS <- NULL
  country <- NULL
  year <- NULL
  discarded_tons <- NULL
  FISHING_TECH <- NULL
  TOTFISHDAYS <- NULL
  # total landings checking ----
  check_landing_tablea <- tablea %>%
    dplyr::group_by(COUNTRY,
                    YEAR) %>%
    dplyr::summarise(totwghtlandg_tablea = sum(TOTWGHTLANDG),
                     .groups = "drop")
  check_landing_tablef <- unique(tablef[, c("COUNTRY",
                                            "YEAR",
                                            "DOMAIN_LANDINGS",
                                            "TOTWGHTLANDG")]) %>%
    dplyr::group_by(COUNTRY,
                    YEAR) %>%
    dplyr::summarise(totwghtlandg_tablef = sum(TOTWGHTLANDG),
                     .groups = "drop")
  check_landing_tableh <- tableh %>%
    dplyr::group_by(COUNTRY,
                    YEAR) %>%
    dplyr::summarise(totwghtlandg_tableh = sum(TOTWGHTLANDG),
                     .groups = "drop")
  check_landing <- check_landing_tablea %>%
    dplyr::full_join(check_landing_tablef, by = c("COUNTRY",
                                                  "YEAR")) %>%
    dplyr::full_join(check_landing_tableh, by = c("COUNTRY",
                                                  "YEAR"))
  # total discards checking ----
  check_discard_tablea <- tablea %>%
    dplyr::mutate(DISCARDS = ifelse(DISCARDS == "NK",
                                    NA,
                                    DISCARDS),
                  DISCARDS = as.numeric(DISCARDS)) %>%
    dplyr::group_by(COUNTRY,
                    YEAR) %>%
    dplyr::summarise(discard_tablea = sum(DISCARDS,
                                          na.rm = TRUE),
                     .groups = "drop")
  check_observe_bycatch <- tablea_bycatch_retained %>%
    dplyr::group_by(country,
                    year) %>%
    dplyr::summarise(discard_observe_bycatch = sum(discarded_tons),
                     .groups = "drop")
  check_discard_tabled <- unique(tabled[, c("COUNTRY",
                                            "YEAR",
                                            "DOMAIN_DISCARDS",
                                            "DISCARDS")]) %>%
    dplyr::mutate(DISCARDS = as.numeric(DISCARDS)) %>%
    dplyr::group_by(COUNTRY,
                    YEAR) %>%
    dplyr::summarise(discard_tabled = sum(DISCARDS),
                     .groups = "drop")
  check_discards <- check_discard_tablea %>%
    dplyr::full_join(check_observe_bycatch, by = c("COUNTRY" = "country",
                                                   "YEAR" = "year")) %>%
    dplyr::full_join(check_discard_tabled, by = c("COUNTRY",
                                                  "YEAR"))
  # effort checking ----
  check_effort_tableg <- tableg %>%
    dplyr::group_by(COUNTRY,
                    YEAR,
                    FISHING_TECH) %>%
    dplyr::summarise(totfishdays = sum(TOTFISHDAYS),
                     .groups = "drop")
  check_effort_tablei <- tablei %>%
    dplyr::group_by(COUNTRY,
                    YEAR,
                    FISHING_TECH) %>%
    dplyr::summarise(totfishdays = sum(TOTFISHDAYS),
                     .groups = "drop")
  check_effort <- check_effort_tableg %>%
    dplyr::full_join(check_effort_tablei,
                     by = c("COUNTRY",
                            "YEAR",
                            "FISHING_TECH"))
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on FDI table consistency checking.\n",
      sep = "")
  return(list("fdi_landings" = check_landing,
              "fdi_discards" = check_discards,
              "fdi_effort" = check_effort))
}
