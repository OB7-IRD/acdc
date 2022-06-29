#' @export
fdi_tabled_discard_length <- function(observe_discard_path,
                                      tablea_catch_summary,
                                      template_checking = TRUE,
                                      template_year = NULL,
                                      table_export_path = NULL) {
  # observers discard data extraction ----
  observe_discard <- lapply(X = list.files(path = observe_discard_path),
                            FUN = function(file_name) {
                              read.csv2(file.path(observe_discard_path,
                                                  file_name),
                                        stringsAsFactors = FALSE)
                            })
  observe_discard <- do.call("rbind",
                             observe_discard) %>%
    dplyr::mutate(nep_sub_region = "NA") %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      mean_weight_at_length = dplyr::case_when(
        is.na(mean_weight_at_length) ~ "NK",
        TRUE ~ as.character(mean_weight_at_length)
      ))
  # final design ----
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
    dplyr::mutate(discard_cv = "NK",
                  discard_ci_upper = "NK",
                  discard_ci_lower = "NK",
                  total_sampled_trips = "NK") %>%
    dplyr::rename(total_trips = no_samples) %>%
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
  return(list("fdi_tables" = list("table_d" = tabled_final)))
}
