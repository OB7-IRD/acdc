#' @export
fdi_tablef_landings_length <- function(balbaya_con,
                                       sardara_con,
                                       t3_con,
                                       periode,
                                       gear,
                                       flag,
                                       tablea_bycatch_retained,
                                       fdi_tablea,
                                       cwp_grid_file_path,
                                       fao_area_file_path,
                                       template_checking = TRUE,
                                       template_year = NULL,
                                       table_export_path = NULL) {
  # process ----
  # bycatch retained data from FDI table A ----
  observe_bycatch_retained <- tablea_bycatch_retained %>%
    dplyr::group_by(country,
                    year,
                    quarter,
                    sub_region,
                    nep_sub_region,
                    metier,
                    school_type,
                    vessel_length,
                    species) %>%
    dplyr::summarise(retained_tons = sum(retained_tons),
                     .groups = "drop") %>%
    dplyr::rename(fishing_mode = school_type) %>%
    dplyr::mutate(fishing_mode = as.character(fishing_mode))
  # landings ----
  balbaya_landing_cwp_query <- paste(readLines(con = system.file("sql",
                                                                 "fdi",
                                                                 "balbaya_landing_cwp_fdi.sql",
                                                                 package = "acdc")),
                                     collapse = '\n')
  balbaya_landing_cwp_query <- DBI::sqlInterpolate(conn = balbaya_con,
                                                   sql = balbaya_landing_cwp_query,
                                                   periode = DBI::SQL(paste0(periode,
                                                                             collapse = ", ")),
                                                   flag = DBI::SQL(paste0(flag,
                                                                          collapse = ", ")),
                                                   gear = DBI::SQL(paste0(gear,
                                                                          collapse = ", ")))
  balbaya_landing_cwp <- DBI::dbGetQuery(conn = balbaya_con,
                                         statement = balbaya_landing_cwp_query)
  balbaya_landing_cwp <- dplyr::mutate(.data = balbaya_landing_cwp,
                                       cwp = paste0("6",
                                                    cwp),
                                       fishing_mode = dplyr::case_when(fishing_mode == 1 ~ "FOB",
                                                                       fishing_mode == 2 ~ "FSC",
                                                                       fishing_mode == 3 ~ "UNK",
                                                                       TRUE ~ "error"),
                                       vessel_length = dplyr::case_when(vessel_length < 10 ~ "VL0010",
                                                                        vessel_length >= 10 & vessel_length < 12 ~ "VL1012",
                                                                        vessel_length >= 12 & vessel_length < 18 ~ "VL1218",
                                                                        vessel_length >= 18 & vessel_length < 24 ~ "VL1824",
                                                                        vessel_length >= 24 & vessel_length < 40 ~ "VL2440",
                                                                        vessel_length >= 40 ~ "VL40XX",
                                                                        TRUE ~ "NK"))
  balbaya_landing_cwp <- dplyr::bind_cols(balbaya_landing_cwp,
                                          (furdeb::lat_lon_cwp_manipulation(manipulation_process = "cwp_to_lat_lon",
                                                                            data_cwp = as.character(balbaya_landing_cwp$cwp),
                                                                            referential_grid_file_path = cwp_grid_5deg_5deg,
                                                                            input_cwp_format = "centroid",
                                                                            output_degree_cwp_parameter = "centroid",
                                                                            output_degree_format = "decimal_degree",
                                                                            output_cwp_format = "centroid_7") %>%
                                             dplyr::mutate(longitude_decimal_degree = as.numeric(longitude_decimal_degree),
                                                           latitude_decimal_degree = as.numeric(latitude_decimal_degree)) %>%
                                             dplyr::select(-cwp)))
  balbaya_landing_cwp <- furdeb::marine_area_overlay(data = balbaya_landing_cwp,
                                                     overlay_expected = "fao_area",
                                                     longitude_name = "longitude_decimal_degree",
                                                     latitude_name = "latitude_decimal_degree",
                                                     fao_area_file_path = fao_area_file_path,
                                                     fao_overlay_level = "division",
                                                     auto_selection_fao = TRUE,
                                                     silent = TRUE)
  if (any(is.na(x = unique(x = balbaya_landing_cwp$best_fao_area)))) {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Warning: at least one position have not fao area associated. Data associated will be deleted. Check the log.\n",
        sep = "")
    na_fao_area <- unique(balbaya_landing_cwp[is.na(x = balbaya_landing_cwp$best_fao_area), c("cwp",
                                                                                              "latitude_decimal_degree",
                                                                                              "longitude_decimal_degree")])
    cat(sapply(X = seq_len(length.out = nrow(x = na_fao_area)),
               FUN = function(na_fao_area_id) {
                 return(paste0("cwp ",
                               na_fao_area[na_fao_area_id,
                                           "cwp"],
                               ", latitude (decimal degree) ",
                               na_fao_area[na_fao_area_id,
                                           "latitude_decimal_degree"],
                               ", longitude (decimal degree) ",
                               na_fao_area[na_fao_area_id,
                                           "longitude_decimal_degree"],
                               ".\n"))
               }),
        sep = "")
    balbaya_landing_cwp <- balbaya_landing_cwp[! is.na(x = balbaya_landing_cwp$best_fao_area), ]
  }
  balbaya_landing_cwp <- dplyr::mutate(.data = balbaya_landing_cwp,
                                       sub_region = dplyr::case_when(
                                         best_fao_area %in% c("47.A.0",
                                                              "47.A.1") ~ "47.A",
                                         best_fao_area %in% c("47.B.1") ~ "47.B",
                                         best_fao_area %in% c("47.1.1",
                                                              "47.1.2",
                                                              "47.1.3",
                                                              "41.1.4") ~ "47.1",
                                         best_fao_area %in% c("34.2") ~ "34.2.0",
                                         TRUE ~ best_fao_area),
                                       nep_sub_region = dplyr::case_when(
                                         major_fao == 27 ~ "error_need_nep_sub_region",
                                         TRUE ~ "NA"),
                                       metier = paste(dplyr::case_when(engin == 1 ~ "PS",
                                                                       engin == 2 ~ "LHP",
                                                                       engin == 3 ~ "LLD",
                                                                       TRUE ~ "error"),
                                                      "LPF",
                                                      "0",
                                                      "0",
                                                      "0",
                                                      sep = "_")) %>%
    dplyr::group_by(country,
                    year,
                    quarter,
                    sub_region,
                    nep_sub_region,
                    metier,
                    fishing_mode,
                    vessel_length,
                    species) %>%
    dplyr::summarise(totwghtlandg = sum(totwghtlandg),
                     .groups = "drop")
  balbaya_landing_tablef <- balbaya_landing_cwp %>%
    dplyr::full_join(observe_bycatch_retained,
                     by = c("country",
                            "year",
                            "quarter",
                            "sub_region",
                            "nep_sub_region",
                            "metier",
                            "fishing_mode",
                            "vessel_length",
                            "species")) %>%
    dplyr::mutate(
      totwghtlandg =  dplyr::case_when(
        is.na(totwghtlandg) ~ 0,
        TRUE ~ totwghtlandg),
      retained_tons = dplyr::case_when(
        is.na(retained_tons) ~ 0,
        TRUE ~ retained_tons),
      totwghtlandg = round(x = totwghtlandg + retained_tons,
                           digits = 3)) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(domain_landings = paste(country,
                                          quarter,
                                          sub_region,
                                          paste(unlist(x = strsplit(x = metier,
                                                                    split = "_"))[1],
                                                fishing_mode,
                                                sep = "-"),
                                          "LPF",
                                          "NA",
                                          "NA",
                                          "NA",
                                          vessel_length,
                                          species,
                                          "NA",
                                          sep = "_"),
                  total_sampled_trips = 1) %>%
    dplyr::select(-quarter,
                  -sub_region,
                  -nep_sub_region,
                  -metier,
                  -fishing_mode,
                  -vessel_length,
                  -retained_tons)
  # CAS from sardara ----
  sardara_cas_query <- paste(readLines(con = system.file("sql",
                                                         "fdi",
                                                         "sardara_cas_fdi.sql",
                                                         package = "acdc")),
                             collapse = '\n')
  sardara_cas_query <- DBI::sqlInterpolate(conn = sardara_con,
                                           sql = sardara_cas_query,
                                           periode = DBI::SQL(paste0(periode,
                                                                     collapse = ", ")),
                                           flag = DBI::SQL(paste0(flag,
                                                                  collapse = ", ")),
                                           gear = DBI::SQL(paste0(gear,
                                                                  collapse = ", ")))
  sardara_cas <- DBI::dbGetQuery(conn = sardara_con,
                                 statement = sardara_cas_query) %>%
    dplyr::mutate(cwp = paste0("6",
                               cwp),
                  vessel_length = dplyr::case_when(gear == 1 ~ "VL2440",
                                                   gear == 2 ~ "VL40XX",
                                                   TRUE ~ "error"),
                  metier = dplyr::case_when(gear == 1 ~ "LHP_LPF_0_0_0",
                                            gear == 2 ~ "PS_LPF_0_0_0",
                                            gear == 3 ~ "LLD_LPF_0_0_0",
                                            TRUE ~ "error"))
  sardara_cas <- dplyr::bind_cols(sardara_cas,
                                  (furdeb::lat_lon_cwp_manipulation(manipulation_process = "cwp_to_lat_lon",
                                                                    data_cwp = sardara_cas$cwp,
                                                                    referential_grid_file_path = cwp_grid_5deg_5deg,
                                                                    input_cwp_format = "centroid",
                                                                    output_degree_cwp_parameter = "centroid",
                                                                    output_degree_format = "decimal_degree",
                                                                    output_cwp_format = "centroid_7") %>%
                                     dplyr::mutate(longitude_decimal_degree = as.numeric(longitude_decimal_degree),
                                                   latitude_decimal_degree = as.numeric(latitude_decimal_degree)) %>%
                                     dplyr::select(-cwp)))
  sardara_cas <- furdeb::marine_area_overlay(data = sardara_cas,
                                             overlay_expected = "fao_area",
                                             longitude_name = "longitude_decimal_degree",
                                             latitude_name = "latitude_decimal_degree",
                                             fao_area_file_path = fao_area_file_path,
                                             fao_overlay_level = "division",
                                             auto_selection_fao = TRUE,
                                             silent = TRUE)
  if (any(is.na(x = unique(x = sardara_cas$best_fao_area)))) {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Warning: at least one position have not fao area associated. Data associated will be deleted. Check the log.\n",
        sep = "")
    na_fao_area <- unique(sardara_cas[is.na(x = sardara_cas$best_fao_area), c("cwp",
                                                                              "latitude_decimal_degree",
                                                                              "longitude_decimal_degree")])
    cat(sapply(X = seq_len(length.out = nrow(x = na_fao_area)),
               FUN = function(na_fao_area_id) {
                 return(paste0("cwp ",
                               na_fao_area[na_fao_area_id,
                                           "cwp"],
                               ", latitude (decimal degree) ",
                               na_fao_area[na_fao_area_id,
                                           "latitude_decimal_degree"],
                               ", longitude (decimal degree) ",
                               na_fao_area[na_fao_area_id,
                                           "longitude_decimal_degree"],
                               ".\n"))
               }),
        sep = "")
    sardara_cas <- sardara_cas[! is.na(x = sardara_cas$best_fao_area), ]
  }
  sardara_cas <- dplyr::mutate(.data = sardara_cas,
                               sub_region = dplyr::case_when(
                                 best_fao_area %in% c("47.A.0",
                                                      "47.A.1") ~ "47.A",
                                 best_fao_area %in% c("47.B.1") ~ "47.B",
                                 best_fao_area %in% c("47.1.1",
                                                      "47.1.2",
                                                      "47.1.3",
                                                      "41.1.4") ~ "47.1",
                                 best_fao_area %in% c("34.2") ~ "34.2.0",
                                 TRUE ~ best_fao_area),
                               nep_sub_region = dplyr::case_when(
                                 major_fao == 27 ~ "error_need_nep_sub_region",
                                 TRUE ~ "NA"))
  t3_lwr_coef_query <- paste(readLines(con = system.file("sql",
                                                         "fdi",
                                                         "balbaya_lwr_fdi.sql",
                                                         package = "acdc")),
                             collapse = '\n')
  t3_lwr_coef <- DBI::dbGetQuery(conn = t3_con,
                                 statement = t3_lwr_coef_query)
  sardara_cas <- sardara_cas %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      ocean_name = dplyr::case_when(
        major_fao %in% c("34",
                         "41",
                         "47") ~ "Atlantique",
        major_fao %in% c("51",
                         "57") ~ "Indien",
        TRUE ~ "new_fao_area"
      ),
      mean_weight_at_length = t3_lwr_coef[t3_lwr_coef$specie_name == species & t3_lwr_coef$ocean_name == ocean_name, "a"] * length ^ t3_lwr_coef[t3_lwr_coef$specie_name == species & t3_lwr_coef$ocean_name == ocean_name, "b"],
      weight_unit = "kg") %>%
    dplyr::rowwise() %>%
    dplyr::mutate(metier_1 = stringr::str_extract(string = metier,
                                                  pattern = stringr::regex(pattern = "[^_]*")),
                  metier_2 = stringr::str_c(metier_1,
                                            dplyr::case_when(fishing_mode == 1 ~ "FOB",
                                                             fishing_mode == 2 ~ "FSC",
                                                             fishing_mode == 3 ~ "UNK",
                                                             TRUE ~ "error"),
                                            sep = "-"),
                  metier_3 = stringr::str_c(metier_2,
                                            "LPF",
                                            "NA",
                                            "NA",
                                            "NA",
                                            sep = "_"),
                  domain_landings = stringr::str_c(country,
                                                   quarter,
                                                   sub_region,
                                                   metier_3,
                                                   vessel_length,
                                                   species,
                                                   "NA",
                                                   sep = "_"))
  sardara_cas <- sardara_cas %>%
    dplyr::group_by(country,
                    year,
                    domain_landings,
                    nep_sub_region,
                    species,
                    length,
                    mean_weight_at_length,
                    weight_unit) %>%
    dplyr::summarise(no_length = sum(no_length),
                     .groups = "drop") %>%
    dplyr::group_by(country,
                    year,
                    domain_landings,
                    nep_sub_region,
                    species) %>%
    dplyr::mutate(min_length = min(length),
                  max_length = max(length),
                  no_length_measurements = dplyr::n()) %>%
    dplyr::ungroup()
  # final design ----
  tablef_final <- balbaya_landing_tablef %>%
    dplyr::inner_join(sardara_cas,
                      by = c("country",
                             "year",
                             "species",
                             "domain_landings")) %>%
    dplyr::mutate(length_unit = "cm") %>%
    dplyr::select(country,
                  year,
                  domain_landings,
                  nep_sub_region,
                  species,
                  totwghtlandg,
                  total_sampled_trips,
                  no_length_measurements,
                  length_unit,
                  min_length,
                  max_length,
                  length,
                  no_length,
                  mean_weight_at_length,
                  weight_unit) %>%
    dplyr::mutate(id_verif = paste0(country,
                                    year,
                                    domain_landings))
  names(tablef_final) <- toupper(names(tablef_final))
  # consistency with FDI table A ----
  cons_tablea <- dplyr::setdiff(unique(tablef_final[,c("COUNTRY", "YEAR", "DOMAIN_LANDINGS")]),
                                unique(fdi_tablea[,c("COUNTRY", "YEAR", "DOMAIN_LANDINGS")])) %>%
    dplyr::mutate(id_verif = paste0(COUNTRY,
                                    YEAR,
                                    DOMAIN_LANDINGS)) %>%
    dplyr::select(id_verif)
  cons_tablea = cons_tablea$id_verif
  tablef_final <- tablef_final[! tablef_final$ID_VERIF %in% cons_tablea, -ncol(x = tablef_final)]
  # template checking ----
  if (template_checking == TRUE) {
    fdi_template_checking(fdi_table = tablef_final,
                          template_year = template_year,
                          table_id = "f")
  } else {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Warning: main table output not checking with official FDI template.\n",
        sep = "")
  }
  # export ----
  if (! is.null(x = table_export_path)) {
    fdi_table_export(fdi_table = tablef_final,
                     export_path = table_export_path,
                     table_id = "f")
  }
  return(list("fdi_tables" = list("table_f" = tablef_final)))
}
