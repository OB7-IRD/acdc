#' @name fdi_tablef_landings_length
#' @title Table F NAO OFR landings length generation (FDI process)
#' @description Process for generation and optionally extraction of the FDI table F (NAO OFR landings length).
#' @param balbaya_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the balbaya database.
#' @param sardara_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the sardara database.
#' @param t3_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the t3 database.
#' @param period {\link[base]{integer}} expected. Year period for data extractions.
#' @param gear {\link[base]{integer}}. Gear(s) selection for data extractions.
#' @param flag {\link[base]{integer}} expected. Flag(s) selection for data extractions.
#' @param tablea_bycatch_retained Output "bycatch_retained" of the function {\link[acdc]{fdi_tablea_catch_summary}}.
#' @param tablea_catch_summary Output "table_a" of the function {\link[acdc]{fdi_tablea_catch_summary}}.
#' @param cwp_grid_file_path {\link[base]{character}} expected. File path of the CWP area grid. The file format has to be .Rdata.
#' @param fao_area_file_path {\link[base]{character}} expected. File path of the FAO area grid. The file format has to be .Rdata.
#' @param template_checking {\link[base]{logical}} expected. By default FALSE Checking FDI table generated regarding the official FDI template.
#' @param template_year {\link[base]{integer}} expected. By default NULL. Template year.
#' @param table_export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @return The process returns a list with the FDI table F inside.
#' @export
#' @importFrom codama r_type_checking file_path_checking
#' @importFrom dplyr rename select mutate case_when group_by summarise full_join mutate rowwise bind_cols n ungroup inner_join setdiff
#' @importFrom DBI sqlInterpolate SQL dbGetQuery
#' @importFrom furdeb marine_area_overlay lat_lon_cwp_manipulation
#' @importFrom stringr str_extract str_c
fdi_tablef_landings_length <- function(balbaya_con,
                                       sardara_con,
                                       t3_con,
                                       period,
                                       gear,
                                       flag,
                                       tablea_bycatch_retained,
                                       tablea_catch_summary,
                                       cwp_grid_file_path,
                                       fao_area_file_path,
                                       template_checking = FALSE,
                                       template_year = NULL,
                                       table_export_path = NULL) {
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on FDI table F generation.\n",
      sep = "")
  # global variables assignement ----
  country <- NULL
  year <- NULL
  quarter <- NULL
  sub_region <- NULL
  nep_sub_region <- NULL
  metier <- NULL
  school_type <- NULL
  vessel_length <- NULL
  species <- NULL
  retained_tons <- NULL
  fishing_mode <- NULL
  cwp <- NULL
  longitude_decimal_degree <- NULL
  latitude_decimal_degree <- NULL
  totwghtlandg <- NULL
  ocean_name <- NULL
  metier_1 <- NULL
  metier_2 <- NULL
  metier_3 <- NULL
  domain_landings <- NULL
  mean_weight_at_length <- NULL
  weight_unit <- NULL
  no_length <- NULL
  total_sampled_trips <- NULL
  no_length_measurements <- NULL
  length_unit <- NULL
  min_length <- NULL
  max_length <- NULL
  COUNTRY <- NULL
  YEAR <- NULL
  DOMAIN_LANDINGS <- NULL
  id_verif <- NULL
  # arguments verifications ----
  if (codama::r_type_checking(r_object = balbaya_con,
                              type = "PostgreSQLConnection",
                              length = 1L,
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = balbaya_con,
                                   type = "PostgreSQLConnection",
                                   length = 1L,
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = sardara_con,
                              type = "PostgreSQLConnection",
                              length = 1L,
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = sardara_con,
                                   type = "PostgreSQLConnection",
                                   length = 1L,
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = t3_con,
                              type = "PostgreSQLConnection",
                              length = 1L,
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = t3_con,
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
  if (codama::r_type_checking(r_object = gear,
                              type = "integer",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = gear,
                                   type = "integer",
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = flag,
                              type = "integer",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = flag,
                                   type = "integer",
                                   output = "message"))
  }
  if (codama::file_path_checking(file_path =  cwp_grid_file_path,
                                 extension = "Rdata",
                                 output = "logical") != TRUE) {
    return(codama::file_path_checking(file_path =  cwp_grid_file_path,
                                      extension = "Rdata",
                                      output = "message"))
  }
  if (codama::file_path_checking(file_path =  fao_area_file_path,
                                 extension = "Rdata",
                                 output = "logical") != TRUE) {
    return(codama::file_path_checking(file_path =  fao_area_file_path,
                                      extension = "Rdata",
                                      output = "message"))
  }
  if (codama::r_type_checking(r_object = template_checking,
                              type = "logical",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = template_checking,
                                   type = "logical",
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = template_year,
                              type = "integer",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = template_year,
                                   type = "integer",
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = table_export_path,
                              type = "character",
                              length = 1L,
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = table_export_path,
                                   type = "character",
                                   length = 1L,
                                   output = "message"))
  }
  # bycatch data extraction ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on bycatch data.\n",
      sep = "")
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
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on bycatch data.\n",
      sep = "")
  # landings data extraction ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on landing data.\n",
      sep = "")
  balbaya_landing_cwp_query <- paste(readLines(con = system.file("sql",
                                                                 "fdi",
                                                                 "balbaya_landing_cwp_fdi.sql",
                                                                 package = "acdc")),
                                     collapse = '\n')
  balbaya_landing_cwp_query <- DBI::sqlInterpolate(conn = balbaya_con,
                                                   sql = balbaya_landing_cwp_query,
                                                   period = DBI::SQL(paste0(period,
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
                                                                            referential_grid_file_path = cwp_grid_file_path,
                                                                            input_cwp_format = "centroid",
                                                                            output_degree_cwp_parameter = "centroid",
                                                                            output_degree_format = "decimal_degree",
                                                                            output_cwp_format = "centroid_7") %>%
                                             dplyr::mutate(longitude_decimal_degree = as.numeric(longitude_decimal_degree),
                                                           latitude_decimal_degree = as.numeric(latitude_decimal_degree)) %>%
                                             dplyr::select(-cwp)))
  balbaya_landing_cwp <- suppressMessages(furdeb::marine_area_overlay(data = balbaya_landing_cwp,
                                                                      overlay_expected = "fao_area",
                                                                      longitude_name = "longitude_decimal_degree",
                                                                      latitude_name = "latitude_decimal_degree",
                                                                      fao_area_file_path = fao_area_file_path,
                                                                      fao_overlay_level = "division",
                                                                      auto_selection_fao = TRUE,
                                                                      silent = TRUE))
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
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on landing data.\n",
      sep = "")
  # CAS from sardara ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on catch at size data.\n",
      sep = "")
  sardara_cas_query <- paste(readLines(con = system.file("sql",
                                                         "fdi",
                                                         "sardara_cas_fdi.sql",
                                                         package = "acdc")),
                             collapse = '\n')
  sardara_cas_query <- DBI::sqlInterpolate(conn = sardara_con,
                                           sql = sardara_cas_query,
                                           period = DBI::SQL(paste0(period,
                                                                    collapse = ", ")),
                                           flag = DBI::SQL(paste0(flag,
                                                                  collapse = ", ")),
                                           gear = DBI::SQL(paste0(gear,
                                                                  collapse = ", ")))
  sardara_cas <- DBI::dbGetQuery(conn = sardara_con,
                                 statement = sardara_cas_query) %>%
    dplyr::mutate(
      cwp = paste0("6",
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
                                                                    referential_grid_file_path = cwp_grid_file_path,
                                                                    input_cwp_format = "centroid",
                                                                    output_degree_cwp_parameter = "centroid",
                                                                    output_degree_format = "decimal_degree",
                                                                    output_cwp_format = "centroid_7") %>%
                                     dplyr::mutate(longitude_decimal_degree = as.numeric(longitude_decimal_degree),
                                                   latitude_decimal_degree = as.numeric(latitude_decimal_degree)) %>%
                                     dplyr::select(-cwp)))
  sardara_cas <- suppressMessages(furdeb::marine_area_overlay(data = sardara_cas,
                                                              overlay_expected = "fao_area",
                                                              longitude_name = "longitude_decimal_degree",
                                                              latitude_name = "latitude_decimal_degree",
                                                              fao_area_file_path = fao_area_file_path,
                                                              fao_overlay_level = "division",
                                                              auto_selection_fao = TRUE,
                                                              silent = TRUE))
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
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on catch at size data.\n",
      sep = "")
  # final design ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start table F design.\n",
      sep = "")
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
                                unique(tablea_catch_summary[,c("COUNTRY", "YEAR", "DOMAIN_LANDINGS")])) %>%
    dplyr::mutate(id_verif = paste0(COUNTRY,
                                    YEAR,
                                    DOMAIN_LANDINGS)) %>%
    dplyr::select(id_verif)
  cons_tablea = cons_tablea$id_verif
  tablef_final <- tablef_final[! tablef_final$ID_VERIF %in% cons_tablea, -ncol(x = tablef_final)]
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful table F design.\n",
      sep = "")
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
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on FDI table F generation.\n",
      sep = "")
  return(list("fdi_tables" = list("table_f" = tablef_final)))
}
