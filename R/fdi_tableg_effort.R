#' @name fdi_tableg_effort
#' @title Table G effort generation (FDI process)
#' @description Process for generation and optionally extraction of the FDI table G (effort).
#' @param balbaya_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the balbaya database.
#' @param period {\link[base]{integer}} expected. Year period for data extractions.
#' @param gear {\link[base]{integer}}. Gear(s) selection for data extractions.
#' @param flag {\link[base]{integer}} expected. Flag(s) selection for data extractions.
#' @param fao_area_file_path {\link[base]{character}} expected. File path of the FAO area grid. The file format has to be .RData.
#' @param eez_area_file_path {\link[base]{character}} expected. File path of the EEZ area grid. The file format has to be .RData.
#' @param template_checking {\link[base]{logical}} expected. By default TRUE. Checking FDI table generated regarding the official FDI template.
#' @param template_year {\link[base]{integer}} expected. By default NULL. Template year.
#' @param table_export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @return The process returns a double list with the FDI table G in the first one and in the second one one accessory outputs useful for another FDI table generation process ("effort_rectangle").
#' @export
#' @importFrom DBI sqlInterpolate SQL dbGetQuery
#' @importFrom dplyr mutate case_when select group_by summarise n_distinct inner_join
#' @importFrom furdeb marine_area_overlay
fdi_tableg_effort <- function(balbaya_con,
                              period,
                              gear,
                              flag,
                              fao_area_file_path,
                              eez_area_file_path,
                              template_checking = TRUE,
                              template_year = NULL,
                              table_export_path = NULL) {
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on FDI table G generation.\n",
      sep = "")
  # global variables assignement ----
  period <- NULL
  gear_type <- NULL
  target_assemblage <- NULL
  hrsea <- NULL
  totseadays <- NULL
  engine_power <- NULL
  vessel_volume_m3 <- NULL
  totfishdays <- NULL
  vessel_id <- NULL
  totkwdaysatsea <- NULL
  totgtdaysatsea <- NULL
  totkwfishdays <- NULL
  totgtfishdays <- NULL
  totgtfishdays <- NULL
  kwhrsea <- NULL
  gthrsea <- NULL
  major_fao <- NULL
  subarea_fao <- NULL
  division_fao <- NULL
  best_fao_area <- NULL
  eez <- NULL
  country <- NULL
  year <- NULL
  quarter <- NULL
  vessel_length <- NULL
  fishing_tech <- NULL
  mesh_size_range <- NULL
  sub_region <- NULL
  eez_indicator <- NULL
  geo_indicator <- NULL
  specon_tech <- NULL
  deep <- NULL
  metier <- NULL
  supra_region <- NULL
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
  if (codama::file_path_checking(file_path =  fao_area_file_path,
                                 extension = "RData",
                                 output = "logical") != TRUE) {
    return(codama::file_path_checking(file_path =  fao_area_file_path,
                                      extension = "RData",
                                      output = "message"))
  }
  if (codama::file_path_checking(file_path =  eez_area_file_path,
                                 extension = "RData",
                                 output = "logical") != TRUE) {
    return( codama::file_path_checking(file_path =  eez_area_file_path,
                                       extension = "RData",
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
  # effort data extraction ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on effort data.\n",
      sep = "")
  balbaya_effort_query <- paste(readLines(con = system.file("sql",
                                                            "fdi",
                                                            "balbaya_effort_fdi.sql",
                                                            package = "acdc")),
                                collapse = '\n')
  balbaya_effort_query <- DBI::sqlInterpolate(conn = balbaya_con,
                                              sql = balbaya_effort_query,
                                              period = DBI::SQL(paste0(period,
                                                                       collapse = ", ")),
                                              flag = DBI::SQL(paste0(flag,
                                                                     collapse = ", ")),
                                              gear = DBI::SQL(paste0(gear,
                                                                     collapse = ", ")))
  balbaya_effort <- DBI::dbGetQuery(conn = balbaya_con,
                                    statement = balbaya_effort_query)
  balbaya_effort <- dplyr::mutate(.data = balbaya_effort,
                                  vessel_length = dplyr::case_when(vessel_length < 10 ~ "VL0010",
                                                                   vessel_length >= 10 & vessel_length < 12 ~ "VL1012",
                                                                   vessel_length >= 12 & vessel_length < 18 ~ "VL1218",
                                                                   vessel_length >= 18 & vessel_length < 24 ~ "VL1824",
                                                                   vessel_length >= 24 & vessel_length < 40 ~ "VL2440",
                                                                   vessel_length >= 40 ~ "VL40XX",
                                                                   TRUE ~ "NK"),
                                  fishing_tech = dplyr::case_when(gear == 1 ~ "PS",
                                                                  gear %in% c(2, 3) ~ "HOK",
                                                                  TRUE ~ "error"),
                                  gear_type = dplyr::case_when(gear == 1 ~ "PS",
                                                               gear == 2 ~ "LHP",
                                                               gear == 3 ~ "LLD",
                                                               TRUE ~ "error"),
                                  target_assemblage = "LPF",
                                  mesh_size_range = dplyr::case_when(gear == 1 ~ "NK",
                                                                     gear %in% c(2, 3) ~ "NA",
                                                                     TRUE ~ "error"),
                                  metier = paste(gear_type,
                                                 target_assemblage,
                                                 "0",
                                                 "0",
                                                 "0",
                                                 sep = "_"),
                                  supra_region = "OFR",
                                  geo_indicator = "IWE",
                                  specon_tech = "NA",
                                  deep = "NA",
                                  totseadays = hrsea / 24,
                                  totkwdaysatsea = totseadays * engine_power * 0.73539875,
                                  totgtdaysatsea = totseadays * (0.2 + 0.02 * log(vessel_volume_m3)) * vessel_volume_m3,
                                  totfishdays = dplyr::case_when(ocean == 1 ~ fishing_time / 12,
                                                                 ocean == 2 ~ fishing_time / 13),
                                  totkwfishdays = totfishdays * engine_power * 0.73539875,
                                  totgtfishdays = totfishdays * (0.2 + 0.02 * log(vessel_volume_m3)) * vessel_volume_m3,
                                  kwhrsea = hrsea * engine_power * 0.73539875,
                                  gthrsea = hrsea * (0.2 + 0.02 * log(vessel_volume_m3)) * vessel_volume_m3)
  balbaya_effort <- suppressMessages(furdeb::marine_area_overlay(data = balbaya_effort,
                                                                 overlay_expected = "fao_eez_area",
                                                                 longitude_name = "longitude",
                                                                 latitude_name = "latitude",
                                                                 fao_area_file_path = fao_area_file_path,
                                                                 fao_overlay_level = "division",
                                                                 auto_selection_fao = TRUE,
                                                                 eez_area_file_path = eez_area_file_path,
                                                                 for_fdi_use = TRUE,
                                                                 silent = TRUE))
  if (any(is.na(x = unique(x = balbaya_effort$best_fao_area)))) {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Warning: at least one position have not fao area associated. Data associated will be deleted. Check the log.\n",
        sep = "")
    na_fao_area <- unique(balbaya_effort[is.na(x = balbaya_effort$best_fao_area), c("cwp",
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
    balbaya_effort <- balbaya_effort[! is.na(x = balbaya_effort$best_fao_area), ]
  }
  balbaya_effort <- dplyr::mutate(.data = balbaya_effort,
                                  sub_region = dplyr::case_when(
                                    best_fao_area %in% c("47.A.0",
                                                         "47.A.1") ~ "47.A",
                                    best_fao_area %in% c("47.B.1") ~ "47.B",
                                    best_fao_area %in% c("47.1.1",
                                                         "47.1.2",
                                                         "47.1.3",
                                                         "41.1.4") ~ "47.1",
                                    best_fao_area %in% c("34.2") ~ "34.2.0",
                                    TRUE ~ best_fao_area))
  balbaya_effort_rectangle <- balbaya_effort %>%
    dplyr::select(-vessel_id,
                  -totseadays,
                  -totkwdaysatsea,
                  -totgtdaysatsea,
                  -totkwfishdays,
                  -totgtfishdays,
                  -hrsea,
                  -kwhrsea,
                  -gthrsea,
                  -major_fao,
                  -subarea_fao,
                  -division_fao,
                  -best_fao_area,
                  -eez)
  balbaya_effort_nb_vessel <- balbaya_effort %>%
    dplyr::group_by(country,
                    year,
                    quarter,
                    vessel_length,
                    fishing_tech,
                    gear_type,
                    target_assemblage,
                    mesh_size_range,
                    metier,
                    supra_region,
                    sub_region,
                    eez_indicator,
                    geo_indicator,
                    specon_tech) %>%
    dplyr::summarise(totves = dplyr::n_distinct(vessel_id),
                     .groups = "drop")
  balbaya_effort <- balbaya_effort %>%
    dplyr::group_by(country,
                    year,
                    quarter,
                    vessel_length,
                    fishing_tech,
                    gear_type,
                    target_assemblage,
                    mesh_size_range,
                    metier,
                    supra_region,
                    sub_region,
                    eez_indicator,
                    geo_indicator,
                    specon_tech,
                    deep) %>%
    dplyr::summarise(totseadays = sum(totseadays),
                     totkwdaysatsea = as.character(x = sum(totkwdaysatsea)),
                     totgtdaysatsea = sum(totgtdaysatsea),
                     totfishdays = sum(totfishdays),
                     totkwfishdays = as.character(x = sum(totkwfishdays)),
                     totgtfishdays = sum(totgtfishdays),
                     hrsea = sum(hrsea),
                     kwhrsea = as.character(x = sum(kwhrsea)),
                     gthrsea = sum(gthrsea),
                     .groups = "drop") %>%
    dplyr::mutate(
      totkwdaysatsea = dplyr::case_when(
        is.na(x = totkwdaysatsea) ~ "NK",
        TRUE ~ totkwdaysatsea),
      totkwfishdays = dplyr::case_when(
        is.na(x = totkwfishdays) ~ "NK",
        TRUE ~ totkwfishdays),
      kwhrsea = dplyr::case_when(
        is.na(x = kwhrsea) ~ "NK",
        TRUE ~ kwhrsea)) %>%
    dplyr::inner_join(balbaya_effort_nb_vessel,
                      by = c("country",
                             "year",
                             "quarter",
                             "vessel_length",
                             "fishing_tech",
                             "gear_type",
                             "target_assemblage",
                             "mesh_size_range",
                             "metier",
                             "supra_region",
                             "sub_region",
                             "eez_indicator",
                             "geo_indicator",
                             "specon_tech")) %>%
    dplyr::mutate(
      confidential = dplyr::case_when(
        fishing_tech == "HOK" ~ "Y",
        TRUE ~ "N"))
  names(balbaya_effort) <- toupper(names(balbaya_effort))
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on effort data.\n",
      sep = "")
  # template checking ----
  if (template_checking == TRUE) {
    fdi_template_checking(fdi_table = balbaya_effort,
                          template_year = template_year,
                          table_id = "g")
  } else {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Warning: main table output not checking with official FDI template.\n",
        sep = "")
  }
  # export ----
  if (! is.null(x = table_export_path)) {
    fdi_table_export(fdi_table = balbaya_effort,
                     export_path = table_export_path,
                     table_id = "g")
  }
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on FDI table G generation.\n",
      sep = "")
  return(list("fdi_tables" = list("table_g" = balbaya_effort),
              "ad_hoc_tables" = list("effort_rectangle" = balbaya_effort_rectangle)))
}
