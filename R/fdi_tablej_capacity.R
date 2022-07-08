#' @name fdi_tablej_capacity
#' @title Table J capacity generation (FDI process)
#' @description Process for generation and optionally extraction of the FDI table J (capacity).
#' @param balbaya_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the balbaya database.
#' @param period {\link[base]{integer}} expected. Year period for data extractions.
#' @param gear {\link[base]{integer}}. Gear(s) selection for data extractions.
#' @param flag {\link[base]{integer}} expected. Flag(s) selection for data extractions.
#' @param fao_area_file_path {\link[base]{character}} expected. File path of the FAO area grid. The file format has to be .RData.
#' @param template_checking {\link[base]{logical}} expected. By default TRUE. Checking FDI table generated regarding the official FDI template.
#' @param template_year {\link[base]{integer}} expected. By default NULL. Template year.
#' @param table_export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @return The process returns a list with the FDI table J inside.
#' @export
#' @importFrom codama r_type_checking file_path_checking
#' @importFrom DBI sqlInterpolate SQL dbGetQuery
#' @importFrom furdeb marine_area_overlay
#' @importFrom dplyr mutate case_when rowwise group_by summarise inner_join select left_join rename
fdi_tablej_capacity <- function(balbaya_con,
                                period,
                                gear,
                                flag,
                                fao_area_file_path,
                                template_checking = TRUE,
                                template_year = NULL,
                                table_export_path = NULL) {
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on FDI table J generation.\n",
      sep = "")
  # global variables assignement ----
  period <- NULL
  country <- NULL
  year <- NULL
  vessel_id <- NULL
  vessel_length <- NULL
  sub_region <- NULL
  fishing_day <- NULL
  value <- NULL
  fishing_tech <- NULL
  supra_region <- NULL
  geo_indicator <- NULL
  tottrips <- NULL
  totkw <- NULL
  totgt <- NULL
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
  # capacity data extraction ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on capacity data.\n",
      sep = "")
  balbaya_capacity_fleet_segment_query <- paste(readLines(con = system.file("sql",
                                                                            "fdi",
                                                                            "balbaya_capacity_fleet_segment_fdi.sql",
                                                                            package = "acdc")),
                                                collapse = '\n')
  balbaya_capacity_fleet_segment_query <- DBI::sqlInterpolate(conn = balbaya_con,
                                                              sql = balbaya_capacity_fleet_segment_query,
                                                              period = DBI::SQL(paste0(period,
                                                                                       collapse = ", ")),
                                                              flag = DBI::SQL(paste0(flag,
                                                                                     collapse = ", ")),
                                                              gear = DBI::SQL(paste0(gear,
                                                                                     collapse = ", ")))
  balbaya_capacity_fleet_segment <- DBI::dbGetQuery(conn = balbaya_con,
                                                    statement = balbaya_capacity_fleet_segment_query)
  balbaya_capacity_query <- paste(readLines(con = system.file("sql",
                                                              "fdi",
                                                              "balbaya_capacity_fdi.sql",
                                                              package = "acdc")),
                                  collapse = '\n')
  balbaya_capacity_query <- DBI::sqlInterpolate(conn = balbaya_con,
                                                sql = balbaya_capacity_query,
                                                period = DBI::SQL(paste0(period,
                                                                         collapse = ", ")),
                                                flag = DBI::SQL(paste0(flag,
                                                                       collapse = ", ")),
                                                gear = DBI::SQL(paste0(gear,
                                                                       collapse = ", ")))
  balbaya_capacity <- DBI::dbGetQuery(conn = balbaya_con,
                                      statement = balbaya_capacity_query)
  balbaya_maxseadays_query <- paste(readLines(con = system.file("sql",
                                                                "fdi",
                                                                "balbaya_maxseadays_fdi.sql",
                                                                package = "acdc")),
                                    collapse = '\n')
  balbaya_maxseadays_query <- DBI::sqlInterpolate(conn = balbaya_con,
                                                  sql = balbaya_maxseadays_query,
                                                  period = DBI::SQL(paste0(period,
                                                                           collapse = ", ")),
                                                  flag = DBI::SQL(paste0(flag,
                                                                         collapse = ", ")),
                                                  gear = DBI::SQL(paste0(gear,
                                                                         collapse = ", ")))
  balbaya_maxseadays <- DBI::dbGetQuery(conn = balbaya_con,
                                        statement = balbaya_maxseadays_query)
  balbaya_capacity_principal_sub_region_query <- paste(readLines(con = system.file("sql",
                                                                                   "fdi",
                                                                                   "balbaya_capacity_principal_sub_region_fdi.sql",
                                                                                   package = "acdc")),
                                                       collapse = '\n')
  balbaya_capacity_principal_sub_region_query <- DBI::sqlInterpolate(conn = balbaya_con,
                                                                     sql = balbaya_capacity_principal_sub_region_query,
                                                                     period = DBI::SQL(paste0(period,
                                                                                              collapse = ", ")),
                                                                     flag = DBI::SQL(paste0(flag,
                                                                                            collapse = ", ")),
                                                                     gear = DBI::SQL(paste0(gear,
                                                                                            collapse = ", ")))
  balbaya_capacity_principal_sub_region <- DBI::dbGetQuery(conn = balbaya_con,
                                                           statement = balbaya_capacity_principal_sub_region_query)
  balbaya_capacity_principal_sub_region <- furdeb::marine_area_overlay(data = balbaya_capacity_principal_sub_region,
                                                                       overlay_expected = "fao_area",
                                                                       longitude_name = "longitude",
                                                                       latitude_name = "latitude",
                                                                       fao_area_file_path = fao_area_file_path,
                                                                       fao_overlay_level = "division",
                                                                       auto_selection_fao = TRUE,
                                                                       silent = TRUE)
  if (any(is.na(x = unique(x = balbaya_capacity_principal_sub_region$best_fao_area)))) {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Warning: at least one position have not fao area associated. Data associated will be deleted. Check the log.\n",
        sep = "")
    na_fao_area <- unique(balbaya_capacity_principal_sub_region[is.na(x = balbaya_capacity_principal_sub_region$best_fao_area), c("cwp",
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
    balbaya_capacity_principal_sub_region <- balbaya_capacity_principal_sub_region[! is.na(x = balbaya_capacity_principal_sub_region$best_fao_area), ]
  }
  balbaya_capacity_principal_sub_region <- dplyr::mutate(.data = balbaya_capacity_principal_sub_region,
                                                         sub_region = dplyr::case_when(
                                                           best_fao_area %in% c("47.A.0",
                                                                                "47.A.1") ~ "47.A",
                                                           best_fao_area %in% c("47.B.1") ~ "47.B",
                                                           best_fao_area %in% c("47.1.1",
                                                                                "47.1.2",
                                                                                "47.1.3",
                                                                                "41.1.4") ~ "47.1",
                                                           best_fao_area %in% c("34.2") ~ "34.2.0",
                                                           TRUE ~ best_fao_area
                                                         )) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      fishing_day = dplyr::case_when(ocean == 1 ~ fishing_time / 12,
                                     ocean == 2 ~ fishing_time / 13)) %>%
    dplyr::group_by(country,
                    year,
                    vessel_id,
                    vessel_length,
                    sub_region) %>%
    dplyr::summarise(fishing_day = sum(fishing_day),
                     .groups = "drop") %>%
    dplyr::arrange(country,
                   year,
                   vessel_id,
                   vessel_length,
                   fishing_day)
  balbaya_capacity_principal_sub_region_final <- balbaya_capacity_principal_sub_region %>%
    dplyr::group_by(country,
                    year,
                    vessel_id,
                    vessel_length) %>%
    dplyr::summarise(value = max(fishing_day),
                     .groups = "drop") %>%
    dplyr::inner_join(balbaya_capacity_principal_sub_region,
                      by = c("country",
                             "year",
                             "vessel_id",
                             "vessel_length",
                             "value" = "fishing_day")) %>%
    dplyr::select(-value)
  balbaya_capacity_final <- balbaya_capacity %>%
    dplyr::inner_join(balbaya_capacity_principal_sub_region_final,
                      by = c("country",
                             "year",
                             "vessel_id",
                             "vessel_length")) %>%
    dplyr::group_by(country,
                    year,
                    vessel_length,
                    fishing_tech,
                    supra_region,
                    geo_indicator,
                    sub_region) %>%
    dplyr::summarise(tottrips = sum(tottrips),
                     totkw = as.character(x = sum(totkw)),
                     totgt = sum(totgt),
                     .groups = "drop") %>%
    dplyr::left_join(balbaya_capacity_fleet_segment,
                     by = c("country",
                            "year",
                            "vessel_length",
                            "fishing_tech",
                            "supra_region",
                            "geo_indicator")) %>%
    dplyr::left_join(balbaya_maxseadays,
                     by = c("country",
                            "year",
                            "vessel_length",
                            "fishing_tech",
                            "supra_region",
                            "geo_indicator")) %>%
    dplyr::mutate(
      totkw = dplyr::case_when(
        is.na(totkw) ~"NK",
        TRUE ~ totkw)) %>%
    dplyr::rename(principal_sub_region = sub_region)
  names(x = balbaya_capacity_final) <- toupper(x = names(x = balbaya_capacity_final))
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on capacity data.\n",
      sep = "")
  # template checking ----
  if (template_checking == TRUE) {
    fdi_template_checking(fdi_table = balbaya_capacity_final,
                          template_year = template_year,
                          table_id = "j")
  } else {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Warning: main table output not checking with official FDI template.\n",
        sep = "")
  }
  # export ----
  if (! is.null(x = table_export_path)) {
    fdi_table_export(fdi_table = balbaya_capacity_final,
                     export_path = table_export_path,
                     table_id = "j")
  }
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on FDI table J generation.\n",
      sep = "")
  return(list("fdi_tables" = list("table_j" = balbaya_capacity_final)))
}
