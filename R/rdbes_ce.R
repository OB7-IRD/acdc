#' @name rdbes_ce
#' @title Table Commercial Effort (CE) generation (RDBES process)
#' @description Process for generation and optionally extraction of the RDBES table CE (Commercial Effort).
#' @param observe_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the observe database.
#' @param balbaya_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the balbaya database.
#' @param fao_area_file_path {\link[base]{character}} expected. File path of the FAO area grid. The file format has to be .Rdata or .RData extension.
#' @param eez_area_file_path {\link[base]{character}} expected. File path of the EEZ area grid. The file format has to be .Rdata or .RData extension.
#' @param year_time_period {\link[base]{integer}} expected. Year(s) selected associated to the databases queries extractions.
#' @param fleet {\link[base]{integer}} expected. Fleet(s) selected associated to the databases queries extractions.
#' @param export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @return A R object with the RDBES table CE with potentially a csv extraction associated.
#' @export
rdbes_cl <- function(observe_con,
                     balbaya_con,
                     fao_area_file_path,
                     eez_area_file_path,
                     year_time_period,
                     fleet,
                     export_path = NULL) {
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on RDBES table CE generation.\n",
      sep = "")
  # 1 - Global variables assignement ----

  # 2 - Arguments verifications ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start arguments verifications.\n",
      sep = "")
  if (codama::r_type_checking(r_object = observe_con,
                              type = "list",
                              length = 2L,
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = observe_con,
                                   type = "list",
                                   length = 2L,
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = balbaya_con,
                              type = "list",
                              length = 2L,
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = balbaya_con,
                                   type = "list",
                                   length = 2L,
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = year_time_period,
                              type = "integer",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = year_time_period,
                                   type = "integer",
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = fleet,
                              type = "integer",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = fleet,
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
  if (codama::file_path_checking(file_path =  eez_area_file_path,
                                 extension = c("Rdata",
                                               "RData"),
                                 output = "logical") != TRUE) {
    return(codama::file_path_checking(file_path =  eez_area_file_path,
                                      extension = c("Rdata",
                                                    "RData"),
                                      output = "message"))
  }
  if ((! is.null(x = export_path))
      && codama::r_type_checking(r_object = export_path,
                                 type = "character",
                                 length = 1L,
                                 output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = export_path,
                                   type = "character",
                                   length = 1L,
                                   output = "message"))
  }
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful arguments verifications.\n",
      sep = "")
  # 3 - Databases extractions ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start databases extractions.\n",
      sep = "")
  balbaya_ce_data_query <- paste(readLines(con = system.file("sql",
                                                             "rdbes",
                                                             "balbaya_ce_rdbes.sql",
                                                             package = "acdc")),
                                 collapse = "\n")
  balbaya_ce_data_query <- DBI::sqlInterpolate(conn = balbaya_con[[2]],
                                               sql = balbaya_ce_data_query,
                                               year_time_period = DBI::SQL(paste0(year_time_period,
                                                                                  collapse = ", ")),
                                               fleet = DBI::SQL(paste0(fleet,
                                                                       collapse = ", ")))
  balbaya_ce_data <- DBI::dbGetQuery(conn = balbaya_con[[2]],
                                     statement = balbaya_ce_data_query)
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful databases extractions.\n",
      sep = "")
  # 4 - Other data extractions ---
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start other data extractions.\n",
      sep = "")
  referential_iso_3166 <- read.csv2(file = system.file("referentials",
                                                       "iso_3166.csv",
                                                       package = "acdc"))
  referential_vessel <- DBI::dbGetQuery(conn = observe_con[[2]],
                                        statement = DBI::SQL(paste(readLines(con = system.file("sql",
                                                                                               "rdbes",
                                                                                               "observe_vessel.sql",
                                                                                               package = "acdc")),
                                                                   collapse = "\n")))
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful other data extractions.\n",
      sep = "")
  # 5 - Data design ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start data design.\n",
      sep = "")
  # area and co.
  balbaya_ce_data_final <- furdeb::marine_area_overlay(data = balbaya_ce_data,
                                                       overlay_expected = "fao_eez_area",
                                                       longitude_name = "longitude_decimal",
                                                       latitude_name = "latitude_decimal",
                                                       fao_area_file_path = fao_area_file_path,
                                                       fao_overlay_level = "division",
                                                       auto_selection_fao = TRUE,
                                                       eez_area_file_path = eez_area_file_path,
                                                       for_fdi_use = TRUE,
                                                       silent = TRUE) %>%
    dplyr::rename(CEArea = best_fao_area)
  # others
  tmp <- balbaya_ce_data_final %>%
    dplyr::left_join(referential_iso_3166[, c("alpha_3_code",
                                              "alpha_2_code")],
                     by = c("vessel_fleet_country_fao" = "alpha_3_code")) %>%
    dplyr::rename(CLvesFlagCou = alpha_2_code) %>%
    dplyr::mutate(CErecType = "CL",
                  CEyear = lubridate::year(x = landing_date),
                  CEquar = lubridate::quarter(x = landing_date),
                  CEmonth = lubridate::month(x = landing_date),
                  CEstatRect = dplyr::case_when(
                    major_fao == "27" ~ "ices_statistical_area_missing",
                    TRUE ~ "-9"
                  ),
                  CEsoucStatRect = "EstPosData",
                  CEgsaSubarea = dplyr::case_when(
                    major_fao == "37" ~ "gsa_sub_area_missing",
                    TRUE ~ "NotApplicable"
                  ),
                  CLfreshWatNam = "NA",
                  CEnatFishAct = dplyr::case_when(
                    vessel_type_code == 6 ~ "PS_LPF_>0_0_0_TRO",
                    vessel_type_code == 2 ~ "LHP_LPF_0_0_0_MSP",
                    TRUE ~ "national_fishing_activity_missing"
                  ),
                  CEmetier6 = dplyr::case_when(
                    vessel_type_code == 6 ~ "PS_LPF_>0_0_0",
                    vessel_type_code == 2 ~ "LHP_LPF_0_0_0",
                    TRUE ~ "metier6_missing"
                  ),
                  CEIBmitiDev = "None",
                  CEvesLenCat = dplyr::case_when(
                    vessel_length < 6 ~ "VL0006",
                    vessel_length >= 6 & vessel_length < 8 ~ "VL0608",
                    vessel_length >= 8 & vessel_length < 10 ~ "VL0810",
                    vessel_length >= 10 & vessel_length < 12 ~ "VL1012",
                    vessel_length >= 12 & vessel_length < 15 ~ "VL0608",
                    vessel_length >= 15 & vessel_length < 18 ~ "VL1518",
                    vessel_length >= 18 & vessel_length < 24 ~ "VL0608",
                    vessel_length >= 24 & vessel_length < 40 ~ "VL2440",
                    vessel_length >= 40 ~ "VL40XX",
                    TRUE ~ "NK"
                  ),
                  CEfishTech = dplyr::case_when(
                    vessel_type_code == 6 ~ "PS",
                    vessel_type_code == 2 ~ "HOK",
                    TRUE ~ "fishing_technique_missing"
                  ),
                  CEsupReg = "OFR",
                  CEgeoInd = "IWE",
                  CEspeConTech = "NA",
                  CLdeepSeaReg = "N",
                  CEoffVesHoursAtSea = round(x = CEoffVesHoursAtSea,
                                             digits = 2),
                  CEoffDaySea = round(x = CEoffVesHoursAtSea / 24,
                                      digits = 2),
                  CESciDaySea = CEoffDaySea,
                  CEoffFishDay = round(x = dplyr::case_when(
                    ocean == 1 ~ fishing_time_hours / 12,
                    ocean == 2 ~ fishing_time_hours / 13,
                    TRUE ~ NA_real_), digits = 2),
                  CEsciFishDay = CEoffFishDay)




  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful data design.\n",
      sep = "")
  # 6 - Extraction ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on RDBES table CE generation.\n",
      sep = "")
  return()
}



