#' @name rdbes_ce
#' @title Table Commercial Effort (CE) generation (RDBES process)
#' @description Process for generation and optionally extraction of the RDBES table CE (Commercial Effort).
#' @param observe_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the observe database.
#' @param balbaya_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the balbaya database.
#' @param fao_area_file_path {\link[base]{character}} expected. File path of the FAO area grid. The file format has to be .Rdata or .RData extension.
#' @param eez_area_file_path {\link[base]{character}} expected. File path of the EEZ area grid. The file format has to be .Rdata or .RData extension.
#' @param year_time_period {\link[base]{integer}} expected. Year(s) selected associated to the databases queries extractions.
#' @param flag {\link[base]{integer}} expected. Flag(s) selected associated to the databases queries extractions.
#' @param major_fao_area_filter {\link[base]{integer}} expected. By default NULL. Sub selection of major fao area.
#' @param hash_algorithms {\link[base]{integer}} expected. By default NULL. The hashing algorithms to be used for the CEencrypVesIds variable. You can choose any modality of the argument "algo" or the function {\link[digest]{digest}}.
#' @param export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @return A R object with the RDBES table CE with potentially a csv extraction associated.
#' @export
#' @importFrom codama r_type_checking file_path_checking
#' @importFrom DBI sqlInterpolate SQL dbGetQuery
#' @importFrom dplyr filter rename setdiff inner_join select left_join mutate case_when group_by reframe n n_distinct
#' @importFrom furdeb marine_area_overlay
#' @importFrom lubridate year quarter month
#' @importFrom stringr str_flatten str_extract
#' @importFrom utils write.csv
#' @importFrom digest digest
rdbes_ce <- function(observe_con,
                     balbaya_con,
                     fao_area_file_path,
                     eez_area_file_path,
                     year_time_period,
                     flag,
                     major_fao_area_filter = NULL,
                     hash_algorithms = NULL,
                     export_path = NULL) {
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Start process on RDBES table CE generation.",
          sep = "")
  # 1 - Global variables assignement ----
  vessel_type_code <- NULL
  best_fao_area <- NULL
  eez_indicator <- NULL
  major_fao <- NULL
  vessel_name <- NULL
  vessel_id <- NULL
  alpha_2_code <- NULL
  landing_date <- NULL
  hours_at_sea <- NULL
  CEoffDaySea <- NULL
  CEoffFishDay <- NULL
  fishing_time_hours <- NULL
  CEoffVesFishHour <- NULL
  CEoffSoakMeterHour <- NULL
  vessel_engine_power_cv <- NULL
  CEoffkWDaySea <- NULL
  CEoffkWFishDay <- NULL
  CEoffkWFishHour <- NULL
  vessel_volume <- NULL
  vessel_flag_country_fao <- NULL
  latitude_decimal <- NULL
  longitude_decimal <- NULL
  vessel_code <- NULL
  vessel_type_label <- NULL
  vessel_length <- NULL
  operation_type_code <- NULL
  operation_type_label <- NULL
  ocean <- NULL
  subarea_fao <- NULL
  division_fao <- NULL
  eez <- NULL
  eez_country <- NULL
  CEloc <- NULL
  CEarea <- NULL
  CEeconZoneIndi <- NULL
  CEencrypVesIds <- NULL
  CEvesFlagCou <- NULL
  CEeconZone <- NULL
  CEyear <- NULL
  CEquar <- NULL
  CEmonth <- NULL
  CEvesLenCat <- NULL
  number_set <- NULL
  CEoffVesHoursAtSea <- NULL
  CESciDaySea <- NULL
  CEsciFishDay <- NULL
  CEsciVesFishHour <- NULL
  CEsciSoakMeterHour <- NULL
  CEscikWDaySea <- NULL
  CEscikWFishDay <- NULL
  CEscikWFishHour <- NULL
  CEgTDaySea <- NULL
  CEgTFishDay <- NULL
  CEgTFishHour <- NULL
  CEnumFracTrips <- NULL
  CEnumDomTrip <- NULL
  CEoffNumHaulSet <- NULL
  CErecType <- NULL
  CEdTypSciEff <- NULL
  CEdSouSciEff <- NULL
  CEsampScheme <- NULL
  CEstatRect <- NULL
  CEsoucStatRect <- NULL
  CEfishManUnit <- NULL
  CEgsaSubarea <- NULL
  CEjurisdArea <- NULL
  CEfishAreaCat <- NULL
  CEfreshWatNam <- NULL
  CEnatFishAct <- NULL
  CEmetier6 <- NULL
  CEIBmitiDev <- NULL
  CEfishTech <- NULL
  CEmesSizRan <- NULL
  CEsupReg <- NULL
  CEgeoInd <- NULL
  CEspeConTech <- NULL
  CEdeepSeaReg <- NULL
  CEsciNumHaulSet <- NULL
  CEnumUniqVes <- NULL
  CEgearDim <- NULL
  CEnumFAD <- NULL
  CEnumSupVes <- NULL
  CEfishDaysErrMeaValTyp <- NULL
  CEfishDaysErrMeaValFirst <- NULL
  CEfishDaysErrMeaValSecond <- NULL
  CEscientificFishingDaysQualBias <- NULL
  CEconfiFlag <- NULL
  CEencrypVesIds_hash <- NULL
  # 2 - Arguments verifications ----
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Start arguments verifications.",
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
  if (codama::file_path_checking(file_path =  eez_area_file_path,
                                 extension = c("Rdata",
                                               "RData"),
                                 output = "logical") != TRUE) {
    return(codama::file_path_checking(file_path =  eez_area_file_path,
                                      extension = c("Rdata",
                                                    "RData"),
                                      output = "message"))
  }
  if (codama::r_type_checking(r_object = major_fao_area_filter,
                              type = "integer",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = major_fao_area_filter,
                                   type = "integer",
                                   output = "message"))
  }
  if ((! is.null(x = hash_algorithms))
      && codama::r_type_checking(r_object = hash_algorithms,
                              type = "character",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = hash_algorithms,
                                   type = "character",
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
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Successful arguments verifications.",
          sep = "")
  # 3 - Databases extractions ----
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Start databases extractions.",
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
                                               flag = DBI::SQL(paste0(flag,
                                                                      collapse = ", ")))
  balbaya_ce_data <- DBI::dbGetQuery(conn = balbaya_con[[2]],
                                     statement = balbaya_ce_data_query)
  # remove vessel_type_code 10 (supply) from data, check with Laurent after, ticket #287 data-analysis
  balbaya_ce_data <- dplyr::filter(.data = balbaya_ce_data,
                                   vessel_type_code != 10)
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Successful databases extractions.",
          sep = "")
  # 4 - Other data extractions ---
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Start other data extractions.",
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
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Successful other data extractions.",
          sep = "")
  # 5 - Data design ----
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Start data design.",
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
    dplyr::rename(CEarea = best_fao_area,
                  CEeconZoneIndi = eez_indicator)
  if (! is.null(x = major_fao_area_filter)) {
    balbaya_ce_data_final <- dplyr::filter(.data = balbaya_ce_data_final,
                                           major_fao %in% major_fao_area_filter)
  }
  # vessel id
  if (! all(unique(x = balbaya_ce_data_final$vessel_code) %in% referential_vessel$vessel_code)) {
    stop(format(x = Sys.time(),
                format = "%Y-%m-%d %H:%M:%S"),
         " - At least one vessel id is not in the vessel referential.\n",
         "Check the following vessel id(s):\n",
         paste(dplyr::setdiff(x = unique(x = balbaya_ce_data_final$vessel_code),
                              y = referential_vessel$vessel_code),
               collapse = ", "),
         sep = "")
  } else {
    balbaya_ce_data_final <- dplyr::inner_join(x = balbaya_ce_data_final,
                                               y = dplyr::select(.data = referential_vessel,
                                                                 -vessel_name),
                                               by = "vessel_code") %>%
      dplyr::rename(CEencrypVesIds = vessel_id)
  }
  if (! is.null(x = hash_algorithms)) {
    balbaya_ce_data_final <- dplyr::inner_join(x = balbaya_ce_data_final,
                             y = dplyr::tibble("CEencrypVesIds" = unique(x = balbaya_ce_data_final$CEencrypVesIds)) %>%
                               dplyr::rowwise() %>%
                               dplyr::mutate(CEencrypVesIds_hash = digest::digest(CEencrypVesIds, algo = !!hash_algorithms)),
                             by = "CEencrypVesIds") %>%
      dplyr::select(-CEencrypVesIds) %>%
      dplyr::rename(CEencrypVesIds = CEencrypVesIds_hash)
  }
  # others
  balbaya_ce_data_suprem <- balbaya_ce_data_final %>%
    dplyr::left_join(referential_iso_3166[, c("alpha_3_code",
                                              "alpha_2_code")],
                     by = c("vessel_flag_country_fao" = "alpha_3_code")) %>%
    dplyr::rename(CEvesFlagCou = alpha_2_code) %>%
    dplyr::left_join(referential_iso_3166[, c("alpha_3_code",
                                              "alpha_2_code")],
                     by = c("eez_country" = "alpha_3_code")) %>%
    dplyr::rename(CEeconZone = alpha_2_code) %>%
    dplyr::mutate(CEyear = lubridate::year(x = landing_date),
                  CEquar = lubridate::quarter(x = landing_date),
                  CEmonth = lubridate::month(x = landing_date),
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
                  CEeconZone = dplyr::case_when(
                    is.na(x = CEeconZone) ~ "NTHH",
                    TRUE ~ CEeconZone
                  ),
                  CEoffVesHoursAtSea = round(x = hours_at_sea,
                                             digits = 2),
                  CEoffDaySea = round(x = hours_at_sea / 24,
                                      digits = 2),
                  CESciDaySea = CEoffDaySea,
                  CEoffFishDay = round(x = dplyr::case_when(
                    ocean == 1 ~ fishing_time_hours / 12,
                    ocean == 2 ~ fishing_time_hours / 13,
                    TRUE ~ NA_real_),
                    digits = 2),
                  CEsciFishDay = CEoffFishDay,
                  CEoffVesFishHour = round(x = fishing_time_hours,
                                           digits = 2),
                  CEsciVesFishHour = CEoffVesFishHour,
                  CEoffSoakMeterHour = NA,
                  CEsciSoakMeterHour = CEoffSoakMeterHour,
                  CEoffkWDaySea = round(x = CEoffDaySea * vessel_engine_power_cv * 0.73539875),
                  CEscikWDaySea = CEoffkWDaySea,
                  CEoffkWFishDay = round(CEoffFishDay * vessel_engine_power_cv * 0.73539875),
                  CEscikWFishDay = CEoffkWFishDay,
                  CEoffkWFishHour = round(fishing_time_hours * vessel_engine_power_cv * 0.73539875),
                  CEscikWFishHour = CEoffkWFishHour,
                  CEgTDaySea = round(CEoffDaySea * (0.2 + 0.02 * log10(vessel_volume)) * vessel_volume),
                  CEgTFishDay = round(CEoffFishDay * (0.2 + 0.02 * log10(vessel_volume)) * vessel_volume),
                  CEgTFishHour = round(fishing_time_hours * (0.2 + 0.02 * log10(vessel_volume)) * vessel_volume)) %>%
    dplyr::select(-vessel_flag_country_fao,
                  -latitude_decimal,
                  -longitude_decimal,
                  -vessel_code,
                  -vessel_type_label,
                  -vessel_length,
                  -hours_at_sea,
                  -fishing_time_hours,
                  -operation_type_code,
                  -operation_type_label,
                  -vessel_engine_power_cv,
                  -vessel_volume,
                  -ocean,
                  -major_fao,
                  -subarea_fao,
                  -division_fao,
                  -eez,
                  -eez_country) %>%
    dplyr::group_by(landing_date,
                    vessel_type_code,
                    CEloc,
                    CEarea,
                    CEeconZoneIndi,
                    CEencrypVesIds,
                    CEvesFlagCou,
                    CEeconZone,
                    CEyear,
                    CEquar,
                    CEmonth,
                    CEvesLenCat) %>%
    dplyr::reframe(number_set = sum(number_set),
                   CEoffVesHoursAtSea = sum(CEoffVesHoursAtSea),
                   CEoffDaySea = sum(CEoffDaySea),
                   CESciDaySea = sum(CESciDaySea),
                   CEoffFishDay = sum(CEoffFishDay),
                   CEsciFishDay = sum(CEsciFishDay),
                   CEoffVesFishHour = sum(CEoffVesFishHour),
                   CEsciVesFishHour = sum(CEsciVesFishHour),
                   CEoffSoakMeterHour = sum(CEoffSoakMeterHour),
                   CEsciSoakMeterHour = sum(CEsciSoakMeterHour),
                   CEoffkWDaySea = sum(CEoffkWDaySea),
                   CEscikWDaySea = sum(CEscikWDaySea),
                   CEoffkWFishDay = sum(CEoffkWFishDay),
                   CEscikWFishDay = sum(CEscikWFishDay),
                   CEoffkWFishHour = sum(CEoffkWFishHour),
                   CEscikWFishHour = sum(CEscikWFishHour),
                   CEgTDaySea = sum(CEgTDaySea),
                   CEgTFishDay = sum(CEgTFishDay),
                   CEgTFishHour = sum(CEgTFishHour)) %>%
    dplyr::group_by(landing_date,
                    CEencrypVesIds) %>%
    dplyr::mutate(CEnumFracTrips = round(x = 1/dplyr::n(),
                                         digits = 2),
                  CEnumDomTrip = dplyr::case_when(
                    CEoffVesHoursAtSea == max(CEoffVesHoursAtSea) ~ 1,
                    TRUE ~ 0
                  )) %>%
    dplyr::group_by(vessel_type_code,
                    CEloc,
                    CEarea,
                    CEeconZoneIndi,
                    CEvesFlagCou,
                    CEeconZone,
                    CEyear,
                    CEquar,
                    CEmonth,
                    CEvesLenCat) %>%
    dplyr::reframe(CEoffVesHoursAtSea = sum(CEoffVesHoursAtSea),
                   CEoffDaySea = sum(CEoffDaySea),
                   CESciDaySea = sum(CESciDaySea),
                   CEoffFishDay = sum(CEoffFishDay),
                   CEsciFishDay = sum(CEsciFishDay),
                   CEoffVesFishHour = sum(CEoffVesFishHour),
                   CEsciVesFishHour = sum(CEsciVesFishHour),
                   CEoffSoakMeterHour = sum(CEoffSoakMeterHour),
                   CEsciSoakMeterHour = sum(CEsciSoakMeterHour),
                   CEoffkWDaySea = sum(CEoffkWDaySea),
                   CEscikWDaySea = sum(CEscikWDaySea),
                   CEoffkWFishDay = sum(CEoffkWFishDay),
                   CEscikWFishDay = sum(CEscikWFishDay),
                   CEoffkWFishHour = sum(CEoffkWFishHour),
                   CEscikWFishHour = sum(CEscikWFishHour),
                   CEgTDaySea = sum(CEgTDaySea),
                   CEgTFishDay = sum(CEgTFishDay),
                   CEgTFishHour = sum(CEgTFishHour),
                   CEoffNumHaulSet = sum(number_set),
                   CEnumFracTrips = sum(CEnumFracTrips),
                   CEnumDomTrip = sum(CEnumDomTrip),
                   CEnumUniqVes = dplyr::n_distinct(CEencrypVesIds),
                   CEencrypVesIds = stringr::str_flatten(unique(CEencrypVesIds),
                                                         collapse = ", ")) %>%
    dplyr::mutate(CErecType = "CL",
                  CEdTypSciEff = "Estimate",
                  CEdSouSciEff = "Combcd",
                  CEsampScheme = NA,
                  CEstatRect = dplyr::case_when(
                    stringr::str_extract(string = CEarea,
                                         pattern = "^[:digit:].") == "27" ~ "ices_statistical_area_missing",
                    TRUE ~ "-9"
                  ),
                  CEsoucStatRect = "EstPosData",
                  CEfishManUnit = NA,
                  CEgsaSubarea = dplyr::case_when(
                    stringr::str_extract(string = CEarea,
                                         pattern = "^[:digit:].") == "37" ~ "gsa_sub_area_missing",
                    TRUE ~ "NotApplicable"
                  ),
                  CEjurisdArea = NA,
                  CEfishAreaCat = "NA",
                  CEfreshWatNam = "NA",
                  CEeconZoneIndi = NA,
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
                  CEfishTech = dplyr::case_when(
                    vessel_type_code == 6 ~ "PS",
                    vessel_type_code == 2 ~ "HOK",
                    TRUE ~ "fishing_technique_missing"
                  ),
                  CEmesSizRan = NA,
                  CEsupReg = "OFR",
                  CEgeoInd = "IWE",
                  CEspeConTech = "NA",
                  CEdeepSeaReg = "N",
                  CEoffNumHaulSet = dplyr::case_when(
                    vessel_type_code == 2 ~ NA_integer_,
                    TRUE ~ CEoffNumHaulSet
                  ),
                  CEsciNumHaulSet = CEoffNumHaulSet,
                  CEgearDim = NA,
                  CEnumFAD = NA,
                  CEnumSupVes = NA,
                  CEfishDaysErrMeaValTyp = NA,
                  CEfishDaysErrMeaValFirst = NA,
                  CEfishDaysErrMeaValSecond = NA,
                  CEscientificFishingDaysQualBias = NA,
                  CEconfiFlag = dplyr::case_when(
                    CEnumUniqVes <= 3 ~ "Y",
                    TRUE ~ "N"
                  )) %>%
    dplyr::select(CErecType,
                  CEdTypSciEff,
                  CEdSouSciEff,
                  CEsampScheme,
                  CEvesFlagCou,
                  CEyear,
                  CEquar,
                  CEmonth,
                  CEarea,
                  CEstatRect,
                  CEsoucStatRect,
                  CEfishManUnit,
                  CEgsaSubarea,
                  CEjurisdArea,
                  CEfishAreaCat,
                  CEfreshWatNam,
                  CEeconZone,
                  CEeconZoneIndi,
                  CEnatFishAct,
                  CEmetier6,
                  CEIBmitiDev,
                  CEloc,
                  CEvesLenCat,
                  CEfishTech,
                  CEmesSizRan,
                  CEsupReg,
                  CEgeoInd,
                  CEspeConTech,
                  CEdeepSeaReg,
                  CEoffVesHoursAtSea,
                  CEnumFracTrips,
                  CEnumDomTrip,
                  CEoffDaySea,
                  CESciDaySea,
                  CEoffFishDay,
                  CEsciFishDay,
                  CEoffNumHaulSet,
                  CEsciNumHaulSet,
                  CEoffVesFishHour,
                  CEsciVesFishHour,
                  CEoffSoakMeterHour,
                  CEsciSoakMeterHour,
                  CEoffkWDaySea,
                  CEscikWDaySea,
                  CEoffkWFishDay,
                  CEscikWFishDay,
                  CEoffkWFishHour,
                  CEscikWFishHour,
                  CEgTDaySea,
                  CEgTFishDay,
                  CEgTFishHour,
                  CEnumUniqVes,
                  CEgearDim,
                  CEnumFAD,
                  CEnumSupVes,
                  CEfishDaysErrMeaValTyp,
                  CEfishDaysErrMeaValFirst,
                  CEfishDaysErrMeaValSecond,
                  CEscientificFishingDaysQualBias,
                  CEconfiFlag,
                  CEencrypVesIds)
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Successful data design.",
          sep = "")
  # remove strange effort equal to 0, check with Laurent ticket #288
  balbaya_ce_data_suprem <- dplyr::filter(.data = balbaya_ce_data_suprem,
                                          CEoffVesHoursAtSea != 0)
  # remove CEofficialFishingDays equal to 0 due to check in data model. Work on that later
  balbaya_ce_data_suprem <- dplyr::filter(.data = balbaya_ce_data_suprem,
                                          CEoffFishDay != 0)
  # remove CEofficialNumberOfHaulsOrSets equal to 0 due to check in data model. Work on that later
  balbaya_ce_data_suprem <- dplyr::filter(.data = balbaya_ce_data_suprem,
                                          CEoffNumHaulSet != 0)
  # 6 - Extraction ----
  if (! is.null(x = export_path)) {
    message(format(x = Sys.time(),
                   format = "%Y-%m-%d %H:%M:%S"),
            " - Start CE data extractions.",
            sep = "")
    utils::write.csv(x = balbaya_ce_data_suprem,
                     file = file.path(export_path,
                                      paste(format(as.POSIXct(Sys.time()),
                                                   "%Y%m%d_%H%M%S"),
                                            "rdbes_ce.csv",
                                            sep = "_")),
                     row.names = FALSE,
                     na = "")
    message(format(x = Sys.time(),
                   format = "%Y-%m-%d %H:%M:%S"),
            " - Successful CE data extractions.\n",
            "File available in the directory ",
            export_path,
            sep = "")
  }
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Successful process on RDBES table CE generation.",
          sep = "")
  return(balbaya_ce_data_suprem)
}



