#' @name rdbes_cl
#' @title Table Commercial Landing (CL) generation (RDBES process)
#' @description Process for generation and optionally extraction of the RDBES table CL (Commercial Landing).
#' @param observe_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the observe database.
#' @param balbaya_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the balbaya database.
#' @param fao_area_file_path {\link[base]{character}} expected. File path of the FAO area grid. The file format has to be .Rdata or .RData extension.
#' @param eez_area_file_path {\link[base]{character}} expected. File path of the EEZ area grid. The file format has to be .Rdata or .RData extension.
#' @param year_time_period {\link[base]{integer}} expected. Year(s) selected associated to the databases queries extractions.
#' @param flag {\link[base]{integer}} expected. Flag(s) selected associated to the databases queries extractions.
#' @param export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @return A R object with the RDBES table CL with potentially a csv extraction associated.
#' @export
#' @importFrom utils write.csv2
#' @importFrom codama r_type_checking file_path_checking
#' @importFrom DBI sqlInterpolate SQL dbGetQuery
#' @importFrom furdeb marine_area_overlay
#' @importFrom tidyr tibble
#' @importFrom dplyr full_join select n_distinct summarise group_by mutate case_when filter inner_join rename rowwise left_join
#' @importFrom worrms wm_name2id
#' @importFrom lubridate year quarter month
rdbes_cl <- function(observe_con,
                     balbaya_con,
                     fao_area_file_path,
                     eez_area_file_path,
                     year_time_period,
                     flag,
                     export_path = NULL) {
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Start process on RDBES table CL generation.",
          sep = "")
  # 1 - Global variables assignement ----
  CLFDIconCod <- NULL
  CLFDIconCod_balbaya <- NULL
  CLFDIconCod_observe <- NULL
  CLIBmitiDev <- NULL
  CLarea <- NULL
  CLcatchCat <- NULL
  CLconfiFlag <- NULL
  CLdSouSciWeig <- NULL
  CLdTypSciWeig <- NULL
  CLdeepSeaReg <- NULL
  CLeconZone <- NULL
  CLencrypVesIds <- NULL
  CLexpDiff <- NULL
  CLfishTech <- NULL
  CLfreshWatNam <- NULL
  CLgeoInd <- NULL
  CLgsaSubarea <- NULL
  CLlanCou <- NULL
  CLlandCat <- NULL
  CLloc <- NULL
  CLmetier6 <- NULL
  CLmonth <- NULL
  CLnatFishAct <- NULL
  CLnumUniqVes <- NULL
  CLnumUniqVes_balbaya <- NULL
  CLnumUniqVes_observe <- NULL
  CLoffWeight <- NULL
  CLquar <- NULL
  CLrecType <- NULL
  CLregDisCat <- NULL
  CLsciWeight <- NULL
  CLspeConTech <- NULL
  CLspecCode <- NULL
  CLspecFAO <- NULL
  CLstatRect <- NULL
  CLsupReg <- NULL
  CLtotOffLanVal <- NULL
  CLvesFlagCou <- NULL
  CLvesLenCat <- NULL
  CLyear <- NULL
  alpha_2_code <- NULL
  best_fao_area <- NULL
  landing_date <- NULL
  specie_scientific_name <- NULL
  vessel_code <- NULL
  vessel_count <- NULL
  vessel_name <- NULL
  worms_aphia_id <- NULL
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
  observe_ps_bb_cl_data_query <- paste(readLines(con = system.file("sql",
                                                                   "rdbes",
                                                                   "observe_ps_bb_cl_rdbes.sql",
                                                                   package = "acdc")),
                                       collapse = "\n")
  observe_ps_bb_cl_data_query <- DBI::sqlInterpolate(conn = observe_con[[2]],
                                                     sql = observe_ps_bb_cl_data_query,
                                                     year_time_period = DBI::SQL(paste0(year_time_period,
                                                                                        collapse = ", ")),
                                                     flag = DBI::SQL(paste0("'",
                                                                            paste0(flag,
                                                                                   collapse = "', '"),
                                                                            "'")))
  observe_ps_bb_cl_data <- DBI::dbGetQuery(conn = observe_con[[2]],
                                           statement = observe_ps_bb_cl_data_query)
  balbaya_cl_data_query <- paste(readLines(con = system.file("sql",
                                                             "rdbes",
                                                             "balbaya_cl_rdbes.sql",
                                                             package = "acdc")),
                                 collapse = "\n")
  balbaya_cl_data_query <- DBI::sqlInterpolate(conn = balbaya_con[[2]],
                                               sql = balbaya_cl_data_query,
                                               year_time_period = DBI::SQL(paste0(year_time_period,
                                                                                  collapse = ", ")),
                                               flag = DBI::SQL(paste0(flag,
                                                                      collapse = ", ")))
  balbaya_cl_data <- DBI::dbGetQuery(conn = balbaya_con[[2]],
                                     statement = balbaya_cl_data_query)
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Successful databases extractions.",
          sep = "")
  # 4 - Other data extractions ----
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
  observe_ps_bb_cl_data_final <- furdeb::marine_area_overlay(data = observe_ps_bb_cl_data,
                                                             overlay_expected = "fao_eez_area",
                                                             longitude_name = "longitude_decimal",
                                                             latitude_name = "latitude_decimal",
                                                             fao_area_file_path = fao_area_file_path,
                                                             fao_overlay_level = "division",
                                                             auto_selection_fao = TRUE,
                                                             eez_area_file_path = eez_area_file_path,
                                                             for_fdi_use = TRUE,
                                                             silent = TRUE) %>%
    dplyr::rename(CLarea = best_fao_area)
  balbaya_cl_data_final <- furdeb::marine_area_overlay(data = balbaya_cl_data,
                                                       overlay_expected = "fao_eez_area",
                                                       longitude_name = "longitude_decimal",
                                                       latitude_name = "latitude_decimal",
                                                       fao_area_file_path = fao_area_file_path,
                                                       fao_overlay_level = "division",
                                                       auto_selection_fao = TRUE,
                                                       eez_area_file_path = eez_area_file_path,
                                                       for_fdi_use = TRUE,
                                                       silent = TRUE) %>%
    dplyr::rename(CLarea = best_fao_area)
  # specie code and name
  warning(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Check the code regarding non dynamic worms aphia id definition.",
          sep = "")
  observe_ps_bb_cl_data_specie <- tidyr::tibble("specie_scientific_name" = unique(observe_ps_bb_cl_data_final$specie_scientific_name)) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(worms_aphia_id = as.character(x = tryCatch(worrms::wm_name2id(name = specie_scientific_name),
                                                             error = function(a) {NA_character_})),
                  worms_aphia_id = dplyr::case_when(
                    ! is.na(x = worms_aphia_id) ~ worms_aphia_id,
                    specie_scientific_name == "Caranx spp" ~ "125936",
                    specie_scientific_name == "Thunnus alalunga" ~ "127026",
                    specie_scientific_name == "Sphyraena barracuda" ~ "345843",
                    TRUE ~ worms_aphia_id
                  ))
  if (any(is.na(x = observe_ps_bb_cl_data_specie$worms_aphia_id))) {
    warning(format(x = Sys.time(),
                   format = "%Y-%m-%d %H:%M:%S"),
            " - At least one specie not match with the worms aphia id referential.\n",
            "The follwing specie(s) will be remove from the data production:\n",
            paste0(dplyr::filter(.data = observe_ps_bb_cl_data_specie,
                                 is.na(x = worms_aphia_id))$specie_scientific_name,
                   collapse = "; "),
            ".\n",
            "Check in the process the dataset \"observe_ps_bb_cl_data_final\" for more details.",
            sep = "")
  }
  observe_ps_bb_cl_data_final <- dplyr::inner_join(x = observe_ps_bb_cl_data_final,
                                                   y = dplyr::filter(.data = observe_ps_bb_cl_data_specie,
                                                                     ! is.na(x = worms_aphia_id)),
                                                   by = "specie_scientific_name") %>%
    dplyr::rename(CLspecCode = worms_aphia_id)
  balbaya_cl_data_specie <- tidyr::tibble("specie_scientific_name" = unique(balbaya_cl_data_final$specie_scientific_name)) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(worms_aphia_id = as.character(x = tryCatch(worrms::wm_name2id(name = specie_scientific_name),
                                                             error = function(a) {NA_character_})),
                  worms_aphia_id = dplyr::case_when(
                    ! is.na(x = worms_aphia_id) ~ worms_aphia_id,
                    specie_scientific_name == "Caranx spp" ~ "125936",
                    specie_scientific_name == "Thunnus alalunga" ~ "127026",
                    specie_scientific_name == "Sphyraena barracuda" ~ "345843",
                    TRUE ~ worms_aphia_id
                  ))
  if (any(is.na(x = balbaya_cl_data_specie$worms_aphia_id))) {
    warning(format(x = Sys.time(),
                   format = "%Y-%m-%d %H:%M:%S"),
            " - At least one specie not match with the worms aphia id referential.\n",
            "The follwing specie(s) will be remove from the data production:\n",
            paste0(dplyr::filter(.data = balbaya_cl_data_specie,
                                 is.na(x = worms_aphia_id))$specie_scientific_name,
                   collapse = "; "),
            ".\n",
            "Check in the process the dataset \"balbaya_cl_data_final\" for more details.",
            sep = "")
  }
  balbaya_cl_data_final <- dplyr::inner_join(x = balbaya_cl_data_final,
                                             y = dplyr::filter(.data = balbaya_cl_data_specie,
                                                               ! is.na(x = worms_aphia_id)),
                                             by = "specie_scientific_name") %>%
    dplyr::rename(CLspecCode = worms_aphia_id)
  # others
  observe_ps_bb_cl_data_final <- observe_ps_bb_cl_data_final %>%
    dplyr::left_join(referential_iso_3166[, c("alpha_3_code",
                                              "alpha_2_code")],
                     by = c("landing_country_fao" = "alpha_3_code")) %>%
    dplyr::rename(CLlanCou = alpha_2_code) %>%
    dplyr::left_join(referential_iso_3166[, c("alpha_3_code",
                                              "alpha_2_code")],
                     by = c("vessel_flag_country_fao" = "alpha_3_code")) %>%
    dplyr::rename(CLvesFlagCou = alpha_2_code) %>%
    dplyr::left_join(referential_iso_3166[, c("alpha_3_code",
                                              "alpha_2_code")],
                     by = c("eez_country" = "alpha_3_code")) %>%
    dplyr::rename(CLeconZone = alpha_2_code) %>%
    dplyr::mutate(CLrecType = "CL",
                  CLdSouLanVal = "Other",
                  CLyear = lubridate::year(x = landing_date),
                  CLquar = lubridate::quarter(x = landing_date),
                  CLmonth = lubridate::month(x = landing_date),
                  CLstatRect = dplyr::case_when(
                    major_fao == "27" ~ "ices_statistical_area_missing",
                    TRUE ~ "-9"
                  ),
                  CLdSoucstatRect = "EstPosData",
                  CLfishManUnit = NA,
                  CLgsaSubarea = dplyr::case_when(
                    major_fao == "37" ~ "gsa_sub_area_missing",
                    TRUE ~ "NotApplicable"
                  ),
                  CLjurisdArea = NA,
                  CLfishAreaCat = "NA",
                  CLfreshWatNam = "NA",
                  CLeconZoneIndi = NA,
                  CLlandCat = dplyr::case_when(
                    specie_fate_code == 6 ~ "Ind",
                    specie_fate_code == 11 ~ "None",
                    TRUE ~ "landing_category_missing"
                  ),
                  CLcatchCat = dplyr::case_when(
                    specie_fate_code == 6 ~ "Lan",
                    specie_fate_code == 11 ~ "RegDis",
                    TRUE ~ "catch_category_missing"
                  ),
                  CLregDisCat = dplyr::case_when(
                    specie_fate_code == 6 ~ "NotApplicable",
                    specie_fate_code == 11 ~ "NotKnown"
                  ),
                  CLsizeCatScale = NA,
                  CLsizeCat = NA,
                  CLnatFishAct = dplyr::case_when(
                    vessel_type_code == 6 ~ "PS_LPF_>0_0_0_TRO",
                    vessel_type_code == 2 ~ "LHP_LPF_0_0_0_MSP",
                    TRUE ~ "national_fishing_activity_missing"
                  ),
                  CLmetier6 = dplyr::case_when(
                    vessel_type_code == 6 ~ "PS_LPF_>0_0_0",
                    vessel_type_code == 2 ~ "LHP_LPF_0_0_0",
                    TRUE ~ "metier6_missing"
                  ),
                  CLIBmitiDev = "None",
                  CLvesLenCat = dplyr::case_when(
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
                  CLfishTech = dplyr::case_when(
                    vessel_type_code == 6 ~ "PS",
                    vessel_type_code == 2 ~ "HOK",
                    TRUE ~ "fishing_technique_missing"
                  ),
                  CLmesSizRan = NA,
                  CLsupReg = "OFR",
                  CLgeoInd = "IWE",
                  CLspeConTech = "NA",
                  CLdeepSeaReg = "N",
                  CLoffWeight = round(x = CLoffWeight,
                                      digits = 1),
                  CLtotOffLanVal = "Unknown",
                  CLtotNumFish = NA,
                  CLconfiFlag = "N")
  balbaya_cl_data_final <- balbaya_cl_data_final %>%
    dplyr::left_join(referential_iso_3166[, c("alpha_3_code",
                                              "alpha_2_code")],
                     by = c("landing_country_fao" = "alpha_3_code")) %>%
    dplyr::rename(CLlanCou = alpha_2_code) %>%
    dplyr::left_join(referential_iso_3166[, c("alpha_3_code",
                                              "alpha_2_code")],
                     by = c("vessel_flag_country_fao" = "alpha_3_code")) %>%
    dplyr::rename(CLvesFlagCou = alpha_2_code) %>%
    dplyr::left_join(referential_iso_3166[, c("alpha_3_code",
                                              "alpha_2_code")],
                     by = c("eez_country" = "alpha_3_code")) %>%
    dplyr::rename(CLeconZone = alpha_2_code) %>%
    dplyr::mutate(CLrecType = "CL",
                  CLsampScheme = NA,
                  CLdSouLanVal = "Other",
                  CLyear = lubridate::year(x = landing_date),
                  CLquar = lubridate::quarter(x = landing_date),
                  CLmonth = lubridate::month(x = landing_date),
                  CLstatRect = dplyr::case_when(
                    major_fao == "27" ~ "ices_statistical_area_missing",
                    TRUE ~ "-9"
                  ),
                  CLdSoucstatRect = "EstPosData",
                  CLfishManUnit = NA,
                  CLgsaSubarea = dplyr::case_when(
                    major_fao == "37" ~ "gsa_sub_area_missing",
                    TRUE ~ "NotApplicable"
                  ),
                  CLjurisdArea = NA,
                  CLfishAreaCat = "NA",
                  CLfreshWatNam = "NA",
                  CLeconZoneIndi = NA,
                  CLlandCat = "Ind",
                  CLcatchCat = "Lan",
                  CLregDisCat = "NotApplicable",
                  CLsizeCatScale = NA,
                  CLsizeCat = NA,
                  CLnatFishAct = dplyr::case_when(
                    vessel_type_code == 6 ~ "PS_LPF_>0_0_0_TRO",
                    vessel_type_code == 2 ~ "LHP_LPF_0_0_0_MSP",
                    TRUE ~ "national_fishing_activity_missing"
                  ),
                  CLmetier6 = dplyr::case_when(
                    vessel_type_code == 6 ~ "PS_LPF_>0_0_0",
                    vessel_type_code == 2 ~ "LHP_LPF_0_0_0",
                    TRUE ~ "metier6_missing"
                  ),
                  CLIBmitiDev = "None",
                  CLvesLenCat = dplyr::case_when(
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
                  CLfishTech = dplyr::case_when(
                    vessel_type_code == 6 ~ "PS",
                    vessel_type_code == 2 ~ "HOK",
                    TRUE ~ "fishing_technique_missing"
                  ),
                  CLmesSizRan = NA,
                  CLsupReg = "OFR",
                  CLgeoInd = "IWE",
                  CLspeConTech = "NA",
                  CLdeepSeaReg = "N",
                  CLsciWeight = round(x = CLsciWeight,
                                      digits = 1),
                  CLtotOffLanVal = "Unknown",
                  CLtotNumFish = NA,
                  CLsciWeightErrMeaValTyp = NA,
                  CLsciWeightErrMeaValFirst = NA,
                  CLsciWeightErrMeaValSecond = NA,
                  CLvalErrMeaValTyp = NA,
                  CLvalErrMeaValFirst = NA,
                  CLvalErrMeaValSecond = NA,
                  CLnumFishInCatchErrMeaValTyp = NA,
                  CLnumFishInCatchErrMeaValFirst = NA,
                  CLnumFishInCatchErrMeaValSecond = NA,
                  CLcom = NA,
                  CLsciWeightQualBias = NA,
                  CLconfiFlag = "N")
  # confidentiality variable
  observe_ps_bb_cl_data_vessel <- observe_ps_bb_cl_data_final %>%
    dplyr::group_by(CLyear,
                    CLquar,
                    CLmonth,
                    CLarea) %>%
    dplyr::summarise(vessel_count = dplyr::n_distinct(vessel_code),
                     .groups = "drop")
  if (dim(x = dplyr::filter(.data = observe_ps_bb_cl_data_vessel,
                            vessel_count < 3))[1] != 0) {
    observe_ps_bb_cl_data_vessel_confidential <- dplyr::filter(.data = observe_ps_bb_cl_data_vessel,
                                                               vessel_count < 3) %>%
      dplyr::mutate(CLFDIconCod_observe = "A")
    observe_ps_bb_cl_data_final <- dplyr::left_join(x = observe_ps_bb_cl_data_final,
                                                    y = dplyr::select(.data = observe_ps_bb_cl_data_vessel_confidential,
                                                                      -vessel_count),
                                                    by = c("CLyear",
                                                           "CLquar",
                                                           "CLmonth",
                                                           "CLarea")) %>%
      dplyr::mutate(CLFDIconCod_observe = dplyr::case_when(
        is.na(x = CLFDIconCod_observe) ~ "N",
        TRUE ~ CLFDIconCod_observe
      ))
  } else {
    observe_ps_bb_cl_data_final <- dplyr::mutate(CLFDIconCod_observe == "N")
  }
  observe_ps_bb_cl_data_final <- dplyr::inner_join(x = observe_ps_bb_cl_data_final,
                                                   y = dplyr::rename(.data = observe_ps_bb_cl_data_vessel,
                                                                     "CLnumUniqVes_observe" = "vessel_count"),
                                                   by = c("CLyear",
                                                          "CLquar",
                                                          "CLmonth",
                                                          "CLarea"))
  balbaya_cl_data_vessel <- balbaya_cl_data_final %>%
    dplyr::group_by(CLyear,
                    CLquar,
                    CLmonth,
                    CLarea) %>%
    dplyr::summarise(vessel_count = dplyr::n_distinct(vessel_code),
                     .groups = "drop")
  if (dim(x = dplyr::filter(.data = balbaya_cl_data_vessel,
                            vessel_count < 3))[1] != 0) {
    balbaya_cl_data_vessel_confidential <- dplyr::filter(.data = balbaya_cl_data_vessel,
                                                         vessel_count < 3) %>%
      dplyr::mutate(CLFDIconCod_balbaya = "A")
    balbaya_cl_data_final <- dplyr::left_join(x = balbaya_cl_data_final,
                                              y = dplyr::select(.data = balbaya_cl_data_vessel_confidential,
                                                                -vessel_count),
                                              by = c("CLyear",
                                                     "CLquar",
                                                     "CLmonth",
                                                     "CLarea")) %>%
      dplyr::mutate(CLFDIconCod_balbaya = dplyr::case_when(
        is.na(x = CLFDIconCod_balbaya) ~ "N",
        TRUE ~ CLFDIconCod_balbaya
      ))
  } else {
    balbaya_cl_data_final <- dplyr::mutate(CLFDIconCod_balbaya == "N")
  }
  balbaya_cl_data_final <- dplyr::inner_join(x = balbaya_cl_data_final,
                                             y = dplyr::rename(.data = balbaya_cl_data_vessel,
                                                               "CLnumUniqVes_balbaya" = "vessel_count"),
                                             by = c("CLyear",
                                                    "CLquar",
                                                    "CLmonth",
                                                    "CLarea"))
  balbaya_cl_data_final <- dplyr::inner_join(x = balbaya_cl_data_final,
                                             y = dplyr::select(.data = referential_vessel,
                                                               -vessel_name),
                                             by = "vessel_code")
  # final design and selection
  observe_ps_bb_cl_data_supreme <- observe_ps_bb_cl_data_final %>%
    dplyr::group_by(CLrecType,
                    CLdSouLanVal,
                    CLlanCou,
                    CLvesFlagCou,
                    CLyear,
                    CLquar,
                    CLmonth,
                    CLarea,
                    CLstatRect,
                    CLdSoucstatRect,
                    CLfishManUnit,
                    CLgsaSubarea,
                    CLjurisdArea,
                    CLfishAreaCat,
                    CLfreshWatNam,
                    CLeconZone,
                    CLeconZoneIndi,
                    CLspecCode,
                    CLspecFAO,
                    CLlandCat,
                    CLcatchCat,
                    CLregDisCat,
                    CLsizeCatScale,
                    CLsizeCat,
                    CLnatFishAct,
                    CLmetier6,
                    CLIBmitiDev,
                    CLloc,
                    CLvesLenCat,
                    CLfishTech,
                    CLmesSizRan,
                    CLsupReg,
                    CLgeoInd,
                    CLspeConTech,
                    CLdeepSeaReg,
                    CLFDIconCod_observe,
                    CLtotOffLanVal,
                    CLtotNumFish,
                    CLnumUniqVes_observe,
                    CLconfiFlag,
                    CLencrypVesIds) %>%
    dplyr::summarise(CLoffWeight = sum(CLoffWeight),
                     .groups = "drop")
  balbaya_cl_data_supreme <- balbaya_cl_data_final %>%
    dplyr::group_by(CLrecType,
                    CLsampScheme,
                    CLdSouLanVal,
                    CLlanCou,
                    CLvesFlagCou,
                    CLyear,
                    CLquar,
                    CLmonth,
                    CLarea,
                    CLstatRect,
                    CLdSoucstatRect,
                    CLfishManUnit,
                    CLgsaSubarea,
                    CLjurisdArea,
                    CLfishAreaCat,
                    CLfreshWatNam,
                    CLeconZone,
                    CLeconZoneIndi,
                    CLspecCode,
                    CLspecFAO,
                    CLlandCat,
                    CLcatchCat,
                    CLregDisCat,
                    CLsizeCatScale,
                    CLsizeCat,
                    CLnatFishAct,
                    CLmetier6,
                    CLIBmitiDev,
                    CLloc,
                    CLvesLenCat,
                    CLfishTech,
                    CLmesSizRan,
                    CLsupReg,
                    CLgeoInd,
                    CLspeConTech,
                    CLdeepSeaReg,
                    CLFDIconCod_balbaya,
                    CLtotOffLanVal,
                    CLtotNumFish,
                    CLnumUniqVes_balbaya,
                    CLsciWeightErrMeaValTyp,
                    CLsciWeightErrMeaValFirst,
                    CLsciWeightErrMeaValSecond,
                    CLvalErrMeaValTyp,
                    CLvalErrMeaValFirst,
                    CLvalErrMeaValSecond,
                    CLnumFishInCatchErrMeaValTyp,
                    CLnumFishInCatchErrMeaValFirst,
                    CLnumFishInCatchErrMeaValSecond,
                    CLcom,
                    CLsciWeightQualBias,
                    CLconfiFlag,
                    CLencrypVesIds) %>%
    dplyr::summarise(CLsciWeight = sum(CLsciWeight),
                     .groups = "drop")
  cl_data <- dplyr::full_join(x = balbaya_cl_data_supreme,
                              y = observe_ps_bb_cl_data_supreme,
                              by = c("CLrecType",
                                     "CLdSouLanVal",
                                     "CLlanCou",
                                     "CLvesFlagCou",
                                     "CLyear",
                                     "CLquar",
                                     "CLmonth",
                                     "CLarea",
                                     "CLstatRect",
                                     "CLdSoucstatRect",
                                     "CLfishManUnit",
                                     "CLgsaSubarea",
                                     "CLjurisdArea",
                                     "CLfishAreaCat",
                                     "CLfreshWatNam",
                                     "CLeconZone",
                                     "CLeconZoneIndi",
                                     "CLspecCode",
                                     "CLspecFAO",
                                     "CLlandCat",
                                     "CLcatchCat",
                                     "CLregDisCat",
                                     "CLsizeCatScale",
                                     "CLsizeCat",
                                     "CLnatFishAct",
                                     "CLmetier6",
                                     "CLIBmitiDev",
                                     "CLloc",
                                     "CLvesLenCat",
                                     "CLfishTech",
                                     "CLmesSizRan",
                                     "CLsupReg",
                                     "CLgeoInd",
                                     "CLspeConTech",
                                     "CLdeepSeaReg",
                                     "CLtotOffLanVal",
                                     "CLtotNumFish",
                                     "CLconfiFlag",
                                     "CLencrypVesIds")) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(CLdTypSciWeig = dplyr::case_when(
      is.na(CLsciWeight) ~ "Official",
      is.na(CLoffWeight) ~ "Estimate",
      TRUE ~ "Estimate"
    ),
    CLdSouSciWeig = dplyr::case_when(
      is.na(CLsciWeight) ~ "Logb",
      is.na(CLoffWeight) ~ "Combination of census data",
      TRUE ~ "Combination of census data"
    ),
    CLFDIconCod = dplyr::case_when(
      is.na(CLFDIconCod_balbaya) ~ CLFDIconCod_observe,
      is.na(CLFDIconCod_observe) ~ CLFDIconCod_balbaya,
      CLFDIconCod_balbaya != CLFDIconCod_observe ~ "N",
      TRUE ~ CLFDIconCod_observe
    ),
    CLnumUniqVes = max(CLnumUniqVes_balbaya,
                       CLnumUniqVes_observe,
                       na.rm = TRUE),
    CLsciWeight = dplyr::case_when(
      is.na(CLsciWeight) ~ CLoffWeight,
      TRUE ~ CLsciWeight
    ),
    CLoffWeight = dplyr::case_when(
      is.na(CLoffWeight) ~ 0,
      TRUE ~ CLoffWeight),
    CLexpDiff = dplyr::case_when(
      CLoffWeight != CLsciWeight ~ "Sampld",
      TRUE ~ "NoDiff")) %>%
    dplyr::select(CLrecType,
                  CLdTypSciWeig,
                  CLdSouSciWeig,
                  CLsampScheme,
                  CLdSouLanVal,
                  CLlanCou,
                  CLvesFlagCou,
                  CLyear,
                  CLquar,
                  CLmonth,
                  CLarea,
                  CLstatRect,
                  CLdSoucstatRect,
                  CLfishManUnit,
                  CLgsaSubarea,
                  CLjurisdArea,
                  CLfishAreaCat,
                  CLfreshWatNam,
                  CLeconZone,
                  CLeconZoneIndi,
                  CLspecCode,
                  CLspecFAO,
                  CLlandCat,
                  CLcatchCat,
                  CLregDisCat,
                  CLsizeCatScale,
                  CLsizeCat,
                  CLnatFishAct,
                  CLmetier6,
                  CLIBmitiDev,
                  CLloc,
                  CLvesLenCat,
                  CLfishTech,
                  CLmesSizRan,
                  CLsupReg,
                  CLgeoInd,
                  CLspeConTech,
                  CLdeepSeaReg,
                  CLFDIconCod,
                  CLoffWeight,
                  CLsciWeight,
                  CLexpDiff,
                  CLtotOffLanVal,
                  CLtotNumFish,
                  CLnumUniqVes,
                  CLtotNumFish,
                  CLsciWeightErrMeaValTyp,
                  CLsciWeightErrMeaValFirst,
                  CLsciWeightErrMeaValSecond,
                  CLvalErrMeaValTyp,
                  CLvalErrMeaValFirst,
                  CLvalErrMeaValSecond,
                  CLnumFishInCatchErrMeaValTyp,
                  CLnumFishInCatchErrMeaValFirst,
                  CLnumFishInCatchErrMeaValSecond,
                  CLcom,
                  CLsciWeightQualBias,
                  CLconfiFlag,
                  CLencrypVesIds)
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Successful data design.",
          sep = "")
  # 6 - Extraction ----
  if (! is.null(x = export_path)) {
    message(format(x = Sys.time(),
                   format = "%Y-%m-%d %H:%M:%S"),
            " - Start cl data extractions.",
            sep = "")
    utils::write.csv2(x = cl_data,
                      file = file.path(export_path,
                                       paste(format(as.POSIXct(Sys.time()),
                                                    "%Y%m%d_%H%M%S"),
                                             "rdbes_cl.csv",
                                             sep = "_")),
                      row.names = FALSE,
                      na = "")
    message(format(x = Sys.time(),
                   format = "%Y-%m-%d %H:%M:%S"),
            " - Successful cl data extractions.\n",
            "File available in the directory ",
            export_path,
            sep = "")
  }
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Successful process on RDBES table CL generation.",
          sep = "")
  return(cl_data)
}
