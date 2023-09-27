#' @name rdbes_cl
#' @title Table Commercial Landing (CL) generation (RDBES process)
#' @description Process for generation and optionally extraction of the RDBES table CL (Commercial Landing).
#' @param observe_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the observe database.
#' @param balbaya_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the balbaya database.
#' @param fao_area_file_path {\link[base]{character}} expected. File path of the FAO area grid. The file format has to be .Rdata or .RData extension.
#' @param eez_area_file_path {\link[base]{character}} expected. File path of the EEZ area grid. The file format has to be .Rdata or .RData extension.
#' @param year_time_period {\link[base]{integer}} expected. Year(s) selected associated to the databases queries extractions.
#' @param flag {\link[base]{integer}} expected. Flag(s) selected associated to the databases queries extractions.
#' @param major_fao_area_filter {\link[base]{integer}} expected. By default NULL. Sub selection of major fao area.
#' @param digit_accuracy {\link[base]{integer}} expected. By default 1. Indicating the number of decimal places to be used.
#' @param hash_algorithms {\link[base]{integer}} expected. By default NULL. The hashing algorithms to be used for the CLencrypVesIds variable. You can choose any modality of the argument "algo" or the function {\link[digest]{digest}}.
#' @param encrypted_vessel_code_separator {\link[base]{character}} expected. By default ", ". Which separator you want to use for the CLencrypVesIds variable.
#' @param export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @return A R object with the RDBES table CL with potentially a csv extraction associated.
#' @export
#' @importFrom codama r_type_checking file_path_checking
#' @importFrom DBI sqlInterpolate SQL dbGetQuery
#' @importFrom furdeb marine_area_overlay
#' @importFrom dplyr rename filter select setdiff inner_join rowwise mutate case_when left_join group_by reframe full_join n_distinct tibble
#' @importFrom worrms wm_name2id
#' @importFrom lubridate year quarter month
#' @importFrom stringr str_flatten
#' @importFrom utils write.csv
#' @importFrom digest digest
rdbes_cl <- function(observe_con,
                     balbaya_con,
                     fao_area_file_path,
                     eez_area_file_path,
                     year_time_period,
                     flag,
                     major_fao_area_filter = NULL,
                     hash_algorithms = NULL,
                     digit_accuracy = 1L,
                     encrypted_vessel_code_separator = ", ",
                     export_path = NULL) {
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Start process on RDBES table CL generation.",
          sep = "")
  # 1 - Global variables assignement ----
  best_fao_area <- NULL
  eez_indicator <- NULL
  major_fao <- NULL
  latitude_decimal <- NULL
  longitude_decimal <- NULL
  subarea_fao <- NULL
  division_fao <- NULL
  eez <- NULL
  vessel_name <- NULL
  vessel_id <- NULL
  specie_scientific_name <- NULL
  worms_aphia_id <- NULL
  alpha_2_code <- NULL
  landing_date <- NULL
  CLspecFAO <- NULL
  CLencrypVesIds <- NULL
  CLloc <- NULL
  CLarea <- NULL
  CLeconZoneIndi <- NULL
  CLspecCode <- NULL
  CLlanCou <- NULL
  CLvesFlagCou <- NULL
  CLeconZone <- NULL
  CLrecType <- NULL
  CLyear <- NULL
  CLquar <- NULL
  CLmonth <- NULL
  CLlandCat <- NULL
  CLcatchCat <- NULL
  CLregDisCat <- NULL
  vessel_type_code <- NULL
  vessel_length <- NULL
  CLoffWeight <- NULL
  CLsciWeight <- NULL
  CLsciWeightErrMeaValTyp <- NULL
  CLsciWeightErrMeaValFirst <- NULL
  CLsciWeightErrMeaValSecond <- NULL
  CLvalErrMeaValTyp <- NULL
  CLvalErrMeaValFirst <- NULL
  CLvalErrMeaValSecond <- NULL
  CLnumFishInCatchErrMeaValTyp <- NULL
  CLnumFishInCatchErrMeaValFirst <- NULL
  CLnumFishInCatchErrMeaValSecond <- NULL
  CLsciWeightQualBias <- NULL
  CLdTypSciWeig <- NULL
  CLdSouSciWeig <- NULL
  CLsampScheme <- NULL
  CLdSouLanVal <- NULL
  CLstatRect <- NULL
  CLdSoucstatRect <- NULL
  CLfishManUnit <- NULL
  CLgsaSubarea <- NULL
  CLjurisdArea <- NULL
  CLfishAreaCat <- NULL
  CLfreshWatNam <- NULL
  CLsizeCatScale <- NULL
  CLsizeCat <- NULL
  CLnatFishAct <- NULL
  CLmetier6 <- NULL
  CLIBmitiDev <- NULL
  CLvesLenCat <- NULL
  CLfishTech <- NULL
  CLmesSizRan <- NULL
  CLsupReg <- NULL
  CLgeoInd <- NULL
  CLspeConTech <- NULL
  CLdeepSeaReg <- NULL
  CLFDIconCod <- NULL
  CLexpDiff <- NULL
  CLtotOffLanVal <- NULL
  CLtotNumFish <- NULL
  CLnumUniqVes <- NULL
  CLcom <- NULL
  CLconfiFlag <- NULL
  CLencrypVesIds_hash <- NULL
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
  if (codama::r_type_checking(r_object = digit_accuracy,
                              type = "integer",
                              length = 1L,
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = digit_accuracy,
                                   type = "integer",
                                   length = 1L,
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
    dplyr::rename(CLarea = best_fao_area,
                  CLeconZoneIndi = eez_indicator)
  if (! is.null(x = major_fao_area_filter)) {
    observe_ps_bb_cl_data_final <- dplyr::filter(.data = observe_ps_bb_cl_data_final,
                                                 major_fao %in% major_fao_area_filter)
  }
  observe_ps_bb_cl_data_final <- dplyr::select(.data = observe_ps_bb_cl_data_final,
                                               -latitude_decimal,
                                               -longitude_decimal,
                                               -subarea_fao,
                                               -division_fao,
                                               -eez)
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
    dplyr::rename(CLarea = best_fao_area,
                  CLeconZoneIndi = eez_indicator)
  if (! is.null(x = major_fao_area_filter)) {
    balbaya_cl_data_final <- dplyr::filter(.data = balbaya_cl_data_final,
                                           major_fao %in% major_fao_area_filter)
  }
  balbaya_cl_data_final <- dplyr::select(.data = balbaya_cl_data_final,
                                         -latitude_decimal,
                                         -longitude_decimal,
                                         -subarea_fao,
                                         -division_fao,
                                         -eez)
  # vessel id
  if (! all(unique(x = balbaya_cl_data_final$vessel_code) %in% referential_vessel$vessel_code)) {
    stop(format(x = Sys.time(),
                format = "%Y-%m-%d %H:%M:%S"),
         " - At least one vessel id is not in the vessel referential.\n",
         "Check the following vessel id(s):\n",
         paste(dplyr::setdiff(x = unique(x = balbaya_cl_data_final$vessel_code),
                              y = referential_vessel$vessel_code),
               collapse = ", "),
         sep = "")
  } else {
    balbaya_cl_data_final <- dplyr::inner_join(x = balbaya_cl_data_final,
                                               y = dplyr::select(.data = referential_vessel,
                                                                 -vessel_name),
                                               by = "vessel_code") %>%
      dplyr::rename(CLencrypVesIds = vessel_id)
  }
  if (! is.null(x = hash_algorithms)) {
    balbaya_cl_data_final <- dplyr::inner_join(x = balbaya_cl_data_final,
                                               y = dplyr::tibble("CLencrypVesIds" = unique(x = balbaya_cl_data_final$CLencrypVesIds)) %>%
                                                 dplyr::rowwise() %>%
                                                 dplyr::mutate(CLencrypVesIds_hash = digest::digest(CLencrypVesIds,
                                                                                                    algo = !!hash_algorithms)),
                                               by = "CLencrypVesIds") %>%
      dplyr::select(-CLencrypVesIds) %>%
      dplyr::rename(CLencrypVesIds = CLencrypVesIds_hash)
    observe_ps_bb_cl_data_final <- dplyr::inner_join(x = observe_ps_bb_cl_data_final,
                                                     y = dplyr::tibble("CLencrypVesIds" = unique(x = observe_ps_bb_cl_data_final$CLencrypVesIds)) %>%
                                                       dplyr::rowwise() %>%
                                                       dplyr::mutate(CLencrypVesIds_hash = digest::digest(CLencrypVesIds,
                                                                                                          algo = !!hash_algorithms)),
                                                     by = "CLencrypVesIds") %>%
      dplyr::select(-CLencrypVesIds) %>%
      dplyr::rename(CLencrypVesIds = CLencrypVesIds_hash)
  }
  # specie code and name
  warning(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Check the code regarding non dynamic worms aphia id definition.",
          sep = "")
  observe_ps_bb_cl_data_specie <- dplyr::tibble("specie_scientific_name" = unique(observe_ps_bb_cl_data_final$specie_scientific_name)) %>%
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
  balbaya_cl_data_specie <- dplyr::tibble("specie_scientific_name" = unique(balbaya_cl_data_final$specie_scientific_name)) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(worms_aphia_id = as.character(x = tryCatch(worrms::wm_name2id(name = specie_scientific_name),
                                                             error = function(a) {NA_character_})),
                  worms_aphia_id = dplyr::case_when(
                    ! is.na(x = worms_aphia_id) ~ worms_aphia_id,
                    specie_scientific_name == "Caranx spp" ~ "125936",
                    specie_scientific_name == "Thunnus alalunga" ~ "127026",
                    specie_scientific_name == "Sphyraena barracuda" ~ "345843",
                    TRUE ~ worms_aphia_id
                  )) %>%
    # modification of specie name Euthynnus alletteratus to Euthynnus alleteratus, ticket github #289
    dplyr::mutate(worms_aphia_id = dplyr::case_when(
      specie_scientific_name == "Euthynnus alleteratus" ~ "127017",
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
                  CLyear = lubridate::year(x = landing_date),
                  CLquar = lubridate::quarter(x = landing_date),
                  CLmonth = lubridate::month(x = landing_date),
                  CLeconZone = dplyr::case_when(
                    is.na(x = CLeconZone) ~ "NTHH",
                    TRUE ~ CLeconZone
                  ),
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
                  )) %>%
    dplyr::group_by(CLspecFAO,
                    CLencrypVesIds,
                    CLloc,
                    CLarea,
                    CLeconZoneIndi,
                    CLspecCode,
                    CLlanCou,
                    CLvesFlagCou,
                    CLeconZone,
                    CLrecType,
                    CLyear,
                    CLquar,
                    CLmonth,
                    CLlandCat,
                    CLcatchCat,
                    CLregDisCat,
                    vessel_type_code,
                    vessel_length) %>%
    dplyr::reframe(CLoffWeight = round(x = sum(CLoffWeight),
                                       digits = digit_accuracy))
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
    dplyr::mutate(CLyear = lubridate::year(x = landing_date),
                  CLquar = lubridate::quarter(x = landing_date),
                  CLmonth = lubridate::month(x = landing_date),
                  CLeconZone = dplyr::case_when(
                    is.na(x = CLeconZone) ~ "NTHH",
                    TRUE ~ CLeconZone
                  )) %>%
    dplyr::group_by(CLyear,
                    CLquar,
                    CLmonth,
                    CLlanCou,
                    CLvesFlagCou,
                    CLeconZone,
                    CLspecCode,
                    CLspecFAO,
                    CLloc,
                    CLarea,
                    CLeconZoneIndi,
                    CLencrypVesIds,
                    vessel_type_code,
                    vessel_length) %>%
    dplyr::reframe(CLsciWeight = round(x = sum(CLsciWeight),
                                       digits = digit_accuracy)) %>%
    dplyr::mutate(CLrecType = "CL",
                  CLlandCat = "Ind",
                  CLcatchCat = "Lan",
                  CLregDisCat = "NotApplicable",
                  CLsciWeightErrMeaValTyp = NA,
                  CLsciWeightErrMeaValFirst = NA,
                  CLsciWeightErrMeaValSecond = NA,
                  CLvalErrMeaValTyp = NA,
                  CLvalErrMeaValFirst = NA,
                  CLvalErrMeaValSecond = NA,
                  CLnumFishInCatchErrMeaValTyp = NA,
                  CLnumFishInCatchErrMeaValFirst = NA,
                  CLnumFishInCatchErrMeaValSecond = NA,
                  CLsciWeightQualBias = NA)
  # data merge
  cl_data <- dplyr::full_join(x = observe_ps_bb_cl_data_final,
                              y = balbaya_cl_data_final,
                              by = c("CLspecFAO",
                                     "CLencrypVesIds",
                                     "CLloc",
                                     "CLarea",
                                     "CLeconZoneIndi",
                                     "CLspecCode",
                                     "CLlanCou",
                                     "CLvesFlagCou",
                                     "CLeconZone",
                                     "CLrecType",
                                     "CLyear",
                                     "CLquar",
                                     "CLmonth",
                                     "CLlandCat",
                                     "CLcatchCat",
                                     "CLregDisCat",
                                     "vessel_type_code",
                                     "vessel_length")) %>%
    dplyr::mutate(CLdTypSciWeig = dplyr::case_when(
      is.na(CLsciWeight) ~ "Official",
      is.na(CLoffWeight) ~ "Estimate",
      TRUE ~ "Estimate"
    ),
    CLdSouSciWeig = dplyr::case_when(
      is.na(CLsciWeight) ~ "Logb",
      TRUE ~ "Combcd"
    ),
    CLoffWeight = dplyr::case_when(
      is.na(CLoffWeight) ~ 0,
      TRUE ~ CLoffWeight),
    CLsciWeight = dplyr::case_when(
      is.na(CLsciWeight) ~ CLoffWeight,
      TRUE ~ CLsciWeight
    )) %>%
    dplyr::group_by(CLspecFAO,
                    CLloc,
                    CLarea,
                    CLeconZoneIndi,
                    CLspecCode,
                    CLlanCou,
                    CLvesFlagCou,
                    CLeconZone,
                    CLrecType,
                    CLyear,
                    CLquar,
                    CLmonth,
                    CLlandCat,
                    CLcatchCat,
                    CLregDisCat,
                    vessel_type_code,
                    vessel_length,
                    CLsciWeightErrMeaValTyp,
                    CLsciWeightErrMeaValFirst,
                    CLsciWeightErrMeaValSecond,
                    CLvalErrMeaValTyp,
                    CLvalErrMeaValFirst,
                    CLvalErrMeaValSecond,
                    CLnumFishInCatchErrMeaValTyp,
                    CLnumFishInCatchErrMeaValFirst,
                    CLnumFishInCatchErrMeaValSecond,
                    CLsciWeightQualBias,
                    CLdTypSciWeig,
                    CLdSouSciWeig) %>%
    dplyr::reframe(CLoffWeight = sum(CLoffWeight),
                   CLsciWeight = sum(CLsciWeight),
                   CLnumUniqVes = dplyr::n_distinct(CLencrypVesIds),
                   CLencrypVesIds = stringr::str_flatten(unique(CLencrypVesIds),
                                                         collapse = !!encrypted_vessel_code_separator)) %>%
    dplyr::mutate(CLsampScheme = NA,
                  CLdSouLanVal = "Other",
                  CLstatRect = dplyr::case_when(
                    stringr::str_extract(string = CLarea,
                                         pattern = "^[:digit:].") == "27" ~ "ices_statistical_area_missing",
                    TRUE ~ "-9"
                  ),
                  CLfishManUnit = NA,
                  CLdSoucstatRect = "EstPosData",
                  CLgsaSubarea = dplyr::case_when(
                    stringr::str_extract(string = CLarea,
                                         pattern = "^[:digit:].") == "37" ~ "gsa_sub_area_missing",
                    TRUE ~ "NotApplicable"
                  ),
                  CLjurisdArea = NA,
                  CLfishAreaCat = "NA",
                  CLfreshWatNam = "NA",
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
                  CLtotOffLanVal = "Unknown",
                  CLtotNumFish = NA,
                  CLcom = NA,
                  CLconfiFlag = dplyr::case_when(
                    CLnumUniqVes <= 3 ~ "Y",
                    TRUE ~ "N"
                  ),
                  CLFDIconCod = dplyr::case_when(
                    CLnumUniqVes <= 3 ~ "A",
                    TRUE ~ "N"
                  ),
                  CLexpDiff = dplyr::case_when(
                    CLoffWeight != CLsciWeight ~ "Sampld",
                    TRUE ~ "NoDiff"),
                  # manual modification regarding the RDBES referential, remove in the future
                  CLspecFAO = dplyr::case_when(
                    CLspecFAO %in% c("DOX", "RMM", "RRU", "SAI") ~ NA_character_,
                    TRUE ~ CLspecFAO
                  ),
    ) %>%
    dplyr::select(-vessel_type_code,
                  -vessel_length) %>%
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
            " - Start CL data extractions.",
            sep = "")
    utils::write.csv(x = cl_data,
                     file = file.path(export_path,
                                      paste(format(as.POSIXct(Sys.time()),
                                                   "%Y%m%d_%H%M%S"),
                                            "rdbes_cl.csv",
                                            sep = "_")),
                     row.names = FALSE,
                     na = "")
    message(format(x = Sys.time(),
                   format = "%Y-%m-%d %H:%M:%S"),
            " - Successful CL data extractions.\n",
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
