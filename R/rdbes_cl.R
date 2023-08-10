#' @name rdbes_cl
#' @title Table Commercial Landing (CL) generation (RDBES process)
#' @description Process for generation and optionally extraction of the RDBES table CL (Commercial Landing).
#' @param observe_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the observe database.
#' @param balbaya_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the balbaya database.
#' @param fao_area_file_path {\link[base]{character}} expected. File path of the FAO area grid. The file format has to be .Rdata or .RData extension.
#' @param eez_area_file_path {\link[base]{character}} expected. File path of the EEZ area grid. The file format has to be .Rdata or .RData extension.
#' @param year_time_period {\link[base]{integer}} expected. Year(s) selected associated to the databases queries extractions.
#' @param fleet {\link[base]{integer}} expected. Fleet(s) selected associated to the databases queries extractions.
#' @param export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @return A R object with the RDBES table CL with potentially a csv extraction associated.
#' @export
#' @importFrom utils write.csv2
#' @importFrom codama r_type_checking
rdbes_cl <- function(observe_con,
                     balbaya_con,
                     fao_area_file_path,
                     eez_area_file_path,
                     year_time_period,
                     fleet,
                     export_path = NULL) {
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on RDBES table CL generation.\n",
      sep = "")
  # 1 - Global variables assignement ----
  # 2 - Arguments verifications ----
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
  # 3 - Databases extractions ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start databases extractions.\n",
      sep = "")
  observe_ps_cl_data_query <- paste(readLines(con = system.file("sql",
                                                                "rdbes",
                                                                "observe_ps_cl_rdbes.sql",
                                                                package = "acdc")),
                                    collapse = "\n")
  observe_ps_cl_data_query <- DBI::sqlInterpolate(conn = observe_con[[2]],
                                                  sql = observe_ps_cl_data_query,
                                                  year_time_period = DBI::SQL(paste0(year_time_period,
                                                                                     collapse = ", ")),
                                                  fleet = DBI::SQL(paste0("'",
                                                                          paste0(fleet,
                                                                                 collapse = "', '"),
                                                                          "'")))
  observe_ps_cl_data <- DBI::dbGetQuery(conn = observe_con[[2]],
                                        statement = observe_ps_cl_data_query)
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
  observe_ps_cl_data_final <- furdeb::marine_area_overlay(data = observe_ps_cl_data,
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
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Warning, check the code below regarding non dynamic worms aphia id definition.\n",
      sep = "")
  observe_ps_cl_data_specie <- tidyr::tibble("specie_scientific_name" = unique(observe_ps_cl_data_final$specie_scientific_name)) %>%
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
  if (any(is.na(x = observe_ps_cl_data_specie$worms_aphia_id))) {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Warning, at least one specie not match with the worms aphia id referential.\n",
        "The follwing specie(s) will be remove from the data production:\n",
        paste0(dplyr::filter(.data = observe_ps_cl_data_specie,
                             is.na(x = worms_aphia_id))$specie_scientific_name,
               collapse = "; "),
        ".\n",
        sep = "")
  }
  observe_ps_cl_data_final <- dplyr::inner_join(x = observe_ps_cl_data_final,
                                                y = dplyr::filter(.data = observe_ps_cl_data_specie,
                                                                  ! is.na(x = worms_aphia_id)),
                                                by = "specie_scientific_name") %>%
    dplyr::rename(CLspecCode = worms_aphia_id)
  # others
  observe_ps_cl_data_final <- observe_ps_cl_data_final %>%
    dplyr::left_join(referential_iso_3166[, c("alpha_3_code",
                                              "alpha_2_code")],
                     by = c("landing_country_fao" = "alpha_3_code")) %>%
    dplyr::rename(CLlanCou = alpha_2_code) %>%
    dplyr::left_join(referential_iso_3166[, c("alpha_3_code",
                                              "alpha_2_code")],
                     by = c("vessel_fleet_country_fao" = "alpha_3_code")) %>%
    dplyr::rename(CLvesFlagCou = alpha_2_code) %>%
    dplyr::left_join(referential_iso_3166[, c("alpha_3_code",
                                              "alpha_2_code")],
                     by = c("eez_country" = "alpha_3_code")) %>%
    dplyr::rename(CLeconZone = alpha_2_code) %>%
    dplyr::mutate(CLrecType = "CL",
                  CLyear = lubridate::year(x = landing_date),
                  CLquar = lubridate::quarter(x = landing_date),
                  CLmonth = lubridate::month(x = landing_date),
                  CLstatRect = dplyr::case_when(
                    major_fao == "27" ~ "ices_statistical_area_missing",
                    TRUE ~ "-9"
                  ),
                  CLgsaSubarea = dplyr::case_when(
                    major_fao == "37" ~ "gsa_sub_area_missing",
                    TRUE ~ "NotApplicable"
                  ),
                  CLfreshWatNam = "NA",
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
                    specie_fate_code == 6 ~ "NotKnown",
                    specie_fate_code == 11 ~ "NotApplicable"
                  ),
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
                  CLsupReg = "OFR",
                  CLgeoInd = "IWE",
                  CLspeConTech = "NA",
                  CLdeepSeaReg = "N",
                  CLoffWeight = round(x = CLoffWeight,
                                      digits = 1),
                  CLtotOffLanVal = "Unknown",
                  CLconfiFlag = "N")
  # confidentiality variable
  observe_ps_cl_data_vessel <- observe_ps_cl_data_final %>%
    dplyr::group_by(CLyear,
                    CLquar,
                    CLmonth,
                    CLarea) %>%
    dplyr::summarise(vessel_count = dplyr::n_distinct(vessel_code),
                     .groups = "drop")
  if (dim(x = dplyr::filter(.data = observe_ps_cl_data_vessel,
                            vessel_count < 3))[1] != 0) {
    observe_ps_cl_data_vessel_confidential <- dplyr::filter(.data = observe_ps_cl_data_vessel,
                                                            vessel_count < 3) %>%
      dplyr::mutate(CLFDIconCod = "A")
    observe_ps_cl_data_final <- dplyr::left_join(x = observe_ps_cl_data_final,
                                                 y = dplyr::select(.data = observe_ps_cl_data_vessel_confidential,
                                                                   -vessel_count),
                                                 by = c("CLyear",
                                                        "CLquar",
                                                        "CLmonth",
                                                        "CLarea")) %>%
      dplyr::mutate(CLFDIconCod = dplyr::case_when(
        is.na(x = CLFDIconCod) ~ "N",
        TRUE ~ CLFDIconCod
      ))
  } else {
    observe_ps_cl_data_final <- dplyr::mutate(CLFDIconCod == "N")
  }
  observe_ps_cl_data_final <- dplyr::inner_join(x = observe_ps_cl_data_final,
                                                y = dplyr::rename(.data = observe_ps_cl_data_vessel,
                                                                  "CLnumUniqVes" = "vessel_count"),
                                                by = c("CLyear",
                                                       "CLquar",
                                                       "CLmonth",
                                                       "CLarea"))
  # final design and selection
  observe_ps_cl_data_supreme <- observe_ps_cl_data_final %>%
    dplyr::group_by(CLrecType,
                    CLlanCou,
                    CLvesFlagCou,
                    CLyear,
                    CLquar,
                    CLmonth,
                    CLarea,
                    CLstatRect,
                    CLgsaSubarea,
                    CLfreshWatNam,
                    CLeconZone,
                    CLspecCode,
                    CLspecFAO,
                    CLlandCat,
                    CLcatchCat,
                    CLregDisCat,
                    CLnatFishAct,
                    CLmetier6,
                    CLIBmitiDev,
                    CLloc,
                    CLvesLenCat,
                    CLfishTech,
                    CLsupReg,
                    CLgeoInd,
                    CLspeConTech,
                    CLdeepSeaReg,
                    CLFDIconCod,
                    CLoffWeight,
                    CLtotOffLanVal,
                    CLnumUniqVes,
                    CLconfiFlag,
                    CLencrypVesIds) %>%
    dplyr::summarise(CLoffWeight = sum(CLoffWeight),
                     .groups = "drop")
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful data design.\n",
      sep = "")
  # 6 - Extraction ----
  if (! is.null(x = export_path)) {
    utils::write.csv2(x = observe_ps_cl_data_supreme,
               file = file.path(export_path,
                                paste(format(as.POSIXct(Sys.time()),
                                             "%Y%m%d_%H%M%S"),
                                      "rdbes_cl.csv",
                                      sep = "_")),
               row.names = FALSE)
  }
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on RDBES table CL generation.\n",
      sep = "")



}
