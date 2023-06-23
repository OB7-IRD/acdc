#' @name fdi_tablea_catch_summary
#' @title Table A catch generation (FDI process)
#' @description Process for generation and optionally extraction of the FDI table A (catch).
#' @param balbaya_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the balbaya database.
#' @param observe_bycatch_path {\link[base]{character}} expected. Directory path of the bycatch data extractions. Check this input with Philippe Sabarros (philippe.sabarros@ird.fr).
#' @param period {\link[base]{integer}} expected. Year period for data extractions.
#' @param gear {\link[base]{integer}}. Gear(s) selection for data extractions.
#' @param flag {\link[base]{integer}} expected. Flag(s) selection for data extractions.
#' @param fao_area_file_path {\link[base]{character}} expected. File path of the FAO area grid. The file format has to be .Rdata or .RData extension.
#' @param eez_area_file_path {\link[base]{character}} expected. File path of the EEZ area grid. The file format has to be .Rdata or .RData extension.
#' @param cwp_grid_file_path {\link[base]{character}} expected. File path of the CWP area grid. The file format has to be .Rdata or .RData extension.
#' @param template_checking {\link[base]{logical}} expected. By default FALSE Checking FDI table generated regarding the official FDI template.
#' @param template_year {\link[base]{integer}} expected. By default NULL. Template year.
#' @param table_export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @return The process returns a double list with the FDI table A in the first one and in the second one two accessory outputs useful for the other FDI table generation processes ("landing_rectangle" and "bycatch_retained").
#' @export
#' @importFrom DBI sqlInterpolate SQL dbGetQuery
#' @importFrom furdeb marine_area_overlay latitude_longitude_cwp_manipulation
#' @importFrom dplyr mutate case_when rowwise select group_by summarise bind_cols rename full_join
#' @importFrom codama file_path_checking r_type_checking
#' @importFrom utils read.csv2
fdi_tablea_catch_summary <- function(balbaya_con,
                                     observe_bycatch_path,
                                     period,
                                     gear,
                                     flag,
                                     fao_area_file_path,
                                     eez_area_file_path,
                                     cwp_grid_file_path,
                                     template_checking = FALSE,
                                     template_year = NULL,
                                     table_export_path = NULL) {
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on FDI table A generation.\n",
      sep = "")
  # global variables assignement ----
  gear_type <- NULL
  target_assemblage <- NULL
  country <- NULL
  quarter <- NULL
  sub_region <- NULL
  vessel_length <- NULL
  species <- NULL
  year <- NULL
  fishing_tech <- NULL
  mesh_size_range <- NULL
  metier <- NULL
  metier_7 <- NULL
  fishing_mode <- NULL
  domain_landings <- NULL
  latitude <- NULL
  longitude <- NULL
  supra_region <- NULL
  eez_indicator <- NULL
  geo_indicator <- NULL
  nep_sub_region <- NULL
  specon_tech <- NULL
  deep <- NULL
  totwghtlandg <- NULL
  cwp <- NULL
  longitude_decimal_degree <- NULL
  latitude_decimal_degree <- NULL
  fao_code <- NULL
  school_type <- NULL
  major_fao <- NULL
  subarea_fao <- NULL
  division_fao <- NULL
  best_fao_area <- NULL
  eez <- NULL
  discarded_tons  <- NULL
  domain_discards <- NULL
  retained_tons <- NULL
  discards <- NULL
  totvallandg <- NULL
  confidential <- NULL
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
  if (codama::r_type_checking(r_object = observe_bycatch_path,
                              type = "character",
                              length = 1L,
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = observe_bycatch_path,
                                   type = "character",
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
  if (codama::file_path_checking(file_path =  cwp_grid_file_path,
                                 extension = c("Rdata",
                                               "RData"),
                                 output = "logical") != TRUE) {
    return(codama::file_path_checking(file_path =  cwp_grid_file_path,
                                      extension = c("Rdata",
                                                    "RData"),
                                      output = "message"))
  }
  if (codama::r_type_checking(r_object = template_checking,
                              type = "logical",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = template_checking,
                                   type = "logical",
                                   output = "message"))
  }
  if ((! is.null(x = template_year))
      && codama::r_type_checking(r_object = template_year,
                                 type = "integer",
                                 output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = template_year,
                                   type = "integer",
                                   output = "message"))
  }
  if ((! is.null(x = table_export_path))
      && codama::r_type_checking(r_object = table_export_path,
                                 type = "character",
                                 length = 1L,
                                 output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = table_export_path,
                                   type = "character",
                                   length = 1L,
                                   output = "message"))
  }
  # landing data extraction ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on landing data.\n",
      sep = "")
  balbaya_landing_query <- paste(readLines(con = system.file("sql",
                                                             "fdi",
                                                             "balbaya_landing_fdi.sql",
                                                             package = "acdc")),
                                 collapse = "\n")
  balbaya_landing_query <- DBI::sqlInterpolate(conn = balbaya_con,
                                               sql = balbaya_landing_query,
                                               period = DBI::SQL(paste0(period,
                                                                        collapse = ", ")),
                                               flag = DBI::SQL(paste0(flag,
                                                                      collapse = ", ")),
                                               gear = DBI::SQL(paste0(gear,
                                                                      collapse = ", ")))
  balbaya_landing <- DBI::dbGetQuery(conn = balbaya_con,
                                     statement = balbaya_landing_query)
  # landings data design ----
  balbaya_landing <- suppressMessages(furdeb::marine_area_overlay(data = balbaya_landing,
                                                                  overlay_expected = "fao_eez_area",
                                                                  longitude_name = "longitude",
                                                                  latitude_name = "latitude",
                                                                  fao_area_file_path = fao_area_file_path,
                                                                  fao_overlay_level = "division",
                                                                  auto_selection_fao = TRUE,
                                                                  eez_area_file_path = eez_area_file_path,
                                                                  for_fdi_use = TRUE,
                                                                  silent = TRUE))
  balbaya_landing <- dplyr::mutate(.data = balbaya_landing,
                                   sub_region = dplyr::case_when(
                                     best_fao_area %in% c("47.A.0",
                                                          "47.A.1") ~ "47.A",
                                     best_fao_area %in% c("47.B.1") ~ "47.B",
                                     best_fao_area %in% c("47.1.1",
                                                          "47.1.2",
                                                          "47.1.3",
                                                          "41.1.4",
                                                          "41.1.5",
                                                          "41.1.6") ~ "47.1",
                                     best_fao_area %in% c("47.2.2") ~ "47.2",
                                     best_fao_area %in% c("34.2") ~ "34.2.0",
                                     TRUE ~ best_fao_area
                                   ),
                                   vessel_length = dplyr::case_when(vessel_length < 10 ~ "VL0010",
                                                                    vessel_length >= 10 & vessel_length < 12 ~ "VL1012",
                                                                    vessel_length >= 12 & vessel_length < 18 ~ "VL1218",
                                                                    vessel_length >= 18 & vessel_length < 24 ~ "VL1824",
                                                                    vessel_length >= 24 & vessel_length < 40 ~ "VL2440",
                                                                    vessel_length >= 40 ~ "VL40XX",
                                                                    TRUE ~ "NK"),
                                   fishing_tech = dplyr::case_when(engin == 1 ~ "PS",
                                                                   engin %in% c(2, 3) ~ "HOK",
                                                                   TRUE ~ "error"),
                                   gear_type = dplyr::case_when(engin == 1 ~ "PS",
                                                                engin == 2 ~ "LHP",
                                                                engin == 3 ~ "LLD",
                                                                TRUE ~ "error"),
                                   target_assemblage = "LPF",
                                   mesh_size_range = dplyr::case_when(engin == 1 ~ "NK",
                                                                      engin %in% c(2, 3) ~ "NA",
                                                                      TRUE ~ "error"),
                                   metier = dplyr::case_when(gear_type == "PS" ~ paste(gear_type,
                                                                                       target_assemblage,
                                                                                       ">0",
                                                                                       "0",
                                                                                       "0",
                                                                                       sep = "_"),
                                                             gear_type == "LHP" ~ paste(gear_type,
                                                                                        target_assemblage,
                                                                                        "0",
                                                                                        "0",
                                                                                        "0",
                                                                                        sep = "_"),
                                                             TRUE ~ "error"),
                                   metier_7 = dplyr::case_when(gear_type == "PS" ~ paste(metier,
                                                                                         "TRO",
                                                                                         sep = "_"),
                                                               gear_type == "LHP" ~ paste(metier,
                                                                                          "MSP",
                                                                                          sep = "_"),
                                                               TRUE ~ "error"),
                                   domain_landings = paste(country,
                                                           quarter,
                                                           sub_region,
                                                           paste(gear_type,
                                                                 dplyr::case_when(fishing_mode == 1 ~ "FOB",
                                                                                  fishing_mode == 2 ~ "FSC",
                                                                                  fishing_mode == 3 ~ "UNK",
                                                                                  TRUE ~ "error"),
                                                                 sep = "-"),
                                                           target_assemblage,
                                                           "NA",
                                                           "NA",
                                                           "NA",
                                                           vessel_length,
                                                           species,
                                                           "NA",
                                                           sep = "_"),
                                   supra_region = "OFR",
                                   geo_indicator = "IWE",
                                   specon_tech = NA_character_,
                                   deep = NA_character_) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(nep_sub_region = ifelse(test = unlist(strsplit(x = sub_region,
                                                                 split = "[.]"))[[1]] == "27",
                                          yes = "error",
                                          no = NA_character_))
  balbaya_landing_rectangle <- dplyr::select(.data = balbaya_landing,
                                             country,
                                             year,
                                             quarter,
                                             vessel_length,
                                             fishing_tech,
                                             gear_type,
                                             target_assemblage,
                                             mesh_size_range,
                                             metier,
                                             metier_7,
                                             fishing_mode,
                                             domain_landings,
                                             latitude,
                                             longitude,
                                             supra_region,
                                             sub_region,
                                             eez_indicator,
                                             geo_indicator,
                                             nep_sub_region,
                                             specon_tech,
                                             deep,
                                             species,
                                             totwghtlandg)
  balbaya_landing <- dplyr::group_by(.data = balbaya_landing,
                                     country,
                                     year,
                                     quarter,
                                     vessel_length,
                                     fishing_tech,
                                     gear_type,
                                     target_assemblage,
                                     mesh_size_range,
                                     metier,
                                     metier_7,
                                     fishing_mode,
                                     domain_landings,
                                     supra_region,
                                     sub_region,
                                     eez_indicator,
                                     geo_indicator,
                                     nep_sub_region,
                                     specon_tech,
                                     deep,
                                     species) %>%
    dplyr::summarise(totwghtlandg = sum(totwghtlandg),
                     .groups = "drop")
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on landing data.\n",
      sep = "")
  # observers bycatch data extraction ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on bycatch data.\n",
      sep = "")
  observe_bycatch <- lapply(X = list.files(path = observe_bycatch_path),
                            FUN = function(file_name) {
                              utils::read.csv(file.path(observe_bycatch_path,
                                                        file_name),
                                              stringsAsFactors = FALSE)
                            })
  observe_bycatch <- do.call("rbind",
                             observe_bycatch) %>%
    dplyr::rename(cwp = cwp11) %>%
    dplyr::mutate(cwp = as.character(x = cwp))
  observe_bycatch <- dplyr::bind_cols(observe_bycatch,
                                      (furdeb::latitude_longitude_cwp_manipulation(manipulation_process = "cwp_to_latitude_longitude",
                                                                                   data_cwp = observe_bycatch$cwp,
                                                                                   referential_grid_file_path = cwp_grid_file_path,
                                                                                   output_degree_parameter = "centroid",
                                                                                   output_degree_format = "decimal_degree") %>%
                                         dplyr::mutate(longitude_decimal_degree = as.numeric(longitude_decimal_degree),
                                                       latitude_decimal_degree = as.numeric(latitude_decimal_degree)) %>%
                                         dplyr::select(-cwp)))
  observe_bycatch <- suppressMessages(furdeb::marine_area_overlay(data = observe_bycatch,
                                                                  overlay_expected = "fao_eez_area",
                                                                  longitude_name = "longitude_decimal_degree",
                                                                  latitude_name = "latitude_decimal_degree",
                                                                  fao_area_file_path = fao_area_file_path,
                                                                  fao_overlay_level = "division",
                                                                  auto_selection_fao = TRUE,
                                                                  eez_area_file_path = eez_area_file_path,
                                                                  for_fdi_use = TRUE,
                                                                  silent = TRUE)) %>%
    dplyr::mutate(
      sub_region = dplyr::case_when(
        best_fao_area %in% c("47.A.0",
                             "47.A.1") ~ "47.A",
        best_fao_area %in% c("47.B.1") ~ "47.B",
        best_fao_area %in% c("47.1.1",
                             "47.1.2",
                             "47.1.3",
                             "47.1.4",
                             "47.1.5",
                             "47.1.6") ~ "47.1",
        best_fao_area %in% c("47.2.2") ~ "47.2",
        best_fao_area %in% c("34.2") ~ "34.2.0",
        TRUE ~ best_fao_area),
      country = "FRA",
      vessel_length = "VL40XX",
      fishing_tech = "PS",
      gear_type = "PS",
      target_assemblage = "LPF",
      mesh_size_range = "NK",
      metier = "PS_LPF_>0_0_0",
      metier_7 = "PS_LPF_>0_0_0_TRO",
      supra_region = "OFR",
      geo_indicator = "IWE",
      nep_sub_region = NA_character_,
      specon_tech = NA_character_,
      deep = NA_character_,
      species = as.character(fao_code),
      retained_tons = dplyr::case_when(
        is.na(retained_tons) ~ 0,
        TRUE ~ retained_tons
      ),
      domain_discards = paste(country,
                              quarter,
                              sub_region,
                              paste("PS",
                                    school_type,
                                    sep = "-"),
                              "LPF",
                              "NA",
                              "NA",
                              "NA",
                              vessel_length,
                              species,
                              "NA",
                              sep = "_"))
  if (any(is.na(x = unique(x = observe_bycatch$best_fao_area)))) {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Warning: at least one position have not fao area associated. Data associated will be deleted. Check the log.\n",
        sep = "")
    na_fao_area <- unique(observe_bycatch[is.na(x = observe_bycatch$best_fao_area), c("cwp",
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
    observe_bycatch <- observe_bycatch[! is.na(x = observe_bycatch$best_fao_area), ]
  }
  observe_bycatch_retained <- observe_bycatch[observe_bycatch$retained_tons != 0, ]
  observe_bycatch <- dplyr::select(.data = observe_bycatch,
                                   -cwp,
                                   -latitude_decimal_degree,
                                   -longitude_decimal_degree,
                                   -major_fao,
                                   -subarea_fao,
                                   -division_fao,
                                   -best_fao_area,
                                   -eez,
                                   -fao_code) %>%
    dplyr::rename("discards" = discarded_tons) %>%
    dplyr::mutate(fishing_mode = as.character(school_type)) %>%
    dplyr::group_by(country,
                    year,
                    quarter,
                    vessel_length,
                    fishing_tech,
                    gear_type,
                    target_assemblage,
                    mesh_size_range,
                    metier,
                    metier_7,
                    fishing_mode,
                    domain_discards,
                    supra_region,
                    sub_region,
                    eez_indicator,
                    geo_indicator,
                    nep_sub_region,
                    specon_tech,
                    deep,
                    species) %>%
    dplyr::summarise(retained_tons = sum(retained_tons),
                     discards = sum(discards),
                     .groups = "drop")
  observe_bycatch <- observe_bycatch[! (observe_bycatch$retained_tons == 0
                                        & observe_bycatch$discards == 0), ]
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on bycatch data.\n",
      sep = "")
  # final design ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start table A design.\n",
      sep = "")
  tablea_final <- balbaya_landing %>%
    dplyr::full_join(observe_bycatch,
                     by = c("country",
                            "year",
                            "quarter",
                            "vessel_length",
                            "fishing_tech",
                            "gear_type",
                            "target_assemblage",
                            "mesh_size_range",
                            "metier",
                            "metier_7",
                            "fishing_mode",
                            "supra_region",
                            "sub_region",
                            "eez_indicator",
                            "geo_indicator",
                            "nep_sub_region",
                            "specon_tech",
                            "deep",
                            "species")) %>%
    dplyr::mutate(discards = ifelse(test = fishing_tech == "HOK",
                                    yes = 0,
                                    no = ifelse(test = is.na(discards),
                                                yes = 0,
                                                no = round(x = discards,
                                                           digits = 3))),
                  retained_tons = ifelse(test = is.na(retained_tons),
                                         yes = 0,
                                         no = retained_tons),
                  totwghtlandg = ifelse(test = is.na(totwghtlandg),
                                        yes = 0,
                                        no = totwghtlandg),
                  totwghtlandg = round(x = totwghtlandg + retained_tons,
                                       digits = 3),
                  domain_landings = ifelse(test = is.na(domain_landings),
                                           yes = domain_discards,
                                           no = domain_landings),
                  domain_discards = ifelse(test = discards == 0,
                                           yes = domain_landings,
                                           no = ifelse(test = discards == "NK",
                                                       yes = "NK",
                                                       no = domain_discards)),
                  confidential = ifelse(test = fishing_tech == "HOK",
                                        yes = "A",
                                        no = "N"),
                  totvallandg = ifelse(test = totwghtlandg == 0,
                                       yes = 0,
                                       no = NA_real_)) %>%
    dplyr::group_by(country,
                    year,
                    quarter,
                    vessel_length,
                    fishing_tech,
                    gear_type,
                    target_assemblage,
                    mesh_size_range,
                    metier,
                    metier_7,
                    domain_discards,
                    domain_landings,
                    supra_region,
                    sub_region,
                    eez_indicator,
                    geo_indicator,
                    nep_sub_region,
                    specon_tech,
                    deep,
                    species,
                    confidential) %>%
    dplyr::summarise(totwghtlandg = sum(totwghtlandg),
                     totvallandg = sum(totvallandg),
                     discards = sum(discards),
                     .groups = "drop") %>%
    dplyr::mutate(totvallandg = dplyr::case_when(
      is.na(x = totvallandg) ~ "NK",
      TRUE ~ as.character(x = totvallandg)
    )) %>%
    dplyr::select(country,
                  year,
                  quarter,
                  vessel_length,
                  fishing_tech,
                  gear_type,
                  target_assemblage,
                  mesh_size_range,
                  metier,
                  metier_7,
                  domain_discards,
                  domain_landings,
                  supra_region,
                  sub_region,
                  eez_indicator,
                  geo_indicator,
                  nep_sub_region,
                  specon_tech,
                  deep,
                  species,
                  totwghtlandg,
                  totvallandg,
                  discards,
                  confidential)
  names(tablea_final) <- toupper(names(tablea_final))
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful table A design.\n",
      sep = "")
  # template checking ----
  if (template_checking == TRUE) {
    fdi_template_checking(fdi_table = tablea_final,
                          template_year = template_year,
                          table_id = "a")
  } else {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Warning: main table output not checking with official FDI template.\n",
        sep = "")
  }
  # export ----
  if (! is.null(x = table_export_path)) {
    fdi_table_export(fdi_table = tablea_final,
                     export_path = table_export_path,
                     table_id = "a")
  }
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on FDI table A generation.\n",
      sep = "")
  return(list("fdi_tables" = list("table_a" = tablea_final),
              "ad_hoc_tables" = list("landing_rectangle" = balbaya_landing_rectangle,
                                     "bycatch_retained" = observe_bycatch_retained)))
}
