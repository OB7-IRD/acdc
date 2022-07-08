#' @name fdi_tableh_landings_rectangle
#' @title Table H landings by rectangle generation (FDI process)
#' @description Process for generation and optionally extraction of the FDI table H (landings by rectangle).
#' @param tablea_bycatch_retained Output "bycatch_retained" of the function {\link[acdc]{fdi_tablea_catch_summary}}.
#' @param tablea_landing_rectangle Output "landing_rectangle" of the function {\link[acdc]{fdi_tablea_catch_summary}}.
#' @param template_checking {\link[base]{logical}} expected. By default TRUE. Checking FDI table generated regarding the official FDI template.
#' @param template_year {\link[base]{integer}} expected. By default NULL. Template year.
#' @param table_export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @return The process returns a list with the FDI table H inside.
#' @export
#' @importFrom codama r_type_checking
#' @importFrom furdeb lat_long_to_csquare
#' @importFrom dplyr select rename mutate group_by summarise full_join case_when
fdi_tableh_landings_rectangle <- function(tablea_bycatch_retained,
                                          tablea_landing_rectangle,
                                          template_checking = TRUE,
                                          template_year = NULL,
                                          table_export_path = NULL) {
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on FDI table H generation.\n",
      sep = "")
  # global variables assignement ----
  country <- NULL
  year <- NULL
  quarter <- NULL
  vessel_length <- NULL
  fishing_tech <- NULL
  gear_type <- NULL
  target_assemblage <- NULL
  mesh_size_range <- NULL
  metier <- NULL
  supra_region <- NULL
  geo_indicator <- NULL
  specon_tech <- NULL
  deep <- NULL
  species <- NULL
  retained_tons <- NULL
  eez_indicator <- NULL
  sub_region <- NULL
  grid_square_0.5 <- NULL
  c_square <- NULL
  rectangle_type <- NULL
  rectangle_lat <- NULL
  rectangle_lon <- NULL
  latitude <- NULL
  longitude <- NULL
  totwghtlandg <- NULL
  totvallandg <- NULL
  confidential <- NULL
  # arguments verifications ----
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
  # bycatch and landing data extraction ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on bycatch and landing data.\n",
      sep = "")
  observe_bycatch_retained_tableh <- furdeb::lat_long_to_csquare(data = tablea_bycatch_retained,
                                                                 grid_square = 0.5,
                                                                 latitude_name = "latitude_decimal_degree",
                                                                 longitude_name = "longitude_decimal_degree") %>%
    dplyr::select(country,
                  year,
                  quarter,
                  vessel_length,
                  fishing_tech,
                  gear_type,
                  target_assemblage,
                  mesh_size_range,
                  metier,
                  supra_region,
                  geo_indicator,
                  specon_tech,
                  deep,
                  species,
                  retained_tons,
                  eez_indicator,
                  sub_region,
                  grid_square_0.5) %>%
    dplyr::rename(c_square = grid_square_0.5) %>%
    dplyr::mutate(rectangle_type = "NA",
                  rectangle_lat = "NA",
                  rectangle_lon = "NA") %>%
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
                    geo_indicator,
                    specon_tech,
                    deep,
                    species,
                    sub_region,
                    eez_indicator,
                    c_square,
                    rectangle_type,
                    rectangle_lat,
                    rectangle_lon) %>%
    dplyr::summarise(retained_tons = sum(retained_tons),
                     .groups = "drop")
  balbaya_landing_rectangle <- furdeb::lat_long_to_csquare(data = tablea_landing_rectangle,
                                                           grid_square = 0.5,
                                                           latitude_name = "latitude",
                                                           longitude_name = "longitude") %>%
    dplyr::mutate(rectangle_type = "NA",
                  rectangle_lat = "NA",
                  rectangle_lon = "NA") %>%
    dplyr::rename(c_square = grid_square_0.5) %>%
    dplyr::select(-latitude,
                  -longitude) %>%
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
                    geo_indicator,
                    specon_tech,
                    deep,
                    species,
                    sub_region,
                    eez_indicator,
                    c_square,
                    rectangle_type,
                    rectangle_lat,
                    rectangle_lon) %>%
    dplyr::summarise(totwghtlandg = sum(totwghtlandg),
                     .groups = "drop")
  tableh_final <- balbaya_landing_rectangle %>%
    dplyr::full_join(observe_bycatch_retained_tableh,
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
                            "geo_indicator",
                            "specon_tech",
                            "deep",
                            "species",
                            "sub_region",
                            "eez_indicator",
                            "c_square",
                            "rectangle_type",
                            "rectangle_lat",
                            "rectangle_lon")) %>%
    dplyr::mutate(
      totwghtlandg = dplyr::case_when(
        is.na(x = totwghtlandg) ~ 0,
        TRUE ~ totwghtlandg),
      retained_tons = dplyr::case_when(
        is.na(x = retained_tons) ~ 0,
        TRUE ~ retained_tons),
      totwghtlandg = round(x = totwghtlandg + retained_tons,
                           digits = 3),
      totvallandg = "NK",
      confidential = dplyr::case_when(
        fishing_tech == "HOK" ~ "A",
        TRUE ~ "N")) %>%
    dplyr::select(country,
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
                  deep,
                  rectangle_type,
                  rectangle_lat,
                  rectangle_lon,
                  c_square,
                  species,
                  totwghtlandg,
                  totvallandg,
                  confidential)
  names(x = tableh_final) <- toupper(x = names(x = tableh_final))
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on bycatch and landing data.\n",
      sep = "")
  # template checking ----
  if (template_checking == TRUE) {
    fdi_template_checking(fdi_table = tableh_final,
                          template_year = template_year,
                          table_id = "h")
  } else {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Warning: main table output not checking with official FDI template.\n",
        sep = "")
  }
  # export ----
  if (! is.null(x = table_export_path)) {
    fdi_table_export(fdi_table = tableh_final,
                     export_path = table_export_path,
                     table_id = "h")
  }
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on FDI table H generation.\n",
      sep = "")
  return(list("fdi_tables" = list("table_h" = tableh_final)))
}
