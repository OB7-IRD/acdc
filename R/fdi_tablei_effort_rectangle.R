#' @name fdi_tablei_effort_rectangle
#' @title Table I effort by rectangle generation (FDI process)
#' @description Process for generation and optionally extraction of the FDI table I (effort by rectangle).
#' @param tableg_effort_rectangle Output "effort_rectangle" of the function {\link[acdc]{fdi_tableg_effort}}.
#' @param template_checking {\link[base]{logical}} expected. By default FALSE Checking FDI table generated regarding the official FDI template.
#' @param template_year {\link[base]{integer}} expected. By default NULL. Template year.
#' @param table_export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @return The process returns a list with the FDI table I inside.
#' @export
#' @importFrom furdeb lat_long_to_csquare
#' @importFrom dplyr mutate rename select group_by summarise select case_when
fdi_tablei_effort_rectangle <- function(tableg_effort_rectangle,
                                        template_checking = FALSE,
                                        template_year = NULL,
                                        table_export_path = NULL) {
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on FDI table I generation.\n",
      sep = "")
  # global variables assignement ----
  grid_square_0.5 <- NULL
  latitude <- NULL
  longitude <- NULL
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
  sub_region <- NULL
  eez_indicator <- NULL
  c_square <- NULL
  rectangle_type <- NULL
  rectangle_lat <- NULL
  rectangle_lon <- NULL
  totfishdays <- NULL
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
  # effort data extraction and design ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on effort data.\n",
      sep = "")
  balbaya_effort_rectangle <- furdeb::lat_long_to_csquare(data = tableg_effort_rectangle,
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
                    sub_region,
                    eez_indicator,
                    c_square,
                    rectangle_type,
                    rectangle_lat,
                    rectangle_lon) %>%
    dplyr::summarise(totfishdays = sum(totfishdays),
                     .groups = "drop") %>%
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
                  totfishdays) %>%
    dplyr::mutate(
      confidential = dplyr::case_when(
        fishing_tech == "HOK" ~ "Y",
        TRUE ~ "N"))
  names(balbaya_effort_rectangle) <- toupper(names(balbaya_effort_rectangle))
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on bycatch data.\n",
      sep = "")
  # template checking ----
  if (template_checking == TRUE) {
    fdi_template_checking(fdi_table = balbaya_effort_rectangle,
                          template_year = template_year,
                          table_id = "i")
  } else {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Warning: main table output not checking with official FDI template.\n",
        sep = "")
  }
  # export ----
  if (! is.null(x = table_export_path)) {
    fdi_table_export(fdi_table = balbaya_effort_rectangle,
                     export_path = table_export_path,
                     table_id = "i")
  }
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on FDI table I generation.\n",
      sep = "")
  return(list("fdi_tables" = list("table_i" = balbaya_effort_rectangle)))
}
