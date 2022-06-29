#' @export
fdi_tablei_effort_rectangle <- function(tableg_effort_rectangle,
                                        template_checking = TRUE,
                                        template_year = NULL,
                                        table_export_path = NULL) {
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
  return(list("fdi_tables" = list("table_i" = balbaya_effort_rectangle)))
}
