#' @export
fdi_tableh_landings_rectangle <- function(tablea_bycatch_retained,
                                          tablea_landing_rectangle,
                                          template_checking = TRUE,
                                          template_year = NULL,
                                          table_export_path = NULL) {

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
  return(list("fdi_tables" = list("table_h" = tableh_final)))
}
