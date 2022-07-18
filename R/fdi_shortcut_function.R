#' @name fdi_shortcut_function
#' @title FDI tables shortcut function (FDI process)
#' @description Shortcut function to generate all FDI tables.
#' @param balbaya_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the balbaya database.
#' @param sardara_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the sardara database.
#' @param t3_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the t3 database.
#' @param period {\link[base]{integer}} expected. Year period for data extractions.
#' @param gear {\link[base]{integer}}. Gear(s) selection for data extractions.
#' @param flag {\link[base]{integer}} expected. Flag(s) selection for data extractions.
#' @param observe_bycatch_path {\link[base]{character}} expected. Directory path of the bycatch data extractions. Check this input with Philippe Sabarros (philippe.sabarros@ird.fr).
#' @param observe_discard_path {\link[base]{character}} expected. Directory path of the discards data extractions. Check this input with Philippe Sabarros (philippe.sabarros@ird.fr).
#' @param fao_area_file_path {\link[base]{character}} expected. File path of the FAO area grid. The file format has to be .Rdata.
#' @param eez_area_file_path {\link[base]{character}} expected. File path of the EEZ area grid. The file format has to be .Rdata.
#' @param cwp_grid_1deg_1deg {\link[base]{character}} expected. File path of the CWP area grid of 1° by 1°. The file format has to be .Rdata.
#' @param cwp_grid_5deg_5deg {\link[base]{character}} expected. File path of the CWP area grid of 5° by 5°. The file format has to be .Rdata.
#' @param template_checking {\link[base]{logical}} expected. By default FALSE Checking FDI table generated regarding the official FDI template.
#' @param template_year {\link[base]{integer}} expected. By default NULL. Template year.
#' @param table_export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @export
#' @importFrom furdeb list_logical_merge
fdi_shortcut_function <- function(balbaya_con,
                                  sardara_con,
                                  t3_con,
                                  period,
                                  gear,
                                  flag,
                                  observe_bycatch_path,
                                  observe_discard_path,
                                  fao_area_file_path,
                                  eez_area_file_path,
                                  cwp_grid_1deg_1deg,
                                  cwp_grid_5deg_5deg,
                                  template_checking = FALSE,
                                  template_year = NULL,
                                  table_export_path = NULL) {
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process for FDI tables generation.\n",
      sep = "")
  # table a process ----
  fdi_tables <- fdi_tablea_catch_summary(balbaya_con = balbaya_con,
                                         observe_bycatch_path = observe_bycatch_path,
                                         period = period,
                                         gear = gear,
                                         flag = flag,
                                         fao_area_file_path = fao_area_file_path,
                                         eez_area_file_path = eez_area_file_path,
                                         cwp_grid_file_path = cwp_grid_1deg_1deg,
                                         template_checking = template_checking,
                                         template_year = template_year,
                                         table_export_path = table_export_path)
  # table d process ----
  fdi_tables <- furdeb::list_logical_merge(first_list = fdi_tables,
                                           second_list = fdi_tabled_discard_length(observe_discard_path = observe_discard_path,
                                                                                   tablea_catch_summary = fdi_tables[["fdi_tables"]][["table_a"]],
                                                                                   template_checking = template_checking,
                                                                                   template_year = template_year,
                                                                                   table_export_path = table_export_path))
  # table f process ----
  fdi_tables <- furdeb::list_logical_merge(first_list = fdi_tables,
                                           second_list = fdi_tablef_landings_length(balbaya_con = balbaya_con,
                                                                                    sardara_con = sardara_con,
                                                                                    t3_con = t3_con,
                                                                                    period = period,
                                                                                    gear = gear,
                                                                                    flag = flag,
                                                                                    tablea_bycatch_retained = fdi_tables[["ad_hoc_tables"]][["bycatch_retained"]],
                                                                                    tablea_catch_summary = fdi_tables[["fdi_tables"]][["table_a"]],
                                                                                    cwp_grid_file_path = cwp_grid_5deg_5deg,
                                                                                    fao_area_file_path = fao_area_file_path,
                                                                                    template_checking = template_checking,
                                                                                    template_year = template_year,
                                                                                    table_export_path = table_export_path))
  # table g process ----
  fdi_tables <- furdeb::list_logical_merge(first_list = fdi_tables,
                                           second_list = fdi_tableg_effort(balbaya_con = balbaya_con,
                                                                           period = period,
                                                                           gear = gear,
                                                                           flag = flag,
                                                                           fao_area_file_path = fao_area_file_path,
                                                                           eez_area_file_path = eez_area_file_path,
                                                                           template_checking = template_checking,
                                                                           template_year = template_year,
                                                                           table_export_path = table_export_path))
  # table h process ----
  fdi_tables <- furdeb::list_logical_merge(first_list = fdi_tables,
                                           second_list = fdi_tableh_landings_rectangle(tablea_bycatch_retained = fdi_tables[["ad_hoc_tables"]][["bycatch_retained"]],
                                                                                       tablea_landing_rectangle = fdi_tables[["ad_hoc_tables"]][["landing_rectangle"]],
                                                                                       template_checking = template_checking,
                                                                                       template_year = template_year,
                                                                                       table_export_path = table_export_path))
  # table i process ----
  fdi_tables <- furdeb::list_logical_merge(first_list = fdi_tables,
                                           second_list = fdi_tablei_effort_rectangle(tableg_effort_rectangle = fdi_tables[["ad_hoc_tables"]][["effort_rectangle"]],
                                                                                     template_checking = template_checking,
                                                                                     template_year = template_year,
                                                                                     table_export_path = table_export_path))
  # table j process ----
  fdi_tables <- furdeb::list_logical_merge(first_list = fdi_tables,
                                           second_list = fdi_tablej_capacity(balbaya_con = balbaya_con,
                                                                             period = period,
                                                                             gear = gear,
                                                                             flag = flag,
                                                                             fao_area_file_path = fao_area_file_path,
                                                                             template_checking = template_checking,
                                                                             template_year = template_year,
                                                                             table_export_path = table_export_path))
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on FDI tables generation.\n",
      sep = "")
  return(fdi_tables)
}
