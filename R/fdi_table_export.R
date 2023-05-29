#' @name fdi_table_export
#' @title Table export (FDI process)
#' @description Process for export in xlsx format FDI tables.
#' @param fdi_table R object expected. FDI table selected for export process.
#' @param export_path {\link[base]{character}} expected. Directory path associated for the export.
#' @param table_id {\link[base]{character}} expected. Identification of the FDI table.
#' @export
#' @importFrom codama r_type_checking
#' @importFrom openxlsx write.xlsx
fdi_table_export <- function(fdi_table,
                             export_path,
                             table_id) {
  # arguments verifications ----
  if (codama::r_type_checking(r_object = export_path,
                              type = "character",
                              length = 1L,
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = export_path,
                                   type = "character",
                                   length = 1L,
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = table_id,
                              type = "character",
                              length = 1L,
                              allowed_value = c("a",
                                                "b",
                                                "c",
                                                "d",
                                                "e",
                                                "f",
                                                "g",
                                                "h",
                                                "i",
                                                "j",
                                                "k"),
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = table_id,
                                   type = "character",
                                   length = 1L,
                                   allowed_value = c("a",
                                                     "b",
                                                     "c",
                                                     "d",
                                                     "e",
                                                     "f",
                                                     "g",
                                                     "h",
                                                     "i",
                                                     "j",
                                                     "k"),
                                   output = "message"))
  }
  # process ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start export of the FDI table ",
      toupper(x = table_id),
      ".\n",
      sep = "")
  openxlsx::write.xlsx(x = as.data.frame(x = fdi_table),
                       file = file.path(export_path,
                                        paste0("TABLE_",
                                               toupper(x = table_id),
                                               "_IRD_",
                                               format(as.POSIXct(Sys.time()),
                                                      "%Y%m%d_%H%M%S"),
                                               ".xlsx"),
                                        fsep = "\\"),
                       sheetName = paste0("TABLE_",
                                          toupper(x = table_id)))
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful export of the FDI table ",
      toupper(x = table_id),
      ".\n",
      sep = "")
}
