#' @export
fdi_table_export <- function(fdi_table,
                             export_path,
                             table_id) {
  # argument verification ----
  if (codama::r_type_checking(r_object = export_path,
                              type = "character",
                              length = 1L,
                              output = "logical") != TRUE) {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Error: ",
        "invalide \"export_path\" argument.\n",
        sep = "")
    codama::r_type_checking(r_object = export_path,
                            type = "character",
                            length = 1L,
                            output = "message")
  }
  if (codama::r_type_checking(r_object = table_id,
                              type = "character",
                              length = 1L,
                              allowed_values = c("a",
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
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Error: ",
        "invalide \"table_id\" argument.\n",
        sep = "")
    codama::r_type_checking(r_object = table_id,
                            type = "character",
                            length = 1L,
                            allowed_values = c("a",
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
                            output = "message")
  }
  # process ----
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
