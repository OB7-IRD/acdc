#' @name fdi_template_checking
#' @title FDI template checking (FDI process)
#' @description Process for template checking of a FDI table.
#' @param fdi_table Output table from a FDI process.
#' @param template_year {\link[base]{integer}} expected. Template year.
#' @param table_id {\link[base]{character}} expected. Identification of the FDI table.
#' @export
#' @importFrom codama r_type_checking vector_comparison
#' @importFrom readxl read_xlsx
fdi_template_checking <- function(fdi_table,
                                  template_year,
                                  table_id) {
  # argument verification ----
  if (codama::r_type_checking(r_object = template_year,
                              type = "integer",
                              length = 1L,
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = template_year,
                                   type = "integer",
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
      " - Start table ",
      toupper(x = table_id),
      " template checking.\n",
      sep = "")
  template_name <- fdi_template_identification(table_id = table_id)
  if (system.file("referentials",
                  "fdi",
                  template_year,
                  paste0(template_name,
                         ".xlsx"),
                  package = "acdc") == "") {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Warning: ",
        template_year,
        " FDI template not available. Main table output not checking.\n",
        sep = "")
  } else {
    current_template_table <- readxl::read_xlsx(path = system.file("referentials",
                                                                   "fdi",
                                                                   template_year,
                                                                   paste0(template_name,
                                                                          ".xlsx"),
                                                                   package = "acdc"),
                                                sheet = paste0("TABLE_",
                                                               toupper(x = table_id)),
                                                col_names = TRUE)
    if (codama::vector_comparison(first_vector = names(x = fdi_table),
                                  second_vector = names(x = current_template_table),
                                  comparison_type = "equal",
                                  output = "logical") != TRUE) {
      cat(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Warning: FDI table is not in line with ",
          template_year,
          " official FDI template.\n",
          sep = "")
    } else {
      cat(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Successful comparison of the FDI table ",
          toupper(x = table_id),
          " with the official FDI template associated.\n",
          sep = "")
    }
  }
}
