# FDI table id checking ----
#' @name fdi_table_id_checking
#' @title FDI table id checking
#' @keywords internal
#' @importFrom codama r_type_checking
fdi_table_id_checking <- function(table_id) {
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
    stop("Invalid argument check(s)")
  }
}

