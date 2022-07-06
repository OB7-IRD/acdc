# global export path checking ----
#' @name global_export_path_checking
#' @title Global export path checking
#' @keywords internal
#' @importFrom codama r_type_checking
global_export_path_checking <- function(export_path) {
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
}

# global database connection checking ----
#' @name global_database_connection_checking
#' @title Global database connection checking
#' @keywords internal
#' @importFrom codama r_type_checking
global_database_connection_checking <- function(database_con) {
  if (codama::r_type_checking(r_object = database_con,
                              type = "PostgreSQLConnection",
                              length = 1L,
                              output = "logical") != TRUE) {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Error: ",
        "invalide \"database_con\" argument.\n",
        sep = "")
    codama::r_type_checking(r_object = database_con,
                            type = "PostgreSQLConnection",
                            length = 1L,
                            output = "message")
  }
}

# global integer checking ----
#' @name global_integer_checking
#' @title Global integer checking
#' @keywords internal
#' @importFrom codama r_type_checking
global_integer_checking <- function(integer_object) {
  if (codama::r_type_checking(r_object = integer_object,
                              type = "integer",
                              output = "logical") != TRUE) {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Error: ",
        "invalide \"integer_object\" argument.\n",
        sep = "")
    codama::r_type_checking(r_object = integer_object,
                            type = "integer",
                            output = "message")
  }
}

# global RData type checking ----
#' @name global_rdata_type_checking
#' @title Global RData type checking
#' @keywords internal
#' @importFrom codama file_path_checking
global_rdata_type_checking <- function(rdata_path) {
  if (codama::file_path_checking(file_path =  rdata_path,
                                 extension = "RData",
                                 output = "logical") != TRUE) {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Error: ",
        "invalide \"rdata_path\" argument.\n",
        sep = "")
    codama::file_path_checking(file_path =  rdata_path,
                               extension = "RData",
                               output = "message")
  }
}

# global logical checking ----
#' @name global_logical_checking
#' @title Global logical checking
#' @keywords internal
#' @importFrom codama r_type_checking
global_logical_checking <- function(logical_object) {
  if (codama::r_type_checking(r_object = logical_object,
                              type = "logical",
                              output = "logical") != TRUE) {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Error: ",
        "invalide \"logical_object\" argument.\n",
        sep = "")
    codama::r_type_checking(r_object = logical_object,
                            type = "logical",
                            output = "message")
  }
}

# FDI r type checking ----
#' @name fdi_r_type_checking
#' @title FDI R type checking
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
}

