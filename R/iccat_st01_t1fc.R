#' @name iccat_st01_t1fc
#' @title ICCAT ST01 - Task 1 Fleet characteristics form design
#' @description Processes (data extraction, quality checks and design) for the ICCAT submission form ST01-T1FC.
#' @param data_input_type Object of class \code{\link[base]{character}} expected. Type of input for data extraction. You can choose between "csv_form" or "database".
#' @param outputs_path Object of class \code{\link[base]{character}} expected. By default NULL. Form file path location. File extension expected is .csv with field separator character as ";" and decimal point  as ".".
#' @export
iccat_st01_t1fc <- function(data_input_type,
                            form_path = NULL) {
  # arguments verification ----
  codama::r_type_checking(r_object = data_input_type,
                          type = "character",
                          length = 1L,
                          allowed_values = c("csv_form",
                                             "database"))
  codama::file_path_checking(file_path = form_path,
                             extension = "csv")
  # data extraction ----
  if (data_input_type == "csv_form") {
    iccat_st01_t1fc <- read.table(file = form_path,
                                  header = TRUE,
                                  sep = ";",
                                  dec = ".")
  } else if (data_input_type == "database") {
    stop("function not developed yet, patient dude/miss!.\n")
  } else {
    stop("invalid \"data_input_type\" argument.\n")
  }
  # quality verification ----
  # global structure

}
