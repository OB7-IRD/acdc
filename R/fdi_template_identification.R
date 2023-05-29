#' @name fdi_template_identification
#' @title Template identification (FDI process)
#' @description Process for template identification of FDI table.
#' @param table_id {\link[base]{character}} expected. Identification of the FDI table.
#' @importFrom codama r_type_checking
#' @export
fdi_template_identification <- function(table_id) {
  # arguments verifications ----
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
  if (table_id == "a") {
    template_name <- "TABLE_A_CATCH"
  } else if (table_id == "b") {
    template_name <- "TABLE_B_REFUSAL_RATE"
  } else if (table_id == "c") {
    template_name <- "TABLE_C_NAO_OFR_DISCARDS_AGE"
  } else if (table_id == "d") {
    template_name <- "TABLE_D_NAO_OFR_DISCARDS_LENGTH"
  } else if (table_id == "e") {
    template_name <- "TABLE_E_NAO_OFR_LANDINGS_AGE"
  } else if (table_id == "f") {
    template_name <- "TABLE_F_NAO_OFR_LANDINGS_LENGTH"
  } else if (table_id == "g") {
    template_name <- "TABLE_G_EFFORT"
  } else if (table_id == "h") {
    template_name <- "TABLE_H_LANDINGS_BY_RECTANGLE"
  } else if (table_id == "i") {
    template_name <- "TABLE_I_EFFORT_BY_RECTANGLE"
  } else if (table_id == "j") {
    template_name <- "TABLE_J_CAPACITY"
  } else if (table_id == "k") {
    template_name <- "TABLE_K_NAO_OFR_DISCARDS"
  }
  return(template_name)
}
