#' @name rwp_table_2_1
#' @title Regional Work Plan table 2.1
#' @description Process for generation the table 2.1, list of required species/stocks, for a Regional Work Plan (RWP).
#' @param reference_period_start {\link[base]{integer}} expected. Start of reference period. Be careful, the process needs 3 years at least to run.
#' @param reference_period_end {\link[base]{integer}} expected. End of reference period. Be careful, the process needs 3 years at least to run.
#' @param country {\link[base]{character}} expected. Country of interest. Use 2-alpha country.
#' @param eurostat {\link[base]{logical}} expected. Landing statistics downloaded from EUROSTAT (https://ec.europa.eu/eurostat).
#' @param rcg_stats {\link[base]{logical}} expected. Landing statistics from the regional database.
#' @param national_stats {\link[base]{logical}} expected. National landing statistics.
#' @param fides {\link[base]{logical}} expected. Use national extraction from FIDES including a country and EEC.
#' @param fides_common {\link[base]{logical}} expected. FIDEScommon is a Regional Coordination Group (RCG) extraction from FIDES and including all EU countries
#' @param rfmo {\link[base]{character}} expected. RFMO's list to include in output.
#' @param output_suffix {\link[base]{character}} expected. Add an suffic to the output name.
#' @param path_input {\link[base]{character}} expected. Input path for input files.
#' @param path_in_confidential_data {\link[base]{character}} expected. Input path for confidential files.
#' @param path_output {\link[base]{character}} expected. Output path for output files.
#' @return I don't know yet dude!
#' @export
rwp_table_2_1 <- function(reference_period_start,
                          reference_period_end,
                          country,
                          eurostat,
                          rcg_stats,
                          national_stats,
                          fides,
                          fides_common,
                          rfmo,
                          output_suffix = NULL,
                          path_input,
                          path_in_confidential_data,
                          path_output) {
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on RWP table 2.1 generation.\n",
      sep = "")
  # global variables assignement ----

  # arguments verifications ----
  if (codama::r_type_checking(r_object = reference_period_start,
                              type = "integer",
                              length = as.integer(x = 1),
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = reference_period_start,
                                   type = "integer",
                                   length = as.integer(x = 1),
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = reference_period_end,
                              type = "integer",
                              length = as.integer(x = 1),
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = reference_period_end,
                                   type = "integer",
                                   length = as.integer(x = 1),
                                   output = "message"))
  }
  if (reference_period_end <= reference_period_start
      || (reference_period_end - reference_period_start + 1 < 3)) {
    stop(format(x = Sys.time(),
                "%Y-%m-%d %H:%M:%S"),
         " - Error, invalid \"reference_period\" arguments.\n",
         "\"reference_period_end\" must be less than or equal to \"reference_period_start\" and we must have at least 3 years between the two.\n")
  }
  if (codama::r_type_checking(r_object = country,
                              type = "character",
                              length = as.integer(x = 1),
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = country,
                                   type = "character",
                                   length = as.integer(x = 1),
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = eurostat,
                              type = "logical",
                              length = as.integer(x = 1),
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = eurostat,
                                   type = "logical",
                                   length = as.integer(x = 1),
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = rcg_stats,
                              type = "logical",
                              length = as.integer(x = 1),
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = rcg_stats,
                                   type = "logical",
                                   length = as.integer(x = 1),
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = national_stats,
                              type = "logical",
                              length = as.integer(x = 1),
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = national_stats,
                                   type = "logical",
                                   length = as.integer(x = 1),
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = fides,
                              type = "logical",
                              length = as.integer(x = 1),
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = fides,
                                   type = "logical",
                                   length = as.integer(x = 1),
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = fides_common,
                              type = "logical",
                              length = as.integer(x = 1),
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = fides_common,
                                   type = "logical",
                                   length = as.integer(x = 1),
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = rfmo,
                              type = "character",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = rfmo,
                                   type = "character",
                                   output = "message"))
  }
  if (! is.null(x = output_suffix)
      && codama::r_type_checking(r_object = output_suffix,
                                 type = "character",
                                 length = as.integer(x = 1),
                                 output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = output_suffix,
                                   type = "character",
                                   length = as.integer(x = 1),
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = path_input,
                              type = "character",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = path_input,
                                   type = "character",
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = path_in_confidential_data,
                              type = "character",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = path_in_confidential_data,
                                   type = "character",
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = path_output,
                              type = "character",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = path_output,
                                   type = "character",
                                   output = "message"))
  }
  # setup ----
  reference_period <- c(reference_period_start:reference_period_end)
  # data imports ----
  if (eurostat == TRUE) {
    eurostat <- global_load_eurostat_data(path = file.path(path_input,
                                                           "eurostat"))
  }


}
