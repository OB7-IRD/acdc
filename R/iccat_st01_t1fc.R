#' @name iccat_st01_t1fc
#' @title ICCAT ST01 - Task 1 Fleet characteristics form design
#' @description Processes (data extraction, quality checks and design) for the ICCAT submission form ST01-T1FC.
#' @param data_input_type Object of class \code{\link[base]{character}} expected. Type of input for data extraction. You can choose between "csv_form" or "database".
#' @param form_path Object of class \code{\link[base]{character}} expected. By default NULL. Form file path location. File extension expected is .csv with field separator character as ";" and decimal point  as ".".
#' @export
#' @importFrom codama r_type_checking file_path_checking vector_comparison
#' @importFrom dplyr mutate
#' @importFrom utils read.table
iccat_st01_t1fc <- function(data_input_type,
                            form_path = NULL) {
  # global variables assignement ----
  metier <- NULL
  supra_region <- NULL
  ICCATSerialNo <- NULL
  NatRegNo <- NULL
  IRCS <- NULL
  VesselName <- NULL
  FlagVesCd <- NULL
  PortZone <- NULL
  GearGrpCd <- NULL
  LOAm <- NULL
  Tnage <- NULL
  TonType <- NULL
  YearC <- NULL
  FishDatl <- NULL
  # arguments verification ----
  if (codama::r_type_checking(r_object = data_input_type,
                              type = "character",
                              length = 1L,
                              allowed_values = c("csv_form",
                                                 "database"),
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = data_input_type,
                                   type = "character",
                                   length = 1L,
                                   allowed_values = c("csv_form",
                                                      "database"),
                                   output = "message"))
  }
  if (codama::file_path_checking(file_path = form_path,
                                 extension = c("csv"),
                                 output = "logical") != TRUE) {
    return(codama::file_path_checking(file_path = form_path,
                                      extension = c("csv"),
                                      output = "message"))
  }
  # data extraction ----
  if (data_input_type == "csv_form") {
    iccat_st01_t1fc <- utils::read.table(file = form_path,
                                         header = TRUE,
                                         sep = ";",
                                         dec = ".")
  } else if (data_input_type == "database") {
    stop(format(x = Sys.time(),
                "%Y-%m-%d %H:%M:%S"),
         " - Error, function not developed yet, patient dude/miss!.\n")
  } else {
    stop(format(x = Sys.time(),
                "%Y-%m-%d %H:%M:%S"),
         " - Error, invalid \"data_input_type\" argument.\n")
  }
  # quality verification ----
  # global structure
  iccat_st01_t1fc_variables <- read.csv2(file = system.file("referentials",
                                                            "iccat_st01_t1fc_variables.csv",
                                                            package = "acdc"))
  if (codama::vector_comparison(first_vector = iccat_st01_t1fc_variables$iccat_st01_t1fc_argument,
                                second_vector = names(iccat_st01_t1fc),
                                comparison_type = "equality",
                                output = "logical") != TRUE) {
    cat(paste0(format(x = Sys.time(),
                      "%Y-%m-%d %H:%M:%S"),
               " - Failure,",
               " problem in the global structure regarding variables names.\n"))
    return(codama::vector_comparison(first_vector = iccat_st01_t1fc_variables$iccat_st01_t1fc_argument,
                                     second_vector = names(iccat_st01_t1fc),
                                     comparison_type = "equality",
                                     output = "report"))
  }
  # typing
  iccat_st01_t1fc <- dplyr::mutate(.data = iccat_st01_t1fc,
                                   ICCATSerialNo = as.character(x = ICCATSerialNo),
                                   NatRegNo = as.character(x = NatRegNo),
                                   IRCS = as.character(x = IRCS),
                                   VesselName = as.character(x = VesselName),
                                   FlagVesCd = as.character(x = FlagVesCd),
                                   PortZone = as.character(x = PortZone),
                                   GearGrpCd = as.character(x = GearGrpCd),
                                   LOAm = as.numeric(x = LOAm),
                                   Tnage = as.integer(x = Tnage),
                                   TonType = as.character(x = TonType),
                                   YearC = as.integer(x = YearC),
                                   FishDatl = as.integer(x = FishDatl))
  # specific verifications
  # FlagVesCd
  iccat_reporting_flags_and_vessel_countries_flags <- read.csv2(file = system.file("referentials",
                                                                                   "iccat_reporting_flags_and_vessel_countries_flags.csv",
                                                                                   package = "acdc"))

  # ending ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Process \"iccat_st01_t1fc\" ran successfully.\n",
      sep = "")
}
