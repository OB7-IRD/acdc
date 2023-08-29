#' @name rdbes_vd
#' @title Table Vessel Details (VD) generation (RDBES process)
#' @description Process for generation and optionally extraction of the RDBES table VD (Vessel Details).
#' @param observe_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the observe database.
#' @param fleet {\link[base]{integer}} expected. Fleet(s) selected associated to the databases queries extractions.
#' @param export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @return A R object with the RDBES table VD with potentially a csv extraction associated.
#' @export
rdbes_vd <- function(observe_con,
                     fleet,
                     export_path = NULL) {
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process on RDBES table VD generation.\n",
      sep = "")
  # 1 - Global variables assignement ----

  # 2 - Arguments verifications ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start arguments verifications.\n",
      sep = "")
  if (codama::r_type_checking(r_object = observe_con,
                              type = "list",
                              length = 2L,
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = observe_con,
                                   type = "list",
                                   length = 2L,
                                   output = "message"))
  }
  if (codama::r_type_checking(r_object = fleet,
                              type = "integer",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = fleet,
                                   type = "integer",
                                   output = "message"))
  }
  if ((! is.null(x = export_path))
      && codama::r_type_checking(r_object = export_path,
                                 type = "character",
                                 length = 1L,
                                 output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = export_path,
                                   type = "character",
                                   length = 1L,
                                   output = "message"))
  }
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful arguments verifications.\n",
      sep = "")
  # 3 - Databases extractions ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start databases extractions.\n",
      sep = "")
  observe_ps_bb_cl_data_query <- paste(readLines(con = system.file("sql",
                                                                   "rdbes",
                                                                   "observe_ps_bb_cl_rdbes.sql",
                                                                   package = "acdc")),
                                       collapse = "\n")
  observe_ps_bb_cl_data_query <- DBI::sqlInterpolate(conn = observe_con[[2]],
                                                     sql = observe_ps_bb_cl_data_query,
                                                     year_time_period = DBI::SQL(paste0(year_time_period,
                                                                                        collapse = ", ")),
                                                     fleet = DBI::SQL(paste0("'",
                                                                             paste0(fleet,
                                                                                    collapse = "', '"),
                                                                             "'")))
  observe_ps_bb_cl_data <- DBI::dbGetQuery(conn = observe_con[[2]],
                                           statement = observe_ps_bb_cl_data_query)
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful databases extractions.\n",
      sep = "")
  # 4 - Other data extractions ---
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start other data extractions.\n",
      sep = "")

  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful other data extractions.\n",
      sep = "")
  # 5 - Data design ----
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start data design.\n",
      sep = "")





  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful data design.\n",
      sep = "")
  # 6 - Extraction ----
  if (! is.null(x = export_path)) {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Start VD data extractions.\n",
        sep = "")
    utils::write.csv2(x = vd_data,
                      file = file.path(export_path,
                                       paste(format(as.POSIXct(Sys.time()),
                                                    "%Y%m%d_%H%M%S"),
                                             "rdbes_cl.csv",
                                             sep = "_")),
                      row.names = FALSE)
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Successful VD data extractions.\n",
        "File available in the directory ",
        export_path,
        "\n",
        sep = "")
  }
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Successful process on RDBES table VD generation.\n",
      sep = "")
  return(vd_data)
}
