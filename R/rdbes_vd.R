#' @name rdbes_vd
#' @title Table Vessel Details (VD) generation (RDBES process)
#' @description Process for generation and optionally extraction of the RDBES table VD (Vessel Details).
#' @param observe_con {\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the observe database.
#' @param rdbes_cl
#' @param flag {\link[base]{integer}} expected. Flag(s) selected associated to the databases queries extractions.
#' @param export_path {\link[base]{character}} expected. By default NULL. Directory path associated for the export.
#' @return A R object with the RDBES table VD with potentially a csv extraction associated.
#' @export
rdbes_vd <- function(observe_con,
                     rdbes_cl,
                     flag,
                     export_path = NULL) {
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Start process on RDBES table VD generation.",
          sep = "")
  # 1 - Global variables assignement ----

  # 2 - Arguments verifications ----
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Start arguments verifications.",
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
  if (codama::r_type_checking(r_object = flag,
                              type = "integer",
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = flag,
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
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Successful arguments verifications.",
          sep = "")
  # 3 - Databases extractions ----
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Start databases extractions.",
          sep = "")
  observe_vd_data_query <- paste(readLines(con = system.file("sql",
                                                             "rdbes",
                                                             "observe_vd_rdbes.sql",
                                                             package = "acdc")),
                                 collapse = "\n")
  observe_vd_data_query <- DBI::sqlInterpolate(conn = observe_con[[2]],
                                               sql = observe_vd_data_query,
                                               flag = DBI::SQL(paste0("'",
                                                                      paste0(flag,
                                                                             collapse = "', '"),
                                                                      "'")))
  observe_vd_data_data <- DBI::dbGetQuery(conn = observe_con[[2]],
                                          statement = observe_vd_data_query)
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Successful databases extractions.",
          sep = "")
  # 4 - Other data extractions ---
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Start other data extractions.",
          sep = "")

  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Successful other data extractions.",
          sep = "")
  # 5 - Data design ----
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Start data design.",
          sep = "")





  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Successful data design.",
          sep = "")
  # 6 - Extraction ----
  if (! is.null(x = export_path)) {
    message(format(x = Sys.time(),
                   format = "%Y-%m-%d %H:%M:%S"),
            " - Start VD data extractions.",
            sep = "")
    utils::write.csv2(x = vd_data,
                      file = file.path(export_path,
                                       paste(format(as.POSIXct(Sys.time()),
                                                    "%Y%m%d_%H%M%S"),
                                             "rdbes_cl.csv",
                                             sep = "_")),
                      row.names = FALSE)
    message(format(x = Sys.time(),
                   format = "%Y-%m-%d %H:%M:%S"),
            " - Successful VD data extractions.\n",
            "File available in the directory ",
            export_path,
            sep = "")
  }
  message(format(x = Sys.time(),
                 format = "%Y-%m-%d %H:%M:%S"),
          " - Successful process on RDBES table VD generation.",
          sep = "")
  return(vd_data)
}
