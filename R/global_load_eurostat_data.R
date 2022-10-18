#' @name global_load_eurostat_data
#' @title Load EUROSTAT landings files
#' @description Process for Load EUROSTAT landings files (http://ec.europa.eu/eurostat/web/fisheries/data/database).
#' @param path {\link[base]{character}} expected. Input path directory where tsv EUROSTAT landings files are located.
#' @return The function return a {\link[base]{data.frame}}.
#' @export
#' @importFrom codama r_type_checking
#' @importFrom stringr str_detect str_extract
#' @importFrom tidyr separate
global_load_eurostat_data <- function(path) {
  cat(format(x = Sys.time(),
             format = "%Y-%m-%d %H:%M:%S"),
      " - Start process for load data from eurostat file(s).\n",
      sep = "")
  # arguments verifications ----
  if (codama::r_type_checking(r_object = path,
                              type = "character",
                              length = as.integer(x = 1),
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = path,
                                   type = "character",
                                   length = as.integer(x = 1),
                                   output = "message"))
  }
  # process ----
  eurostat_files <- list.files(path = path)
  eurostat_files <- eurostat_files[stringr::str_detect(string = eurostat_files,
                                                       pattern = "^fish_ca.*.tsv$")]
  if (length(x = eurostat_files) != 0) {
    eurostat_files_final <- lapply(X = seq_len(length.out = length(x = eurostat_files)),
                                   FUN = function(eurostat_file_id) {
                                     eurostat_file_ori <- read.table(file.path(path,
                                                                               eurostat_files[eurostat_file_id]),
                                                                     header = TRUE,
                                                                     sep = '\t',
                                                                     as.is = TRUE)
                                     first_columns <- unlist(x = strsplit(x = names(eurostat_file_ori)[1],
                                                                          split = "[.]"))
                                     first_columns <- first_columns[-length(x = first_columns)]
                                     eurostat_file_final <- tidyr::separate(data = eurostat_file_ori,
                                                                            col = 1,
                                                                            into = first_columns,
                                                                            sep = ",")
                                     names(eurostat_file_final) <- c(first_columns,
                                                                     stringr::str_extract(string = names(x = eurostat_file_ori)[-1],
                                                                                          pattern = "[:digit:]{4}"))
                                     for (column_name in stringr::str_extract(string = names(x = eurostat_file_ori)[-1],
                                                                              pattern = "[:digit:]{4}")) {
                                       eurostat_file_final[, column_name] <- as.numeric(x = stringr::str_replace_all(eurostat_file_final[, column_name],
                                                                                                                     c("^[:blank:]*[[:digit:]+[:punct:][:digit:]+][:blank:]*[:alpha:]*$" = NA_character_,
                                                                                                                       "(^[:digit:]+[:punct:]*[:digit:]*)[:blank:]*[:alpha:]+$" = "\\1",
                                                                                                                       "[:blank:]+$" = "")))
                                     }
                                     eurostat_file_final
                                   })
    eurostat_files_final <- do.call(what = rbind,
                                    args = eurostat_files_final)
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Successful process for load data from eurostat file(s).\n",
        sep = "")
    return(eurostat_files_final)
  } else {
    stop(format(x = Sys.time(),
                format = "%Y-%m-%d %H:%M:%S"),
         " - Error, no eurostat file found for the path selected.\n",
         sep = "")
  }
}
