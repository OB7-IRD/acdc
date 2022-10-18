#' @name rwp_table_2_1
#' @title Regional Work Plan table 2.1
#' @description Process for generation the table 2.1, list of required species/stocks, for a Regional Work Plan (RWP).
#' @param reference_period_start {\link[base]{integer}} expected. Start of reference period. Be careful, the process needs 3 years at least to run.
#' @param reference_period_end {\link[base]{integer}} expected. End of reference period. Be careful, the process needs 3 years at least to run.
#' @param country {\link[base]{character}} expected. Country of interest. Use 2-alpha country.
#' @param eurostat {\link[base]{logical}} expected. Landing statistics downloaded from EUROSTAT (https://ec.europa.eu/eurostat).
#' @param rcg_stats {\link[base]{logical}} expected. Landing statistics from the regional database.
#' @param national_stats {\link[base]{logical}} expected. National landing statistics.
#' @param fides {\link[base]{character}} expected. {\link[base]{character}} expected. Source of FIDES data. You can choose between "unique" for national extraction from FIDES data (one file per country and years), "common" for global FIDES data for all EU countries or "public" to use a public source (https://griffincarpenter.org/reports/european-fishing-quotas-2001-2021/).
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
  geo <- NULL
  Geopolitical_entity <- NULL
  fishreg <- NULL
  Level.Description <- NULL
  Scientific_name <- NULL
  area <- NULL
  species_code <- NULL
  area_code <- NULL
  definition_year <- NULL
  level_code <- NULL
  initial_quantity <- NULL
  total_quota <- NULL
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
                              type = "character",
                              length = as.integer(x = 1),
                              allowed_values = c("unique",
                                                 "common",
                                                 "public"),
                              output = "logical") != TRUE) {
    return(codama::r_type_checking(r_object = fides,
                                   type = "character",
                                   length = as.integer(x = 1),
                                   allowed_values = c("unique",
                                                      "common",
                                                      "public"),
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
  if (length(x = reference_period) < 3) {
    stop(format(x = Sys.time(),
                format = "%Y-%m-%d %H:%M:%S"),
         " - Error, the reference period contains less than 3 years.\n",
         sep = "")
  }
  # data imports ----
  # eurostat
  if (eurostat == TRUE) {
    eurostat_data <- global_load_eurostat_data(path = file.path(path_input,
                                                                "eurostat"))
    geo_data <- read.table(file = system.file("referentials",
                                              "geo.def",
                                              package = "acdc"),
                           header = TRUE,
                           sep = ";") %>%
      dplyr::filter(geo != "EU28") %>%
      dplyr::rename(country = Geopolitical_entity)
  }
  # rcg stats
  if (rcg_stats == TRUE) {
    stop(format(x = Sys.time(),
                format = "%Y-%m-%d %H:%M:%S"),
         " - Error, process associated to \"rcg_stats\" argument not developed yet.\n")
  }
  # national stats
  if (national_stats == TRUE) {
    stop(format(x = Sys.time(),
                format = "%Y-%m-%d %H:%M:%S"),
         " - Error, process associated to \"national_stats\" argument not developed yet.\n")
  }
  # fides
  fides_data <- global_load_fides_data(fides_format = fides,
                                       reference_period = reference_period,
                                       path = file.path(path_input,
                                                        "fides"),
                                       country = country)
  # asfis
  asfis <- read.table(system.file("asfis_sp_feb_2018.txt",
                                  package = "acdc"),
                      header = TRUE,
                      sep = "\t",
                      as.is = TRUE)
  # table 2.1 linkage
  table_2_1_linkage <- read.csv(file = system.file("eumap_table_2_1_linkage_version_2022.csv",
                                                   package = "acdc"),
                                sep = ';',
                                header = TRUE,
                                as.is = TRUE,
                                encoding = 'UTF-8')
  # table 2.1 design ----
  table_2_1_information_final <- data.frame()
  table_control_final <- data.frame()
  for (table_2_1_linkage_id in seq_len(length.out = nrow(x = table_2_1_linkage))) {
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Be patient, work in progress on row id ",
        table_2_1_linkage_id,
        ".\n",
        sep = "")
    # from eurostat data ----
    if (eurostat == TRUE) {
      reference_period_eurostat <- reference_period[which(x = reference_period %in% names(x = eurostat_data))]
      if (length(x = reference_period_eurostat) != length(x = reference_period)) {
        cat(format(x = Sys.time(),
                   format = "%Y-%m-%d %H:%M:%S"),
            " - Warning: years of the \"reference_period\" argument are not all available in the EUROSTAT data imported.\n",
            "Year(s) available in EUROSTAT data are: \n",
            paste0(reference_period_eurostat,
                   collapse = ", "),
            ".\n",
            sep = "")
      }
      eurostat_data_final <- dplyr::select(.data = eurostat_data,
                                           species,
                                           fishreg,
                                           geo,
                                           as.character(x = !!reference_period_eurostat)) %>%
        dplyr::left_join(geo_data,
                         by = "geo") %>%
        dplyr::filter(! is.na(x = country)) %>%
        dplyr::left_join(asfis[,c(3:6)],
                         by = c("species" = "X3A_CODE"))
      country_name <- dplyr::filter(.data = geo_data,
                                    Level.Description %in% !!country)$country
      species <- unlist(x = strsplit(x = as.character(x = table_2_1_linkage$latin_name[table_2_1_linkage_id]),
                                     split = ','))
      region <- unlist(x = strsplit(x = as.character(x = table_2_1_linkage$area_bis[table_2_1_linkage_id]),
                                    split=','))
      current_eurostat_data <- dplyr::filter(.data = eurostat_data_final,
                                             Scientific_name %in% !!species
                                             & fishreg %in% !!region) %>%
        dplyr::filter(Level.Description != "GBR")
      if (nrow(x = current_eurostat_data) == 0) {
        current_eurostat_data <- dplyr::tibble(x3a_code = rep(x = table_2_1_linkage[table_2_1_linkage_id, "x3a_code"],
                                                              2),
                                               geo = c(!!country,
                                                       "EU27_2020"),
                                               area = rep(x = table_2_1_linkage[table_2_1_linkage_id, "area"],
                                                          2),
                                               country = c(country,
                                                           "European union (27 MS)"),
                                               source = TRUE,
                                               level_description = c(dplyr::filter(.data = geo_data,
                                                                                   Level.Description %in% !!country)$Level.Description,
                                                                     "EU27_2020"),
                                               scientific_name = rep(x = table_2_1_linkage$latin_name[table_2_1_linkage_id],
                                                                     2),
                                               english_name = rep(x = NA,
                                                                  2),
                                               french_name = rep(x = NA,
                                                                 2))
        for (current_year in reference_period_eurostat) {
          current_expression <- parse(text = paste0("current_eurostat_data[1:2, \"",
                                                    current_year,
                                                    "\"] <- as.numeric(x = rep(x = NA, 2))"))
          eval(expr = current_expression)
        }
        years_columns <- names(x = current_eurostat_data)[(length(x = current_eurostat_data) - length(x = reference_period_eurostat) + 1):length(x = current_eurostat_data)]
        current_eurostat_data <- dplyr::relocate(.data = current_eurostat_data,
                                                 !!years_columns,
                                                 .after = area)
      } else {
        current_eurostat_data$source <- TRUE
      }
      current_eurostat_data$mean_years <- round(x = apply(X = current_eurostat_data[,4:6],
                                                          MARGIN = 1,
                                                          FUN = mean,
                                                          na.rm = TRUE),
                                                digits = 4)
      current_eurostat_data_country <- dplyr::filter(.data = current_eurostat_data,
                                                     geo %in% !!country)
      n_current_eurostat_data_country <- nrow(x = current_eurostat_data_country)
      if (n_current_eurostat_data_country == 0) {
        current_eurostat_data_country <- dplyr::tibble(x3a_code = table_2_1_linkage[table_2_1_linkage_id, "x3a_code"],
                                                       geo = !!country,
                                                       area = table_2_1_linkage[table_2_1_linkage_id, "area"],
                                                       mean_years = NA,
                                                       country = !!country_name,
                                                       source = TRUE,
                                                       level_description = !!country,
                                                       scientific_name = table_2_1_linkage$latin_name[table_2_1_linkage_id],
                                                       english_name = NA,
                                                       french_name = NA)
        for (current_year in reference_period_eurostat) {
          current_expression <- parse(text = paste0("current_eurostat_data_country[1, \"",
                                                    current_year,
                                                    "\"] <- as.numeric(x = NA)"))
          eval(expr = current_expression)
        }
        years_columns <- names(x = current_eurostat_data_country)[(length(x = current_eurostat_data_country) - length(x = reference_period_eurostat) + 1):length(x = current_eurostat_data_country)]
        current_eurostat_data_country <- dplyr::relocate(.data = current_eurostat_data_country,
                                                         !!years_columns,
                                                         .after = area)
      }
      current_eurostat_data_eu <- dplyr::filter(.data = current_eurostat_data,
                                                geo == "EU27_2020")
      if (current_eurostat_data_eu[1, "source"] == TRUE) {
        source_eu <- "eurostat"
      } else {
        source_eu <- "-"
      }
      if (current_eurostat_data_country[1, "source"] == TRUE) {
        source_national <- "eurostat"
      } else {
        source_national <- ""
      }
      # construction table 2.1 line of information
      table_2_1_information <- dplyr::tibble(ms = !!country,
                                             reference_years = paste0(reference_period_eurostat,
                                                                      collapse = ", "),
                                             region = table_2_1_linkage[table_2_1_linkage_id, "region"],
                                             rfmo = table_2_1_linkage[table_2_1_linkage_id, "rfmo"],
                                             spp = table_2_1_linkage[table_2_1_linkage_id, "latin_name"],
                                             area = table_2_1_linkage[table_2_1_linkage_id, "area"],
                                             landings = NA,
                                             source_national = !!source_national,
                                             tac = NA,
                                             share_landing = NA,
                                             source_eu = !!source_eu,
                                             thresh = "None",
                                             reg_coord = NA,
                                             covered_length = NA,
                                             selected_bio = NA,
                                             comments_eurostat = NA,
                                             national_stats = NA)
      if (sum(is.na(x = current_eurostat_data_country$mean_years)) < nrow(x = current_eurostat_data_country)) {
        table_2_1_information$landings <- sum(current_eurostat_data_country$mean_years,
                                              na.rm = TRUE)
        table_2_1_information$share_landing <- table_2_1_information$landings / sum(current_eurostat_data_eu$mean_years,
                                                                                    na.rm = TRUE)
      } else {
        table_2_1_information$landings <- 0
        table_2_1_information$share_landing <- 0
      }
      # construction of the control table
      table_control <- dplyr::tibble(region = table_2_1_linkage[table_2_1_linkage_id,
                                                                "region"],
                                     rfmo = table_2_1_linkage[table_2_1_linkage_id,
                                                              "rfmo"],
                                     spp = table_2_1_linkage[table_2_1_linkage_id,
                                                             "latin_name"],
                                     geo = rep(!!country,
                                               each = length(x = reference_period_eurostat)),
                                     area = table_2_1_linkage[table_2_1_linkage_id,
                                                              "area"],
                                     year = reference_period_eurostat,
                                     eurostat_country = apply(current_eurostat_data_country[, as.character(x = reference_period_eurostat)],
                                                              2,
                                                              sum,
                                                              na.rm = TRUE),
                                     eurostat_eu = apply(current_eurostat_data_eu[, as.character(x = reference_period_eurostat)],
                                                         2,
                                                         sum,
                                                         na.rm = TRUE))
    }
    # from rcg data ----
    if (rcg_stats == TRUE) {
      stop(format(x = Sys.time(),
                  format = "%Y-%m-%d %H:%M:%S"),
           " - Error, process associated to \"rcg_stats\" argument not developed yet.\n")
    }
    # from national stats data ----
    if (national_stats == TRUE) {
      stop(format(x = Sys.time(),
                  format = "%Y-%m-%d %H:%M:%S"),
           " - Error, process associated to \"national_stats\" argument not developed yet.\n")
    }
    # from fides data ----
    if (fides == "unique") {
      stop(format(x = Sys.time(),
                  format = "%Y-%m-%d %H:%M:%S"),
           " - Error, process associated to value \"unique\" for the argument \"fides\" is  not developed yet.\n")
      # fides_area <- unlist(x = strsplit(x = as.character(x = table_2_1_linkage[table_2_1_linkage_id,
      #                                                                          "fides_area"]),
      #                                   split = ","))
      # fides_specie <- unlist(x = strsplit(x = as.character(x = table_2_1_linkage[table_2_1_linkage_id,
      #                                                                            "x3a_code"]),
      #                                     split = ","))
      # reference_period_fides <- reference_period[which(x = reference_period %in% unique(x = fides_data$definition_year))]
      # if (length(x = reference_period_fides) != length(x = reference_period)) {
      #   cat(format(x = Sys.time(),
      #              format = "%Y-%m-%d %H:%M:%S"),
      #       " - Warning: years of the \"reference_period\" argument are not all available in the FIDES data imported.\n",
      #       "Year(s) available in FIDES data are: \n",
      #       paste0(reference_period_fides,
      #              collapse = ", "),
      #       ".\n",
      #       sep = "")
      # }
      # current_fides_data <- dplyr::filter(.data = fides_data,
      #                                     species_code %in% !!fides_specie
      #                                     & area_code %in% !!fides_area
      #                                     & definition_year %in% !!reference_period_fides) %>%
      #   dplyr::mutate(level_code = dplyr::case_when(
      #     level_code == "XEU" ~ "EEC",
      #     TRUE ~ level_code
      #   )) %>%
      #   dplyr::left_join(geo_data,
      #                    by = c("level_code" = "Level.Description"))
      # if (all(! fides_area %in% c("No TAC",
      #                             ""))
      #     & nrow(x = current_fides_data) > 0) {
      #   if (length(x = fides_area) != 1) {
      #     current_fides_data <- current_fides_data %>%
      #       dplyr::group_by(level_code,
      #                       definition_year) %>%
      #       dplyr::summarise(adapted_quota = sum(adapted_quota,
      #                                            na.rm = TRUE),
      #                        .groups = "drop")
      #   }
      #   current_fides_data_country <- current_fides_data %>%
      #     dplyr::group_by(level_code) %>%
      #     dplyr::summarise(adapted_quota = mean(x = adapted_quota,
      #                                           na.rm = TRUE),
      #                      .groups = "drop")
      #   fides_country <- dplyr::filter(.data = current_fides_data_country,
      #                                  level_code == !!country)$adapted_quota
      #   fides_eu <- dplyr::filter(.data = current_fides_data_country,
      #                             level_code == "EEC")$adapted_quota
      #   source_fides <- c("FIDES",
      #                     "Public TAC DB")
      #   names(x = source_fides) <- c("TRUE",
      #                                "FALSE")
      #   if (length(x = fides_country) == 0) {
      #     table_2_1_information$tac <- NA
      #   } else {
      #     table_2_1_information$tac <- fides_country / fides_eu
      #   }
      #   table_2_1_information$source_eu <- source_fides[[fides]]
      #   table_2_1_information$comments_fides <- NA
      #   total_quota <- tapply(current_fides_data_country$adapted_quota,
      #                         current_fides_data_country$level_code,
      #                         sum,
      #                         na.rm = TRUE) / current_fides_data_country$adapted_quota[current_fides_data_country$level_code == "EEC"]
      #   # keep only EU countries to calculate the 25% rule
      #   total_quota <- total_quota[names(x = total_quota) %in% geo_data$Level.Description]
      #   if ((!is.na(x = table_2_1_information$tac)
      #        & table_2_1_information$tac < 0.1
      #        & table_2_1_information$tac > 0)) {
      #     table_2_1_information$comments_fides <- sum(total_quota[which(total_quota < 0.1)])
      #   }
      #   fides_quota<- tapply(current_fides_data$adapted_quota,
      #                        list(current_fides_data$definition_year,
      #                             current_fides_data$level_code),
      #                        mean,
      #                        na.rm = TRUE)
      #   table_control_fides <- data.frame(region = table_2_1_linkage[table_2_1_linkage_id,
      #                                                                "region"],
      #                                     rfmo = table_2_1_linkage[table_2_1_linkage_id,
      #                                                              "rfmo"],
      #                                     spp = table_2_1_linkage[table_2_1_linkage_id,
      #                                                             "latin_name"],
      #                                     geo = rep(country,
      #                                               each = length(x = reference_period_fides)),
      #                                     area = table_2_1_linkage[table_2_1_linkage_id,
      #                                                              "area"],
      #                                     year = sort(x = unique(current_fides_data$definition_year)),
      #                                     fides_country = fides_quota[, country],
      #                                     fides_eu = fides_quota[, "EEC"],
      #                                     fides_share = fides_quota[, country] / fides_quota[, "EEC"])
      #   table_control <- dplyr::inner_join(x = table_control,
      #                                      y = table_control_fides,
      #                                      by = c("region",
      #                                             "rfmo",
      #                                             "spp",
      #                                             "geo",
      #                                             "area",
      #                                             "year"))
      # } else {
      #   table_control$fides_country <- NA
      #   table_control$fides_eu <- NA
      #   table_control$fides_share <- NA
      # }
    } else if (fides == "common") {
      if (! table_2_1_linkage[table_2_1_linkage_id,
                              "fides_area"] %in% "No TAC") {
        fides_area <- unlist(x = strsplit(x = as.character(x = table_2_1_linkage[table_2_1_linkage_id,
                                                                                 "fides_area"]),
                                          split = ","))
        fides_specie <- unlist(x = strsplit(x = as.character(x = table_2_1_linkage[table_2_1_linkage_id,
                                                                                   "x3a_code"]),
                                            split = ","))
        reference_period_fides <- reference_period[which(x = reference_period %in% unique(x = fides_data$definition_year))]
        if (length(x = reference_period_fides) != length(x = reference_period)) {
          cat(format(x = Sys.time(),
                     format = "%Y-%m-%d %H:%M:%S"),
              " - Warning: years of the \"reference_period\" argument are not all available in the FIDES data imported.\n",
              "Year(s) available in FIDES data are: \n",
              paste0(reference_period_fides,
                     collapse = ", "),
              ".\n",
              sep = "")
        }
        current_fides_data <- dplyr::filter(.data = fides_data,
                                            species_code %in% !!fides_specie
                                            & area_code %in% !!fides_area
                                            & definition_year %in% !!reference_period_fides)
        if (all(! fides_area %in% c("No TAC",
                                    ""))
            && nrow(x = current_fides_data) > 0) {
          if (length(x = fides_area) != 1) {
            current_fides_data <- current_fides_data %>%
              dplyr::group_by(level_code,
                              definition_year) %>%
              dplyr::summarise(initial_quantity = sum(initial_quantity),
                               .groups = "drop")
          }
          current_fides_data_country <- current_fides_data %>%
            dplyr::group_by(level_code) %>%
            dplyr::summarise(initial_quantity = sum(initial_quantity),
                             .groups = "drop")
          fides_country <- dplyr::filter(.data = current_fides_data_country,
                                         level_code == !!country)$initial_quantity
          fides_eu <- dplyr::filter(.data = current_fides_data_country,
                                    level_code == "XEU")$initial_quantity
          if (length(x = fides_country) == 1) {
            table_2_1_information$tac <- fides_country / fides_eu
          } else {
            table_2_1_information$tac <- NA
          }
          current_fides_data_country <- dplyr::mutate(.data = current_fides_data_country,
                                                      total_quota = initial_quantity / !!fides_eu)
          if (! is.na(x = table_2_1_information$tac)
              && (table_2_1_information$tac < 0.1
                  & table_2_1_information$tac > 0)) {
            table_2_1_information$comment_fides <- sum(dplyr::filter(.data = current_fides_data_country,
                                                                     total_quota < 0.1)$total_quota)

          } else {
            table_2_1_information$comment_fides <- NA
          }
          if (! is.na(x = fides_country)
              && length(x = fides_country) != 0) {
            table_2_1_information$comment_fides_bis <- paste("FIDES initial quantity mean = ",
                                                             round(x = fides_country / length(x = reference_period_fides),
                                                                   digits = 0))
          } else {
            table_2_1_information$comment_fides_bis <- NA
          }
          if (! is.na(x = table_2_1_information$comment_fides)) {
            table_2_1_information$comment_fides_25_rule <- paste0("Sum of MS TAC's below 10% = ",
                                                                  round(x = as.numeric(x = table_2_1_information$comment_fides) * 100,
                                                                        digits = 0),
                                                                  "%")
          } else {
            table_2_1_information$comment_fides_25_rule <- NA
          }
        } else {
          table_2_1_information$comment_fides <- NA
          table_2_1_information$comment_fides_bis <- NA
          table_2_1_information$comment_fides_25_rule <- NA
        }
      } else {
        table_2_1_information$tac <- -999
        table_2_1_information$comment_fides <- NA
        table_2_1_information$comment_fides_bis <- NA
        table_2_1_information$comment_fides_25_rule <- NA
      }
    } else if (fides == "public") {
      stop(format(x = Sys.time(),
                  format = "%Y-%m-%d %H:%M:%S"),
           " - Error, process associated to fides argument \"public\" not developed yet.\n")
    }
    # from national stats ----
    if (national_stats == TRUE) {
      stop(format(x = Sys.time(),
                  format = "%Y-%m-%d %H:%M:%S"),
           " - Error, process associated to \"national_stats\" argument not developed yet.\n")
    }
    table_2_1_information_final <- rbind(table_2_1_information_final,
                                         table_2_1_information)
    table_control_final <- rbind(table_control_final,
                                 table_control)
    cat(format(x = Sys.time(),
               format = "%Y-%m-%d %H:%M:%S"),
        " - Work done on row id ",
        table_2_1_linkage_id,
        ".\n",
        sep = "")
  }
  # formatting ----

  # export ----
  rwp_table_2_1_export = list("table_2_1_template" = table_2_1_information_final,
                              "table_2_1_template_control" = table_control_final)
  names_export <- names(rwp_table_2_1_export)
  for (export_id in names_export) {
    names(rwp_table_2_1_export)[export_id] <- paste(names(rwp_table_2_1_export)[1],
                                            tolower(x = country),
                                            sep = "_")
  }
  return(rwp_table_2_1_export)
}

