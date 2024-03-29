% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/fdi_tablea_catch_summary.R
\name{fdi_tablea_catch_summary}
\alias{fdi_tablea_catch_summary}
\title{Table A catch generation (FDI process)}
\usage{
fdi_tablea_catch_summary(
  balbaya_con,
  observe_bycatch_path,
  period,
  gear,
  flag,
  fao_area_file_path,
  eez_area_file_path,
  cwp_grid_file_path,
  template_checking = FALSE,
  template_year = NULL,
  table_export_path = NULL
)
}
\arguments{
\item{balbaya_con}{{\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the balbaya database.}

\item{observe_bycatch_path}{{\link[base]{character}} expected. Directory path of the bycatch data extractions. Check this input with Philippe Sabarros (philippe.sabarros@ird.fr).}

\item{period}{{\link[base]{integer}} expected. Year period for data extractions.}

\item{gear}{{\link[base]{integer}}. Gear(s) selection for data extractions.}

\item{flag}{{\link[base]{integer}} expected. Flag(s) selection for data extractions.}

\item{fao_area_file_path}{{\link[base]{character}} expected. File path of the FAO area grid. The file format has to be .Rdata or .RData extension.}

\item{eez_area_file_path}{{\link[base]{character}} expected. File path of the EEZ area grid. The file format has to be .Rdata or .RData extension.}

\item{cwp_grid_file_path}{{\link[base]{character}} expected. File path of the CWP area grid. The file format has to be .Rdata or .RData extension.}

\item{template_checking}{{\link[base]{logical}} expected. By default FALSE Checking FDI table generated regarding the official FDI template.}

\item{template_year}{{\link[base]{integer}} expected. By default NULL. Template year.}

\item{table_export_path}{{\link[base]{character}} expected. By default NULL. Directory path associated for the export.}
}
\value{
The process returns a double list with the FDI table A in the first one and in the second one two accessory outputs useful for the other FDI table generation processes ("landing_rectangle" and "bycatch_retained").
}
\description{
Process for generation and optionally extraction of the FDI table A (catch).
}
