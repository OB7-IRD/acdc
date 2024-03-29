% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rdbes_cl.R
\name{rdbes_cl}
\alias{rdbes_cl}
\title{Table Commercial Landing (CL) generation (RDBES process)}
\usage{
rdbes_cl(
  observe_con,
  balbaya_con,
  fao_area_file_path,
  eez_area_file_path,
  year_time_period,
  flag,
  major_fao_area_filter = NULL,
  hash_algorithms = NULL,
  digit_accuracy = 1L,
  encrypted_vessel_code_separator = ", ",
  export_path = NULL
)
}
\arguments{
\item{observe_con}{{\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the observe database.}

\item{balbaya_con}{{\link[base]{list}} expected. Output of the function {\link[furdeb]{postgresql_dbconnection}} for a connection to the balbaya database.}

\item{fao_area_file_path}{{\link[base]{character}} expected. File path of the FAO area grid. The file format has to be .Rdata or .RData extension.}

\item{eez_area_file_path}{{\link[base]{character}} expected. File path of the EEZ area grid. The file format has to be .Rdata or .RData extension.}

\item{year_time_period}{{\link[base]{integer}} expected. Year(s) selected associated to the databases queries extractions.}

\item{flag}{{\link[base]{integer}} expected. Flag(s) selected associated to the databases queries extractions.}

\item{major_fao_area_filter}{{\link[base]{integer}} expected. By default NULL. Sub selection of major fao area.}

\item{hash_algorithms}{{\link[base]{integer}} expected. By default NULL. The hashing algorithms to be used for the CLencrypVesIds variable. You can choose any modality of the argument "algo" or the function {\link[digest]{digest}}.}

\item{digit_accuracy}{{\link[base]{integer}} expected. By default 1. Indicating the number of decimal places to be used.}

\item{encrypted_vessel_code_separator}{{\link[base]{character}} expected. By default ", ". Which separator you want to use for the CLencrypVesIds variable.}

\item{export_path}{{\link[base]{character}} expected. By default NULL. Directory path associated for the export.}
}
\value{
A R object with the RDBES table CL with potentially a csv extraction associated.
}
\description{
Process for generation and optionally extraction of the RDBES table CL (Commercial Landing).
}
