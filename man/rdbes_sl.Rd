% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rdbes_sl.R
\name{rdbes_sl}
\alias{rdbes_sl}
\title{Table Species List (SL) details generation (RDBES process)}
\usage{
rdbes_sl(rdbes_table_cl = NULL, export_path = NULL)
}
\arguments{
\item{rdbes_table_cl}{{\link[base]{data.frame}} or {\link[tibble]{tibble}} expected. By default NULL. Table CL generated by the function {\link[acdc]{rdbes_cl}}.}

\item{export_path}{{\link[base]{character}} expected. By default NULL. Directory path associated for the export.}
}
\value{
A R object with the RDBES table SL with potentially a csv extraction associated.
}
\description{
Process for generation and optionally extraction of the RDBES table SL (Species List details).
}
\details{
If the argument "rdbes_table_cl" is provided, the process generates a table SL according to input data of the data.frame/tibble associated (not from a database SQL extraction).
}
