% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/fdi_tableh_landings_rectangle.R
\name{fdi_tableh_landings_rectangle}
\alias{fdi_tableh_landings_rectangle}
\title{Table H landings by rectangle generation (FDI process)}
\usage{
fdi_tableh_landings_rectangle(
  tablea_bycatch_retained,
  tablea_landing_rectangle,
  template_checking = FALSE,
  template_year = NULL,
  table_export_path = NULL
)
}
\arguments{
\item{tablea_bycatch_retained}{Output "bycatch_retained" of the function {\link[acdc]{fdi_tablea_catch_summary}}.}

\item{tablea_landing_rectangle}{Output "landing_rectangle" of the function {\link[acdc]{fdi_tablea_catch_summary}}.}

\item{template_checking}{{\link[base]{logical}} expected. By default FALSE. Checking FDI table generated regarding the official FDI template.}

\item{template_year}{{\link[base]{integer}} expected. By default NULL. Template year.}

\item{table_export_path}{{\link[base]{character}} expected. By default NULL. Directory path associated for the export.}
}
\value{
The process returns a list with the FDI table H inside.
}
\description{
Process for generation and optionally extraction of the FDI table H (landings by rectangle).
}
