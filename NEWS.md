# acdc 1.0.0 - 2023-08-01

## Added
* Add new variable "metier_7" link to metier level 7 definiton. Referential associated was taking from the ICES RCGs website (https://github.com/ices-eg/RCGs/tree/master/Metiers/Reference_lists) and stored in the package "inst" directory.

## Changed
* Improvement of the variable "metier", in relation to the last metier referential (link to the addition of the variable "metier_7" below).
* Improvement of the function "fdi_template_checking" (integration of the code of the function "specific_arguments_controls" and removing it).
* Update of the controls checking related to the last improvement of the "codama" package.
* Improvement of the function "fdi_tabled_discard_length" by extract and process data directly from the Observe database.
