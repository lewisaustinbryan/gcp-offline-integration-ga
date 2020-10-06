library(googleAuthR)
library(googleAnalyticsR)
library(Rcpp)
library(rlang)
library(dplyr)
library(stringr)
library(lubridate)
library(tidyr)


ga_names_map <- function (
  account_id, property_id, cd_names, #Base set up
  more_than_one_data_import = FALSE, type_filler = NULL, #For Creating Custom Dimensions
  join_id_name, join_index, cd_names, write_csv = TRUE #For creating names map
) {

  create_custom_dimensions <- function(account_id, property_id, cd_names, more_than_one_data_import = FALSE, type_filler = NULL) {
    
    
    cds <- ga_custom_vars_list(account_id, property_id)
    
    if (isTRUE(more_than_one_data_import)) {
      cdnames <-  gsub('([[:upper:]])|\\.|\\_', ' \\1', cd_names)
      cdnames <- gsub("I D", "ID", cdnames)
      cdnames <- gsub("V R M", "VRM", cdnames)
      cdnames <- gsub("B C A", "BCA", cdnames)
      cdnames <- gsub("^ ", paste(type_filler, ""), cdnames)
      
    } else {}
    
    namesMap <- NULL
    for (customDimension in 1:length(cd_names)) {
      
      ga_custom_vars_create(cdnames[customDimension],
                            index = last(cds)$index + 1 + customDimension,
                            accountId = account_id,
                            webPropertyId = property_id,
                            scope = "SESSION",
                            active = TRUE)  
      if (is.null(namesMap)) {
        namesMap <- data.frame(name = cd_names, id = paste("ga:dimension",customDimension, sep = ""))
      } else {
        namesMap <- rbind(namesMap, data.frame(name = cd_names, id = paste("ga:dimension",customDimension, sep = "")))
      }
    }
    
    return(namesMap)  
  } 

  create_ga_names_map <- function(account_id, property_id, join_id_name, join_index, cd_names, write_csv = TRUE) {
    
    cds <- ga_custom_vars_list(accountId, property_id)
    
    map  <- cds %>% filter(grepl(cd_names, name)) %>% select(name, id)
    map <- data.frame(name = cd_names, id = map$id, stringsAsFactors = FALSE)
    map <- rbind(c(join_id_name, paste("ga:dimension",join_index,sep = ""), names), map)
    
    if (isTRUE(write_csv)) {
      return(write.csv(map, "ga_names_map.csv"))
    } else {
      return(map)
    }
     
  }

  create_custom_dimensions(account_id, property_id, names, more_than_one_data_import = FALSE, type_filler = NULL)

  return(create_ga_names_map(account_id, property_id, join_id_name, join_index, cd_names, write_csv = TRUE))

}
