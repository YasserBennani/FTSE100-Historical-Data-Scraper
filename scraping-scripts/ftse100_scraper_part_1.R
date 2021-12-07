#!/usr/bin/env Rscript
## ----------------------------------------------------------------------------------------------------------------------------
#Install necessary libraries
install_pckgs <- function( packages = "favourites" ) {
    if( length( packages ) == 1L && packages == "favourites" ) {
        packages <- c("data.table", "chron", "pdftools", "dplyr", "devtools","rmarkdown", "knitr","lubridate","pacman")
    }
    packagecheck <- match( packages, utils::installed.packages()[,1] )

    packagestoinstall <- packages[ is.na( packagecheck ) ]

    if( length( packagestoinstall ) > 0L ) {
        utils::install.packages( packagestoinstall,
                             repos = "https://cloud.r-project.org/"
        )
    } else {
        print( "All requested packages already installed" )
    }

    for( package in packages ) {
        suppressPackageStartupMessages(
            library( package, character.only = TRUE, quietly = TRUE )
        )
    }
}

#Load necessary libraries
library(knitr)
library(pdftools)
library(dplyr)
library(lubridate)
require(data.table)
require("pacman")
pacman::p_load(tidyverse, rvest)

## ----------------------------------------------------------------------------------------------------------------------------
# Scrape FTSE 100 Changes 1984 - June 2021
# P.S.: The pdf file from the link described below is updated regularly by the owner when new changes are made to the index
# so if the formatting of the file is updated, the code will need to be updated accordingly for the most accurate results
##########################################################################################################################

scrape_constituents_history <- function(pdf_url){
  # scrape the content of the file from the URL and convert from PDF to text
  df <- pdf_text(pdf_url)
  # remove first 2 pages as well as the last page because we are not interested in that content
  df <- df[2:(length(df) - 1)]
  # Removing the footer from every page
  df <- gsub("\n\n\n\n\nFTSE Russell An LSEG Business[ ]+?\\d{1,2}\n|\n\n\n\n\nftserussell\\.com[ ]+?An LSEG Business[ ]+?June 2021\n", "", df)
  # Splitting not just by \n, but by \n that goes right before a date (positive look ahead)
  df <- str_split(df, pattern = "(\n)(?=\\d{2}-\\w{3}-\\d{2})")
  
  # Apply to each pdf page
  df <- lapply(df, function(changes) {
    # Split vectors into 4 columns
    changes <- str_split_fixed(changes, "(\n)*[ ]{2,}", 4)
    # Replace any remaining 
    changes <- gsub("(\n)*[ ]{2,}", " ", changes)
    colnames(changes) <- c("Date", "Added", "Deleted", "Notes")
    data.frame(changes[-1, ])
  })
  changes <- do.call("rbind",df)
  
  # Convert date to a standard format
  changes <- mutate(changes, Date = as.Date(Date, format = '%d-%B-%y'))
  # Clean remaining artifacts
  changes <- changes[!(changes$Added == "No Constituent Changes"),]
  changes$Deleted <- gsub("\n", "", changes$Deleted)
  # Replace innacurate entries
  changes$Deleted <- gsub("Reinshaw", "Renishaw PLC", changes$Deleted)
  changes$Added <- gsub("Reinshaw", "Renishaw PLC", changes$Added)
  changes <- changes[rev(1:nrow(changes)),]
  
  return(changes)
}

#####################################################################################################
#inspired from https://ramblingquant.com/2018/12/22/ftse100-historical-consituents%E2%80%8B%E2%80%8B/

## ----------------------------------------------------------------------------------------------------------------------------
# Scrape current constituents (08/2021)
scrape_current_constituents <- function(url){
  #scrape content of the web page
  page <- read_html(url)
  #extract table with id #constituents (needs to be updated according to scraped page)
  currentconstituents <- page %>%
  html_node('#constituents') %>%
  html_table(header = TRUE) 
}

## ----------------------------------------------------------------------------------------------------------------------------
# check if company name features in a string   
check_companies <- function(df, added){
  if(added != '') {
    return(df[grepl(tolower(added), tolower(df$Company), fixed = TRUE),])
  }
  else{
    return(data.frame(Date = Date(), Company = character()))
  }
}

## ----------------------------------------------------------------------------------------------------------------------------
# function to reconstruct monthly historical constituents of the FTSE 100
reconstruct_historical_constituents <- function(current_constituents, constituents_changes, start_date, end_date){
  # Set start month and end month sequence for reconstruction (dates should be strings in format 'yyyy-mm-dd')
  currentmonth <- as.Date(start_date)
  monthseq <- seq.Date(as.Date(end_date), as.Date(currentmonth), by = 'month') %>% rev()
  ftse_stocks <- current_constituents %>% mutate(Date = currentmonth) %>% select(Date,Company)
  # init last constitutents
  last_constituents <- ftse_stocks
  # loop through the months to reconstruct monthly constituents
  for (i in 2:length(monthseq)) {
    d <- monthseq[i]
    y <- year(d)
    m <- month(d)
  
    changes <- constituents_changes %>% select(Date, Added, Deleted) %>% filter(year(Date)==y, month(Date)==m)
  
    thismonth <- data.frame(Date = Date(), Company = character())
    dlt <- data.frame(Date = Date(), Company = character())
  
    # add companies in Deleted column
    toadd <- changes %>%
      filter(!Deleted == '') %>%
          transmute(Date = d, Company = Deleted)
    #
    last_constituents <- rbind(last_constituents, toadd)
  
    # remove companies in Added column 
    if(nrow(changes) > 0){
      for(i in 1:nrow(changes)){
        # check if
        checked = check_companies(last_constituents, changes$Added[i])
        if(nrow(checked) > 0){
          # reverse the checked vector in case there is a duplicated company that was added before
          revchecked = checked[nrow(checked):1, ]
          # remove duplicate before the anti join to avoid removing multiple records with same company
          checked <- revchecked[!duplicated(revchecked[,"Company"]),]
        }
  
        dlt = rbind(dlt, checked)
      }
    }
  
    # Constituents to keep
    tokeep <- last_constituents %>%
      anti_join(dlt, by = c("Company" = "Company", "Date" = "Date")) %>%
      mutate(Date = d) %>% select(Date, Company)
  
    # Merging and cleaning data
    thismonth <- rbind(thismonth, tokeep)
  
    thismonth <- thismonth[!duplicated(thismonth),]
  
    ftse_stocks <- rbind(ftse_stocks, thismonth)
  
    #
    last_constituents <- thismonth
  }
  return (ftse_stocks)
}

####################################################################################################

#  URL to pdf file containing ftse 100 historical additions and deletions  
pdf_file <- "https://research.ftserussell.com/products/downloads/FTSE_100_Constituent_history.pdf"
#
url <- 'https://en.wikipedia.org/wiki/FTSE_100_Index#Constituents_in_June_2021'
# 
constituents_changes <- scrape_constituents_history(pdf_file)
# 
current_constituents <- scrape_current_constituents(url)

# set reconstruction start and end date
start_date = '2021-08-01'
# stopping at month 92 (01/2014) because a change to the index before that date was not 
# reported in the changes document hence the rest of reconstructed constituents will be inaccurate
end_date = '2014-01-01'

# Call reconstruction function
historical_constituents <- reconstruct_historical_constituents(current_constituents, constituents_changes, start_date, end_date)
write.csv(historical_constituents,"historical_consts.csv")
