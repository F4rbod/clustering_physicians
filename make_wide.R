numcores <- 7

library(tidyverse)
library(parallel)
library(data.table)
library(fst)
library(comorbidity)
library(zeallot)
library(reshape)
library(dtplyr)
library(vroom)
library(dplyr)

# library(icd)
`%!in%` <- Negate(`%in%`)

setDTthreads(numcores)


# read the data
carrier_data <- read_fst("/work/postresearch/Shared/Projects/Farbod/Clustering/carrier_data.fst", as.data.table = TRUE)
inpatient_data <- read_fst("/work/postresearch/Shared/Projects/Farbod/Clustering/inpatient_data.fst", as.data.table = TRUE)
outpatient_data <- read_fst("/work/postresearch/Shared/Projects/Farbod/Clustering/outpatient_data.fst", as.data.table = TRUE)

find_all_diagnosis <- function(data) {
    data <- data[, .(
        id = DESY_SORT_KEY,
        year = year,
        # diagnosis_prncpl = PRNCPAL_DGNS_CD,
        diagnosis1 = ICD_DGNS_CD1,
        diagnosis2 = ICD_DGNS_CD2,
        diagnosis3 = ICD_DGNS_CD3,
        diagnosis4 = ICD_DGNS_CD4,
        diagnosis5 = ICD_DGNS_CD5,
        diagnosis6 = ICD_DGNS_CD6,
        diagnosis7 = ICD_DGNS_CD7,
        diagnosis8 = ICD_DGNS_CD8,
        diagnosis9 = ICD_DGNS_CD9,
        diagnosis10 = ICD_DGNS_CD10,
        diagnosis11 = ICD_DGNS_CD11,
        diagnosis12 = ICD_DGNS_CD12,
        diagnosis13 = ICD_DGNS_CD13,
        diagnosis14 = ICD_DGNS_CD14,
        diagnosis15 = ICD_DGNS_CD15,
        diagnosis16 = ICD_DGNS_CD16,
        diagnosis17 = ICD_DGNS_CD17,
        diagnosis18 = ICD_DGNS_CD18,
        diagnosis19 = ICD_DGNS_CD19,
        diagnosis20 = ICD_DGNS_CD20,
        diagnosis21 = ICD_DGNS_CD21,
        diagnosis22 = ICD_DGNS_CD22,
        diagnosis23 = ICD_DGNS_CD23,
        diagnosis24 = ICD_DGNS_CD24,
        diagnosis25 = ICD_DGNS_CD25,
        icd_procedure1 = ICD_PRCDR_CD1,
        icd_procedure2 = ICD_PRCDR_CD2,
        icd_procedure3 = ICD_PRCDR_CD3,
        icd_procedure4 = ICD_PRCDR_CD4,
        icd_procedure5 = ICD_PRCDR_CD5,
        icd_procedure6 = ICD_PRCDR_CD6,
        icd_procedure7 = ICD_PRCDR_CD7,
        icd_procedure8 = ICD_PRCDR_CD8,
        icd_procedure9 = ICD_PRCDR_CD9,
        icd_procedure10 = ICD_PRCDR_CD10,
        icd_procedure11 = ICD_PRCDR_CD11,
        icd_procedure12 = ICD_PRCDR_CD12,
        icd_procedure13 = ICD_PRCDR_CD13,
        icd_procedure14 = ICD_PRCDR_CD14,
        icd_procedure15 = ICD_PRCDR_CD15,
        icd_procedure16 = ICD_PRCDR_CD16,
        icd_procedure17 = ICD_PRCDR_CD17,
        icd_procedure18 = ICD_PRCDR_CD18,
        icd_procedure19 = ICD_PRCDR_CD19,
        icd_procedure20 = ICD_PRCDR_CD20,
        icd_procedure21 = ICD_PRCDR_CD21,
        icd_procedure22 = ICD_PRCDR_CD22,
        icd_procedure23 = ICD_PRCDR_CD23,
        icd_procedure24 = ICD_PRCDR_CD24,
        icd_procedure25 = ICD_PRCDR_CD25
    )]
    # melt the data to include the procedure and diagnosis codes in one column
    data <- melt(data, id.vars = c("id", "year")) %>% as.data.table()

    # change the data to wide format so that each diagnosis code is a column and the value is the number of times the diagnosis code was used
    # and also each procedure code is a column and the value is the number of times the procedure code was used
    # the data will be more than the machine row limit so we will split the data based on id and then dcast then combine the data

    # split the data based on id
    data_split <- split(data, list(data$id))

    # dcast the data, I will use mclapply to do this in parallel
    data_split <- mclapply(data_split,
        function(x) {
            x <- dcast(x, id + year ~ value, value.var = "value", fun.aggregate = length)
            return(x)
        },
        mc.cores = numcores
    )

    # combine the data back
    data <- rbindlist(data_split, use.names = T, fill = TRUE)

    # replace NA with 0

    data[is.na(data)] <- 0

    return(data)
}

inpatient_data_wide <- find_all_diagnosis(inpatient_data)
outpatient_data_wide = find_all_diagnosis(outpatient_data)

# create a similar function for carrier data

find_all_diagnosis_carrier <- function(data) {
    data <- data[, .(
        id = id,
        year = year,
        # diagnosis_prncpl = PRNCPAL_DGNS_CD,
        diagnosis = diagnosis,
        hcpcs = hcpcs
    )]

    # melt the data to include the procedure and diagnosis codes in one column
    data <- melt(data, id.vars = c("id", "year")) %>% as.data.table()

    # change the data to wide format so that each diagnosis code is a column and the value is the number of times the diagnosis code was used
    # and also each procedure code is a column and the value is the number of times the procedure code was used
    # the data will be more than the machine row limit so we will split the data based on id and year and then dcast then combine the data

    # split the data based on id
    data_split <- split(data, list(data$id))

    # dcast the data, I will use mclapply to do this in parallel
    data_split <- mclapply(data_split,
        function(x) {
            x <- dcast(x, id + year ~ value, value.var = "value", fun.aggregate = length)
            return(x)
        },
        mc.cores = numcores
    )

    # combine the data
    data <- rbindlist(data_split, use.names = T, fill = T)

    # replace NA with 0

    data[is.na(data)] <- 0

    return(data)
}

carrier_data_wide = find_all_diagnosis_carrier(carrier_data)


# Now, let's merge the data on the id and year

wide_data <- full_join(outpatient_data_wide, inpatient_data_wide, by = c("id", "year"))
wide_data <- full_join(wide_data, carrier_data_wide, by = c("id", "year")) %>% as.data.table()

# change NA to 0

wide_data[is.na(wide_data)] <- 0

head(wide_data)

write.fst(wide_data, "/work/postresearch/Shared/Projects/Farbod/Clustering/wide_data.fst")
