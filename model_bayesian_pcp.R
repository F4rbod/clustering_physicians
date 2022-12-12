numcores <- 4

library(tidyverse)
library(parallel)
library(data.table)
library(fst)
library(comorbidity)
library(zeallot)
library(dtplyr)
library(vroom)
library(dplyr)
library(FactoMineR)
library(factoextra)
library(plyr)

# library(icd)
`%!in%` <- Negate(`%in%`)

setDTthreads(numcores)

modelling_data <- read.fst("/work/postresearch/Shared/Projects/Farbod/Clustering/modelling_data.fst",
    as.data.table = TRUE
)

# use bayesian mixed effects (brms)
library(brms)

model_death_next_year <- brm(
    died_2016_2017 ~
        cluster_centered_age_10 +
        score_group +
        cluster_age_mean_10 +
        cluster_severity_group * quintile +
        (1 | cluster),
    data = modelling_data[died_2016_2017 %in% c(0, 1)][provider_specialty %in% c("01", "08", "11", "38") & top_physician == 1],
    family = "bernoulli",
    cores = numcores,
    iter = 2000,
    warmup = 1000,
    chains = 4
)


summary(model_death_next_year)

# save summary of model
saveRDS(model_death_next_year, file = "/work/postresearch/Shared/Projects/Farbod/Clustering/model_death_next_year_pcp.rds")
