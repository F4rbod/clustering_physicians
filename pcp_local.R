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

# run the multilevel model (physicians nested in clsuters) only for main cardiologists
# these will be physicians nested in specialties nested in clusters
library(lme4)

model_death_next_year_pcp <- glmer(
    died_2016_2017 ~
        cluster_centered_age_10 +
        score_group +
        cluster_age_mean_10 +
        cluster_severity_group *
            quintile +
        (1 | cluster),
    data = modelling_data[died_2016_2017 %in% c(0, 1)][provider_specialty %in% c("01", "08", "11", "38") & top_physician == 1],
    family = binomial(link = "logit"),
    control = glmerControl(optimizer = "bobyqa", calc.derivs = FALSE)
)

summary(model_death_next_year_pcp)


# save summary of model
saveRDS(model_death_next_year_pcp,
    file = "/work/postresearch/Shared/Projects/Farbod/Clustering/model_death_next_year_pcp_lmer.rds"
)
