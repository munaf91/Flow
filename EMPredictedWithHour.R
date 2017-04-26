##########################################################################
# PROJECT : Daily forecasted admissions broken down by hour, day and site

# DETAILS : Hourly data from previous years were plotted into a bar charts
#           by day and site.
#           The forecasted hourly breakdown was caluclated by:
#           (Frequency of the hour of the day/Total of day)*Forecasted total for day

# AUTHOR  : Amir Munaf

# DATE    : 18/04/2017
##########################################################################

setwd ("//SRHTDW2DB/e$/R/AE")

#install.packages("RODBC")
#install.packages("forecast")

library("RODBC")
library("forecast")

###############################################################################
# Import data

#Admissions <- read.csv(file = "PennineAdmissions.csv", header = TRUE, sep = ",")
#HourlyAdmissions <- read.csv(file = "EM_HOUR.csv", header = TRUE, sep = ",")


dbhandle <-
  odbcDriverConnect(
    'driver={SQL Server};server=SRHTDW2DB;database=nhs_import;trusted_connection=true'
  )
Admissions <- sqlQuery(
  dbhandle,
  "
  select CAST(AdmissionDate AS DATE) AS AdmissionDate
  , datepart(dw, AdmissionDate) DayOfWeek
  , 'RM301' AS SiteNationalCode
  , count(*) Total
  from srft_dwsources.pas.ProviderSpells
  where AdmissionDate >='20150401'
  and AdmissionDate < dateadd(week, datediff(week, 0, getdate()), 0)
  and AdmissionMethod in ('AE','EM')
  group by CAST(AdmissionDate AS DATE)
  , datepart(dw, AdmissionDate)
  order by AdmissionDate"
)


HourlyAdmissions <-
  sqlQuery(
    dbhandle,
    "select CAST(AdmissionDate AS DATE) AS AdmissionDate
    , datepart(dw, AdmissionDate) DayOfWeek
    , datepart(hour, AdmissionDate) Hourofday
    , 'RM301' AS SiteNationalCode
    , count(*) Total
    from srft_dwsources.pas.WardStays
    where AdmissionDate >='2015-04-01'
    and AdmissionDate < dateadd(week, datediff(week, 0, getdate()), 0)
    group by CAST(AdmissionDate AS DATE)
    , datepart(dw, AdmissionDate)
    , datepart(hour,AdmissionDate)
    order by AdmissionDate, Hourofday
    
    "
  )

#Get Site List
SiteList <- unique(Admissions$SiteNationalCode)

###############################################################################
#Day Predictions
###############################################################################

df = NULL

for (dw in (1:7)) {
  for (site in SiteList) {
    d <-
      data.frame(ses(
        subset(Admissions, DayOfWeek == dw &
                 SiteNationalCode == site)[, 4],
        h = 1,
        alpha = 0.4,
        initial = "simple"
      )$mean)
    
    d[, 1] <- as.character(d[, 1])
    d$dw <- as.character(dw)
    d$site <- as.character(site)
    
    df <- rbind(df, data.frame(d[, 1], d$dw, d$site))
    
  }
}

names(df) <- c("predicted", "day", "site")


###############################################################################
#Hour Frequency
###############################################################################
site.hour.df = NULL

for (site in SiteList) {
  for (day in (1:7)) {
    site.day <-
      subset(HourlyAdmissions, SiteNationalCode == site &
               DayOfWeek == day)
    site.day.freq <- data.frame(table(site.day$Hour))
    
    site.day.freq$total <- sum(site.day.freq$Freq)
    site.day.freq$site <- as.character(site)
    site.day.freq$day <- as.character(day)
    
    
    site.hour.df <- rbind(site.hour.df, data.frame(site.day.freq))
  }
}


###############################################################################
#Merge Day predictions and hour frequencies
###############################################################################
merged.df <- merge(df, site.hour.df, by = c("site", "day"))
merged.df$predicted <- as.character(merged.df$predicted)
merged.df$predicted <- as.numeric(merged.df$predicted)
merged.df$predicted_hour <- (merged.df$Freq / merged.df$total) * (merged.df$predicted)


#############################################################
# round numbers so that the sum of the indiviudal hours
# within a day and site equal predicted figure for that day
#############################################################

diffround <- function(x) {
  diff(c(0, round(cumsum(x))))
}

smart.round <- function(x) {
  y <- floor(x)
  indices <- tail(order(x - y), round(sum(x)) - sum(y))
  y[indices] <- y[indices] + 1
  y
}

merged.df$smart_round_2 <- round(merged.df$predicted) #smart.round(merged.df$predicted)
merged.df$diff_round <- diffround(merged.df$predicted_hour)


#############################################################
# subset and rename columns
#############################################################

merged.df$Freq <- merged.df$total <- NULL
names(merged.df) <-
  c(
    "site",
    "day",
    "predicted",
    "hour",
    "predicted_hour",
    "rounded_predicted",
    "rounded_hour"
  )

#############################################################
# Export to SQL
#############################################################
# Export to sql
sqlDrop(dbhandle, sqtable = "r.EMPredictedWithHour")
sqlSave(dbhandle, merged.df, tablename = "r.EMPredictedWithHour", append=TRUE)
