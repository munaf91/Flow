setwd("//srhtdw2db/e$/R/AE")

library(RODBC) 
library(TTR)
library(plyr)
library(forecast)

dbhandle <- odbcDriverConnect('driver={SQL Server};server=SRHTDW2DB;database=nhs_import;trusted_connection=true')
data.d <- sqlQuery(dbhandle, "SELECT * FROM nhs_data_warehouse.dbo.vw_AE_Date_R ORDER BY ArrivalDate")
data.t <- sqlQuery(dbhandle, "SELECT * FROM nhs_data_warehouse.dbo.vw_AE_Time_R ORDER BY ArrivalDate")
data.ip <- sqlQuery(dbhandle, "SELECT * FROM nhs_data_warehouse.dbo.vw_IP_Time_R ORDER BY ArrivalDate")

########################################### IP Linear Model for short term forecast################################################

# ip_linear_df = data.frame()
# 
# for (day in 1:7) {
#     for (hour in 0:23) {
# 
#         pred_ip_hour <-
#         as.data.frame(predict(
#         lm(Attendances ~ ArrivalHour, subset(data.ip, ArrivalDay == day & ArrivalHour == hour)),
#         newdata = data.frame(ArrivalHour = hour, ArrivalDay = day),
#         interval = "predict"))
# 
#         pred_ip_hour$hour_of_day <- hour
#         pred_ip_hour$day_week <- day
#         pred_ip_hour$run_date <- Sys.Date()
# 
#         ip_df_inside_loop <- data.frame(pred_ip_hour$day_week, pred_ip_hour$hour_of_day, pred_ip_hour$fit, pred_ip_hour$run_date)
#         ip_linear_df <- rbind(ip_linear_df, ip_df_inside_loop)
# 
#     }
# }
# 
# 
# ip_linear_df <- rename(ip_linear_df, c("pred_ip_hour.day_week" = "day_week", "pred_ip_hour.hour_of_day" = "hour_of_day", "pred_ip_hour.fit" = "fit", "pred_ip_hour.run_date" = "run_date"), warn_missing = FALSE)
# varTypes = c(run_date = "datetime")
# 
# sqlDrop(dbhandle, sqtable = "r.IPHourForecast")
# sqlSave(dbhandle, ip_linear_df, tablename = "r.IPHourForecast", append = TRUE, varTypes = varTypes)

ip_linear_df = data.frame()

for (day in 1:7) {

    pred_ip_hour <-
      as.data.frame(predict(
        lm(Attendances ~ ArrivalDay, subset(data.ip, ArrivalDay == day)),
        newdata = data.frame(ArrivalDay = day),
        interval = "predict"))
    
    pred_ip_hour$day_week <- day
    pred_ip_hour$run_date <- as.POSIXct(Sys.time())
    
    ip_df_inside_loop <- data.frame(pred_ip_hour$day_week, pred_ip_hour$fit, pred_ip_hour$run_date)
    ip_linear_df <- rbind(ip_linear_df, ip_df_inside_loop)
    
}

ip_linear_df <- rename(ip_linear_df, c("pred_ip_hour.day_week" = "day_week", "pred_ip_hour.fit" = "fit", "pred_ip_hour.run_date" = "run_date"), warn_missing = FALSE)
varTypes = c(run_date="datetime")

sqlDrop(dbhandle, sqtable = "r.IPDayForecast")
sqlSave(dbhandle, ip_linear_df, tablename = "r.IPDayForecast", append = TRUE, varTypes = varTypes)

########################################### AE Linear Model for short term forecast################################################

linear_df = data.frame()

for (day in 1:7) {
  for (hour in 0:23) {
    
    pred_ae_hour <-
      as.data.frame(predict(
        lm(Attendances ~ ArrivalHour, subset(data.t, ArrivalDay == day & ArrivalHour == hour )),
        newdata = data.frame(ArrivalHour = hour , ArrivalDay = day),
        interval = "predict"
      ))
    
    pred_ae_hour$hour_of_day <- hour
    pred_ae_hour$day_week <- day 
    pred_ae_hour$run_date <- as.POSIXct(Sys.time())
    
    df_inside_loop <- data.frame(pred_ae_hour$day_week, pred_ae_hour$hour_of_day, pred_ae_hour$fit, pred_ae_hour$run_date)
    linear_df <- rbind(linear_df, df_inside_loop)
    
  }
}

linear_df <-rename(linear_df, c("pred_ae_hour.day_week"="day_week" ,"pred_ae_hour.hour_of_day"="hour_of_day", "pred_ae_hour.fit"="fit", "pred_ae_hour.run_date"="run_date"), warn_missing = FALSE)
varTypes = c(run_date="datetime")

sqlDrop(dbhandle, sqtable = "r.AEHourForecast")
sqlSave(dbhandle, linear_df, tablename = "r.AEHourForecast", append=TRUE, varTypes = varTypes)

########################################### AE Arima Model for long term forecast################################################

# arima_fc = list()
# 
# for (day in 1:7) {
#   
#   pred_arima <-
#     Arima(
#       subset(data.d, ArrivalDayOfWeek == day)[, 3],
#       order = c(0, 1, 1),
#       include.drift = TRUE,
#       seasonal = list(order = c(0, 1, 1), period = 7)
#     )
#   
#   d <- as.data.frame.character(forecast(pred_arima,h=9)$mean)
#   d$day <- day
#   arima_fc[[day]] <- d
#   
#   #plot(forecast(pred_arima,h=9 ))
#   
# }
# 
# arima_df <- do.call(rbind, arima_fc)
# arima_df$run_date <- Sys.timeDate()
# varTypes = c(run_date="datetime")
# arima_df <- rename(arima_df, c("forecast(pred_arima, h = 9)$mean"="fit", "day"="day_week"), warn_missing = FALSE)
# 
# sqlDrop(dbhandle, sqtable = "r.AEArimaForecast")
# sqlSave(dbhandle, arima_df, tablename = "r.AEArimaForecast", append=TRUE, varTypes = varTypes)