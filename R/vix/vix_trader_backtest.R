library(magrittr)
library(broom)
library(dplyr)
hedge_calc <- function(train, date){
  vix_fit <- lm(vix~spx+I(VIX_TTS*spx), data=train)
  prev = data.frame(train) %>%
    filter(DATE == date)
  b1<-coef(vix_fit)[2]
  b2<-coef(vix_fit)[3]
  hr <- (b1*100 + b2*prev$VIX_TTS*100)/(0.01*prev$SP_CLOSE) # / 50
  return(hr)
}
# load data
indexes = read.csv("vix_sp500_front_futures.csv")
indexesdt = as.matrix(indexes[,c("SP_CLOSE", "VIX_CLOSE")])
colnames(indexesdt) <- c("spx","vix")
returns = data.frame(DATE= as.Date(indexes[-1,]$DATE), SP_FUT=indexes[-1,]$SP_NAME, 
                     VIX_FUT=indexes[-1,]$VIX_NAME, VIX_TTS=indexes[-1,]$VIX_DAYS_LEFT,
                     SP_CLOSE=indexes[-1,]$SP_CLOSE, VIX_CLOSE=indexes[-1,]$VIX_CLOSE,
                     VIX_SPOT_CLOSE=indexes[-1,]$VIX_SPOT_CLOSE,
                     spot_return=diff(indexes$VIX_SPOT_CLOSE),
                     sp_return=diff(indexes$SP_CLOSE),
                     diff(indexesdt)/indexesdt[-nrow(indexesdt),])

# fit the model
vix_spot_fit <- lm(spot_return~sp_return, data=returns)
vix_spx_fit <- lm(vix~spx, data=returns)
vix_fit <- lm(vix~spx+I(VIX_TTS*spx), data=returns)
# mean squared error (MSE). samller is better
mean(vix_spot_fit$residuals^2)
mean(vix_spx_fit$residuals^2)
mean(vix_fit$residuals^2)
# lower AIC better
vix_spx_step <- stepAIC(vix_spx_fit, direction = "both")
vix_spx_step$anova
vix_fit_step <- stepAIC(vix_fit, direction = "both")
vix_fit_step$anova
vix_spot_step <- stepAIC(vix_spot_fit, direction = "both")
vix_spot_step$anova

#use
position_open = FALSE
results <- data.frame(DATE=as.Date(character()), ACTION=numeric(0), 
                      VIX_FUT_PRICE= numeric(0),SP_PRICE= numeric(0),HR= numeric(0))
returns[order(as.Date(returns$DATE, format="%Y-%m-%d")),]

test = data.frame(returns) %>%
  filter(DATE >= '2009-1-1') 
i = 1
for (row in 1:nrow(test)) {
  date  <- test[row, "DATE"]
  tts = test[row, "VIX_TTS"]
  train = data.frame(returns) %>%
    filter(DATE <= date) 
  hr <- hedge_calc(train, date)
  roll = (test[row, "VIX_CLOSE"]-test[row, "VIX_SPOT_CLOSE"])/tts
  if ((roll < 0.05 || tts < 9) && position_open) {
  # if ((tts < 2) && position_open) {
    position_open = FALSE
    results[i, ] <- c(format(date, "%Y-%m-%d"), 1, test[row, "VIX_CLOSE"], test[row, "SP_CLOSE"],hr)
    i <- i + 1
    print(paste("Buy On", date, " roll is ", roll))
  }
  if (roll > 0.10 && tts > 10 && !position_open) {
  # if (roll > 0.10 && !position_open) {
    position_open = TRUE
    results[i, ] <- c(format(date, "%Y-%m-%d"),-1, test[row, "VIX_CLOSE"], test[row, "SP_CLOSE"],hr)
    i <- i + 1
    print(paste("Sell On", date, " roll is ", roll))
  }
  
}
results$return <- append(diff(as.numeric(results$VIX_FUT_PRICE)), 0, after = 0)
results$hedge <- append(diff(as.numeric(results$SP_PRICE)), 0, after = 0)
results$unhedgedPnL <-  ((-100) * as.numeric(results$ACTION) *  as.numeric(results$VIX_FUT_PRICE))
results$hedgedReturn <- 0
results$unhedgedPnL <- cumsum(results$unhedgedPnL)


results$unhedgedPnL <- round(results$unhedgedPnL,2)
results$return <- round(results$return,2)
results$hedge <- round(results$hedge,2)
results$HR <- round(as.numeric(results$HR),2)
results$hedge <- (-1)*as.numeric(results$hedge)
results$return <- (-1)*as.numeric(results$return)
current_ratio = 0
for (row in 1:nrow(results)) {
  if (results[row, "ACTION"] == "-1") {
    results[row, "unhedgedPnL"] <- ""
    results[row, "hedge"] <- ""
    results[row, "return"] <- 0
    current_ratio = abs(as.numeric(results[row, "HR"]))
  }
  if (results[row, "ACTION"] == "1") {
    results[row, "hedgedReturn"] <- round(as.numeric(results[row, "return"]) +
      as.numeric(results[row, "hedge"])*current_ratio, 2)
  }
}
results$totalReturn <- as.numeric(results$hedgedReturn) + as.numeric(results$return)*100
results$hedgedPnL <- cumsum(results$totalReturn)


