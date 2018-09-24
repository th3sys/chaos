library(magrittr)
library(broom)
library(dplyr)

# load data
indexes = read.csv("vix_sp500_front_futures.csv")
indexesdt = as.matrix(indexes[,c("SP_CLOSE", "VIX_CLOSE")])
colnames(indexesdt) <- c("spx","vix")
returns = data.frame(DATE= as.Date(indexes[-1,]$DATE), SP_FUT=indexes[-1,]$SP_NAME, 
                     VIX_FUT=indexes[-1,]$VIX_NAME, VIX_TTS=indexes[-1,]$VIX_DAYS_LEFT,
                     SP_CLOSE=indexes[-1,]$SP_CLOSE, VIX_CLOSE=indexes[-1,]$VIX_CLOSE,
                     diff(indexesdt)/indexesdt[-nrow(indexesdt),])

# fit the model
vix_spx_fit <- lm(vix~spx, data=returns)
vix_fit <- lm(vix~spx+I(VIX_TTS*spx), data=returns)
# mean squared error (MSE). samller is better
mean(vix_spx_fit$residuals^2)
mean(vix_fit$residuals^2)
# lower AIC better
vix_spx_step <- stepAIC(vix_spx_fit, direction = "both")
vix_spx_step$anova
vix_fit_step <- stepAIC(vix_fit, direction = "both")
vix_fit_step$anova


#use
#sell TTS > 10 and (VIX_FUT-VIX_SPOT)/TTS > 0.10
#exit if TTS < 9 or  (VIX_FUT-VIX_SPOT)/TTS < 0.5
#buy TTS > 10 and (VIX_FUT-VIX_SPOT)/TTS < -0.10
#exit if TTS < 9 or  (VIX_FUT-VIX_SPOT)/TTS > -0.5
train = data.frame(returns) %>%
  filter(DATE < '2012-1-3') 
vix_fit <- lm(vix~spx+I(VIX_TTS*spx), data=train)
prev = data.frame(returns) %>%
  filter(DATE == '2012-1-3')
b1<-coef(vix_fit)[2]
b2<-coef(vix_fit)[3]
hr <- (b1*100 + b2*prev$VIX_TTS*100)/(0.01*prev$SP_CLOSE) # / 50
hr

