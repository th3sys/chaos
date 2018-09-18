library(xts)
library(PerformanceAnalytics)
library(quantmod)
library(TTR)
momersion <- function(R, n, returnLag = 1) {
  momentum <- sign(R * lag(R, returnLag))
  momentum[momentum < 0] <- 0
  momersion <- runSum(momentum, n = n)/n * 100
  colnames(momersion) <- "momersion"
  return(momersion)
}

getSymbols('VIX', src = 'av', adjusted = TRUE, output.size = 'full', api.key = 'ZOB6F3XS7T9QI336')
getSymbols('XIV', src = 'av', adjusted = TRUE, output.size = 'full', api.key = 'ZOB6F3XS7T9QI336')
getSymbols('VXX', src = 'av', adjusted = TRUE, output.size = 'full', api.key = 'ZOB6F3XS7T9QI336')
getSymbols('SVXY', src = 'av', adjusted = TRUE, output.size = 'full', api.key = 'ZOB6F3XS7T9QI336')

svxyMRets <- Return.calculate(Ad(SVXY))
vxxMRets <- Return.calculate(Ad(VXX))
volMSpread <- svxyMRets + vxxMRets
volSpreadMomersion <- momersion(volMSpread, n = 252)
plot(volSpreadMomersion)

#both sides
sigM <- -lag(sign(volMSpread))
longMShort <- sigM * volMSpread
charts.PerformanceSummary(longMShort['2011-10::'], main = 'long and short spread')

#long spread only
sigM <- -lag(sign(volMSpread))
sigM[sigM < 0] <- 0
longMOnly <- sigM * volMSpread
charts.PerformanceSummary(longMOnly['2011-10::'], main = 'long spread only')

#short spread only
sigM <- -lag(sign(volMSpread))
sigM[sigM > 0] <- 0
shortMOnly <- sigM * volMSpread
charts.PerformanceSummary(shortMOnly['2011-10::'], main = 'short spread only')

threeMStrats <- na.omit(cbind(longMShort, longMOnly, shortMOnly))["2011-10::"]
colnames(threeMStrats) <- c("LongShort", "Long", "Short")
rbind(table.AnnualizedReturns(threeMStrats), CalmarRatio(threeMStrats))

###########
cleanSVXY <- Ad(SVXY)['2011-10::']
svxyDailyReturns <- cleanSVXY - lag(cleanSVXY)
cleanVXX <- Ad(VXX)['2011-10::']
vxxDailyReturns <- cleanVXX - lag(cleanVXX)

tradingDays <- shortMOnly['2011-10::']
tradingDays[tradingDays == 0] <- NA
tradingDays <- na.omit(tradingDays)

totalReturns<- merge(merge(svxyDailyReturns, vxxDailyReturns), 
                     tradingDays, join = 'inner')


colSums(totalReturns)







