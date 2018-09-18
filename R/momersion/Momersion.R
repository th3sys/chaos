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

xiv <- xts(read.zoo("longXIV.txt", format="%Y-%m-%d", sep=",", header=TRUE))
vxx <- xts(read.zoo("longVXX.txt", format="%Y-%m-%d", sep=",", header=TRUE))

xivRets <- Return.calculate(Cl(xiv))
vxxRets <- Return.calculate(Cl(vxx))

volSpread <- xivRets + vxxRets
volSpreadMomersion <- momersion(volSpread, n = 252)
plot(volSpreadMomersion)
xivRetsMomersion <- momersion(xivRets, n = 252)
plot(xivRetsMomersion)

#both sides
sig <- -lag(sign(volSpread))
longShort <- sig * volSpread
charts.PerformanceSummary(longShort['2011::'], main = 'long and short spread')

#long spread only
sig <- -lag(sign(volSpread))
sig[sig < 0] <- 0
longOnly <- sig * volSpread
charts.PerformanceSummary(longOnly['2011::'], main = 'long spread only')


#short spread only
sig <- -lag(sign(volSpread))
sig[sig > 0] <- 0
shortOnly <- sig * volSpread
charts.PerformanceSummary(shortOnly['2011::'], main = 'short spread only')

threeStrats <- na.omit(cbind(longShort, longOnly, shortOnly))["2011::"]
colnames(threeStrats) <- c("LongShort", "Long", "Short")
rbind(table.AnnualizedReturns(threeStrats), CalmarRatio(threeStrats))