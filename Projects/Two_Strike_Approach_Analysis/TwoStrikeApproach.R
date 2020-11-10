# Pete Berryman ----- Team Two-Strike Approach Analysis ----- October 11th, 2018

cat("\014")
install.packages("dplyr")
install.packages("magrittr")
install.packages("readxl")
library(dplyr)
library(magrittr)
library(readxl)

#Find which teams' have lowest K% over last 10 years (best contact team)
Kpct <- read.csv("~/Downloads/Teams K pct over last 10.csv")
KpctSummary <- Kpct %>%
  group_by(Team)%>%
  summarise(meanKpct = mean(K.), '25th pctile' = quantile(K., probs = 0.25), '75th pctile' = quantile(K., probs = 0.75), SD = sd(K.))
KpctRank <- Kpct %>%
  select(Season, Team, K., wRC.)%>%
  group_by(Season)%>%
  arrange(K.)%>%
  mutate(rank = rank(K.))
KpctRankSummary <- KpctRank%>%
  group_by(Team)%>%
  summarise(meanrank = mean(rank), '25th pctile' = quantile(rank, probs = 0.25), '75th pctile' = quantile(rank, probs = 0.75), SD = sd(rank))%>%
  arrange(meanrank)

#Possible expansion on study: Analyze 0/1 strike counts contact percentage for comparison

#---------------------------------------------------------------------------------------------------------------

#Find which teams' quality of contact is least hurt with two strikes (Launch Speed & xwOBA)

NotTwoStrike.xwOBA.Results <- read.csv("~/Downloads/NotTwoStrike xwOBA Results.csv")
TwoStrike.xwOBA.Results <- read.csv("~/Downloads/TwoStrike xwOBA Results.csv")
ContactPct <- read.csv("~/Downloads/ContactPct.csv")
Runs <- read.csv("~/Downloads/Runs.csv")


Team_xwOBA_2 <- TwoStrike.xwOBA.Results %>%
  mutate(pitch_percent_2strike = pitch_percent, ContactRate_2 = 1 - whiffs/swings, xwOBA_2 = xwoba, launch_speed_2 = launch_speed) %>%
  select(player_id, pitch_percent_2strike, ContactRate_2, xwOBA_2, launch_speed_2)

Team_xwOBA <- NotTwoStrike.xwOBA.Results %>%
  mutate(ContactRate = 1 - whiffs/swings) %>%
  select(player_id, pitches, ContactRate, xwoba, launch_speed)

Comparison <- full_join(Team_xwOBA, Team_xwOBA_2, by = "player_id") %>%
  mutate(Contact_Rate_diff = ContactRate_2 - ContactRate, xwOBA_diff = xwOBA_2 - xwoba, launch_speed_diff = launch_speed_2 - launch_speed) %>%
  select(player_id, pitch_percent_2strike, Contact_Rate_diff, xwOBA_diff, launch_speed_diff)

ContactToRuns <-full_join(ContactPct, Runs, by= "Team") %>%
  select(Team, Contact., O.Swing., Z.Contact., R)%>%

  #Export Comparison to csv and then to Excel to make better graph
  write.table(Comparison, file = "~/Desktop/MLBTeamTwoStrikeComparison.csv", sep = ",", col.names = TRUE)
  write.table(ContactToRuns, file = "~/Desktop/ContactToRuns.csv", sep = ",", col.names = TRUE)
#---------------------------------------------------------------------------------------------------------------

# Which players’ have the largest and greatest drops in production (wOBA) with two strikes?

library(dplyr)
library(magrittr)

notTwoStrikes <- read.csv("~/Downloads/0or1 strike.csv")
TwoStrikes <- read.csv("~/Downloads/2 strike.csv")

notTwoStrikes <- notTwoStrikes %>%
  mutate(wOBA = woba, no_in_play_0or1 = pitches) %>%
  select(player_id, player_name, no_in_play_0or1, wOBA)

TwoStrikes <- TwoStrikes %>%
  mutate(wOBA_2 = woba, no_in_play_2 = pitches) %>%
  select(player_id, player_name, no_in_play_2, wOBA_2)

HitterComparison <- inner_join(notTwoStrikes, TwoStrikes)%>%
  mutate(wOBAdiff = wOBA - wOBA_2)%>%
  arrange(wOBAdiff)

  #T test to determine whether difference between 0/1 strike wOBA and 2 strike wOBA is statistically significant
  t.test(HitterComparison$wOBA, HitterComparison$wOBA_2, alternative = "two.sided", var.equal = FALSE) #H0: wOBA=wOBA_2; HA: wOBA≠wOBA_2
  t.test(HitterComparison$wOBA, HitterComparison$wOBA_2, alternative = "greater", var.equal = FALSE) #H0: wOBA=wOBA_2; HA: wOBA>wOBA_2