# Pete Berryman ----- Yelich pre and post Mayfield shoutout ----- October 10th, 2018

cat("\014")
install.packages("dplyr")
install.packages("magrittr")
install.packages("readxl")
library(dplyr)
library(magrittr)
library(readxl)

NL.wRCplus.Leaders.2018 <- read.csv("~/Downloads/NL wRCplus Leaders 2018.csv")
NL.wRCplus.Leaders.pre <- read.csv("~/Downloads/NL wRCplus Leaders pre.csv")
NL.wRCplus.Leaders.post <- read.csv("~/Downloads/NL wRCplus Leaders post.csv")

#2018 Rankings
Season_wRCplus_Rankings <- NL.wRCplus.Leaders.2018 %>%
arrange(desc(wRC.)) %>%
filter(PA>300) %>%
mutate(wRCplus=wRC.) %>%
top_n(15, wRCplus) %>%
select(Name, playerid, Team, G, PA, wRCplus)

#2018 Rankings before shoutout (beginning of season to Sep. 20)
Pre_wRCplus_Rankings <- NL.wRCplus.Leaders.pre %>%
arrange(desc(wRC.)) %>%
filter(PA>250) %>%
mutate(wRCplus=wRC.) %>%
top_n(15, wRCplus) %>%
select(Name, playerid, Team, G, PA, wRCplus)

#2018 Rankings after shoutout (Sep. 21 to end of season)
Post_wRCplus_Rankings <- NL.wRCplus.Leaders.post %>%
arrange(desc(wRC.)) %>%
filter(PA>20) %>%
mutate(wRCplus=wRC.) %>%
top_n(15, wRCplus) %>%
select(Name, playerid, Team, G, PA, wRCplus)
