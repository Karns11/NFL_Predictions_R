library(bigrquery)
library(dplyr)
library(rvest)
library(zoo)
library(car)
library(MASS)
library(writexl)
library(gt)
library(gtExtras)
library(scales)
library(stringr)



Sys.setenv(BIGQUERY_TEST_KEY = "C:\\Users\\natha\\OneDrive\\Desktop\\gcs_serviceaccountkeys\\nfl-data-434817-e402c419bba3.json")

bq_auth(path = Sys.getenv("BIGQUERY_TEST_KEY"))

project_id <- "nfl-data-434817"
#dataset_id <- "nfl-data-434817.nfl_db"
#table_id <- "nfl-data-434817.nfl_db.pbp_nfl_data"


weekly_nfl_data_query <- "SELECT * FROM `nfl-data-434817.nfl_db.weekly_nfl_data`"

weekly_nfl_data_query_results <- bq_project_query(project_id, weekly_nfl_data_query)

weekly_nfl_data <- bq_table_download(weekly_nfl_data_query_results)

weekly_nfl_data <- weekly_nfl_data %>%
  distinct(player_id, player_name, season, week, .keep_all = TRUE)


#View(weekly_nfl_data)

#Finish loading in data
#------------------------------------------------------------------------------#
#Start transforming the data

no_na_opp_team <- weekly_nfl_data %>%
  filter(!is.na(opponent_team)) 

tst_max_week <- no_na_opp_team %>%
  filter(as.numeric(season) == max(as.numeric(season))) %>%
  arrange(desc(as.numeric(week)))



missing_percentage <- sort(colSums(is.na(no_na_opp_team)) / nrow(no_na_opp_team), decreasing = TRUE)

missing_percentage


no_na_opp_team <- no_na_opp_team %>%
  dplyr::rename('name' = 'player_display_name') %>%
  dplyr::rename('team' = 'recent_team')


no_na_opp_team <- dplyr::select(no_na_opp_team, -player_id, -pacr, -dakota, -racr, -wopr, -special_teams_tds, -passing_epa, -rushing_epa, -receiving_epa, -player_name, -position_group)


add_features <- function(df) {
  # Filter for regular season games
  df <- df[df$season_type %in% c("REG"), ]
  
  # Filter for offensive players
  off_players <- c("QB", "RB", "TE", "WR")
  df <- df[df$position %in% off_players, ]
  
  # Replace NA values in 'air_yards_share' and 'target_share' with 0
  df[c("air_yards_share", "target_share")][is.na(df[c("air_yards_share", "target_share")])] <- 0
  
  # Round 'fantasy_points' and 'fantasy_points_ppr' to 2 decimal places
  df[c("fantasy_points", "fantasy_points_ppr")] <- round(df[c("fantasy_points", "fantasy_points_ppr")], 2)
  
  # Calculate total yards
  yards <- c("passing_yards", "rushing_yards", "receiving_yards")
  df$total_yards <- rowSums(df[yards], na.rm = TRUE)
  
  # Calculate yards per attempt (YPA), yards per carry (YPC), and yards per reception (YPR)
  df$ypa <- round(df$passing_yards / df$attempts, 2)
  df$ypa[is.na(df$ypa)] <- 0
  
  df$ypc <- round(df$rushing_yards / df$carries, 2)
  df$ypc[is.na(df$ypc)] <- 0
  
  df$ypr <- round(df$receiving_yards / df$receptions, 2)
  df$ypr[is.na(df$ypr)] <- 0
  
  # Calculate total touches (attempts, receptions, carries)
  touches <- c("attempts", "receptions", "carries")
  df$touches <- rowSums(df[touches], na.rm = TRUE)
  
  # Count column to indicate how many games/weeks the player participated in
  df$count <- 1
  
  # Calculate completion percentage, pass touchdown percentage, interception percentage
  df$comp_percentage <- round(df$completions / df$attempts, 3)
  df$comp_percentage[is.na(df$comp_percentage)] <- 0
  
  df$pass_td_percentage <- round(df$passing_tds / df$attempts, 3)
  df$pass_td_percentage[is.na(df$pass_td_percentage)] <- 0
  
  df$int_percentage <- round(df$interceptions / df$attempts, 3)
  df$int_percentage[is.na(df$int_percentage)] <- 0
  
  # Calculate rush touchdown percentage and reception touchdown percentage
  df$rush_td_percentage <- round(df$rushing_tds / df$carries, 3)
  df$rush_td_percentage[is.na(df$rush_td_percentage)] <- 0
  
  df$rec_td_percentage <- round(df$receiving_tds / df$receptions, 3)
  df$rec_td_percentage[is.na(df$rec_td_percentage)] <- 0
  
  # Calculate total touchdowns and touchdown percentage
  tds <- c("passing_tds", "receiving_tds", "rushing_tds")
  df$total_tds <- rowSums(df[tds], na.rm = TRUE)
  
  df$td_percentage <- round(df$total_tds / df$touches, 3)
  df$td_percentage[is.na(df$td_percentage)] <- 0
  
  return(df)
}


no_na_opp_team <- add_features(no_na_opp_team)







no_na_opp_team <- no_na_opp_team %>%
   dplyr::mutate(comp_percentage = ifelse(comp_percentage < 0.3, 0.3,
                                   ifelse(comp_percentage > 0.775, 0.775, comp_percentage)))

no_na_opp_team <- no_na_opp_team %>%
   dplyr::mutate(ypa = ifelse(ypa > 12.5, 12.5,
                                          ifelse(ypa <= 3, 3, ypa)))

no_na_opp_team <- no_na_opp_team %>%
   dplyr::mutate(pass_td_percentage = ifelse(pass_td_percentage > 0.11875, 0.11875, pass_td_percentage))

no_na_opp_team <- no_na_opp_team %>%
   dplyr::mutate(int_percentage = ifelse(int_percentage > 0.095, 0.095, int_percentage))




no_na_opp_team$passer_rating <- round(((((no_na_opp_team$comp_percentage - 0.3) * 5) +
                                        ((no_na_opp_team$ypa - 3) * 0.25) +
                                        (no_na_opp_team$pass_td_percentage * 20) +
                                        ((no_na_opp_team$int_percentage * 25) - 0.3425)) / 6) * 100, 2)



# Function to calculate passer rating
# calculate_passer_rating <- function(completions, attempts, yards, touchdowns, interceptions) {
#   if (attempts == 0) return(0)  # Avoid division by zero
#   
#   CP <- (completions / attempts - 0.3) * 5
#   YPA <- (yards / attempts - 3) * 0.25
#   TD_percent <- (touchdowns / attempts) * 20
#   INT_percent <- (interceptions / attempts) * 25
#   
#   passer_rating <- (CP + YPA + TD_percent - INT_percent) / 6 * 100
#   return(passer_rating)
# }
# 
# # Apply the function to each row
# no_na_opp_team$passer_rating <- mapply(calculate_passer_rating, 
#                                        no_na_opp_team$completions, 
#                                        no_na_opp_team$attempts, 
#                                        no_na_opp_team$passing_yards, 
#                                        no_na_opp_team$passing_tds, 
#                                        no_na_opp_team$interceptions)

# tst <- no_na_opp_team %>%
#   filter(season == 2024 & name == 'Jalen Hurts')


#finish transforming data
#------------------------------------------------------------------------------#
#Begin main prediction process

USE_2024_ONLY <- 1

if (USE_2024_ONLY == 1) {
  NFL_Weekly_Data <- no_na_opp_team %>%
    filter(season == max(season)) %>%
    arrange(name, week)
  
  NFL_Weekly_Data <- NFL_Weekly_Data %>%
    group_by(name) %>%
    mutate(next_week_ppr_points = lead(fantasy_points_ppr)) %>%
    ungroup()
  
  NFL_Weekly_Data <- NFL_Weekly_Data %>%
    group_by(name) %>%
    mutate(next_week_opponent_team = lead(opponent_team)) %>%
    ungroup()
  
} else {
  NFL_Weekly_Data <- no_na_opp_team %>%
    arrange(name, season, week)
  
  NFL_Weekly_Data <- NFL_Weekly_Data %>%
    group_by(name, season) %>%
    mutate(next_week_ppr_points = lead(fantasy_points_ppr)) %>%
    ungroup()
  
  NFL_Weekly_Data <- NFL_Weekly_Data %>%
    group_by(name, season) %>%
    mutate(next_week_opponent_team = lead(opponent_team)) %>%
    ungroup()
}





################################################################################
# url <- "https://www.pro-football-reference.com/years/2024/opp.htm"
# web_page <- read_html(url)
# team_def <- web_page %>%
#   html_node("table") %>%  
#   html_table(fill = TRUE) 
# 
# colnames(team_def) <- team_def[1, ] 
# team_def <- team_def[-1, ]  
# 
# team_def_df <- as.data.frame(team_def)
# 
# any(duplicated(names(team_def_df)))
# 
# names(team_def_df) <- make.unique(names(team_def_df))
# 
# team_def_df_filtered <- team_def_df %>%
#   filter(Rk != '')
# 
# 
# team_def_df_PERGAME <- team_def_df_filtered
# team_def_df_PERGAME[, 3:28] <- lapply(team_def_df_filtered[, 3:28], as.numeric)
# 
# 
# team_def_df_filtered[, 3:28] <- lapply(team_def_df_filtered[, 3:28], as.numeric)
# team_def_df_PERGAME[, 3:28] <- team_def_df_filtered[, 3:28] / team_def_df_filtered$G
# 
# 
# team_def_df_PERGAME$`Y/P` <- team_def_df_filtered$`Y/P`
# team_def_df_PERGAME$`NY/A` <- team_def_df_filtered$`NY/A`
# team_def_df_PERGAME$TD.1 <- team_def_df_filtered$TD.1
# team_def_df_PERGAME$`Y/A` <- team_def_df_filtered$`Y/A`
# team_def_df_PERGAME$`Sc%` <- team_def_df_filtered$`Sc%`
# team_def_df_PERGAME$`TO%` <- team_def_df_filtered$`TO%`
# team_def_df_PERGAME$G <- team_def_df_filtered$G
# 
# colnames(team_def_df_PERGAME) <- c("Rank", "Team", "Games", "PointsAllowed_pg", "YardsAllowed_pg", 
#                                    "OffensivePlays_pg", "YardsPerOffensivePlay_pg", "Turnovers_pg", "FumblesLost_pg", 
#                                    "FirstDowns_pg", "Completions_pg", "Attempts_pg", "PassingYards_pg", 
#                                    "PassingTouchdowns_pg", "Interceptions_pg", "PassingNetYards_pa", 
#                                    "PassingFirstDowns_pg", "RushingAttempts_pg", "RushingYards_pg", 
#                                    "RushingTouchdowns_pg", "RushingYards_pa", 
#                                    "RushingFirstDowns_pg", "Penalties_pg", "PenaltyYards_pg", 
#                                    "PenaltyFirstDowns_pg", "OffensiveScore_perc", "Turnover_perc", 
#                                    "EXP")
# str(team_def_df_PERGAME)
#################################################################################


teams <- c("Arizona Cardinals", "Atlanta Falcons", "Baltimore Ravens", "Buffalo Bills", 
           "Carolina Panthers", "Chicago Bears", "Cincinnati Bengals", "Cleveland Browns", 
           "Dallas Cowboys", "Denver Broncos", "Detroit Lions", "Green Bay Packers", 
           "Houston Texans", "Indianapolis Colts", "Jacksonville Jaguars", "Kansas City Chiefs", 
           "Las Vegas Raiders", "Los Angeles Chargers", "Los Angeles Rams", "Miami Dolphins", 
           "Minnesota Vikings", "New England Patriots", "New Orleans Saints", "New York Giants", 
           "New York Jets", "Philadelphia Eagles", "Pittsburgh Steelers", "San Francisco 49ers", 
           "Seattle Seahawks", "Tampa Bay Buccaneers", "Tennessee Titans", "Washington Commanders")

abbreviations <- c("ARI", "ATL", "BAL", "BUF", 
                   "CAR", "CHI", "CIN", "CLE", 
                   "DAL", "DEN", "DET", "GB", 
                   "HOU", "IND", "JAX", "KC", 
                   "LV", "LAC", "LA", "MIA", 
                   "MIN", "NE", "NO", "NYG", 
                   "NYJ", "PHI", "PIT", "SF", 
                   "SEA", "TB", "TEN", "WAS")

nfl_teams_df <- data.frame(Team = teams, Abbreviation = abbreviations, stringsAsFactors = FALSE)


defense_nfl_stats_query <- "SELECT * FROM `nfl-data-434817.nfl_db.defense_nfl_data` where processdate = (select max(processdate) from `nfl-data-434817.nfl_db.defense_nfl_data`)"

defense_nfl_stats_query_query_results <- bq_project_query(project_id, defense_nfl_stats_query)

team_def_df_PERGAME <- bq_table_download(defense_nfl_stats_query_query_results)
team_def_df_PERGAME <- dplyr::select(team_def_df_PERGAME, -processdate)



team_def_df_PERGAME <- team_def_df_PERGAME %>% left_join(nfl_teams_df, by=c("Team"))

team_def_df_PERGAME <- team_def_df_PERGAME %>%
  dplyr::rename(next_week_opponent_team = Abbreviation)


weekly_data_with_def_stats <- NFL_Weekly_Data %>%
  left_join(team_def_df_PERGAME, by='next_week_opponent_team')

weekly_data_with_def_stats <- weekly_data_with_def_stats %>%
  arrange(name, week)

weekly_data_with_def_stats <- weekly_data_with_def_stats[is.finite(weekly_data_with_def_stats$ypr), ]

weekly_data_with_def_stats <- weekly_data_with_def_stats %>%
  dplyr::select(-next_week_opponent_team, everything(), next_week_opponent_team)





weekly_data_with_def_stats_with_rolling_avg <- weekly_data_with_def_stats %>%
  arrange(name, week) %>%
  group_by(name) %>%
  mutate(
    first_row = row_number() == 1,
    passing_air_yards_avg = ifelse(week >= 3 & !first_row, rollmean(passing_air_yards, k = 3, fill = NA, align = "right"), passing_air_yards),
    passing_yards_after_catch_avg = ifelse(week >= 3 & !first_row, rollmean(passing_yards_after_catch, k = 3, fill = NA, align = "right"), passing_yards_after_catch),
    carries_avg = ifelse(week >= 3 & !first_row, rollmean(carries, k = 3, fill = NA, align = "right"), carries),
    rushing_yards_avg = ifelse(week >= 3 & !first_row, rollmean(rushing_yards, k = 3, fill = NA, align = "right"), rushing_yards),
    rushing_tds_avg = ifelse(week >= 3 & !first_row, rollmean(rushing_tds, k = 3, fill = NA, align = "right"), rushing_tds),
    rushing_first_downs_avg = ifelse(week >= 3 & !first_row, rollmean(rushing_first_downs, k = 3, fill = NA, align = "right"), rushing_first_downs),
    receiving_first_downs_avg = ifelse(week >= 3 & !first_row, rollmean(receiving_first_downs, k = 3, fill = NA, align = "right"), receiving_first_downs),
    receiving_2pt_conversions_avg = ifelse(week >= 3 & !first_row, rollmean(receiving_2pt_conversions, k = 3, fill = NA, align = "right"), receiving_2pt_conversions),
    target_share_avg = ifelse(week >= 3 & !first_row, rollmean(target_share, k = 3, fill = NA, align = "right"), target_share),
    air_yards_share_avg = ifelse(week >= 3 & !first_row, rollmean(air_yards_share, k = 3, fill = NA, align = "right"), air_yards_share),
    td_percentage_avg = ifelse(week >= 3 & !first_row, rollmean(td_percentage, k = 3, fill = NA, align = "right"), td_percentage),
    completions_avg = ifelse(week >= 3 & !first_row, rollmean(completions, k = 3, fill = NA, align = "right"), completions),
    attempts_avg = ifelse(week >= 3 & !first_row, rollmean(attempts, k = 3, fill = NA, align = "right"), attempts),
    passing_yards_avg = ifelse(week >= 3 & !first_row, rollmean(passing_yards, k = 3, fill = NA, align = "right"), passing_yards),
    passing_tds_avg = ifelse(week >= 3 & !first_row, rollmean(passing_tds, k = 3, fill = NA, align = "right"), passing_tds),
    interceptions_avg = ifelse(week >= 3 & !first_row, rollmean(interceptions, k = 3, fill = NA, align = "right"), interceptions),
    passing_first_downs_avg = ifelse(week >= 3 & !first_row, rollmean(passing_first_downs, k = 3, fill = NA, align = "right"), passing_first_downs),
    receptions_avg = ifelse(week >= 3 & !first_row, rollmean(receptions, k = 3, fill = NA, align = "right"), receptions),
    targets_avg = ifelse(week >= 3 & !first_row, rollmean(targets, k = 3, fill = NA, align = "right"), targets),
    receiving_yards_avg = ifelse(week >= 3 & !first_row, rollmean(receiving_yards, k = 3, fill = NA, align = "right"), receiving_yards),
    receiving_tds_avg = ifelse(week >= 3 & !first_row, rollmean(receiving_tds, k = 3, fill = NA, align = "right"), receiving_tds),
    fantasy_points_ppr_avg = ifelse(week >= 3 & !first_row, rollmean(fantasy_points_ppr, k = 3, fill = NA, align = "right"), fantasy_points_ppr),
    total_yards_avg = ifelse(week >= 3 & !first_row, rollmean(total_yards, k = 3, fill = NA, align = "right"), total_yards),
    touches_avg = ifelse(week >= 3 & !first_row, rollmean(touches, k = 3, fill = NA, align = "right"), touches),
    comp_percentage_avg = ifelse(week >= 3 & !first_row, rollmean(comp_percentage, k = 3, fill = NA, align = "right"), comp_percentage),
    tot_touchdowns = rushing_tds + receiving_tds + passing_tds,
    tot_touchdowns_avg = ifelse(week >= 3 & !first_row, rollmean(tot_touchdowns, k = 3, fill = NA, align = "right"), tot_touchdowns)
  ) %>%
  dplyr::select(-first_row)



weekly_data_with_def_stats_with_rolling_avg_new <- na.omit(weekly_data_with_def_stats_with_rolling_avg[!is.na(weekly_data_with_def_stats_with_rolling_avg$next_week_ppr_points), ])





ppr_lm_model <- lm(next_week_ppr_points ~ completions + attempts + passing_yards + passing_tds + interceptions + sacks + sack_yards + sack_fumbles + 
                 sack_fumbles_lost + passing_air_yards + passing_yards_after_catch + passing_first_downs + passing_2pt_conversions + carries + rushing_yards + rushing_tds + 
                 rushing_fumbles + rushing_fumbles_lost + rushing_first_downs + rushing_2pt_conversions + receptions + targets + receiving_yards + receiving_tds + receiving_fumbles + receiving_fumbles_lost + 
                 receiving_air_yards + receiving_yards_after_catch + receiving_first_downs + receiving_2pt_conversions + target_share + air_yards_share + fantasy_points + 
                 fantasy_points_ppr + total_yards + ypa + ypc + ypr + touches + count + comp_percentage + pass_td_percentage + int_percentage + rush_td_percentage + 
                 rec_td_percentage + total_tds + td_percentage + passer_rating + PointsAllowed_pg + YardsAllowed_pg + OffensivePlays_pg + 
                 YardsPerOffensivePlay_pg + Turnovers_pg + FumblesLost_pg + FirstDowns_pg + Completions_pg + Attempts_pg + PassingYards_pg + PassingTouchdowns_pg + 
                 Interceptions_pg + PassingNetYards_pa + PassingFirstDowns_pg + RushingAttempts_pg + RushingYards_pg + RushingTouchdowns_pg + RushingYards_pa + 
                 RushingFirstDowns_pg + Penalties_pg + PenaltyYards_pg + PenaltyFirstDowns_pg + OffensiveScore_perc + Turnover_perc + EXP + 
                 passing_air_yards_avg + passing_yards_after_catch_avg + carries_avg + rushing_yards_avg + rushing_tds_avg + rushing_first_downs_avg + receiving_first_downs_avg + receiving_2pt_conversions_avg + 
                 target_share_avg + air_yards_share_avg + td_percentage_avg + completions_avg + attempts_avg + passing_yards_avg + passing_tds_avg + interceptions_avg + 
                 passing_first_downs_avg + receptions_avg + targets_avg + receiving_yards_avg + receiving_tds_avg + fantasy_points_ppr_avg + total_yards_avg + 
                 touches_avg + comp_percentage_avg + tot_touchdowns + tot_touchdowns_avg, data = weekly_data_with_def_stats_with_rolling_avg_new)
summary(ppr_lm_model)

ppr_lm_model_NONA <- lm(next_week_ppr_points ~ completions + attempts + passing_yards + passing_tds + interceptions + sacks + sack_yards + sack_fumbles + 
                     sack_fumbles_lost + passing_air_yards + passing_yards_after_catch + passing_first_downs + passing_2pt_conversions + carries + rushing_yards + rushing_tds + 
                     rushing_fumbles + rushing_fumbles_lost + rushing_first_downs + rushing_2pt_conversions + receptions + targets + receiving_yards + receiving_tds + receiving_fumbles + receiving_fumbles_lost + 
                     receiving_air_yards + receiving_yards_after_catch + receiving_first_downs + receiving_2pt_conversions + target_share + air_yards_share + fantasy_points + 
                     ypa + ypc + ypr + comp_percentage + pass_td_percentage + int_percentage + rush_td_percentage + 
                     rec_td_percentage + td_percentage + passer_rating + PointsAllowed_pg + YardsAllowed_pg + OffensivePlays_pg + 
                     YardsPerOffensivePlay_pg + Turnovers_pg + FumblesLost_pg + FirstDowns_pg + Completions_pg + Attempts_pg + PassingYards_pg + PassingTouchdowns_pg + 
                     PassingNetYards_pa + PassingFirstDowns_pg + RushingAttempts_pg + RushingTouchdowns_pg + RushingYards_pa + 
                     RushingFirstDowns_pg + Penalties_pg + PenaltyYards_pg + OffensiveScore_perc + Turnover_perc + EXP + 
                     passing_air_yards_avg + passing_yards_after_catch_avg + carries_avg + rushing_yards_avg + rushing_tds_avg + rushing_first_downs_avg + receiving_first_downs_avg + 
                     target_share_avg + air_yards_share_avg + td_percentage_avg + receptions_avg + targets_avg, data = weekly_data_with_def_stats_with_rolling_avg_new)
summary(ppr_lm_model_NONA)

ppr_lm_model_NONA_reduced <- lm(next_week_ppr_points ~ passing_tds + interceptions + sacks + sack_yards + 
                          carries + 
                          targets + 
                          target_share + air_yards_share + 
                          comp_percentage + int_percentage + 
                          passer_rating + YardsAllowed_pg + 
                          FumblesLost_pg + air_yards_share_avg + receptions_avg, data = weekly_data_with_def_stats_with_rolling_avg_new)
summary(ppr_lm_model_NONA_reduced)




# ppr_lm_model_NONA_reduced_under05 <- lm(next_week_ppr_points ~ passing_tds + sacks + 
#                                   carries +
#                                   YardsAllowed_pg + receptions_avg, data = weekly_data_with_def_stats_with_rolling_avg_new)
# summary(ppr_lm_model_NONA_reduced_under05)



plot(ppr_lm_model_NONA_reduced)

vif(ppr_lm_model_NONA_reduced)


ppr_lm_model_NONA_reduced_VIFOPTIMIZED <- lm(next_week_ppr_points ~ passing_tds + interceptions + sacks + sack_yards + 
                                  carries + 
                                  targets + 
                                  target_share +
                                  comp_percentage + int_percentage + 
                                  passer_rating + YardsAllowed_pg + 
                                  FumblesLost_pg + air_yards_share_avg + receptions_avg, data = weekly_data_with_def_stats_with_rolling_avg_new)
summary(ppr_lm_model_NONA_reduced_VIFOPTIMIZED)
vif(ppr_lm_model_NONA_reduced_VIFOPTIMIZED)

ppr_lm_model_NONA_reduced_VIFOPTIMIZED <- lm(next_week_ppr_points ~ carries + 
                                               comp_percentage + 
                                               YardsAllowed_pg + 
                                               receptions_avg, data = weekly_data_with_def_stats_with_rolling_avg_new)
summary(ppr_lm_model_NONA_reduced_VIFOPTIMIZED)
vif(ppr_lm_model_NONA_reduced_VIFOPTIMIZED)
plot(ppr_lm_model_NONA_reduced_VIFOPTIMIZED)











weekly_data_with_def_stats_with_rolling_avg_new_allPos <- subset(weekly_data_with_def_stats_with_rolling_avg_new, next_week_ppr_points > 0)

boxcox_result <- boxcox(next_week_ppr_points ~ carries + 
                          comp_percentage + 
                          YardsAllowed_pg + 
                          receptions_avg, data = weekly_data_with_def_stats_with_rolling_avg_new_allPos)
optimal_lambda <- boxcox_result$x[which.max(boxcox_result$y)]
print(optimal_lambda)
ppr_model_lambda <- lm(next_week_ppr_points^(optimal_lambda) ~ carries + 
                         comp_percentage + 
                         YardsAllowed_pg + 
                         receptions_avg, data = weekly_data_with_def_stats_with_rolling_avg_new_allPos)
summary(ppr_model_lambda)
plot(ppr_model_lambda)
vif(ppr_model_lambda)



test_data <- weekly_data_with_def_stats_with_rolling_avg %>%
  filter(is.na(next_week_ppr_points)) 

max_week_test_data <- max(test_data$week)
test_data <- test_data %>%
  filter(week == max_week_test_data)




schedule_nfl_data_query <- "SELECT * FROM `nfl-data-434817.nfl_db.schedule_nfl_data_new` where processdate = (select max(processdate) from `nfl-data-434817.nfl_db.schedule_nfl_data_new`)"

schedule_nfl_data_query_results <- bq_project_query(project_id, schedule_nfl_data_query)

schedule_nfl_data <- bq_table_download(schedule_nfl_data_query_results)
#View(schedule_nfl_data)

schedule_nfl_data_next_week <- schedule_nfl_data %>%
  filter(week == (max_week_test_data + 1))

schedule_nfl_data_next_week <- dplyr::select(schedule_nfl_data_next_week, week, away_team, home_team)

new_schedule_df <- bind_rows(
  schedule_nfl_data_next_week %>% dplyr::select(week, team = home_team, opp_team = away_team),
  schedule_nfl_data_next_week %>% dplyr::select(week, team = away_team, opp_team = home_team)
)
new_schedule_df <- new_schedule_df %>%
  distinct(.keep_all = TRUE)



test_data_cleaned <- test_data %>% dplyr::select_if(~ !all(is.na(.)))

test_data_withOppTeam <- test_data_cleaned %>% left_join(dplyr::select(new_schedule_df, -week), by="team")

test_data_withOppTeam <- test_data_withOppTeam %>%
  dplyr::rename(next_week_opponent_team = opp_team)


test_data_withOppTeamSTATS <- test_data_withOppTeam %>%
  left_join(team_def_df_PERGAME, by='next_week_opponent_team')


test_data_withOppTeamSTATS$next_week_ppr_points <- NA



selected_columns_next_week_model <- c("name", "position", "team", "season", "week", "next_week_opponent_team", "carries", 
                                        "comp_percentage", "YardsAllowed_pg","receptions_avg", "next_week_ppr_points")
test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF <- subset(test_data_withOppTeamSTATS)[, selected_columns_next_week_model]
#View(test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF)


REAL_selected_columns_next_week_model <- c("carries", "comp_percentage", "YardsAllowed_pg","receptions_avg")
REAL_test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF <- subset(test_data_withOppTeamSTATS)[, REAL_selected_columns_next_week_model]
#View(REAL_test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF)




predict(ppr_lm_model_NONA_reduced_VIFOPTIMIZED, newdata = REAL_test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF, interval = "prediction")
test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF$Predicted_Next_week_PPR_PTS_lwr <- (predict(ppr_lm_model_NONA_reduced_VIFOPTIMIZED, newdata = REAL_test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF, interval = "prediction"))[,"lwr"]
test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF$Predicted_Next_week_PPR_PTS <- (predict(ppr_lm_model_NONA_reduced_VIFOPTIMIZED, newdata = REAL_test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF, interval = "prediction"))[,"fit"]
test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF$Predicted_Next_week_PPR_PTS_upr <- (predict(ppr_lm_model_NONA_reduced_VIFOPTIMIZED, newdata = REAL_test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF, interval = "prediction"))[,"upr"]
test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF <- test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF %>%
  arrange(desc(Predicted_Next_week_PPR_PTS))

test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF <- dplyr::select(test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF, name, position, team, season, week, next_week_opponent_team, Predicted_Next_week_PPR_PTS)
test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF$rank <- order(desc(test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF$Predicted_Next_week_PPR_PTS))
#View(test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF)

test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF <- test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF %>%
  ungroup()



colors <- hue_pal(direction = -1)(3)

test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF %>%
  select(rank, name, team, position, next_week_opponent_team, Predicted_Next_week_PPR_PTS) %>%
  gt() %>%
  cols_label(team = "Team",
             rank = "",
             next_week_opponent_team = "Opp",
             Predicted_Next_week_PPR_PTS = "Points")  %>%
  tab_style(
    style = list(cell_text(style="italic")),
    locations = cells_body(columns = name)
  ) %>%
  tab_options(heading.title.font.size = px(24),
              heading.subtitle.font.size = px(12)) %>%
  tab_header(
    title = paste("Predicted Fantasy Points Week", max_week_test_data + 1),
    subtitle = "PPR"
  ) %>%
  gt_color_rows(Predicted_Next_week_PPR_PTS, palette = c("white", colors[2])) %>%
  gt_theme_espn()


write_xlsx(test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF, "C:/Users/natha/OneDrive/Desktop/pprpredictions09-25.xlsx")

#------------------------------------------------------------------------------#
#Now predict with Lambda model


selected_columns_next_week_model_LAMBDA <- c("name", "position", "team", "season", "week", "next_week_opponent_team", "carries", 
                                        "comp_percentage", 
                                        "YardsAllowed_pg", 
                                        "receptions_avg", "next_week_ppr_points")
test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA <- subset(test_data_withOppTeamSTATS)[, selected_columns_next_week_model_LAMBDA]
#View(test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA)


REAL_selected_columns_next_week_model_LAMBDA <- c("carries", "comp_percentage", "YardsAllowed_pg", "receptions_avg")
REAL_test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA <- subset(test_data_withOppTeamSTATS)[, REAL_selected_columns_next_week_model_LAMBDA]
#View(REAL_test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA)




predict(ppr_model_lambda, newdata = REAL_test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA, interval = "prediction")^(optimal_lambda)
test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA$Predicted_Next_week_PPR_PTS_lwr <- ((predict(ppr_model_lambda, newdata = REAL_test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA, interval = "prediction"))^(1/optimal_lambda))[,"lwr"]
test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA$Predicted_Next_week_PPR_PTS <- ((predict(ppr_model_lambda, newdata = REAL_test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA, interval = "prediction"))^(1/optimal_lambda))[,"fit"]
test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA$Predicted_Next_week_PPR_PTS_upr <- ((predict(ppr_model_lambda, newdata = REAL_test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA, interval = "prediction"))^(1/optimal_lambda))[,"upr"]
test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA <- test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA %>%
  arrange(desc(Predicted_Next_week_PPR_PTS))

test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA <- dplyr::select(test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA, name, position, team, season, week, next_week_opponent_team, Predicted_Next_week_PPR_PTS)
test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA$rank <- order(desc(test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA$Predicted_Next_week_PPR_PTS))
#View(test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA)


test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA <- test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA %>%
  ungroup()



colors <- hue_pal(direction = -1)(3)


test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA %>%
  select(rank, name, team, position, next_week_opponent_team, Predicted_Next_week_PPR_PTS) %>%
  gt() %>%
  cols_label(team = "Team",
             rank = "",
             next_week_opponent_team = "Opp",
             Predicted_Next_week_PPR_PTS = "Points")  %>%
  tab_style(
    style = list(cell_text(style="italic")),
    locations = cells_body(columns = name)
  ) %>%
  tab_options(heading.title.font.size = px(24),
              heading.subtitle.font.size = px(12)) %>%
  tab_header(
    title = paste("Predicted Fantasy Points Week", max_week_test_data + 1),
    subtitle = "PPR"
  ) %>%
  gt_color_rows(Predicted_Next_week_PPR_PTS, palette = c("white", colors[2])) %>%
  gt_theme_espn()



write_xlsx(test_data_withOppTeamSTATS_SELECTEDCOLUMNS_DF_LAMBDA, "C:/Users/natha/OneDrive/Desktop/pprpredictions09-25-lambda.xlsx")
