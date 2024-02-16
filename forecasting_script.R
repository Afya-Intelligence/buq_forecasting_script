#script for training, testing and forecasting cleaned data set

#Project:"BUQ-PAM"
#Author:"Afya Intelligence"
#writer:"Mbwambo"
#year:"2023"
#Version:"September 2023"

#load the required packages
library(tidyverse)
library(prophet)
library(modeltime)
library(timetk)
library(tidymodels)
library (mongolite)
library(lubridate)
library(doMC)
doMC::registerDoMC(cores = 2)
# Set scipen to a large value to disable scientific notation
options(scipen = 999)
# Set offset for preventing negative log values
offset <- 1e-10



# Pull list  of completed forecasts
con <- mongo(
  collection = "pipeline_forecasts",
  db="in-supply",
  url = "mongodb://admin:weloveafya@157.230.179.1:27017")

forecasts_pipeline <- '[{
  "$group": {
   "_id": {
    "facility_code": "$facility_code"
   },
  "count": {"$sum":1}
  }
}]'
completed_facilities <- con$aggregate(forecasts_pipeline)

con$disconnect()

# Pull list of pending cleaned data
con <- mongo(
  collection = "facility-cleaned",
  db="in-supply",
  url = "mongodb://admin:weloveafya@157.230.179.1:27017")


facilities_pipeline <- '[{
  "$group": {
   "_id": {
    "facility_code": "$facility_code"
   },
  "count": {"$sum":1}
  }
}]'
pending_facilities <- con$aggregate(facilities_pipeline)

con$disconnect()




# write the query for the data we want to retrieve ------------------------
#
# # Define the aggregation pipeline to project the desired columns and exclude "_id"
# data_pipeline <- '[{
#     "$project": {
#       "date": 1,
#       "program": 1,
#       "product_code": 1,
#       "item_description": 1,
#       "facility_code": 1,
#       "adj_consumption_in_packs": 1,
#       "timestamp": 1,
#       "_id": 0
#     }
# }]'
#
#
# # Execute the aggregation query
# data <- con$aggregate(data_pipeline)

# path ~/Projects/AfyaIntel/JSI insupply/consumption_data_quantification_tanzania/scripts/automation_script/chamwino_data.csv

# Determine the fiscal year start and end dates
fiscal_year_start <- as.Date("July 1",format = "%B %d")
fiscal_year_end <- as.Date("June 30", format = "%B %d")

# steps to forecast in the future

forecasting_years=3
forecasting_steps=forecasting_years*6

# how many records for testing
length_test=3

# function to back transform the a log column

# Define the back-transformation function
back_transform_log_column <- function(log_transformed_values, offset = 1e-10) {
  original_values <- round(exp(log_transformed_values) - offset)
  return(original_values)
}



######################################################################################


# Query unique facility codes from the "facilities" dataframe


if (length(pending_facilities) > 0) {
  pending_facilities <- pending_facilities |> pull(`_id`) |> pull(facility_code)
} else {
  print("pending_facilities is empty")
}


if (length(completed_facilities) > 0) {
  completed_facilities <- completed_facilities |> pull(`_id`) |> pull(facility_code)
} else {
  print("completed_facilities is empty")
}



# Loop through unique facility codes
foreach (facility_code = pending_facilities) %dopar% {

  if (facility_code %in% completed_facilities) {
    next  # Skip this iteration and move to the next facility
  }


  sink(file.path(paste0(getwd(), "/logs/",facility_code, ".log")))



  cat("Pulling data for Facility Code:", facility_code, "\n")

  con <- mongo(
    collection = "facility-cleaned",
    db="in-supply",
    url = "mongodb://admin:weloveafya@157.230.179.1:27017")

  # Define the query to filter by facility_code and find the latest timestamp
  query <- paste0('{"facility_code": "', facility_code, '"}')
  # Sort by timestamp in descending order to get the latest record
  sort_order <- '{"timestamp": -1}'
  # Execute the find query with the specified filter and sort order
  data <- con$find(query, sort = sort_order)
  # Close the MongoDB connection
  con$disconnect()

  print("Processing...")

  data <- data |>
    mutate(date=as.Date(date)) |>
    mutate(id_var=paste0(product_code,"_",item_description,"_",facility_code)) |>
    select(id_var, date, adj_consumption_in_packs)


  # record important date records in the dataset
  length_actual <- data |> distinct(date) |> nrow()
  last_data_date <- max(data$date)


  # Calculate the length of the future based on the last data date
  if (last_data_date < fiscal_year_start) {
    # If the last data date is before July 1, set length_future to 12 (covers full fiscal year)
    length_future <- forecasting_steps
  } else {

    # If the last data date is on or after July 1, set length_future to the remaining months in the fiscal year
    interval_result <- lubridate::interval(last_data_date, (fiscal_year_end + years(1))) |> lubridate::as.duration()
    reports_that_year <- floor(interval_result/dmonths(2))
    length_future <- (forecasting_steps-6)+reports_that_year
  }


  # Generate bimonthly dates for the future
  future_dates <- seq((last_data_date + months(2)), by = "2 months", length.out = length_future)

  # Create a data frame with the extended dates
  future_df <- data.frame(
    id_var = rep(unique(data$id_var), each = length_future),
    date = rep(future_dates, times = length(unique(data$id_var))),
    adj_consumption_in_packs = NA  # You can fill this with appropriate values if needed
  )

  # Reset row names
  rownames(future_df) <- NULL

  # Combine the extended data with the original data
  dt_ext <- rbind(data, future_df) |>
    mutate(month=factor(month(date,label=T), ordered = T),
           numeric_date=as.numeric(date))

  # transform target variable to log scale
  dt_ext$adj_consumption_log <- log(dt_ext$adj_consumption_in_packs + offset)


  #data partition set for training and test
  # nest the data by product_code
  dt_nested <- dt_ext |>
    modeltime::nest_timeseries(.id_var = id_var,
                               .length_future = length_future, # initially hard coded to 6
                               .length_actual = length_actual) |>
    modeltime::split_nested_timeseries(.length_test = length_test) # 3 time points will be used for testing and the rest for training of the models

  # extract train and test sets
  train <- extract_nested_train_split(dt_nested)
  test <- extract_nested_test_split(dt_nested)

  print("modelling...")

  # Set model workflows
  arima_workflow <- workflow() |>
    add_model(
      arima_reg() |>
        set_engine("auto_arima") |>
        set_mode("regression")
    ) |>
    add_recipe(
      recipe(adj_consumption_log~date, data = train)
    )

  #-------------------------------------------------------------------------ARIMABOOSTED
  # arima_boosted_workflow <- workflow() |>
  #   add_model(
  #     arima_boost(
  #       min_n = 2,
  #       learn_rate = 0.015
  #     ) |>
  #       set_engine("auto_arima_xgboost") |>
  #       set_mode("regression")
  #   ) |>
  #   add_recipe(
  #     recipe(adj_consumption_log~date+numeric_date+month, data= train)
  #   )

  #------------------------------------------------------------------------PROPHET
  # prophet_workflow <- workflow() |>
  #   add_model(
  #     prophet_reg(growth = "linear", seasonality_yearly = F,
  #                 changepoint_range = 0.9) |>
  #       set_engine("prophet") |>
  #       set_mode("regression")
  #   ) |>
  #   add_recipe(
  #     recipe(adj_consumption_log~date+month, data = train)
  #   )

  #------------------------------------------------------------------------XGBOOST
  xgboost_workflow <- workflow() |>
    add_model(
      boost_tree() |>
        set_engine("xgboost")|>
        set_mode("regression")
    ) |>
    add_recipe(
      recipe(adj_consumption_log~date+month, data = train)  |>
        step_timeseries_signature(date) |>
        # there are no nominal predictors
        step_dummy(all_nominal_predictors(), one_hot = TRUE)  |>
        step_rm(date)  |>
        step_zv(all_predictors())
    )
  #----------------------------------------------------------------------------ETS
  ets_workflow <- exp_smoothing() |>
    set_engine(engine = "ets") |>
    fit(adj_consumption_log~date+month, data = train)


  #-----------------------------------------------------------model time take over
  set.seed(2402)

  print("Comparing model accuracy & forecasting...")


  parallel_start(2,.method = "parallel")

  nested_modeltime_tbl <- modeltime_nested_fit(
    nested_data = dt_nested,
    model_list = list(arima_workflow,ets_workflow,xgboost_workflow),
    control = control_nested_fit(
      verbose = T,
      allow_par = T
    )
  )


  accuracies <- nested_modeltime_tbl |>
    extract_nested_test_accuracy()

  #selecting best model based on MPE
  best_model_tbl <- nested_modeltime_tbl |>
    modeltime_nested_select_best(metric = "rmse",
                                 minimize = T,
                                 filter_test_forecasts = T)
  best_model_tbl |>
    extract_nested_best_model_report() |>
    count(.model_desc)

  best_model_tbl_refit <- best_model_tbl |>
    modeltime_nested_refit(
      control = control_nested_refit(
        verbose = T,
        allow_par = T
      ))

  parallel_stop()

  forecasts <-  best_model_tbl_refit  |>
    extract_nested_future_forecast() |>
    filter(.key== "prediction") |>
    mutate_if(is.numeric, ~ back_transform_log_column(.)) |>
    select(-c(.model_id, .key)) |>
    select(
      date=.index,
      id_var,
      forecast=.value,
      conf_low=.conf_lo,
      conf_high=.conf_hi,
      model_desc=.model_desc
    ) |>
    separate(id_var,into = c("product_code", "item_description", "facility_code"), sep = "_") |>
    mutate(quarter = tsibble::yearquarter(date, fiscal_start = 7),
           fiscal_year = tsibble::fiscal_year(quarter),
           financial_year = paste0(fiscal_year-1, "-", fiscal_year)) |>
    relocate(financial_year) |>
    select(-fiscal_year, -quarter)


  # Create a connection to MongoDB
  con <- mongo(
    collection = "pipeline_forecasts",
    db="in-supply",
    url = "mongodb://admin:weloveafya@157.230.179.1:27017")

  # Test the connection
  #send the data
  con$insert(forecasts)

  #close the connection
  con$disconnect()

  print(paste0("Data for ", facility_code, " uploaded"))


  sink()
}

