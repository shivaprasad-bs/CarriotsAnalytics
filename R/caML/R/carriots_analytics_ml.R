##################################################################################
#' Default LEARN mechanism of CA datasources
#'
#' Reads the data and build the models
#'###############################################################################
#'@export
learn.ca = function() {
  withCallingHandlers({
    #connect to CA
    con <- caDB::connect.ca()

    #Load data
    df <- con$load()

    #Get the target column name
    target <- con$getParam("TARGET_NAME")

    if(is.null(target))
      stop("No target specified- unable to learn")

    #Perform learn
    modelInfo <- autoClassify(df,target)

    #Add the models
    con$addModel(model = modelInfo$model,label = "autoClassify",description = "Default_AutoClassify_model_from_CA"
                 ,predictors = modelInfo$predictors,metrics = modelInfo$metrics)
  },
  error = function(e) {
    msg = paste(conditionMessage(e), sapply(sys.calls(),function(sc)deparse(sc)[1]), sep="\n   ")
    stop(msg)
  })
}

##################################################################################
#' Default SCORE mechanism of CA datasources
#'
#' Reads learnt models and score the datasource
#'###############################################################################
#'@export
score.ca = function() {
  withCallingHandlers({
    #connect to CA and load data
    con <- caDB::connect.ca()

    #Load Data
    df <- con$load()

    #get the models back
    modelList <- con$getModels();
    if(is.null(modelList))
      stop("No models we got -- EXITING")

    #Defaults have only 1 model
    blackboxModel <- modelList[[1]]$model
    model_name <- modelList[[1]]$name
    model_label <- modelList[[1]]$label

    newDf <- autoClassifyScore(df,blackboxModel)

    #push to CA
    con$update(df = newDf,modelName = model_name, modelLabel = model_label)
  },
  error = function(e) {
    msg = paste(conditionMessage(e), sapply(sys.calls(),function(sc)deparse(sc)[1]), sep="\n   ")
    stop(msg)
  })
}

##################################################################################
#' Default FORECAST mechanism for CA datasources
#'
#' Forecasts the target dimenstion, and saves the models if there any created
#' on this forecast request
#'###############################################################################
#'@export
forecast.ca = function() {
  withCallingHandlers({
    #connect to CA and load data
    con <- caDB::connect.ca()

    #Load Data
    df <- con$load()

    #get the user selected models back from CA
    modelList <- con$getModels();
    blackboxModel <- NULL
    isModelAvailable <- FALSE

    if(!is.null(modelList) && length(modelList) > 0) {
      #Defaults have only 1 model
      blackboxModel <- modelList[[1]]$model
      model_name <- modelList[[1]]$name
      model_label <- modelList[[1]]$label
      isModelAvailable <- TRUE
    }

    #User defined settings in CA

    #Get the target column name
    target <- con$getParam("TARGET_NAME")

    target_label <-  con$getParam("TARGET_LABEL")

    #Get temporal dimension
    temporalDim <- con$getParam("TEMPORAL_DIM")

    #Get forecast step
    forecastStep <- con$getParam("FORECAST_STEP")

    #Get cardinal dims
    cardinalDims <- con$getParam("CARDINAL_DIMS")

    #Actual Forecasting Algorithm
    output <- autoForecast(df,temporalDim,target_label,forecastStep,blackboxModel,cardinalDims)

    if(is.null(output$df))
      stop("Algorithm Failed to generate the forecasted dataframe")

    #Add models only if no models passed <- logic might change
    if(!is.null(output$model) && !isModelAvailable) {

      #Add the models
      mLabel <- "autoForecast"
      con$addModel(model = output$model,label = mLabel,description = "Default_AutoForecast_model_from_CA",
                   metrics = output$accuracy_metrics)
      model_name <- mLabel
      #If there are any special characters, do MD5- same logic as CA App
      if(!grepl("^[a-zA-Z0-9\\s\\(\\)_/]+$",model_name))
        model_name = digest::digest(model_name,"md5",serialize = FALSE)
      model_label <- mLabel
    }else {
      #Nullify the modelList
      print("successfully nullified the models")
      con$deleteModels()
    }

    #push to CA
    con$update(df = output$df,modelName = model_name, modelLabel = model_label)
  },
  error = function(e) {
    msg = paste(conditionMessage(e), sapply(sys.calls(),function(sc)deparse(sc)[1]), sep="\n   ")
    stop(msg)
  })
}

init <- function() {
  library(data.table)
  library(Hmisc)
  require(foreign)
  require(MASS)
  require(pls)
  require(rpart)
  library(jsonlite)
  library(missForest)
  library(caret)
  library(mlbench)
  library(aod)
  library(ggplot2)
  library(pROC)
  require(nnet)
  require(zoo)
  require(lubridate)
  require("R.utils")

  #for forecast
  require(fpp)
  require(forecast)
  library(fBasics)
  library(timeSeries)
  library(xts)
  library(randomForest)
  library(gbm)
  library(arm)
  library(AUC)
  library(fastAdaboost)
  library(LiblineaR)
  library(naivebayes)
  library(caretEnsemble)
  library(moments)
  library(rpart.plot)
  library(RColorBrewer)
  library(party)
  require(stringr)
  library(parsedate)
}

autoClassify <- function(df, col2bclassified) {

  #init
  init()

  #store response
  myresponse <- col2bclassified
  #check the data types
  dat.typ <- capture.output(str(df))

  #Put underscore for column names with spaces
  names(df) <- gsub(" ", "_", names(df))

  column.names <- names(df)
  df.orig <- df # saving the original data frame

  #remove dollar sign from numeric columns. If this leads to na then the column is rest back to default data type
  for(i in 1:ncol(df)){
    df[[column.names[i]]] <- as.numeric(sub(',','', sub('\\$','',df[[column.names[i]]])))
    if(is.na(df[[column.names[i]]])){
      print("Has na, resetting column type to default")
      df[[column.names[i]]] <-  df.orig[[column.names[i]]]
    }else{
      print("No na, column type is set to numeric")
    }
  }
  #Look for the column name passed as regressand, make a list of its content, store in a variable and remove from df
  col.names <- names(df)
  for(a in 1:length(col.names)){
    print(a)
    if(col.names[a] == col2bclassified){
      print(a)
      col2bclassified <- df[[col2bclassified]]
      print("listing is done")
      df[,c(a)] <- NULL
    }
    if(typeof(col2bclassified) == "character"){ #check for response data type and ship with the blackbox
      print("here")
      y.dat.typ <- "lable"
    }else{
      y.dat.typ <- "something else"
    }
  }
  #seperate numeric and non  numeric columns
  df <- as.data.frame(df)
  df.nums <- sapply(df, is.numeric)
  df.num <- as.data.frame(df[ , df.nums])
  #below 2 lines will keep the numeric data frame column names as valid names
  num.names <- names(df.nums)
  names(df.nums) <- c(num.names)
  df.char <- as.data.frame(df[, !(df.nums)])
  #below 2 lines will keep the character data frame column names as valid names
  char.names <- names(df.char)
  names(df.char) <- c(char.names)

  #check and impute
  m <- sapply(df, function(x) sum(is.na(x)))
  for(j in 1:length(m)){
    # print("Here")
    print(j)
    if((m[[j]]) == 0){
      df.imp1 <- df
      next
    }else{
      print("Imputing")
      df.imp <- na.aggregate(df.num)
      df.imp1 <- NULL #df.imp1 will be made later in case of impute
      break
    }
  }
  #df.imp object retains only imputed columns, which are numeric. Join non numeric and numeric only if imputation happened
  #ximp is the imputed table inside df.imp. df.imp is of class type called 'missforest'
  if(exists("df.imp1") && is.data.frame(df.imp1)){
    print ("No impute required")
    delayedAssign("do.next", {next}) #without this r gives an error "Error: no loop for break/next, jumping to top level"
  }else{
    df.imp1 <- cbind(df.imp, df.char)
  }

  #Convert column with type character to factors
  imp.column.names <- names(df.imp1)
  for(k in 1:length(imp.column.names)){
    col.class <- class(df.imp1[[imp.column.names[k]]])
    if(col.class == "character"){
      print("This is a character type")
      df.imp1[[imp.column.names[k]]] <- as.factor(df.imp1[[imp.column.names[k]]])
    }else{
      print("No character types found")
    }

  }
  #remove obvious columns. Target.other can take any col name that needs to be removed.
  attach(df.imp1)
  target.other <- list("^TARGET_","^INDEX","^ID","^serial_no")
  for(n in 1:length(imp.column.names)){
    col.rm <- grep(target.other[n], imp.column.names)

    if(isTRUE(length(col.rm) != 0)){

      for(p in 1:length(col.rm)){
        t1 <- imp.column.names[col.rm[p]]
        df.imp1[t1] <- NULL
      }
    }
  }
  #seperate the factor and non factor columns
  #initialize matrices
  df.imp1.fac <- matrix()
  df.imp1.nofac <- matrix()
  colnames.fac.lst <- vector()
  colnames.nofac.lst <- vector()
  imp.column.names <- names(df.imp1)
  na<-c()

  for(p in 1:length(imp.column.names)){
    if((is.factor(df.imp1[[imp.column.names[p]]]))){
      col.fac <- as.data.frame(df.imp1[[imp.column.names[p]]])
      df.imp1.fac <- cbind(df.imp1.fac,col.fac)
      colnames.fac.lst[p] <- imp.column.names[p]
      print("factor detected")
      colnames.fac.lst <- na.omit(colnames.fac.lst)
      colnames.fac <- c("na",colnames.fac.lst)
      colnames(df.imp1.fac) <- colnames.fac
    }else{
      col.nofac <- as.data.frame(df.imp1[[imp.column.names[p]]])
      df.imp1.nofac <- cbind(df.imp1.nofac,col.nofac)
      colnames.nofac.lst[p] <- imp.column.names[p]
      print("nofactors detected")
      colnames.nofac.lst <- na.omit(colnames.nofac.lst)
      colnames.nofac <- c("na",colnames.nofac.lst)
      colnames(df.imp1.nofac) <- colnames.nofac
    }
  }
  #remove the default na column from both data frames
  df.imp1.fac <- df.imp1.fac[!is.na(names(df.imp1.fac))]
  df.imp1.nofac <- df.imp1.nofac[!is.na(names(df.imp1.nofac))]
  #or
  df.imp1.fac$na <- NULL
  df.imp1.nofac$na <- NULL

  #remove columns with more than 5 levels, only if the columns with factor exists
  colnames.fac <- names(df.imp1.fac)
  df.imp1.lev5 <- matrix()
  if(exists("colnames.fac") && is.data.frame(df.imp1.fac)){
    for(q in 1:length(colnames.fac)){
      lev <- levels(df.imp1.fac[[colnames.fac[q]]])
      if((length(lev)) >=5){
        df.imp1.fac[[colnames.fac[q]]] <- NULL
      }
    }
  }else{
    print("No columns removed, no factors")
    delayedAssign("do.next", {next})
  }

  #consolidated data frame with 5 level and numeric columns
  if(exists("df.imp1.fac") && is.data.frame(df.imp1.fac)){
    df.imp1.lev5.num <- cbind(df.imp1.nofac,df.imp1.fac)
  }else{
    print("No factors at 5 level consolidate loop")
    delayedAssign("do.next", {next})
  }

  #put back the original target flag. Only one column with name target_flag is taken
  if(exists("df.imp1.lev5.num") && is.data.frame(df.imp1.lev5.num)){
    print("Has factors")
  }else{
    print("No factors")
    df.imp1.lev5.num <- df.imp1
  }

  #coerce the regressand back to the df
  df.imp1.lev5.num$col2bclassified <- col2bclassified
  df.imp1.lev5.num$col2bclassified <- as.factor(df.imp1.lev5.num$col2bclassified) #make target a factor
  fac.lev <- levels(df.imp1.lev5.num$col2bclassified)

  if(length(fac.lev) > 2){
    print("Iam multi")
    if(nrow(df.imp1.lev5.num) < 50000){
      mylogit <- multinom(col2bclassified ~ ., data = df.imp1.lev5.num)
    }
    if(nrow(df.imp1.lev5.num) > 50000){
      mylogit <- multinom(col2bclassified ~ ., data = df.imp1.lev5.num,maxit = 20)
    }
  }else{
    print("I am not multi")
    mylogit <- glm(col2bclassified ~ ., data = df.imp1.lev5.num,family = binomial(link="logit"), x=TRUE)
  }

  #remove the regressand from the final data frame
  df.imp1.lev5.num[,ncol(df.imp1.lev5.num)] <- NULL

  #get the list of regressors to be used in the model
  col.name.final <- names(df.imp1.lev5.num)

  #check for response data type and ship with the blackbox
  # (
  # This is done above
  # )

  #put the model and colnames into a list
  mylogit <- serialize(mylogit,NULL)
  blackbox <- list(mylogit,fac.lev,dat.typ,y.dat.typ,myresponse)
  model.colnames.lev <- list(blackbox,col.name.final)
  names(model.colnames.lev) <- c("model","predictors")

  return(model.colnames.lev)
}

############  OUT of the box Score function ########
autoClassifyScore <- function(df.test, mod.lev.typ,posteriorCutoff) {

  #init
  init()

  df.test.save <- df.test

  df<- df.test

  #check the data types
  dat.typ <- capture.output(str(df))

  #Put underscore for column names with spaces
  names(df) <- gsub(" ", "_", names(df))

  column.names <- names(df)
  df.orig <- df # saving the original data frame

  #remove dollar sign from numeric columns. If this leads to na then the column is rest back to default data type
  for(i in 1:ncol(df)){
    df[[column.names[i]]] <- as.numeric(sub(',','', sub('\\$','',df[[column.names[i]]])))
    if(is.na(df[[column.names[i]]])){
      print("Has na, resetting column type to default")
      df[[column.names[i]]] <-  df.orig[[column.names[i]]]
    }else{
      print("No na, column type is set to numeric")
    }
  }

  #seperate numeric and non  numeric columns
  df <- as.data.frame(df)
  df.nums <- sapply(df, is.numeric)
  df.num <- as.data.frame(df[ , df.nums])
  #below 2 lines will keep the numeric data frame column names as valid names
  num.names <- names(df.nums)
  names(df.nums) <- c(num.names)
  df.char <- as.data.frame(df[, !(df.nums)])
  #below 2 lines will keep the character data frame column names as valid names
  char.names <- names(df.char)
  names(df.char) <- c(char.names)

  #check and impute
  m <- sapply(df, function(x) sum(is.na(x)))
  for(j in 1:length(m)){
    # print("Here")
    print(j)
    if((m[[j]]) == 0){
      df.imp1 <- df
      next
    }else{
      print("Imputing")
      df.imp <- na.aggregate(df.num)
      df.imp1 <- NULL #df.imp1 will be made later in case of impute
      break
    }
  }
  #df.imp object retains only imputed columns, which are numeric. Join non numeric and numeric only if imputation happened
  #ximp is the imputed table inside df.imp. df.imp is of class type called 'missforest'.
  #missforest is slow,switching to mean impute
  if(exists("df.imp1") && is.data.frame(df.imp1)){
    print ("No impute required")
    delayedAssign("do.next", {next}) #without this r gives an error "Error: no loop for break/next, jumping to top level"
  }else{
    df.imp1 <- cbind(df.imp, df.char)
  }
  #Convert column with type character to factors
  imp.column.names <- names(df.imp1)
  for(k in 1:length(imp.column.names)){
    col.class <- class(df.imp1[[imp.column.names[k]]])
    if(col.class == "character"){
      print("This is a character type")
      df.imp1[[imp.column.names[k]]] <- as.factor(df.imp1[[imp.column.names[k]]])
    }else{
      print("No character types found")
    }
  }

  #get the level and model from the black box
  #blackbox <- model.colnames.lev[1] # They should pass me this
  #mylogit <- unserialize(blackbox$model[1])
  #blackbox.lev <- blackbox[[1]]
  mod <- unserialize(mod.lev.typ[[1]])
  lev <- length(mod.lev.typ[[2]])
  typ <- mod.lev.typ[[3]]
  #Instead of an extra arguement called multinomial, check for the levels for resposne in train model and decide for multinomial
  # lev <- length(levels(as.factor(mylogit$lev)))
  if(lev > 2){
    print("Selecting multinomial")
    print("Posterior cutoff is ignored")
    predictions <- predict(mod, newdata=df.imp1, "class")
    df.test.save$predictions <- as.integer(predictions)
  }else{
    print("Selecting binomial")
    glm_response_scores <- round(predict(mod, df.imp1, type="response"),2)
    #get lables
    if(missing(posteriorCutoff)){
      print("Using default posterior cutoff")
      posteriorCutoff <- 0.5
      if(mod.lev.typ[[4]] == "lable"){
        predictions <- ifelse(glm_response_scores>posteriorCutoff, 'yes','no')
        df.test.save$predictions <- predictions
      }else{
        predictions <- ifelse(glm_response_scores>posteriorCutoff, '1','0')
        df.test.save$predictions <- as.integer(predictions)
      }
    }else{
      print("Using user supplied posterior cutoff")
      if(mod.lev.typ[[4]] == "lable"){
        predictions <- ifelse(glm_response_scores>posteriorCutoff, 'yes','no')
        df.test.save$predictions <- predictions
      }else{
        predictions <- ifelse(glm_response_scores>posteriorCutoff, '1','0')
        df.test.save$predictions <- as.integer(predictions)
      }
    }
  }
  #rename the predictions column to the train response column name
  names(df.test.save)[length(names(df.test.save))] <- mod.lev.typ[[5]]

  return(df.test.save)
}

########### OUT of the box Forecast algorithm ##################
autoForecast <- function(df,dateCol,col2forecast,fcastFrequency,supplied_model,cardinaldim){

  class.df.char <- class(df[[dateCol]])
  if(class.df.char == "character"){
    df <- df[!df[[dateCol]] =="", ]
  }

  df <- df[!is.na(df[[dateCol]]),]
  #df <- df[complete.cases(df),]
  df <- na.omit(df)

  names(df)
  df.pm <- df
  date.freq <- df.pm[[dateCol]]

  date.class <- class(date.freq[1])
  if((date.class == "POSIXct" || date.class == "POSIXt" || date.class == "POSIXlt") && (!is.integer(df.pm[[dateCol]]))){
    carriots_iot_format = 1
    print("Carriots iot format found")
  }else{
    carriots_iot_format = 0
  }

  if(!is.integer(df.pm[[dateCol]])){
    if(!is.na(str_count(date.freq[1],"\\-")) && str_count(date.freq[1],"\\-") > 0){
      print("dashes")
      count.dash <- str_count(date.freq[1],"\\-")
    }else if((str_count(date.freq[1],"/")) > 0){
      print("Slashes")
      count.dash <- str_count(date.freq[1],"/")
    }
    colon.exist <- str_count(date.freq[1],"\\:")


    if((is.character(df.pm[[dateCol]]) || is.factor(df.pm[[dateCol]])) && (is.na(parse_iso_8601(date.freq[1])))){
      print("date came in as character type")
      # if(is.character(df.pm[[dateCol]])){
      df.pm[[dateCol]] <- as.Date(df.pm[[dateCol]])

    }else if(!is.integer( df.pm[[dateCol]]) && !is.na(parse_iso_8601(date.freq[1]))){
      print("ISO, treating it later")
    }else{
      print("Date better be 4 digit year")
    }
  }

  l<-0
  if(exists("count.dash")  && count.dash == 1){
    print("Just year or year-month format")
    Setfreq=12
    break()
  }else if(exists("count.dash") && count.dash == 2 && colon.exist == 0){
    for(k in 1:13){
      month <- substr(df.pm[[dateCol]], start = 6, stop = 7)
      if(month[k] == month[k]){
        print("month is repeating beyond 12 counts, considering daily data")
        Setfreq=365
        col2forecast_agg <- df.pm
        x <- ts(col2forecast_agg[[col2forecast]], frequency = Setfreq) #convert to time series


        ####future date sequence####
        last.val <- as.character(as.Date(tail(df[[dateCol]],1)) + 1)
        last.val.date.yr <- substr(last.val, start = 0, stop = 4)
        last.val.date.month <- substr(last.val, start = 6, stop = 7)
        last.val.date.day <- substr(last.val, start = 9, stop = 10)
        last.val.date.day.future <- as.character(as.integer(last.val.date.day) + fcastFrequency)

        end.date <- as.Date(last.val) + (fcastFrequency-1)
        future.seq <- seq(as.Date(last.val), as.Date(end.date), "day")

      }else{
        print("month is not repeating beyond 12 counts, considering monthly data")
        Setfreq=12
        col2forecast_agg <- df.pm
        x <- ts(col2forecast_agg[[col2forecast]], frequency = Setfreq) #convert to time series
        break()
      }
    }
  }else{
    Setfreq=8766
    #check for ISO 8601
    if((is.null(parse_iso_8601(date.freq[1])) == FALSE && !is.integer(df.pm[[dateCol]])) || (carriots_iot_format == 1)){
      print("Date is ISO 8601")
      # df.pm$date <- as.POSIXct(df.pm[[dateCol]],format="%Y-%m-%d%H:%M")
      #df.pm$date <- as.character(format_iso_8601(df.pm[[dateCol]]))
      if(carriots_iot_format == 1){
        df[[dateCol]] <- format_iso_8601(df[[dateCol]])
      }

      l=0
      for(k in 1:10){
        dailydata <- strsplit(df[[dateCol]], "T")
        time.stamp <- dailydata[[1]][2]
        hour <- substr(time.stamp, start = 1, stop = 2)
        if(isTRUE(hour == hour)){
          print("hour is repeating, need mean aggregation")
          l = l+1
        }
      }
      if(l > 2){
        df.pm$date <- as.POSIXct(df.pm[[dateCol]],format="%Y-%m-%dT%H:%M")
        print("Aggregation done")
        col2forecast_agg <- aggregate(df.pm[[col2forecast]],list(hour=cut(as.POSIXct(df.pm$date), "hour")),mean)
        date.first.element <- as.Date(col2forecast_agg$hour[1])
        year <- format(as.Date(date.first.element, format="%m/%d/%Y"),"%Y") #need for ts frequency
        month <- format(as.Date(date.first.element, format="%m/%d/%Y"),"%m") #need for ts frequency
        x <- ts(col2forecast_agg$x, frequency = Setfreq) #ts conversion for aggregate case

        #future sequence
        last.val <- (tail(df[[dateCol]],1))
        last.val.date <- parse_iso_8601(last.val)
        last.val.date.yr <- substr(last.val.date, start = 0, stop = 4)
        last.val.date.month <- substr(last.val.date, start = 6, stop = 7)
        last.val.date.day <- substr(last.val.date, start = 9, stop = 10)
        last.val.date.hour <- substr(last.val.date, start = 12, stop = 13)

        future.seq <- list()
        last.val.date.hour.next <- as.character(as.numeric(last.val.date.hour)+1)
        future.hour <- as.character(as.numeric(last.val.date.hour) + 1)
        fcastFrequency.future.seq <- fcastFrequency - 1
        till.date <- ISOdatetime(last.val.date.yr,last.val.date.month,last.val.date.day,future.hour,0,0) + hours(fcastFrequency.future.seq)

        #till.date <- till.date + hours(8)

        #get the to values for ISOdatetime
        last.val.date.yr.till <- substr(till.date, start = 0, stop = 4)
        last.val.date.month.till <- substr(till.date, start = 6, stop = 7)
        last.val.date.day.till <- substr(till.date, start = 9, stop = 10)
        last.val.date.hour.till <- substr(till.date, start = 12, stop = 13)

        future.seq <- append(future.seq,(seq(ISOdatetime(last.val.date.yr,last.val.date.month,last.val.date.day,last.val.date.hour.next,0,0),
                                             ISOdatetime(last.val.date.yr.till,last.val.date.month.till,last.val.date.day.till,last.val.date.hour.till,0,0, tz=""), "hour")))
        if(carriots_iot_format == 0){
          future.seq <- format_iso_8601(future.seq)
        }


      }else{
        col2forecast_agg <- df.pm[[col2forecast]]
        print("Non ISO")
        year <- year(as.Date(col2forecast_agg[[dateCol]][1], format = "%m/%d/%Y"))
        month <- month(as.Date(col2forecast_agg[[dateCol]][1], format = "%m/%d/%Y"))
        x <- ts(col2forecast_agg[[col2forecast]], frequency = Setfreq) #convert to time series
      }
    }
  }

  ##year has just 4 digit year coming as integer
  if(is.integer(df.pm[[dateCol]])){
    dateCol.diff <- df.pm[[dateCol]][1] - (df.pm[[dateCol]][2]) #df.pm[[dateCol]] changes in line 43, check this
    if(dateCol.diff != 0){
      check.order <- df.pm[order(df.pm[[dateCol]])]
      dateCol.diff <- check.order[[dateCol]][1] - (check.order[[dateCol]][2])
    }
    #check if the year is repeating
    if(is.integer(df.pm[[dateCol]]) && (is.unsorted(df.pm[[dateCol]])) | dateCol.diff == 0){
      col.names <- names(df.pm)
      for(j in 1:length(col.names)){
        if(col.names[j] != dateCol){
          print (col.names[j])
          check.order <-  df.pm[order(df.pm[[col.names[1]]],df.pm[[dateCol]]),] #order will affect all columns, sso output is a data frame
          diff <-  check.order[[dateCol]][2] -  check.order[[dateCol]][1]
          print(diff)
          print(check.order)
          if(diff == 1){
            #df.pm[[dateCol]] <-  df.
            print("achieved")
            col2forecast_agg <- check.order
            Setfreq <- 1
            x <- ts(col2forecast_agg[[col2forecast]], frequency = Setfreq) #convert to time series

            names(col2forecast_agg)[names(col2forecast_agg) == col2forecast] <- 'x'

            #future date sequence
            from.val <-  (as.integer(tail((substr(df[[dateCol]], start = 1, stop = 4)),1)) + 1)
            to.val <- from.val+(fcastFrequency-1)
            future.seq <- seq(from.val,to.val)

            break()
          }
        }
      }
    }
  }

  # ###process cardinality check - as in per bill code revenue - Not implementing now ###
  # #Eventually this will contain both stationarity check and model bulding

  #get the unique lables in col2forecast

  # unique.rows.length <- list()

  if(missing(cardinaldim) || is.null(cardinaldim)){
    unique.lables <- 1
  }else{
    unique.lables <- unique(col2forecast_agg[[cardinaldim]])
  }

  unique.rows.length <- 0
  myForecast.card <- NULL
  for(j in 1:length(unique.lables)){
    # if(!missing(cardinaldim) || !is.null(cardinaldim)){
    if(!is.null(cardinaldim)){
      #print("came here")
      unique.rows <- col2forecast_agg[col2forecast_agg[[cardinaldim]]==unique.lables[j],]
      print(unique.rows)
      col2forecast_agg <- unique.rows
      unique.rows.length <- unique.rows.length + nrow(unique.rows)
    }else{
      #print(j)
      col2forecast_agg <- col2forecast_agg
      #break()
    }



    #Stationarity check
    x.diff <- ndiffs(col2forecast_agg[[col2forecast]])
    if(x.diff == 1){
      print("Taking first difference")
      first.diff <- diff(col2forecast_agg[[col2forecast]])
      col2forecast_agg <- col2forecast_agg[nrow(col2forecast_agg)-1:nrow(col2forecast_agg), ]
      col2forecast_agg$x <- first.diff
    }else if(x.diff ==2){
      print("Taking second difference")
      col2forecast_agg$x <- diff(diff(col2forecast_agg[[col2forecast]]))
    }else{
      print("Time series is stationary")
    }
    ##########Build models##########
    if(missing(supplied_model) || is.null(supplied_model)){
      #if(is.null(supplied_model)){
      #linear model
      linear.trend <- tslm(x ~ + trend)

      #Linear with seasonality
      if(Setfreq > 1){
        linear.season <- tslm(x ~+ trend + season)
        #linear seasonality model
        linear.season.mse <- forecast::accuracy(linear.season)
        linear.season.mse <- linear.season.mse[,6]
        # linear.season.rsq <- (linear.season$residuals^2)
        # linear.season.n.na <- length(linear.season.rsq)- length(na.omit(linear.season.rsq))
        # linear.season.mse <- sum(na.omit(linear.season.rsq))/(length(linear.season.rsq)-linear.season.n.na)

        if(linear.season.mse == 0){
          linear.season.mse = NaN
        }

        seasonality.count <- summary(linear.season)$coef[,4] <= .05
        seasonality.count.len <- length(seasonality.count[seasonality.count==TRUE])

        linear.season <- serialize(linear.season,NULL)
      }else{
        linear.season.mse <- NA
        linear.season <-  NA
      }
      #check how many statistically significant seasonalities exist

      if(exists("seasonality.count.len") && seasonality.count.len > 10){
        seasonality.exist=1
      }else{
        seasonality.exist=0
      }


      #arima model, not using seasonal arima becasue of the excessive computation time.
      #If seasonality exisits then arima mse value will be large
      #this check is based on if the seasonality exists in linear model, it is faster that way
      if(exists("seasonality.exist") && seasonality.exist == 1){
        fit.aa <- auto.arima(x,max.order=3, stepwise = TRUE, seasonal = FALSE)
      }else{
        fit.aa <- auto.arima(x,max.order=3, stepwise = TRUE, seasonal = FALSE) #if truely seasonality doesnt exist, then this model make sense
      }

      #fit.aa <- auto.arima(x)

      #Neurel net
      set.seed(1234)
      fit.nn<- nnetar(x, repeats = 2)

      #evaluate model accuracy

      #linear trend model
      linear.trend.metrics <- forecast::accuracy(linear.trend)
      linear.trend.mse <- linear.trend.metrics[,6]
      # linear.trend.rsq <- (linear.trend$residuals^2)
      # linear.trend.n.na <- length(linear.trend.rsq)- length(na.omit(linear.trend.rsq))
      # linear.trend.mse <- sum(na.omit(linear.trend.rsq))/(length(linear.trend.rsq)-linear.trend.n.na)

      #linear seasonality is conditional on Setfreq object, see above

      #arima
      aa.metrics <- forecast::accuracy(fit.aa)
      aa.mse <- aa.metrics[,6]

      # aa.rsq <- (fit.aa$residuals^2)
      # aa.n.na <- length(aa.rsq)- length(na.omit(aa.rsq))
      # aa.mse <- sum(na.omit(aa.rsq))/(length(aa.rsq)-aa.n.na)

      # neural net
      nn.metrics <- forecast::accuracy(fit.nn)
      nn.mse <- nn.metrics[,6]
      if(is.nan(linear.trend.mse) || is.nan(aa.mse) || is.nan(nn.mse)){
        linear.trend.metrics<- forecast::accuracy(linear.trend)
        aa.metrics <- forecast::accuracy(fit.aa)
        nn.metrics <- forecast::accuracy(fit.nn)
        linear.trend.mse <- linear.trend.metrics[,2]
        aa.mse <- aa.metrics[,2]
        nn.mse <- nn.metrics[,2]

        metric.rmse = 1

      }else{
        metric.rmse = 0
      }

      # nn.rsq <- (fit.nn$residuals^2)
      # nn.n.na <- length(nn.rsq)- length(na.omit(nn.rsq))
      # nn.mse <- sum(na.omit(nn.rsq))/(length(nn.rsq)-nn.n.na)
      #
      #compare model accuracy
      model.accuracy <- list(linear.trend.mse,linear.season.mse,aa.mse,nn.mse)
      names(model.accuracy) <- c("linear trend", "linear season and trend", "auto arima", "neural nets")
      best_model <- model.accuracy[which.min(model.accuracy)]


      #serialize models
      linear.trend <- serialize(linear.trend,NULL)
      #linear seasonal model is above, condition based
      fit.aa <- serialize(fit.aa,NULL)
      #fit.aa.xreg <- serialize(fit.aa.xreg,NULL)
      fit.nn <- serialize(fit.nn,NULL)
      #fit.nn.xreg <- serialize(fit.nn.xreg,NULL)

      #list and name the serialized models
      #blackbox.bestmodel <- linear.season
      blackbox <- list(linear.trend,linear.season,fit.aa,fit.nn)
      names(blackbox) <- c("linear trend", "Linear season and trend",
                           "auto arima","neural nets")

      if(names(best_model) == "linear trend"){
        print("Best model is linear trend")
        blackbox <- (blackbox[[1]])
      }
      if(names(best_model) == "linear season and trend"){
        print("Best model is linear season and trend")
        blackbox <- (blackbox[[2]])
      }
      if(names(best_model) == "auto arima"){
        print("Best model is auto arima")
        blackbox <- (blackbox[[3]])
      }
      if(names(best_model) == "neural nets"){
        print("Best model is neural nets")
        blackbox <- (blackbox[[4]])
      }
      supplied = 0
    }else{
      print("model supplied")
      blackbox <- supplied_model
      supplied = 1
    }

    blackbox.names <- names(blackbox)
    myforecastModel <- unserialize(blackbox, NULL)



    #check for xreg and fake xreg
    model.names <- names(myforecastModel)
    a=1
    model.typ <- NULL
    if("xreg" %in% model.names){
      #check for fake xreg which is just a sequence
      xreg.count = 0
      for(m in 1:4){
        fake.xreg <- (myforecastModel$xreg[m] + 1) - myforecastModel$xreg[m]
        print("looping for xreg")
        if(fake.xreg == 1){
          xreg.count = xreg.count + 1
        }
      }
      if(xreg.count == 4){
        print("Fake xreg passed with auto arima. It is just a sequence")
        xreg = NULL
      }else if(xreg < 4){
        print("Found xreg")
        xreg <-
          xreg <- as.data.frame(xreg)
      }
    }else{
      print("No xregs found")
      xreg = NULL
    }

    for(a in 1:length(model.names)){
      print(a)
      if(model.names[a] == "nnetargs"){
        print("found nnetargs")
        model.typ = "nnetargs"
      }
      if(model.names[a] == "arma"){
        print("Found auto arima")
        model.typ <- "arima"
      }
      if(model.names[a] == "df.residual"){
        print("Found linear")
        model.typ <- "linear"
      }
    }

    if(missing(fcastFrequency)){
      fcastFrequency=Setfreq
    }
    # xreg="blank"
    myForecast <- c()
    if(is.data.frame(xreg)) {
      if((exists("xreg") && is.data.frame(xreg)) && model.typ == "nnetargs"){
        print("found nn with xreg")
        #xreg <- myforecastModel$xreg
        point.preds <- predict(myforecastModel, data=train, xreg = xreg)
        myForecast <- as.data.frame(point.preds$mean)
        names(myForecast)<- c("Forecast")
      }
      if(((exists("xreg") && is.data.frame(xreg))) && (model.typ == "arima")){
        print("found arima with xreg")
        #xreg <- myforecastModel$xreg
        point.preds <- predict(myforecastModel, data=train, newxreg = xreg,h=fcastFrequency)
        myForecast <- as.data.frame(point.preds$pred)
        myForecast <- myForecast$x
        names(myForecast)<- c("Forecast")
      }
    }
    if(is.null(xreg)){
      print("neural net without xreg found")
      if(model.typ == "linear" || model.typ == "nnetargs"){
        #print(fcastFrequency)
        print("socomecs model could be linear or neural net")
        if(model.typ == "linear"){
          point.preds <- forecast(myforecastModel,h=fcastFrequency)
          print("Its linear")
        }else{
          point.preds <- forecast(myforecastModel,h=fcastFrequency)
          print("Its neural net")
        }

        myForecast <- as.data.frame(point.preds)
        names(myForecast)<- c("Forecast")
        # break()
      }
      if(model.typ == "arima"){
        print("arima without xreg")
        point.preds <- forecast(myforecastModel,h=fcastFrequency)
        myForecast <- as.data.frame(point.preds$mean)
        names(myForecast) <- c("Forecast")
      }
    }

    #collect all forcasted rows
    myForecast.card <- rbind(myForecast, myForecast.card)

  } #cardinality for loop closing brace



  if(missing(cardinaldim) || is.null(cardinaldim)){

    nrow=fcastFrequency

  }else{
    nrow = fcastFrequency*(length(unique.lables))
  }

  # df <- rbind(df)
  names <- names(df)
  output <- data.frame()
  # names(myforecast) <- c(names)
  df.ncol <- ncol(df)
  output <- data.frame(matrix(ncol = df.ncol, nrow =nrow))
  x <- c(names)
  colnames(output) <- c(x)

  if(missing(cardinaldim) || is.null(cardinaldim)){
    output[col2forecast] <- myForecast
  }else{
    output[col2forecast] <- myForecast.card
  }

  output[dateCol] <- future.seq #fill the spatial dimesnion with the future sequence

  #fill in cardinal dimension
  cardinaldim.list <-list()
  for(k in 1:length(unique.lables)){
    label <- unique.lables[k]
    for(l in 1:fcastFrequency){
      cardinaldim.list<- append(cardinaldim.list,label)
    }
    #output[cardinaldim] <- list(unique.lables)
  }
  output[cardinaldim] <- unlist(cardinaldim.list)

  #model accuracy metrics
  models.accuracy.metrics <- data.frame()
  # names(myforecast) <- c(names)
  models.accuracy.metrics <- data.frame(matrix(ncol = 3, nrow =4))
  names(models.accuracy.metrics) <- c("model_name", "metric_name", "metric_value")

  if(metric.rmse == 1){
    metric_name <- c("Root Mean Squared Error")
  }else{
    metric_name <- c("Mean Abs Squared Error")
  }

  model_name <- c("Linear Seasonality","Linear Trend","Auto ARIMA","Neurelnet")
  metric_value <- c(linear.season.mse, linear.trend.mse ,aa.mse,nn.mse)
  models.accuracy.metrics$model_name <- model_name
  models.accuracy.metrics$metric_name <- metric_name
  models.accuracy.metrics$metric_value <- metric_value


  if(supplied == 0){
    output <- list(blackbox, output, models.accuracy.metrics)
    names(output) <- c("model", "df", "accuracy_metrics")
    print("First time, returning the best model")
    return(output)
  }else{
    print("No model returned")
    output <- list(output)
    names(output) <- c("df","accuracy_metrics")
    return(output)
  }
}
