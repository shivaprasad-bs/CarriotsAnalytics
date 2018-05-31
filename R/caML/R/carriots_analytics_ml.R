#SOME ENUMS for different date/datetime formats
CA_NON_DATE = 0
CA_STRING_DATE = 1
CA_STRING_DATE_TIME = 2
CA_STRING_DATE_YYYY_MM = 3
CA_POSIXct = 9

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

    #Get forecast steps
    forecastSteps <- con$getParam("FORECAST_STEP")

    #Get cardinal dims
    cardinalDims <- con$getParam("CARDINAL_DIMS")

    #Actual Forecasting Algorithm
    output <- autoForecastHelper(df = df,temporalDim,target_label,forecastSteps,blackboxModel,cardinalDims)

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

#######################################################################################################
#   Helper function to do some prerequsites before running the autoforecasting algorithm
#
#1. First step is to convert the user selected temporalDim to POSIXct, errors out
#in case not able to convert to POSIXct
#2. Process the cardinalDims - recursively runs the forecasting algorithm with a dataframe having an unique
# cardinalDim filter applied, then obtains the resultant forcasted dataframe and model and merges it to final
# dataframe and modelsList
#3.Process back the temporalDim to its original types
##########################################################################################################

autoForecastHelper = function(df=NULL,temporalDim = NULL,target = NULL,forecastSteps = NULL,
                              blackboxModel = NULL, cardinalDims = NULL) {
  #process Temporal dim
  resp = processTemporalDim(df=df,temporalDim = temporalDim)

  df <- resp$df
  caType <- resp$caType

  print(caType)

  #process cardinal dims and filter the dataframe, run the autoForecast using filtered DF
  output = processCardinalDimsAndForecast(df,temporalDim,target,forecastSteps,blackboxModel,cardinalDims)

  #processForecasted values
  output$df = processForecastedValues(output$df,temporalDim,caType)

  output
}

#########################################################################################################
# Function will run the autoforecast based on cardinalDim
#
#1. Gets the unique values for cardinal dims
#2. Applies the above values as a filter on the actual dataframe and gets the filtered DF
#3. Runs the autoforecast using, obtains the forecasted values and model(if existing model is available
# passed the existing model to autoforecast) and saves it to the final Df and final modelList
#4.Returns the final modelList and final df(with all the merged results)

processCardinalDimsAndForecast = function(df=NULL,temporalDim = NULL,target = NULL,forecastSteps = NULL,
                              blackboxModel = NULL, cardinalDims = NULL) {

  #check whether cardinalDims exists
  if(!is.null(cardinalDims)) {

    df_final <- NULL

    updateModel <- FALSE
    isModelAvailable <- FALSE

    if(!is.null(blackboxModel)) {
      isModelAvailable <- TRUE
    }else
      blackboxModel <- list()

    # Do group by on the cardinalDims
    df_distinct<- dplyr::distinct(dplyr::select(df,cardinalDims))
    final_metrics <- NULL

    for(i in 1:nrow(df_distinct)) {
      isSubModelAvailable <- FALSE
      row <- df_distinct[i,]
      #model identifier in the modelList
      modelStr <- paste(row,collapse = "_")

      subModel <- NULL
      if(isModelAvailable) {
          subModel <- blackboxModel[[modelStr]]
          if(!is.null(subModel))
            isSubModelAvailable <- TRUE
      }
      #hack to conver the single valued row to dataframe(which includes header info as well)
      if(length(row) == 1)  {
        temp <- list()
        temp[[colnames(df_distinct)]] = row
        row <- as.data.frame(temp)
      }

      selectCols <- c(temporalDim,target)
      df_agg <- getFilteredDf(df = df,cardinalFilter = row, selectCols = selectCols)

      #hack for autoForecast to work - needs to be removed
      #df_agg[[temporalDim]] <- as.POSIXlt(df_agg[[temporalDim]])

      #Now call the actual autoforecast with this new dataframe
      #Passing cardinal dims as null, as it is erroring out
      output <- autoForecast(df_agg,temporalDim,target,forecastSteps,subModel)

      #Merge cardinal dim columns to DF
      if(is.null(output$df))
        stop("No values have been forecasted, exiting here")
      else {
        output$df <- mergeCardinalColumns(df = output$df,cardinalFilter = row)
      }

      #append the results to final dataframe
      if(is.null(df_final)) {
        df_final <- output$df
      }else {
        df_final <- unique(rbind(df_final,output$df))
      }

      #Add the newly added model to modelList
      if(!isSubModelAvailable) {
          updateModel <- TRUE
          blackboxModel[[modelStr]] <- output$model
          final_metrics <- mergeAccuracyMetrics(final_metrics,output$accuracy_metrics,row)
      }
    }

    if(updateModel) {
      output <- list()
      output[["model"]] <- blackboxModel
      output[["accuracy_metrics"]] <- final_metrics
    }
    else {
      output[["model"]] <- NULL
      output[["accuracy_metrics"]] <- NULL
    }

    #Merged dataframe
    output[["df"]] <- df_final

  } else {

    #If there is no cardinal dims selected call the autoforecast directly
    output <- autoForecast(df,temporalDim,target,forecastSteps,blackboxModel)
    if(!is.null(blackboxModel)) {
      #Black box model available, so ignore the returned model
      output$model <- NULL
      output$accuracy_metrics <- NULL
    }
  }

  output
}

mergeCardinalColumns <- function(df = NULL, cardinalFilter = NULL) {
  if(!is.null(cardinalFilter)) {
    if(!is.null(df)) {
      #Append the cardinal dims values as additional columns
      for(i in 1:ncol(cardinalFilter)) {
        df[[colnames(cardinalFilter[i])]] <- rep(cardinalFilter[[i]],each=nrow(df))
      }
    }
  }

  df
}

mergeAccuracyMetrics <- function(finalMetrics,currentMetrics,cardinalFilter) {

  if(!is.null(currentMetrics)) {
    #Append the cardinal dims values as additional columns
    for(i in 1:ncol(cardinalFilter)) {
      currentMetrics[[colnames(cardinalFilter[i])]] <- rep(cardinalFilter[[i]],each=nrow(currentMetrics))
    }
  }

  if(is.null(finalMetrics))
    finalMetrics <- currentMetrics
  else {
    finalMetrics <- unique(rbind(finalMetrics,currentMetrics))
  }

  finalMetrics

}

getFilteredDf <- function(df = NULL, cardinalFilter = NULL, selectCols = NULL) {

  filterStr <- ""

  #prepare a filter string for cardinal dims
  for(i in 1:ncol(cardinalFilter)) {
    if(nchar(filterStr) > 0) filterStr <- paste(filterStr," & ")
    filterStr <- paste(filterStr,paste(colnames(cardinalFilter[i]),paste("'",cardinalFilter[[i]],"'",sep = ""),sep = " == "),sep="")
  }

  #build a dataframe filtered on the cardinal dims
  # df_agg <- filter(
  #   summarise(
  #     select(group_by_at(df,vars(one_of(select_cols)))
  #            ,columns
  #     ),
  #     caReplace = sum(!!as.name(target))
  #   ),
  #   !!!rlang::parse_expr(filterStr)
  # )

  #just filtering is enough, because query is already doing  aggregation and group by
  print(filterStr)
  df_agg <- dplyr::select(dplyr::filter(df,!!!rlang::parse_expr(filterStr)),selectCols)

  df_agg

}

processTemporalDim = function(df=NULL,temporalDim=NULL) {

  #Response
  resp <- list()

  #Get the original type
  type <- class(df[[temporalDim]])
  caType <- CA_POSIXct

  #Incase of POSIXlt and POSIXct, class will return a list with 2 values - we have to pick first value of it
  if(length(type) > 1)
    type <- type[1]

  #if temporalDim already a POSIXlt, then just return it
  if(type == "POSIXct")
    print("TemporalDim is in POSIXct, not changing anything")
  #Date can be directly converted in to POSIXct
  else if(type == "Date")
    df[[temporalDim]] <- as.POSIXct(df[[temporalDim]])
  #Other formats like numeric,integer, string has to be handled here
  else {
    caType <- findIsStringDate(df[[temporalDim]])
    #this should appropriately cast numeric/integer/ string
    print("entering non - date time")
    temp <- as.POSIXct(parsedate::parse_iso_8601(df[[temporalDim]]))
    if(all(is.na(temp)))
      stop("Unable to cast the temporal dim to Date/DateTime, Exiting here !!!")
    else
      df[[temporalDim]] <- temp
  }

  #change the timezone to UTC
  attributes(df[[temporalDim]])$tzone <- "UTC"

  resp[["df"]] <- df
  resp[["caType"]] <- caType

  resp
}

findIsStringDate = function(vals) {

  caType <- CA_NON_DATE
  nonNAIndex <- min(which(!is.na(vals)))
  valToTest <- vals[nonNAIndex]

  if(is.character(valToTest)) {
    if(IsDate(valToTest))
      caType <- CA_STRING_DATE
    else if(IsDateTime(valToTest))
      caType <- CA_STRING_DATE_TIME
  }

  caType
}

IsDate <- function(mydate, date.format = "%Y-%m-%d") {
  tryCatch(!is.na(as.Date(mydate, date.format)),
           error = function(err) {FALSE})
}

IsDateTime <- function(mydate, date.format = "%Y-%m-%d %H:%M:%S") {
  tryCatch(!is.na(as.POSIXct(mydate, format = date.format)),
           error = function(err) {FALSE})
}

processForecastedValues = function(df,temporalDim,caType) {
  #If it is non- date/dateTime format, get only 'year' part from POSIXlt
  if(!is.null(df)) {
    #pick only year part if it is not date
    if(caType == CA_NON_DATE)
      df[[temporalDim]] <- lubridate::year(df[[temporalDim]])
  }

  df
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
  library(dplyr)
  library(rlang)
}

autoClassify <- function(df, col2bclassified) {

  #init
  init()

  #store response
  myresponse <- col2bclassified
  #check the data types
  dat.typ <- capture.output(str(df))

  #Put underscore for column names with spaces
  #names(df) <- gsub(" ", "_", names(df))

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
  #names(df) <- gsub(" ", "_", names(df))

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
autoForecast <- function (df,temporalDim,columnToForecast,fcastFrequency,supplied_model){
  #init
  init()

  # df <- processtemporal_dim(df, temporalDim);
  df <- df[!is.na(df[[temporalDim]]),]
  #df <- df[complete.cases(df),]
  df <- na.omit(df)

  freqNumber <- computeTemporalFreq(df,temporalDim)

  futureSeq <- futureTemporalDim(df, temporalDim, freqNumber, fcastFrequency)

  forecasting(df, temporalDim, futureSeq, columnToForecast, supplied_model, fcastFrequency, freqNumber);
}

# processtemporal_dim <- function(df, temporaldim){
#
#
#   return(df)
# }


computeTemporalFreq <- function(df,temporalDim) {

  temoralDimContent <- df[[temporalDim]]

  if(length(unique(hour(df[[temporalDim]]))) > 10){
    print("hourly data")
    freqNumber <- 8766
  }else if(length(unique(minute(df[[temporalDim]]))) > 10){
    print("minute level data")
    freqNumber <- 1440
  }else if(length(unique(second(df[[temporalDim]]))) > 10){
    print("seconds level data")
    freqNumber <- 60
  }else if(length(unique(day(df[[temporalDim]]))) > 10){
    print("daily data")
    freqNumber <- 365.25
  }else if(length(unique(month(df[[temporalDim]]))) > 10){
    print("monthly data")
    freqNumber <- 12
  }else{
    print("yearly data")
    freqNumber <- 1
  }
  return(freqNumber)
}


aggregateForecastColumn <- function(df, columnToForecast, freqNumber,temporalDim){

  if(freqNumber == 8766){
    dfWithAggregatedColumn <- aggregate(df[[columnToForecast]],list(hour=cut(as.POSIXct(df[[temporalDim]]), "hour")),mean)
  }else if(freqNumber == 1440){
    fWithAggregatedColumn <- aggregate(df[[columnToForecast]],list(hour=cut(as.POSIXct(df[[temporalDim]]), "minute")),mean)
  }else if(freqNumber == 60){
    dfWithAggregatedColumn <- aggregate(df[[columnToForecast]],list(temp_dim=cut(as.POSIXct(df[[temporalDim]]), "sec")),mean)
  }else if(freqNumber == 365.25){
    dfWithAggregatedColumn <- aggregate(df[[columnToForecast]],list(temp_dim=cut(as.Date(df[[temporalDim]]), "day")),mean)
  }else if(freqNumber == 12){
    dfWithAggregatedColumn <- aggregate(df[[columnToForecast]],list(temp_dim=cut(as.Date(df[[temporalDim]]), "month")),mean)
  }else{
    dfWithAggregatedColumn <- aggregate(df[[columnToForecast]],list(temp_dim=cut(as.Date(df[[temporalDim]]), "year")),mean)
  }

  names(dfWithAggregatedColumn) <- c(temporalDim, columnToForecast)

  return(dfWithAggregatedColumn)
}

# aggregatedColumn <- dfWithAggregatedColumn$x #it will always be x

convertToTSclass <- function(freqNumber, aggregatedColumn,dfWithAggregatedColumn) {

  if(length(aggregatedColumn) < 10 && (((length(nrow(dfWithAggregatedColumn)) < 25) #first and second condiiton are the same - check again.
          || (is_true(isZero(dfWithAggregatedColumn[[columnToForecast]])))))){
    tryCatch(
      # This is what I want to do:
      ts_class <- ts(aggregatedColumn, frequency = freqNumber)
      ,
      # ... but if an error occurs, tell me what happened:
      error=function(error_message) {
        message("And below is the error message from R:")
        message(error_message)
        return(NA)
      }
    )

    return(NULL)

  }else{
    ts(aggregatedColumn, frequency = freqNumber)
  }

  # ts_class <- ts(aggregatedColumn, frequency = freqNumber)

}

futureTemporalDim <- function(df, temporalDim, freqNumber, fcastFrequency){
  temporalDimLastVal <- (tail(df[[temporalDim]],1))

  if(freqNumber == 8766){
    print("Hourly future dates")


    end.date <- temporalDimLastVal
    hour(end.date) <- hour(end.date) + (fcastFrequency-1)
    futureSeq <- seq(temporalDimLastVal, end.date, "hour")


  }else if(freqNumber == 365.25){

    print("Daily future dates")

    end.date <- temporalDimLastVal
    day(end.date) <- day(end.date) + (fcastFrequency-1)
    futureSeq <- seq(temporalDimLastVal, end.date, "day")

  }else if(freqNumber == 12){

    print("Monthly future dates")

    end.date <- temporalDimLastVal
    month(end.date) <- month(end.date) + (fcastFrequency-1)
    futureSeq <- seq(temporalDimLastVal, end.date, "month")


  }else {

    print("Yearly future dates")
    # futureStartDate <- as.integer(year(temporalDimLastVal)) + 1
    # futureEndDate <- futureStartDate + (fcastFrequency-1)
    # futureSeq <- seq(futureStartDate,futureEndDate)

    end.date <- temporalDimLastVal
    year(end.date) <- year(end.date) + (fcastFrequency-1)
    futureSeq <- seq(temporalDimLastVal, end.date, "year")

  }

  return(futureSeq)

}




forecasting <- function(df, temporalDim, futureSeq, columnToForecast, supplied_model, fcastFrequency, freqNumber){

  dfWithAggregatedColumn <- aggregateForecastColumn(df,columnToForecast, freqNumber, temporalDim)


  # ##########Split data for train and test##########
  if(nrow(dfWithAggregatedColumn) >10){
    dfWithAggregatedColumn$rowcount <- 1:nrow(dfWithAggregatedColumn);
    set.seed(1234);
    splitIndex <- createDataPartition(dfWithAggregatedColumn[["rowcount"]], p = .65, list = FALSE, times = 1);
    trainDF <- dfWithAggregatedColumn[ splitIndex,];
    testDF  <- dfWithAggregatedColumn[-splitIndex,];
  }


  aggregatedColumn <- trainDF[[columnToForecast]]

  # aggregatedColumn <- dfWithAggregatedColumn[[columnToForecast]]



  ts_class <- convertToTSclass(freqNumber, aggregatedColumn,dfWithAggregatedColumn)
  ts_class_test <- convertToTSclass(freqNumber, testDF[[columnToForecast]],dfWithAggregatedColumn)

  x.diff <- ndiffs(dfWithAggregatedColumn[[columnToForecast]])

  if(x.diff == 1){
    print("Taking first difference")
    # first.diff <- diff(col2forecast_agg[[col2forecast]])
    first.diff <- diff(dfWithAggregatedColumn[[columnToForecast]])
    # col2forecast_agg <- col2forecast_agg[nrow(col2forecast_agg)-1:nrow(col2forecast_agg), ]
    dfWithAggregatedColumn <- dfWithAggregatedColumn[nrow(dfWithAggregatedColumn)-1:nrow(dfWithAggregatedColumn), ]
    # col2forecast_agg$x <- first.diff
    dfWithAggregatedColumn$columnToForecast <- first.diff
  }else if(x.diff ==2){
    print("Taking second difference")
      # dfWithAggregatedColumn$columnToForecast <- diff(diff((dfWithAggregatedColumn[[columnToForecast]])))
    secondDiff <- diff(diff((dfWithAggregatedColumn[[columnToForecast]])))

  }else{
    print("Time series is stationary")
  }


  ##########Build models##########


  # #check for short time series <50 obs after aggregation. if true use auto arima - refer Hyndman's blog
  # if((missing(supplied_model) || is.null(supplied_model)) && (length(dfWithAggregatedColumn[[columnToForecast]]) <= 10) && (!is.null(ts_class))){
  #
  #   fit.aa <- auto.arima(ts_class,max.order=3, stepwise = TRUE, seasonal = FALSE)
  #   fit.aa <- serialize(fit.aa,NULL)
  #   point.preds <- forecast(fit.aa.original,h=fcastFrequency)
  #   myForecast <- as.data.frame(point.preds$mean)
  #
  #   linear.season.mse <- NULL; linear.trend.mse <- NULL; aa.mse <- NULL; nn.mse <- NULL
  #
  #   shortSeries = 1
  # }


  if(missing(supplied_model) || is.null(supplied_model) && ((!is.null(ts_class)) || !is.null(ts_class_test))){

    # ||

    linear.trend <- tslm(ts_class ~ trend)

    #Linear with seasonality
    if(freqNumber > 1){
      linear.season <- tslm(ts_class ~+ trend + season)
      #linear seasonality model
      # linear.season.mse.test <- tslm(testDF, model=linear.season)
      linear.season.mse <- forecast::accuracy(linear.season)
      linear.season.mse <- linear.season.mse[,6]

        # tryCatch(
        #   # This is what I want to do:
          if(is.nan(linear.season.mse)){
            print("setting linear model metric to nan")
          }else{
            linear.season.mse = NaN
          }

      seasonality.count <- summary(linear.season)$coef[,4] <= .05
      seasonality.count.len <- length(seasonality.count[seasonality.count==TRUE])

      linear.season <- serialize(linear.season,NULL)
    }else{
      linear.season.mse <- NA
      linear.season <-  NA
    }

    if(exists("seasonality.count.len") && seasonality.count.len >10){
      linear.season.mse = NaN
    }

    #check how many statistically significant seasonalities exist

    if(exists("seasonality.count.len") && seasonality.count.len > 10){
      seasonality.exist=1
    }else{
      seasonality.exist=0
    }

    #arima model, not using seasonal arima because of the excessive computation time.
    #If seasonality exisits then arima mse value will be large
    #this check is based on if the seasonality exists in linear model, it is faster that way
    if(exists("seasonality.exist") && seasonality.exist == 1){
      fit.aa <- auto.arima(ts_class,max.order=3, stepwise = TRUE, seasonal = FALSE)
      print("Seasonality exists")
    }else{
      fit.aa <- auto.arima(ts_class,max.order=3, stepwise = TRUE, seasonal = FALSE)
      #it is the same - fix this! - if truely seasonality doesnt exist, then this model make sense. how?
    }
    fit.aa.original <- fit.aa
    #fit.aa <- auto.arima(x)

    #Neurel net
    set.seed(1234)
    #fit.nn <- nnetar(ts_class, repeats = 3)

    ################################
    modelNeurelnet <- function(){
      tryCatch(
        # This is what I want to do:
        fit.nn <- nnetar(ts_class, repeats = 3)
        ,
        # ... but if an error occurs, tell me what happened:
        error=function(error_message) {
          message("And below is the error message from R:")
          message(error_message)
          return(NaN)
        }
      )
    }
    ################################
    fit.nn <- modelNeurelnet();

    if(exists("fit.nn") && is.nnetar(fit.nn)){
      nn.mse <- NaN
    }

    linear.trend.metrics <- forecast::accuracy(linear.trend)
    linear.trend.mse <- linear.trend.metrics[,6]


    #refit on test data
    #linear seasonality is conditional on Setfreq object, see above

    if(is.null(ts_class_test)){

      ts_class_test <- ts_class

    }

    #arima

    fit.aa.test <- Arima(testDF[[columnToForecast]], model=fit.aa,stepwise = TRUE)
    aa.metrics <- forecast::accuracy(fit.aa.test)
    aa.mse <- aa.metrics[,6]

    # neural net
    # refit on test data if the test observations are more. Checking only for daily data. Need to check for other
    #frequencies.
    if(!is.nnetar(fit.nn) && (freqNumber <= 365.25 && length(ts_class_test) < 365)){
      nn.mse <- NaN
      fit.nn.test <- NaN
    }else if(exists("fit.nn") && is.nnetar(fit.nn)){

      #if fit.nn exists and isa valid nnetar object then refit fit.nn on test data with try catch. TestDF may not be long enough for a successful refit
      modelNeurelnetTest <- function(){
        tryCatch(
          # This is what I want to do:
          fit.nn.test <- nnetar(model=fit.nn, ts_class_test)
          ,
          # ... but if an error occurs, tell me what happened:
          error=function(error_message) {
            message("And below is the error message from R:")
            message(error_message)


            return(NaN)
          }
        )
      }

      ################################
      fit.nn.test <- modelNeurelnetTest();

      if(exists("fit.nn.test") && is.nnetar(fit.nn.test)){
        nn.metrics <- forecast::accuracy(fit.nn.test)
        nn.mse <- nn.metrics[,6]

      }else{
        # fit.nn.test <- nnetar(model=fit.nn, ts_class_test)
        nn.mse <- NaN
      }

    }else{
      fit.nn.test <- NaN
      nn.mse <- NaN
    }


        # fit.nn.testFit <- fitted(fit.nn.testModel)
        # nn.metrics <- forecast::accuracy(ts_class, fit.nn.testFit, test=testDF[[columnToForecast]])



        if((exists("fit.nn.test") && (exists("fit.aa.test"))) && (is.nan(linear.trend.mse) || is.nan(aa.mse) || is.nan(nn.mse))){
          linear.trend.metrics<- forecast::accuracy(linear.trend)
          aa.metrics <- forecast::accuracy(fit.aa.test)
          if(exists("fit.nn.test") & class(fit.nn.test) == "nnetar"){
            nn.metrics <- forecast::accuracy(fit.nn.test)
            nn.mse <- nn.metrics[,2]
          }else{
            nn.metrics <- NA #not sure here
          }

          linear.trend.mse <- linear.trend.metrics[,2]
          aa.mse <- aa.metrics[,2]

          metric.rmse = 1

        }else{
          metric.rmse = 0
        }



    #compare model accuracy
    model.accuracy <- list(linear.trend.mse,linear.season.mse,aa.mse,nn.mse)
    names(model.accuracy) <- c("linear trend", "linear season and trend", "auto arima", "neural nets")
    best_model <- model.accuracy[which.min(model.accuracy)]


    #serialize models
    linear.trend <- serialize(linear.trend,NULL)
    #linear seasonal model is above, condition based
    fit.aa <- serialize(fit.aa,NULL)
    #fit.aa.xreg <- serialize(fit.aa.xreg,NULL)
    if(exists("fit.nn") && is.nnetar(fit.nn)){
      fit.nn <- serialize(fit.nn,NULL)
    }else{
      nn.mse <- NaN
    }

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

  }else if((missing(supplied_model) || is.null(supplied_model)) && ((is_true(isZero(dfWithAggregatedColumn[[columnToForecast]]))))) {

    print("Just zeros - no forecast possible")

    linear.season.mse <- NaN; linear.trend.mse <- NaN; aa.mse <- NaN; nn.mse <- NaN
    blackbox <- NULL
    myForecast <- 0
    supplied = 0

  }else if((missing(supplied_model) || is.null(supplied_model)) && (is.null(ts_class_test) || is.null(ts_class))){

    print("Not enough data - JUst going to be a mean forecast")

    linear.season.mse <- NaN; linear.trend.mse <- NaN; aa.mse <- NaN; nn.mse <- NaN
    blackbox <- NULL

    is.na(dfWithAggregatedColumn[[columnToForecast]]) <- (dfWithAggregatedColumn[[columnToForecast]])==0 #convert 0 values to NA before taking mean

    myForecast <- mean(dfWithAggregatedColumn[[columnToForecast]], na.rm=TRUE)
    supplied = 0

  }else{

    print("model supplied")
    blackbox <- supplied_model
    supplied = 1
    set.seed(12345)
  }

  # blackbox.names <- names(blackbox)
  # myforecastModel <- unserialize(blackbox, NULL)


  if(exists("blackbox") && !is.null(blackbox)){
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
    fcastFrequency=freqNumber
  }
  # xreg="blank"
  myForecast <- c()
  if(is.data.frame(xreg)) {
    if((exists("xreg") && is.data.frame(xreg)) && model.typ == "nnetargs"){
      print("found nn with xreg")
      #xreg <- myforecastModel$xreg
      point.preds <- predict(myforecastModel, data=testDF, xreg = xreg)
      myForecast <- as.data.frame(point.preds$mean)
      names(myForecast)<- c("Forecast")
    }
    if(((exists("xreg") && is.data.frame(xreg))) && (model.typ == "arima")){
      print("found arima with xreg")
      #xreg <- myforecastModel$xreg
      point.preds <- predict(myforecastModel, data=testDF, newxreg = xreg,h=fcastFrequency)
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
        point.preds <- point.preds$mean
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
}



  names <- names(df)
  output <- data.frame()
  # names(myforecast) <- c(names)
  df.ncol <- ncol(df)
  output <- data.frame(matrix(ncol = df.ncol, nrow =fcastFrequency))
  x <- c(names)
  colnames(output) <- c(x)
  output[columnToForecast] <- myForecast

  output[temporalDim] <- futureSeq #fill the spatial dimesnion with the future sequence

  # #model accuracy metrics
  # models.accuracy.metrics <- data.frame()
  # # names(myforecast) <- c(names)
  # models.accuracy.metrics <- data.frame(matrix(ncol = 3, nrow =4))
  # names(models.accuracy.metrics) <- c("model_name", "metric_name", "metric_value")
  #
  # if(exists("metric.rmse") && metric.rmse == 1){
  #   metric_name <- c("Root Mean Squared Error")
  # }else{
  #   metric_name <- c("Mean Abs Squared Error")
  # }
  #
  # model_name <- c("Linear Seasonality","Linear Trend","Auto ARIMA","Neurelnet")
  # metric_value <- c(linear.season.mse, linear.trend.mse ,aa.mse,nn.mse)
  # models.accuracy.metrics$model_name <- model_name
  # models.accuracy.metrics$metric_name <- metric_name
  # models.accuracy.metrics$metric_value <- metric_value


  inputMeasureType <- class(df[[columnToForecast]])

  #convert 'columnToForecast' to input class

  convertToClass <- function(output, measureType){

    inputClass <- inputMeasureType

    if(inputClass == "integer"){

      output[[columnToForecast]] <- as.integer(output[[columnToForecast]])

    }else{

      output[[columnToForecast]] <- as.numeric(output[[columnToForecast]])

    }

    return(output)

  }

  output <- convertToClass(output, measureType);

  if(supplied == 0){

    #model accuracy metrics
    models.accuracy.metrics <- data.frame()
    # names(myforecast) <- c(names)
    models.accuracy.metrics <- data.frame(matrix(ncol = 3, nrow =4))
    names(models.accuracy.metrics) <- c("model_name", "metric_name", "metric_value")

    if(exists("metric.rmse") && metric.rmse == 1){
      metric_name <- c("Root Mean Squared Error")
    }else{
      metric_name <- c("Mean Abs Squared Error")
    }

    model_name <- c("Linear Seasonality","Linear Trend","Auto ARIMA","Neurelnet")
    metric_value <- c(linear.season.mse, linear.trend.mse ,aa.mse,nn.mse)
    models.accuracy.metrics$model_name <- model_name
    models.accuracy.metrics$metric_name <- metric_name
    models.accuracy.metrics$metric_value <- metric_value

    output <- list(blackbox, output, models.accuracy.metrics)
    names(output) <- c("model", "df", "accuracy_metrics")
    print("First time, returning the best model")
    return(output)
  }else{
    print("No model returned")
    output <- list(output)
    names(output) <- c("df")
    return(output)
  }

}


