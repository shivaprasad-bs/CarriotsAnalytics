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
    con$addModel(model = modelInfo$model,label = "autoClassify",description = "Default_AutoClassify_model_from_CA",predictors = modelInfo$predictors)
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
    index <- grep("predictions",colnames(newDf))
    colnames(newDf)[[index]] <- .caParams$TARGET_LABEL

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
      isModelAvailable <- TRUE
    }

    #User defined settings in CA

    #Get the target column name
    target <- con$getParam("TARGET_NAME")

    #Get temporal dimension
    temporalDim <- con$getParam("TEMPORAL_DIM")

    #Get forecast step
    forecastStep <- con$getParam("FORECAST_STEP")

    #Actual Forecasting Algorithm
    output <- autoForecast(df,target,temporalDim,forecastStep,blackboxModel)

    if(is.null(output$df))
      stop("Algorithm Failed to generate the forecasted dataframe")

    #Add models only if no models passed <- logic might change
    if(!is.null(output$model) && !isModelAvailable) {

      #Add the models
      mLabel <- "autoForecast"
      con$addModel(model = output$model,label = mLabel,description = "Default_AutoForecast_model_from_CA")
      model_name <- mLabel
      #If there are any special characters, do MD5- same logic as CA App
      if(!grepl("^[a-zA-Z0-9\\s\\(\\)_/]+$",model_name))
        model_name = digest::digest(model_name,"md5",serialize = FALSE)
      model_label <- mLabel
    }

    #push to CA
    newDf <- output$df
    index <- grep("predictions",colnames(newDf))
    colnames(newDf)[[index]] <- .caParams$TARGET_LABEL
    print(names(newDf))
    con$update(df = newDf,modelName = model_name, modelLabel = model_label)
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
}

# load dataset

autoClassify <- function(df, col2bclassified) {
  #Init
  init()

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
  df.num <- df[ , df.nums]
  df.char <- df[, !(df.nums)]

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
  blackbox <- list(mylogit,fac.lev,dat.typ,y.dat.typ)
  model.colnames.lev <- list(blackbox,col.name.final)
  names(model.colnames.lev) <- c("model","predictors")

  return(model.colnames.lev)
}
############ Score function ########
# load test dataset
autoClassifyScore <- function(df.test, mod.lev.typ,posteriorCutoff) {
  #Init
  init()

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
  df.num <- df[ , df.nums]
  df.char <- df[, !(df.nums)]

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
    df.test$predictions <- as.integer(predictions)
  }else{
    print("Selecting binomial")
    glm_response_scores <- round(predict(mod, df.imp1, type="response"),2)
    #get lables
    if(missing(posteriorCutoff)){
      print("Using default posterior cutoff")
      posteriorCutoff <- 0.5
      if(mod.lev.typ[[4]] == "lable"){
        predictions <- ifelse(glm_response_scores>posteriorCutoff, 'yes','no')
        df.test$predictions <- predictions
      }else{
        predictions <- ifelse(glm_response_scores>posteriorCutoff, '1','0')
        df.test$predictions <- as.integer(predictions)
      }
    }else{
      print("Using user supplied posterior cutoff")
      if(mod.lev.typ[[4]] == "lable"){
        predictions <- ifelse(glm_response_scores>posteriorCutoff, 'yes','no')
        df.test$predictions <- predictions
      }else{
        predictions <- ifelse(glm_response_scores>posteriorCutoff, '1','0')
        df.test$predictions <- as.integer(predictions)
      }
    }
  }

  return(df.test)
}

#Just for testing
autoForecast <- function(df,target,temporalDim=NULL,forecastStep=NULL,blackboxModel=NULL){
  output <- list()
  modelInfo <- autoClassify(df,target)
  newDf <- df
  newDf[["predictions"]] <- df[["Parch"]]
  output$model <- modelInfo$model
  output$df <- newDf

  output
}
