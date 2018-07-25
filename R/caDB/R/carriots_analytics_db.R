CA_DEFAULT_PAGE_SIZE = 10000
###############################################################
#'Get datasource connection
#'
#' This is a function to obtain connection to a
#' datasource from the envision server by providing the token
#' obtained from the envision App for the specific datasource
#'
#' @param baseUrl - CA server URL
#' @param token - Token for the datasource
#'                    obtained from the App
#' @param apiKey  - ApiKey for the user/company
#' @export
connect.ca <- function(url=NULL, token=NULL, apiKey=NULL, tunnelHost) {

  BAConnectionData <- R6::R6Class(
    "BAConnectionData",
    ############### public api's/variables ###############
    public = list (
      factTable = "",
      jdbc = NULL,
      columns = NULL,
      colTypes = NULL,
      engineType = NULL,
      ba2DBTypes = NULL,
      q = NULL,
      username = "",
      ############### Constructor ######################
      initialize = function(factTable,
                            jdbc,
                            columns,
                            colTypes,
                            ba2DBTypes,
                            qVar,
                            uname) {
        self$factTable <- factTable
        self$jdbc <- jdbc
        self$columns <- columns
        self$colTypes <- colTypes
        self$ba2DBTypes <- ba2DBTypes
        self$q <- qVar
        self$username <- uname
      },

      # ////////////////////////////////////////////////////
      quot = function(attribute) {
        tmp <- ""
        tmp <- paste(self$q, attribute, self$q, sep = "")
        tmp
      },

      ############### Destructor ######################
      finalize = function() {
        if (!is.null(self$jdbc))
          RJDBC::dbDisconnect(self$jdbc)
      }
    )
  )

  # //////////////////////////////////////////////////////
  BAConnection <- R6::R6Class(
    "BAConnection",
    ################## private api's/ variables ################
    private = list(
      conn_data = '',
      #///////////////////////////////////////////////
      createTable = function(df, extraColumns, type) {
        if (is.null(extraColumns))
          stop("Invalid column name")

        #Facttable name is already set in the CA App
        if(!exists("carriots.analytics.fact_table_name"))
          stop("Factable Name not set, exiting here")

        temp <- carriots.analytics.fact_table_name
        temp <- unlist(strsplit(temp, split = "[.]"))
        dsName <- NULL
        schema <- NULL
        if (length(temp) > 1) {
          schema <- temp[1]
          dsName <- temp[2]
        }
        else
          dsName <- temp

        md5table <- private$conn_data$quot(dsName)
        if (!is.null(schema))
          md5table <-
          paste(private$conn_data$quot(schema), md5table, sep = ".")

        private$dropIfExists(md5table)

        colNames <- colnames(df)
        colNames <- setdiff(colNames,extraColumns)

        query <- paste("CREATE TABLE",md5table)

        colString <- "("
        for(i in 1:length(colNames)) {
          if(i > 1)
            colString <- paste(colString,",",sep="")
          colString <- paste(colString,private$conn_data$quot(colNames[[i]]),
                             private$conn_data$ba2DBTypes[[private$getColumnType(private$conn_data$colTypes[[colNames[[i]]]])]])
        }

        colString <- paste(colString,")",sep = "")

        # query <- paste(query, md5table, "AS")
        # colString <-
        #   paste(private$conn_data$quot(colNames), ",", collapse = "")
        # colString <- substr(colString, 1, nchar(colString) - 1)
        #
        # orgQuery <-
        #   paste("(",
        #         "SELECT",
        #         colString,
        #         "FROM",
        #         private$conn_data$factTable,
        #         "WHERE 1=2",
        #         ")")
        # query <- paste(query, orgQuery)

        query <- paste(query,colString,sep = "")
        print(query)
        RJDBC::dbSendUpdate(private$conn_data$jdbc, query)


        md5table

      },
      # ///////////////////////////////////////////////////////
      addColumn = function(md5table,
                           name = NULL,
                           type = NULL,
                           default = NULL) {
        if (is.null(name) | is.null(type))
          stop("Column name or type is empty")

        if (!(type %in% names(private$conn_data$ba2DBTypes)))
          stop(paste("Invalid Type selected", "-", type))

        alterQuery <- "ALTER TABLE"
        alterQuery <-
          paste(
            alterQuery,
            md5table,
            "ADD COLUMN",
            private$conn_data$quot(name),
            private$conn_data$ba2DBTypes[[type]]
          )
        if (!is.null(default)) {
          alterQuery <- paste(alterQuery, "NOT NULL DEFAULT")
          if (is.numeric(default))
            alterQuery <- paste(alterQuery, " (", default, ") ", sep = "")
          else
            alterQuery <- paste(alterQuery, " '", default, "' ", sep = "")
        }
        print(alterQuery)
        RJDBC::dbSendUpdate(private$conn_data$jdbc, alterQuery)
      },

      #////////////////////////////////////////////////////////////////////
      insertData = function(tablename, dataframe) {
        if (nrow(dataframe) < 1)
          stop("No data available in dataframe")

        #handle NA's - Actual DF might have NA strings as well
        dataframe <- sapply(dataframe, as.character)
        dataframe[is.na(dataframe)] <- "<NA>"

        query <- "INSERT INTO"
        query <- paste(query, tablename)
        colNames <- paste("\"", colnames(dataframe), "\"", sep = "")
        query <- paste(query, "(", paste(colNames, collapse = ","), ")")

        query <- paste(query, "VALUES")

        #Prepare values
        values <- paste0(apply(dataframe, 1, function(x) paste0("('", paste0(gsub("'","''",x), collapse = "', '"), "')")), collapse = ", ")
        values <- stringr::str_replace_all(values, "'<NA>'", "NULL")


        query <- paste(query, values)
        print("INSERT QUERY - NOT Printing and it was huge")
        #print(query)

        RJDBC::dbSendUpdate(private$conn_data$jdbc, query)

      },

      #//////////////////////////////////////////////////////////////////
      dropIfExists = function(tableName) {
        t <- tableName
        temp <- unlist(strsplit(t, split = "[.]"))
        if (length(temp) > 1) {
          RJDBC::dbSendUpdate(private$conn_data$jdbc, paste("set schema", temp[1]))
          t <- temp[2]
        }

        t <- gsub("\"", "", t)

        if (RJDBC::dbExistsTable(private$conn_data$jdbc, t)) {
          RJDBC::dbRemoveTable(private$conn_data$jdbc,
                               DBI::dbQuoteIdentifier(private$conn_data$jdbc, t))
        }
      },
      #Internal use - BADimention type to DBType match
      getColumnType = function(val) {
        colType = self$dataTypes$NUMERIC
        if(val == 1)
          colType = self$dataTypes$STRING
        else if(val == 3)
          colType = self$dataTypes$INTEGER
        else if(val == 5)
          colType = self$dataTypes$DATE
        else if(val == 6)
          colType = self$dataTypes$TIME
        else if(val == 7)
          colType = self$dataTypes$DATETIME

        colType

      },

      updateDataFrameHeaders = function(df = NULL) {
        if(is.null(df))
          stop("Unable to load the data frame exiting here")

        if(exists(".ca.modelMap") && length(.ca.modelMap) > 0)
          col2Label <- .ca.modelMap
        else
          col2Label <- getColumn2Label(private$conn_data$columns)

        orgNames <- names(df)

        result = tryCatch({
          for (i in 1:length(orgNames)) {
            temp <- gsub("\\\"","",names(df)[i])
            names(df)[i] <- col2Label[[temp]]
          }
        },
        error = function(err) {
          print("Error in replacing the dataframe headers")
          msg = paste("Error in replacing the dataframe headers:",err)
          stop(msg)
        })

        #update the data types of the data frame
        df <- private$updateDFDataTypes(df)

        df
      },

      updateDFDataTypes = function(df) {
        if(is.null(self$resultSet))
          stop("Unable to get the column info/DBI Connection lost")

        colInfo <- NULL
        result = tryCatch({
          colInfo <- DBI::dbColumnInfo(self$resultSet)
        },
        error = function(err) {
          print("Error in Getting the column information from ResultSet")
          msg = paste("Error in Getting the column information from ResultSet:",err)
          stop(msg)
        })

        label2Col <- private$conn_data$columns

        #get the column to label map
        col2label <- getColumn2Label(label2Col)

        if(!is.null(colInfo)) {
          for (i in 1:nrow(colInfo)) {
            row <- colInfo[i, ]
            # there actual type is factors with multiple levels, cast it to single string
            col_name <- as.character(row$field.name)
            col_type <- as.character(row$field.type)

            #trim enclosed quotes, as quotes will be appended if header has space
            col_name <- gsub("^\"|\"$","",col_name)

            #get the label name from the coumn name
            col_label <- col2label[[col_name]]

            if( col_type == 'int') {
              df[[col_label]] = as.integer(df[[col_label]])
            }
            else if(col_type == 'date') {
              # works for default format of %Y-%m-%d and typeof() will return double
              #though class() will return the Date'
              df[[col_label]] = as.Date(as.character(df[[col_label]]))
            }
            else if(col_type == 'timestamp') {
              df[[col_label]] = as.POSIXct(df[[col_label]],tz = "UTC")
            }
          }
        }

        df
      },

      getDFtoCATypes = function(type) {

        caType <- 1
        if(!is.null(type)) {
          dfType <- type

          #In cas of POSIXct
          if(length(type) > 1)
            dfType <- type[[1]]

          if(dfType == "integer")
            caType <- 3
          else if(dfType == "numeric")
            caType <- 4
          else if(dfType == "Date")
            caType <- 5
          else if(dfType == "POSIXlt" || dfType == "POSIXct")
            caType <- 7
        }

        caType
      },

      handleSimulateUpdate = function(df = NULL,label2Col = NULL,type = NULL)
      {
        #Identify the newly added column
        oldColumns <- names(label2Col)
        newColumns <- names(df)

        print(paste("oldColumns:",paste(oldColumns,collapse = ",")))
        print(paste("newColumns:",paste(newColumns,collapse = ",")))

        #get the extra columns added
        extraColumns <- setdiff(newColumns,oldColumns)

        #If there are more than 1 extra column added- throw error
        if(length(extraColumns) > 1)
          stop("More than 1 column added to the dataframe/ headers mismatch in new dataframe - exiting here")

        # newly added column in the dataframe
        colName <- extraColumns[[1]]

        #check whether the type is passed, else default it to string
        if(is.null(type))
          type <- self$dataTypes$STRING

        if(is.null(colName) || is.na(colName))
          stop("Additional column added is empty or invalid")
        #changing the dataframe headers back to actual factTable column names
        result = tryCatch({
          orgNames <- names(df)
          for (i in 1:length(orgNames)) {
            if (orgNames[i] != colName) {
              label <- label2Col[[orgNames[i]]]
              if (is.null(label))
                stop("Dataframe has an unexpected column")
              names(df)[i] <- label
              print(paste("label:",label))
            } else {
              #This setting should have been done from the application
              if (exists(paste("carriots.analytics.derived_dim_name"))) {
                names(df)[i] <- carriots.analytics.derived_dim_name
                colName <- carriots.analytics.derived_dim_name
              }

            }
          }
        },
        error = function(err) {
          print("Error in changing the dataframe headers back to actual factTable column names")
          msg = paste("Error in changing the dataframe headers back to actual factTable column names:",err)
          stop(msg)
        })

        #create table
        md5Table <- NULL
        result = tryCatch({
          md5Table <- private$createTable(df, colName, type)
        },
        error = function(err) {
          print("Error in creating table")
          msg = paste("Error in CREATE TABLE:",err)
          stop(msg)
        })

        #Add a column first
        private$addColumn(md5Table, name = colName, type = type)

        #insert in to new table
        result = tryCatch({
          private$insertData(md5Table, df)
        },
        error = function(err) {
          print("Error in INSERT table")
          msg = paste("Error in INSERT TABLE:",err)
          stop(msg)
        })

        #Add newly created dimension
        if(exists(".caNewDims")) {
          myDim <- list()
          myDim[["name"]] <- carriots.analytics.derived_dim_name
          myDim[["datasource"]] <- private$conn_data$factTable
          myDim[["supportTable"]] <- md5Table

          .caNewDims[[1]] <<- myDim
        }

      },
      handleScoreUpdate = function(df = NULL,label2Col = NULL,modelName = NULL, modelLabel = NULL) {

        if(is.null(df) || is.null(label2Col) || is.null(modelName) || is.null(modelLabel))
          stop("Unable to perform update for score request as required params missing: All are mandatory params")
        # Update request is from score
        # We have to prerequsite some values which user is not set
        # For score we have to set the derived dim name and the factable name

        #Identify the newly added column
        oldColumns <- names(label2Col)
        newColumns <- names(df)

        print(paste("oldColumns:",paste(oldColumns,collapse = ",")))
        print(paste("newColumns:",paste(newColumns,collapse = ",")))

        #get the extra columns added
        extraColumns <- setdiff(newColumns,oldColumns)

        if(!(.caParams$TARGET_LABEL %in% extraColumns))
          stop(paste("TARGET COLUMN is not available in the data frame, missing:",.caParams$TARGET_LABEL))

        #intialize the placeholder for new Dims
        if(!exists(".caNewDims"))
          .caNewDims <<- list()

        #Prepare the fact table Name
        temp <- private$conn_data$factTable
        temp <- unlist(strsplit(temp, split = "[.]"))
        dsName <- NULL
        schema <- NULL
        if (length(temp) > 1) {
          schema <- gsub("\"", "", temp[1])
          dsName <- gsub("\"", "", temp[2])
        }else
          dsName <- temp[1]

         dsName <- paste(dsName,.caParams$TARGET_NAME,.caParams$MODEL_GROUP_NAME,modelName,sep = "_")
         print(paste("dsName:",dsName))
         carriots.analytics.fact_table_name <<- digest::digest(dsName,"md5",serialize = FALSE)
         if(!is.null(schema))
           carriots.analytics.fact_table_name <<- paste(schema,carriots.analytics.fact_table_name,sep = ".")

        #First add the targetColumn first
        myDim <- list()
        dimName = paste(.caParams$TARGET_NAME,.caParams$MODEL_GROUP_NAME,
                        modelName,sep = "_")
        print(dimName)
        dimName = paste("0x",digest::digest(dimName,"md5",serialize = FALSE),sep="_")
        dimLabel = paste(.caParams$TARGET_LABEL,modelLabel,sep = "_")
        myDim[["name"]] <- dimName
        myDim[["label"]] <- dimLabel
        myDim[["supportTable"]] <- carriots.analytics.fact_table_name
        myDim[["dataType"]] <- .caParams$TARGET_TYPE

        #add this mapping to label2colMap
        label2Col[[.caParams$TARGET_LABEL]] <- dimName

        #add the dimension
        .caNewDims[[length(.caNewDims)+1]] <<- myDim

        #Remove the target column
        targetColumn <- dimName
        newColumns <- extraColumns[extraColumns != .caParams$TARGET_LABEL]

        #prepare the new columns now
        for(val in newColumns) {
          dimName = paste(.caParams$TARGET_NAME,.caParams$MODEL_GROUP_NAME,
                          .caParams$MODEL_NAME,sep = "_")
          print(dimName)
          dimName = paste("0x",digest::digest(dimName,"md5",serialize = FALSE),sep="_")

          dimLabel = paste(val,modelLabel,sep = "_")

          #add this mapping to label2colMap
          label2Col[[val]] <- dimName

          myDim <- list()
          myDim[["name"]] <- dimName
          myDim[["label"]] <- dimLabel
          myDim[["supportTable"]] <- carriots.analytics.fact_table_name
          myDim[["dataType"]] <- private$getDFtoCATypes(class(df[[val]]))
          myDim[["base"]] <- targetColumn

          #add the dimension
          .caNewDims[[length(.caNewDims)+1]] <<- myDim
        }

        #changing the dataframe headers back to actual factTable column names
        extraColNames <- c();
        result = tryCatch({
          orgNames <- names(df)
          for (i in 1:length(orgNames)) {
            colName <- label2Col[[orgNames[[i]]]]
            if (is.null(colName))
              stop("Dataframe has an unexpected column")
            #Get the extra colNames to omit from table creation
            if(orgNames[[i]] %in% extraColumns)
              extraColNames[[length(extraColNames) + 1]] <- colName
            names(df)[i] <- colName
          }
        },
        error = function(err) {
          print("Error in changing the dataframe headers back to actual factTable column names")
          msg = paste("Error in changing the dataframe headers back to actual factTable column names:",err)
          stop(msg)
        })

        #create table
        md5Table <- NULL
        result = tryCatch({
          md5Table <- private$createTable(df, extraColNames, type)
        },
        error = function(err) {
          print("Error in creating table")
          msg = paste("Error in CREATE TABLE:",err)
          stop(msg)
        })

        #Add new columns
        for(val in extraColNames) {
          if(val == targetColumn)
            type <- private$getColumnType(.caParams$TARGET_TYPE)
          else
            type <- private$getColumnType(private$getDFtoCATypes(class(df[[val]])))

          private$addColumn(md5Table, name = val, type = type)
        }

        #insert in to new table
        result = tryCatch({
          private$insertData(md5Table, df)
        },
        error = function(err) {
          print("Error in INSERT table")
          msg = paste("Error in INSERT TABLE:",err)
          stop(msg)
        })

      },
      handleForecastUpdate = function(df = NULL,label2Col = NULL,modelName = NULL, modelLabel = NULL) {

        if(is.null(df) || is.null(label2Col) || is.null(modelName) || is.null(modelLabel))
          stop("Unable to perform update for forecast request as required params missing: All are mandatory params")
        # Update request is from Forecast
        # We have to prerequsite some values which user is not set
        # For score we have to set the derived dim name and the factable name

        #Identify the newly added column
        oldColumns <- names(label2Col)
        newColumns <- names(df)

        print(paste("oldColumns:",paste(oldColumns,collapse = ",")))
        print(paste("newColumns:",paste(newColumns,collapse = ",")))

        #get the extra columns added
        extraColumns <- setdiff(newColumns,oldColumns)

        if(!(.caParams$TARGET_LABEL %in% extraColumns))
          stop(paste("TARGET COLUMN is not available in the data frame, missing:",.caParams$TARGET_LABEL))

        #intialize the placeholder for new Dims
        if(!exists(".caNewDims"))
          .caNewDims <<- list()

        #Prepare the fact table Name
        temp <- private$conn_data$factTable
        temp <- unlist(strsplit(temp, split = "[.]"))
        dsName <- NULL
        schema <- NULL
        if (length(temp) > 1) {
          schema <- gsub("\"", "", temp[1])
          dsName <- gsub("\"", "", temp[2])
        }else
          dsName <- temp[1]

        dsName <- paste(dsName,.caParams$TARGET_NAME,.caParams$MODEL_GROUP_NAME,modelName,
                        .caParams$TEMPORAL_DIM,.caParams$FORECAST_STEP, sep = "_")
        print(paste("dsName:",dsName))
        carriots.analytics.fact_table_name <<- digest::digest(dsName,"md5",serialize = FALSE)
        if(!is.null(schema))
          carriots.analytics.fact_table_name <<- paste(schema,carriots.analytics.fact_table_name,sep = ".")

        #First add the targetColumn first
        myDim <- list()
        dimName = paste(.caParams$TARGET_NAME,.caParams$MODEL_GROUP_NAME,
                        modelName,.caParams$TEMPORAL_DIM,.caParams$FORECAST_STEP,sep = "_")
        print(dimName)
        dimName = paste("0x",digest::digest(dimName,"md5",serialize = FALSE),sep="_")
        dimLabel = paste(.caParams$TARGET_LABEL,modelLabel,sep = "_")
        myDim[["name"]] <- dimName
        myDim[["label"]] <- dimLabel
        myDim[["supportTable"]] <- carriots.analytics.fact_table_name
        myDim[["dataType"]] <- .caParams$TARGET_TYPE
        myDim[["modelName"]] <- modelName
        myDim[["base"]] <- .caParams$TARGET_NAME

        targetColumn <- dimName

        #add this mapping to label2colMap
        label2Col[[.caParams$TARGET_LABEL]] <- dimName

        #add the dimension
        .caNewDims[[length(.caNewDims)+1]] <<- myDim

        #Remove the target column
        newColumns <- extraColumns[extraColumns != .caParams$TARGET_LABEL]

        #prepare the new columns now
        for(val in newColumns) {
          dimName = paste(.caParams$TARGET_NAME,.caParams$MODEL_GROUP_NAME,
                          .caParams$MODEL_NAME,.caParams$TEMPORAL_DIM,.caParams$FORECAST_STEP,sep = "_")
          print(dimName)
          dimName = paste("0x",digest::digest(dimName,"md5",serialize = FALSE),sep="_")

          dimLabel = paste(val,modelLabel,sep = "_")

          #add this mapping to label2colMap
          label2Col[[val]] <- dimName

          myDim <- list()
          myDim[["name"]] <- dimName
          myDim[["label"]] <- dimLabel
          myDim[["supportTable"]] <- carriots.analytics.fact_table_name
          myDim[["dataType"]] <- private$getDFtoCATypes(class(df[[val]]))
          myDim[["base"]] <- .caParams$TARGET_NAME
          myDim[["modelName"]] <- modelName

          #add the dimension
          .caNewDims[[length(.caNewDims)+1]] <<- myDim
        }

        #changing the dataframe headers back to actual factTable column names
        extraColNames <- c();
        result = tryCatch({
          orgNames <- names(df)
          for (i in 1:length(orgNames)) {
            colName <- label2Col[[orgNames[[i]]]]
            if (is.null(colName))
              stop("Dataframe has an unexpected column")
            #Get the extra colNames to omit from table creation
            if(orgNames[[i]] %in% extraColumns)
              extraColNames[[length(extraColNames) + 1]] <- colName
            names(df)[i] <- colName
          }
        },
        error = function(err) {
          print("Error in changing the dataframe headers back to actual factTable column names")
          msg = paste("Error in changing the dataframe headers back to actual factTable column names:",err)
          stop(msg)
        })

        #create table
        md5Table <- NULL
        result = tryCatch({
          md5Table <- private$createTable(df, extraColNames, type)
        },
        error = function(err) {
          print("Error in creating table")
          msg = paste("Error in CREATE TABLE:",err)
          stop(msg)
        })

        #Add new columns
        for(val in extraColNames) {
          if(val == targetColumn)
            type <- private$getColumnType(.caParams$TARGET_TYPE)
          else
            type <- private$getColumnType(private$getDFtoCATypes(class(df[[val]])))

          private$addColumn(md5Table, name = val, type = type)
        }

        #insert in to new table
        result = tryCatch({
          private$insertData(md5Table, df)
        },
        error = function(err) {
          print("Error in INSERT table")
          msg = paste("Error in INSERT TABLE:",err)
          stop(msg)
        })

      },

      getColumnNames = function() {
        actualName2Label <- getColumn2Label(private$conn_data$columns)
        cols <- names(actualName2Label)
        cols
      },
      cleanUpResultSet = function() {
        status = TRUE
        if (!is.null(self$resultSet))
          status = DBI::dbClearResult(self$resultSet)

        status
      }
    ),
    ################ public apis/ variables ################################
    public = list (
      dataTypes = '',
      resultSet = NULL,
      ############### Constructor ######################
      initialize = function(conn_data) {
        private$conn_data <- conn_data
        self$dataTypes <-
          list(
            STRING = "STRING",
            NUMERIC = "NUMERIC",
            INTEGER = "INTEGER",
            DATE = "DATE",
            DATETIME = "DATETIME",
            TIME = "TIME"
          )
      },

      #//////////////////////////////////////////////////////////////////
      load = function(columns = NULL,nrows = NULL,retainHeaders = FALSE) {

        #Build the load query to prepare data frame
        dimData <- list()
        result = tryCatch({

          #boolean will set if there are not predictors from app/ user defined prection list is empty
          load_all <- FALSE
          if(is.null(columns)) {
            if(exists(".caParams")) {
              #For learn/forecast selected columns in the dialog is the predictor names
              if(.caParams[["REQ_TYPE"]] == "LEARN" || .caParams[["REQ_TYPE"]] == "FORECAST") {
                if(.caParams[["REQ_TYPE"]] == "FORECAST")
                  columns <- .caParams[["CARDINAL_DIMS"]]
                else
                  columns <- .caParams[["predictors"]]

                # check whether forecast and temporal dims there in the selection list,
                #if not add it
                if(.caParams[["REQ_TYPE"]] == "FORECAST") {
                  temporal <- .caParams[["TEMPORAL_DIM"]]
                  forecast <- .caParams[["TARGET_NAME"]]

                  if(!(temporal %in% columns))
                    columns[[length(columns)+1]] <- temporal

                  #Remove the Forecast dim if exixts and pass it as measure always
                  columns <- columns[! columns %in% forecast]
                  #add forecast dim as measure
                  dimData[["measure"]] <- c(forecast)

                  #Add the temporal dim as a first element in the list
                  columns <- columns[! columns %in% temporal]
                  columns <- c(temporal,columns)
                } 
                else if(.caParams[["REQ_TYPE"]] == "LEARN"){
                  targetDim <- .caParams[["TARGET_NAME"]]
                  #Add the TARGET_DIM as well to the data frame
                  if(!(targetDim %in% columns))
                    columns[[length(columns)+1]] <- targetDim
                }

              }
              else if(.caParams[["REQ_TYPE"]] == "SCORE") {
                columns <- names(.ca.modelMap)
              }

              if(is.null(columns)) {
                print("Failed to set the predictors from application, hence loading whole datasource")
                load_all <- TRUE
              } else {
                #Driven through the application
                dimData[["coldim"]] <- columns
              }
            }
            else
              load_all <- TRUE
          }else {
            #user defined column names
            dimData[["coldim"]] <- getColumns(colSelected = columns, private$conn_data)
          }

          if(load_all) {
            #load all the columns in the datasource
            dimData[["coldim"]] <- getColumns(NULL, private$conn_data)
          }
        },
        error = function(err) {
          print("Error in preparing load query for the data frame")
          msg = paste("Error in generating LOAD_QUERY for DATA_FRAME:",err)
          stop(msg)
        })

        print(dimData)
        params <-
          list(dstoken = token,
               dim = jsonlite::toJSON(dimData,pretty = TRUE, auto_unbox = TRUE))

        if(.caParams[["REQ_TYPE"]] == "FORECAST") {
          params[["advancedModelGroup"]] = .caParams[["MODEL_GROUP_NAME"]]
        }

        params[["type"]] = .caParams[["REQ_TYPE"]]
        headerParams <- c('X-CA-apiKey' = apiKey)

        #Reload datasource
        res <- NULL
        query <- NULL
        result = tryCatch({
          res <- doHttpCall(url, "dsExtConnect", params, headerParams)
          if(is.null(res[["sql"]]))
            stop("Unable to retrieve the SQL from CA server, Exiting!!!")

          query <- res[["sql"]]
        },
        error = function(err) {
          print("Error in datasource connect info")
          msg = paste("Error in ExtQuery generation:",err)
          stop(msg)
        })


        #Load datasource in to dataframe
        df <- NULL

        #Ensure the previous Results where GCed
        if(!is.null(self$resultSet))
          DBI::dbClearResult(self$resultSet)

        result = tryCatch({
          self$resultSet <- RJDBC::dbSendQuery(private$conn_data$jdbc, query)
          exit <- FALSE
          rowsToFetch <- nrows
          size <- CA_DEFAULT_PAGE_SIZE

          #if no.of rows provided less that default page size
          if(!is.null(rowsToFetch) && rowsToFetch < CA_DEFAULT_PAGE_SIZE)
            size <- rowsToFetch

          #Get the results
          while(!exit) {
            dfLimit <- DBI::dbFetch(self$resultSet,n=size)
            if(!is.null(nrows)) {
              rowsToFetch <- rowsToFetch - size

              #To get the left over rows(multiple iteration case)
              if(rowsToFetch < CA_DEFAULT_PAGE_SIZE)
                size = rowsToFetch
              exit <- (rowsToFetch <= 0)
            }
            #break if we exceed the no.of rows limit/ there is no result to fetch
            if(exit || nrow(dfLimit) < 1) {
              exit <- TRUE
              if(!is.null(nrows)){}
              else
                private$cleanUpResultSet()
            }

            df <- rbind(df,dfLimit)
          }

        },
        error = function(err) {
          print("Error in Getting dataframe")
          msg = paste("Error in getting dataframe:",err)
          stop(msg)
        })

        #update the headers/data types of the data frame
        df = private$updateDataFrameHeaders(df)
        df
      },
      # //////////////////////////////////////////////////////////////////////////////////////////

      nextSet = function(nrows = NULL) {
        res = NULL
        if(is.null(self$resultSet))
            prints("No results to fetch/ no active db connection")
        else {
          result = tryCatch({
            exit <- FALSE
            rowsToFetch <- nrows
            size <- CA_DEFAULT_PAGE_SIZE

            #if no.of rows provided less that default page size
            if(!is.null(rowsToFetch) && rowsToFetch < CA_DEFAULT_PAGE_SIZE)
              size <- rowsToFetch

            #Get the results
            while(!exit) {
              dfLimit <- DBI::dbFetch(self$resultSet,n=size)
              if(!is.null(nrows)) {
                rowsToFetch <- (rowsToFetch - size)

                #To get the left over rows(multiple iteration case)
                if(rowsToFetch < CA_DEFAULT_PAGE_SIZE)
                  size = rowsToFetch
                exit <- (rowsToFetch <= 0)
              }
              #break if we exceed the no.of rows limit/ there is no result to fetch
              if(exit || nrow(dfLimit) < 1) {
                exit <- TRUE
                if(!is.null(nrows)){}
                else
                  private$cleanUpResultSet()
              }


              res <- rbind(res,dfLimit)
            }

          },
          error = function(err) {
            print("Error in Getting dataframe")
            msg = paste("Error in getting dataframe:",err)
            stop(msg)
          })

        }
        #update the headers/data types of the data frame
        res = private$updateDataFrameHeaders(res)
        res
      },

      # /////////////////////////////////////////////////////////////////////////////////////
      update = function(df = NULL,
                        type = NULL,
                        modelName = NULL,
                        modelLabel = NULL)
      {
        if (is.null(df))
          stop("Required parameters were missing")

        isScore <- FALSE
        isForecast <- FALSE

        label2Col <- private$conn_data$columns

        #CHeck whether the update call is from score
        if(exists(".caParams")){
          if(.caParams[["REQ_TYPE"]] == "SCORE") {
            #GET Label to column mapping
            result = tryCatch({
              if(!exists(".ca.modelMap"))
                stop("CA model map not available exiting now")

              label2Col <- getColumn2Label(.ca.modelMap)
              isScore <- TRUE
            },
            error = function(err) {
              print("Error in Getting label to column mapping")
              msg = paste("Error in Lable 2 column mapping:",err)
              stop(msg)
            })
          }else if(.caParams[["REQ_TYPE"]] == "FORECAST") {
            newMapping <- list()
            # get the name to label mapping
            col2Label <- getColumn2Label(label2Col)

            #insert mapping only for the cardinal dims, temporal, forecasted dims selected
            #here predictors is a collection of above 3
            predictors <- .caParams$CARDINAL_DIMS

            temporal <- .caParams$TEMPORAL_DIM

            #Add the temporal dim as well. if not already exists
            if(!is.null(temporal) && (! temporal %in% predictors))
              predictors[[length(predictors)+1]] <- temporal

            #remove the target from the predictors list, as we have the
            #new forecasted column
            target <- .caParams[["TARGET_NAME"]]
            predictors <- predictors[! predictors %in% target]
            for(p in predictors) {
              newMapping[[col2Label[[p]]]] <- p
            }
            label2Col <- newMapping
            isForecast <- TRUE
          }
        }

        if(isScore) {
          private$handleScoreUpdate(df = df, label2Col = label2Col, modelName = modelName, modelLabel = modelLabel)
        }
        else if(isForecast) {
          private$handleForecastUpdate(df = df, label2Col = label2Col,modelName = modelName, modelLabel = modelLabel)
        }
        else
          private$handleSimulateUpdate(df = df, label2Col = label2Col, type = type)



        # Fire an event to reload the table in the app
        # params <-
        #   list(dstoken = token,
        #        dim = carriots.analytics.derived_dim_name,
        #        support_table = md5Table)
        # headerParams <- c('X-CA-apiKey' = apiKey)
        #
        # #Reload datasource
        # res <- NULL
        # result = tryCatch({
        #   res <- doHttpCall(url, "reloadext", params, headerParams)
        # },
        # error = function(err) {
        #   print("Error in RELOAD datasource")
        #   msg = paste("Error in RELOAD_DATASOURCE:",err)
        #   stop(msg)
        # })
        #
        #
        # res
      },

      # ////////////////////////////////////////////////////////////////////////////////////////////

      addModel = function(model = NULL,label = NULL,description=NULL,
                          predictors = NULL, params = NULL, metrics = NULL) {

        if(is.null(model)) {
          print("No models provided to register")
          return (FALSE)
        }

        if(!exists(".ca.modelList"))
          stop("Unable to store the models, FATAL ERROR!!!")

        myModel <- list()
        myModel$label = label
        myModel$description = description
        myModel$model = model
        myModel$predictors = predictors
        myModel$params = params
        myModel$metrics = metrics

        .ca.modelList[[length(.ca.modelList)+1]] <<- myModel

        return(TRUE)

      },

      getModels = function() {
        if(!exists(".ca.modelList"))
          print("No models have been saved")
        else {
          return(.ca.modelList)
        }
      },

      deleteModels = function() {
        if(!exists(".ca.modelList"))
          print("No models to delete")
        else
          .ca.modelList <<- NULL
      },

      getParam = function(key) {
        if(!exists(".caParams"))
          stop("CA params were not set, FATAL ERROR!!!")

        .caParams[[key]]
      },

      listParams = function() {
        if(!exists(".caParams"))
          stop("No Params were set")

        names(.caParams)
      },

      getColumnLabels = function() {
        cols <- names(private$conn_data$columns)
        cols
      },
      ############### Destructor ######################
      finalize = function() {
        result = tryCatch({
          res <- private$cleanUpResultSet()
          if(!res)
            print("Unable to clear the resultsset")
        },
        error = function(err) {
          print("Unable to clear the resultsset")
        })
      }
    )
  )

  # //////////////////////////////////////////////////////
  # Call the CA REST API to get the connection data
  # Based on the engine type, get the appropriate JDBC driver

  #check URL,token and API set in the app
  if(exists(".caParams")) {
    if(is.null(url))
      url <- .caParams[["URL"]]

    if(is.null(token))
      token <- .caParams[["DSTOKEN"]]

    if(is.null(apiKey))
      apiKey <- .caParams[["APIKEY"]]
  }

  data <- getDatasourceConnection(url, token, apiKey,tunnelHost)

  jdbc <- data$jdbc
  factTable <- data$ftable
  columns <- data$columns
  colTypes <- data$colDataTypes
  quot <- data$quot
  ba2DBTypes <- getDataTypes(data$engineType)

  connect_data <-
    BAConnectionData$new(factTable, jdbc, columns,colTypes, ba2DBTypes, quot, data$username)
  conn <- BAConnection$new(connect_data)
  conn
}


#################################################################################
#' Method to load the table data
#'
#' User can pass the selected column names as the data frame returned by this
#' method will have only the selected columns
#'
#' @param columns - vector of selected column names
#' @param conn  - BAConnection object obtained from connect API
#'
#'@export
load.ca = function(conn = NULL, columns = NULL) {
  conn$load(columns = columns)
}

#################################################################################
#' Method to update the data in the table
#'
#' Update the table with the values provided in the data frame. Below is the
#' structure of the data frame for WhereClause
#'
#'  @param conn - BAConnection object obtained from connect API
#'  @param dataframe - DataFrame for where Clause
#'  @param model_name - Name of the model by running which the dataframe has been created(required in case of score/forecast)
#'  @param model_label - Label of the model by running which the dataframe has been created(required in case of score/forecast)
#'
#'@export
update.ca = function(conn = NULL,
                     dataframe = NULL,
                     model_name = NULL,
                     model_label = NULL) {
  conn$update(df = dataframe,
              colName = colname,
              modelName = model_name,
              modelLabel = model_label)
}


#################################################################################
#' Method to the forecast/learn models
#'
#' Serializes and add the models in the CA database for future use
#'
#'  @param conn - BAConnection object obtained from connect API
#'  @param model - model object which wants to be stored for future use
#'  @param label - label for the model
#'  @param description - short description about the model
#'  @param params - model specific parameters to set for future execution
#'  @param metrics - A dataFrame with metrics about the internal forecast/learn model that can be displayed to the user
#'
#'@export
addModel.ca = function(conn = NULL,
                     model = NULL,label = NULL,description=NULL,
                     predictors = NULL, params = NULL, metrics = NULL) {
  conn$addModel(model = model,label = label,description=description,
                predictors = predictors, params = params, metrics = metrics)
}

##################################################################################
# UTILITIES
##################################################################################
asc <- function(x) {
  strtoi(charToRaw(x), 16L)
}

chr <- function(n) {
  rawToChar(as.raw(n))
}

#supported data types - ba2DBTypes

getDataTypes <- function(engineType) {
  dataTypes <-
    list(
      STRING = "VARCHAR(256)",
      NUMERIC = "DOUBLE",
      INTEGER = "INT",
      DATE = "DATE",
      DATETIME = "TIMESTAMP",
      TIME = "TIME"
    )

  if (toupper(engineType) == "ORACLE") {
    dataTypes$NUMERIC = "NUMBER(20,4)"
    dataTypes$INTEGER = "NUMBER(20)"
  }
  else if (toupper(engineType) == "MYSQL") {
    dataTypes$NUMERIC = "DECIMAL(20,4)"
    dataTypes$DATETIME = "DATETIME"
  }
  else if (toupper(engineType) == "REDSHIFT") {
    dataTypes$NUMERIC = "DOUBLE PRECISION"
    dataTypes$INTEGER = "BIGINT"
  }

  dataTypes
}

getColumns <- function(colSelected = NULL, conn) {
  cols <- conn$columns
  colNames <- NULL
  if (!is.null(colSelected) &  length(colSelected) > 0) {
    length <- length(colSelected)
    colNames <- c()
    for (i in 1:length) {
      if (!is.null(cols[[colSelected[[i]]]])) {
        colNames[[i]] <- cols[[colSelected[[i]]]]
      } else
        stop(paste("ColName:", colSelected[[i]], "doesn't exists in the table"))
      i <- i + 1
    }
  } else {
      colNames <- names(getColumn2Label(cols))
  }

  colNames
}

getColumn2Label <- function(colList) {
  column <- paste(colList)
  labels <- names(colList)

  col2Label <- list()
  for (i in 1:length(labels))
    col2Label[column[i]] = labels[i]

  col2Label

}

# Get DataSource Meta Data
getDataSourceMetaData <- function(baseUrl, token, apiKey) {
  if(is.null(baseUrl))
    stop("App URL not provided")

  if(is.null(token))
    stop("Data source not provided")

  if(is.null(apiKey))
    stop("API Key not provided")

  headerParams <- c('X-CA-apiKey' = apiKey)
  queryParams <- list(dstoken = token)
  res <- doHttpCall(baseUrl, "dsExtConnect", queryParams, headerParams)
  res
}


# Get DataSource Connection
getDatasourceConnection <- function(baseUrl, token, apiKey=NULL,tunnelHost) {
  data <- getDataSourceMetaData(baseUrl, token, apiKey)
  if (!is.null(data$connect_data)) {
    connect_data <- data$connect_data
    jdbcDetails <- getDriverDetails(connect_data,tunnelHost)
    if (is.null(jdbcDetails$driverClass) ||
        is.null(jdbcDetails$driver) || is.null(jdbcDetails$connString))
      stop("Unable to create JDBC connection- required info missing")

    #decrypt password
    passWord <- character()
    encrypt <- strsplit(connect_data$password, "")[[1]]
    for (i in 1:stringr::str_length(connect_data$password)) {
      passWord[[i]] <- chr(asc(encrypt[[i]]) - 4)
    }
    decrypt <- paste(passWord, collapse = "")
    passWord <- stringi::stri_reverse(decrypt)
    passWord <- rawToChar(base64enc::base64decode(passWord))

    #passPhrase <- digest::AES(connect_data$ftable,mode="CBC")
    #passPhrase <- substr(passPhrase,0,8)
    #aes <- digest::AES(passPhrase,mode="CBC")
    jdbcDriver <- RJDBC::JDBC(
      driverClass = jdbcDetails$driverClass,
      classPath = system.file("extdata", jdbcDetails$driver, package = "caDB"),
      identifier.quote = jdbcDetails$quot
    )

    conn <-
      RJDBC::dbConnect(jdbcDriver,
                       jdbcDetails$connString,
                       connect_data$username,
                       passWord)
    ftable <- connect_data$ftable
    data <- NULL
    data$ftable <- ftable
    data$username <- connect_data$user_login_name
    data$jdbc <- conn
    data$quot <- jdbcDetails$quot
    data$columns <- connect_data$columns
    data$colDataTypes <- connect_data$colDataTypes
    data$engineType <- connect_data$engine_type

  } else {
    stop("Connect data of the datasource not available")
  }
  data
}

doHttpCall <-
  function(baseUrl,
           identifier,
           queryParams,
           headerParams) {
    url_length <- stringr::str_length(baseUrl)

    if (substr(baseUrl, url_length, url_length) == "/") {
      baseUrl <- substr(baseUrl, 0, url_length - 1)
    }

    baseUrl <-
      paste(baseUrl, "/datasource.do?action=", identifier, sep = "")

    ua      <-
      "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0"

    doc <- httr::POST(baseUrl,
                      httr::add_headers(.headers = headerParams),
                      query = queryParams,
                      httr::user_agent(ua),
                      config = httr::config(ssl_verifypeer = FALSE,ssl_verifyhost=FALSE))
    res <- httr::content(doc, useInternalNodes = T)

    data <- jsonlite::fromJSON(res)

    data
  }

getDriverDetails <- function(connect_data,tunnelHost) {
  if (is.null(connect_data$engine_type))
    stop("Engine Type not found")
  engineType <- connect_data$engine_type

  if(!missing(tunnelHost)) {
    host_port <- tunnelHost
  } else {
    if (is.null(connect_data$hostname))
      stop("Host Name not found")
    host_port <- connect_data$hostname

    if (!is.null(connect_data$port))
      host_port <- paste(host_port, connect_data$port, sep = ":")
  }
  if (is.null(connect_data$dbName))
    stop("DBName not found")

  jdbcDetails <- NULL
  if (toupper(engineType) == "MONETDB") {
    jdbcDetails$driverClass <- "nl.cwi.monetdb.jdbc.MonetDriver"
    jdbcDetails$driver <- "monetdb-jdbc-2.8.jar"
    jdbcDetails$connString <-
      paste("jdbc:monetdb://",
            host_port,
            "/",
            connect_data$dbName,
            sep = "")
    jdbcDetails$quot <- "\""

  } else if (toupper(engineType) == "MYSQL") {
    jdbcDetails$driverClass <- "com.mysql.jdbc.Driver"
    jdbcDetails$driver <- "mysql-connector-java-5.1.21-bin"
    jdbcDetails$connString <-
      paste("jdbc:mysql://", host_port, "/", connect_data$dbName, sep = "")
    jdbcDetails$quot <- "`"

  } else if (toupper(engineType) == "POSTGRESQL" ||
             toupper(engineType) == "REDSHIFT") {
    jdbcDetails$driverClass <- "org.postgresql.Driver"
    jdbcDetails$driver <- "postgresql-9.2-1002.jdbc4.jar"
    jdbcDetails$connString <-
      paste("jdbc:postgresql://",
            host_port,
            "/",
            connect_data$dbName,
            sep = "")
    jdbcDetails$quot <- "\""

  } else if (toupper(engineType) == "SQLSERVER") {
    jdbcDetails$driverClass <-
      "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    jdbcDetails$driver <- "sqljdbc4.jar"
    jdbcDetails$connString <-
      paste("jdbc:sqlserver://",
            getSQLServerConnString(connect_data),
            sep = "")
    jdbcDetails$quot <- "\""

  } else if (toupper(engineType) == "ORACLE") {
    jdbcDetails$driverClass <- "oracle.jdbc.driver.OracleDriver"
    jdbcDetails$driver <- "ojdbc6.jar"
    jdbcDetails$connString <-
      paste("jdbc:oracle:thin:@",
            host_port,
            "/",
            connect_data$dbName,
            sep = "")
    jdbcDetails$quot <- "\""

  } else if (toupper(engineType) == "SUNDB") {
    jdbcDetails$driverClass <- "sunje.sundb.jdbc.SundbDriver"
    jdbcDetails$driver <- "sundb6.jar"
    jdbcDetails$connString <-
      paste("jdbc:sundb://", host_port, "/", connect_data$dbName, sep = "")
    jdbcDetails$quot <- "\""

  } else if (toupper(engineType) == "MARIADB") {
    jdbcDetails$driverClass <- "org.mariadb.jdbc.Driver"
    jdbcDetails$driver <- "mariadb-java-client-1.3.3.jar"
    jdbcDetails$connString <-
      paste("jdbc:mariadb://",
            host_port,
            "/",
            connect_data$dbName,
            sep = "")
    jdbcDetails$quot <- "\""
  }

  jdbcDetails
}

getSQLServerConnString <- function(connect_data) {
  conn_string <-
    paste(connect_data$hostname, connect_data$dbName, sep = "\\")
  if (!is.null(connect_data$port))
    conn_string <- paste(conn_string, connect_data$port, sep = ":")

  conn_string
}
