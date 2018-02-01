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
      engineType = NULL,
      ba2DBTypes = NULL,
      q = NULL,
      username = "",
      ############### Constructor ######################
      initialize = function(factTable,
                            jdbc,
                            columns,
                            ba2DBTypes,
                            qVar,
                            uname) {
        self$factTable <- factTable
        self$jdbc <- jdbc
        self$columns <- columns
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
      createTable = function(df, colName, type) {
        if (is.null(colName))
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
        colNames <- colNames[colNames != colName]

        if (!all(colNames %in% self$getColumnNames()))
          stop("Data Frame has more than one new column compared to fact table")

        query <- "CREATE TABLE"
        query <- paste(query, md5table, "AS")
        colString <-
          paste(private$conn_data$quot(colNames), ",", collapse = "")
        colString <- substr(colString, 1, nchar(colString) - 1)

        orgQuery <-
          paste("(",
                "SELECT",
                colString,
                "FROM",
                private$conn_data$factTable,
                "WHERE 1=2",
                ")")
        query <- paste(query, orgQuery)

        print(query)
        RJDBC::dbSendUpdate(private$conn_data$jdbc, query)

        #Add a column first
        private$addColumn(md5table, name = colName, type = type)

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

        query <- "INSERT INTO"
        query <- paste(query, tablename)
        colNames <- paste("\"", colnames(dataframe), "\"", sep = "")
        query <- paste(query, "(", paste(colNames, collapse = ","), ")")

        query <- paste(query, "VALUES")

        values <- ""

        for (i in 1:nrow(dataframe)) {
          if (i > 1)
            values <- paste(values, ",")
          row <- dataframe[i, ]
          #string values are printed with index
          #values <- paste(values,paste("(",paste(paste("'",row,"'",sep=""),collapse=","),")",sep = ""))
          values <-
            paste(values, paste("(", paste(apply(row, 1, function(k) {
              paste(paste("'", gsub("'","''",k), "'", sep = ""), collapse = ",")
            })), ")", sep = ""))
        }

        query <- paste(query, values)
        print(query)

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

      }
    ),
    ################ public apis/ variables ################################
    public = list (
      dataTypes = '',
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
      load = function(columns = NULL,retainHeaders = FALSE) {

        #Build the load query to prepare data frame
        query <- ""
        result = tryCatch({
          if(exists(".caParams") && !is.null(.caParams[["loadPredictors"]])) {
            if(is.null(columns))
              columns <- .caParams[["predictors"]]
            colStr <- paste0('"', paste(columns, collapse='", "'), '"')
            query <-paste("SELECT", colStr)
          }
          else if(exists(".ca.modelMap") && !is.null(.ca.modelMap) && length(.ca.modelMap) > 0){
            columns <- names(.ca.modelMap)
            colStr <- paste0('"', paste(columns, collapse='", "'), '"')
            query <-paste("SELECT", colStr)
          }
          else {
            query <-
              paste("SELECT ",
                    getColumns(colSelected = columns, private$conn_data),
                    sep = "")
          }
        },
        error = function(err) {
          print("Error in preparing load query for the data frame")
          msg = paste("Error in generating LOAD_QUERY for DATA_FRAME:",err)
          stop(msg)
        })


        query <- paste(query, "FROM", private$conn_data$factTable)
        # if(!is.null(limit) & !is.na(limit) & limit > 0)
        #   query <- paste(query, "LIMIT",limit)

        #Load datasource in to dataframe
        df <- NULL
        result = tryCatch({
          df <- RJDBC::dbGetQuery(private$conn_data$jdbc, query)
        },
        error = function(err) {
          print("Error in Getting dataframe")
          msg = paste("Error in getting dataframe:",err)
          stop(msg)
        })

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


        df
      },

      # ////////////////////////////////////////////////////////////
      update = function(df = NULL,
                        type = NULL,
                        modelName = NULL)
      {
        if (is.null(df) || is.na(df))
          stop("Required parameters were missing")

        isScore <- FALSE
        label2Col <- private$conn_data$columns

        if(exists(".ca.modelMap")){
          #GET Label to column mapping
          result = tryCatch({
            label2Col <- getColumn2Label(.ca.modelMap)
            isScore <- TRUE
          },
          error = function(err) {
            print("Error in Getting label to column mapping")
            msg = paste("Error in Lable 2 column mapping:",err)
            stop(msg)
          })
        }

        #Identify the newly added column
        oldColumns <- names(label2Col)
        newColumns <- names(df)

        #get the extra columns added
        extraColumns <- setdiff(newColumns,oldColumns)

        #If there are more than 1 extra column added- throw error
        if(length(extraColumns) > 1)
          stop("More than 1 column added to the dataframe/ headers mismatch in new dataframe - exiting here")

        # newly added column in the dataframe
        colName <- extraColumns[[1]]

        if(is.null(colName) || is.na(colName))
          stop("Additional column added is empty or invalid")

        if(isScore) {
          # Update request is from score
          # We have to prerequsite some values which user is not set
          # For score we have to set the derived dim name and the factable name

          if(!is.null(modelName) && !is.null(.caParams$TARGET_NAME)) {
            carriots.analytics.derived_dim_name <- paste(.caParams$TARGET_NAME,gsub(" ","_",modelName),sep = "_")
            if(!is.null(.caParams$LABEL_PREFIX))
              carriots.analytics.derived_dim_name <- paste(.caParams$LABEL_PREFIX,carriots.analytics.derived_dim_name,sep = "_")
          }

          temp <- private$conn_data$factTable
          temp <- unlist(strsplit(temp, split = "[.]"))
          dsName <- NULL
          schema <- NULL
          if (length(temp) > 1) {
            schema <- gsub("\"", "", temp[1])
            dsName <- gsub("\"", "", temp[2])
          }else
            dsName <- temp[1]

          tableName <- dsName
          if(!is.null(.caParams$LABEL_PREFIX))
            tableName <- paste(tableName,.caParams$LABEL_PREFIX,sep = "_")

          tableName <- paste(tableName,.caParams$TARGET_NAME,gsub(" ","_",modelName),sep = "_")

          md5table <- digest::digest(tableName,"md5",serialize = FALSE)
          if(!is.null(schema))
            carriots.analytics.fact_table_name <<- paste(schema,md5table,sep = ".")
          else
            carriots.analytics.fact_table_name <<- md5table

          #now set the dataType of the derived dimension
          #if type is null set the TARGET dimensions data type
          if(is.null(type)){
            val <- .caParams$TARGET_TYPE
            if(is.null(val)) val <- 4
            type <- private$getColumnType(val)
          }
        }

        print(paste0("FACT_TABLE:",carriots.analytics.fact_table_name))

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

        #insert in to new table
        result = tryCatch({
          private$insertData(md5Table, df)
        },
        error = function(err) {
          print("Error in INSERT table")
          msg = paste("Error in INSERT TABLE:",err)
          stop(msg)
        })

        #Fire an event to reload the table in the app
        params <-
          list(dstoken = token,
               dim = carriots.analytics.derived_dim_name,
               support_table = md5Table)
        headerParams <- c('X-CA-apiKey' = apiKey)

        #Reload datasource
        res <- NULL
        result = tryCatch({
          res <- doHttpCall(url, "reloadext", params, headerParams)
        },
        error = function(err) {
          print("Error in RELOAD datasource")
          msg = paste("Error in RELOAD_DATASOURCE:",err)
          stop(msg)
        })


        res
      },

      addModel = function(model = NULL,label = NULL,description=NULL,
                          predictors = NULL, params = NULL) {

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

      getParam = function(key) {
        if(!exists(".caParam"))
          stop("CA params were not set, FATAL ERROR!!!")

        .caParam[[key]]
      },

      listParams = function() {
        if(!exists(".caParam"))
          stop("No Params were set")

        names(.caParam)
      },

      getColumnNames = function() {
        cols <- names(private$conn_data$columns)
        cols
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
  quot <- data$quot
  ba2DBTypes <- getDataTypes(data$engineType)

  connect_data <-
    BAConnectionData$new(factTable, jdbc, columns, ba2DBTypes, quot, data$username)
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
#'  @param colname -  colname in dataframe which is to be added in to table
#'  @param type - Data type of column, supports specific types  available in the conn$dataTypes list
#'
#'@export
update.ca = function(conn = NULL,
                     dataframe = NULL,
                     colname = NULL,
                     type = NULL) {
  conn$update(df = dataframe,
              colName = colname,
              type = type)
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
  colString <- ""
  if (!is.null(colSelected) &  length(colSelected) > 0) {
    length <- length(colSelected)
    for (i in 1:length) {
      if (i > 1 & i <= length)
        colString <- paste(colString, ",")
      if (!is.null(cols[[colSelected[[i]]]])) {
        colString <- paste(colString, conn$quot(cols[[colSelected[[i]]]]))
      } else
        stop(paste("ColName:", colSelected[[i]], "doesn't exists in the table"))
      i <- i + 1
    }
  } else
    colString <- "*"

  colString
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
      classPath = system.file("extdata", jdbcDetails$driver, package = "CarriotsAnalytics"),
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
