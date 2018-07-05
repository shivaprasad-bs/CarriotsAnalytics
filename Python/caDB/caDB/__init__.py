"""
Get datasource connection

     This is a function to obtain connection to a
     datasource from the envision server by providing the token
     obtained from the envision App for the specific datasource

    @param baseUrl - CA server URL
    @param token - Token for the datasource
                    obtained from the App
    @param apiKey  - ApiKey for the user/company
"""
from os import sep,path
from sys import exc_info,modules
from requests import post
from base64 import b64decode
from pandas import DataFrame,concat
from re import sub
import numpy as np
from json import dumps
import hashlib
#import jaydebeapi
import jpype
import pymonetdb as monet

CA_DEFAULT_PAGE_SIZE = 100000
_caParams = None
_ca_modelMap = None
_caNewDims = None

def connect_ca(url=None,token=None,apikey=None,tunnelHost = None):
    
    class BAConnectionData:
        factTable = ""
        jdbc = None
        columns = None
        colTypes = None
        engineType = None
        ba2DBTypes = None
        qVar = None
        username = None
        
        # The class "constructor" - It's actually an initializer 
        def __init__(self, factTable,jdbc,columns,colTypes,ba2DBTypes,qVar,uname):
            self.factTable = factTable
            self.jdbc = jdbc
            self.columns = columns
            self.colTypes = colTypes
            self.ba2DBTypes = ba2DBTypes
            self.qVar = qVar
            self.username = uname
    
        def quot(self,attribute):
            return self.qVar + attribute + self.qVar
    
        def __del__(self):
            self.jdbc.close()
            
    
    class BAConnection:
        __conn_data__ = None
        dataTypes = None
        resultSet = None
        
        def __init__(self,conn_data):
            self.__conn_data__ = conn_data
            self.dataTypes = {"STRING" : "STRING",
                              "NUMERIC" : "NUMERIC",
                              "INTEGER" : "INTEGER",
                              "DATE" :"DATE",
                              "DATETIME" : "DATETIME",
                              "TIME" : "TIME"}
            
        def __createTable__(self,mapd_cursor,df,extraColumns):
            if extraColumns is None:
                raise Exception("Invalid colName")
            
            #Facttable name is already set in the CA App
            if(_caParams['carriots_analytics_fact_table_name'] is None):
                raise Exception("Fact table name not set, exiting here")
                
            temp = getParam('carriots_analytics_fact_table_name')
            _vals = temp.split(".")
            schema = None
            if(isinstance(_vals,list) and len(_vals) > 1):
                schema = _vals[0]
                dsName = _vals[1]
            else:
                dsName = temp
                
            _md5table = self.__conn_data__.quot(dsName)
            if(schema is not None):
                _md5table = self.__conn_data__.quot(schema) + "." + _md5table
            
            self.__dropIfExists__(mapd_cursor,_md5table)
            colNames = list(df.columns.values)
            print(colNames)
            colNames = np.setdiff1d(colNames,extraColumns).tolist()
                
            
            if(not(set(colNames) < set(self.__getColumnNames__()))):
                raise Exception("Data Frame has more than one new column compared to fact table")
            
            query = "CREATE TABLE "
            query = query + _md5table
            
            colString = "("
            for i,e in enumerate(colNames):
                if(i > 1):
                    colString = colString + ","
                    
                colString = colString + self.__conn_data__.quot(e) +" "+\
                self.__conn_data__.ba2DBTypes[self.__getColumnType__(self.__conn_data__.colTypes[e])]
                
            colString = colString + ")"
            
            query = query + colString    
            
            #colString = ",".join(str(self.__conn_data__.quot(x)) for x in colNames)            
            #orgQuery = "(SELECT " + colString + " FROM " + self.__conn_data__.factTable + " WHERE 1=2 " + ")"
            #query = query + orgQuery
            
            print (query)
            mapd_cursor.execute(query)
                
            #add column
            #self.__addColumn__(mapd_cursor,_md5table,colname,_type,default)
                               
            return _md5table
        
        def __addColumn__(self,mapd_cursor,_md5table,name = None,
                          _type = None, default = None):
            if(name is None or _type is None):
                raise Exception("Column name or Type is empty")
            
            if(_type not in self.__conn_data__.ba2DBTypes.keys()):
                raise Exception("Invalid Type selected - "+ _type)
            
            alterQuery = "ALTER TABLE "+ _md5table + " ADD COLUMN "+ self.__conn_data__.quot(name) + " "+ self.__conn_data__.ba2DBTypes[_type]
            if(default is not None):
                alterQuery = alterQuery + " NOT NULL DEFAULT"
                if(default.isdigit()):
                    alterQuery = alterQuery + " (" + default + ")"
                else:
                    alterQuery = alterQuery + " '" + default + "'"
                    
            print(alterQuery)
            mapd_cursor.execute(alterQuery)
                
        
        def __dropIfExists__(self,mapd_cursor,tableName):            
            dsName = tableName
            _vals = tableName.split(".")
            schema = None
            if(isinstance(_vals,list) and len(_vals) > 1):
                schema = _vals[0]
                dsName = schema +"."+_vals[1]
            try:
                query = "DROP TABLE "+ dsName
                print(query)
                mapd_cursor.execute(query)
            except:
                e = exc_info()[0]
                print(e)
                
            
        def __insertData__(self,mapd_cursor,tablename,df):
            if(df is None or df.empty):
                raise Exception("No data available in dataframe")
                
            query = "INSERT INTO "+tablename
            colNames = "(" + (",".join("\""+str(x)+"\"" for x in df.keys()))+ ")"
            query = query + colNames + " VALUES "
            
            values = ""
            
            for index,row in df.iterrows():
                if(index > 0):
                    values = values + " , "
                
                _row = df.loc[index,].values
                values = values + "(" + (",".join("'"+sub("'","''",str(x)) + "'" for x in _row))+ ")"
                values = values.replace('<NA>','NULL')
            
            query = query + values
            print("Insert query not printing as it is huge")
            #print(query)
            
            #excecute query
            mapd_cursor.execute(query)

        def __updateDataFrameHeaders__(self, df = None):
            if(df is None or df.empty):
               raise Exception("No data available in dataframe")
            
            col2Label = None
            if (_ca_modelMap is not None and len(_ca_modelMap)):
                col2Label = _ca_modelMap
            else:
                col2Label = __getColumn2Label__(self.__conn_data__.columns)
                
            orgNames = df.columns.values
            
            try:
                for i,e in enumerate(orgNames):
                    temp = sub("\\\"","",e)
                    df.columns.values[i] = col2Label[temp]
            except:
                print("Error in replacing the dataframe headers")
                raise Exception("Error in replacing the dataframe headers")
            
            return df
                
        def __getDFtoCATypes__(_type = None):
            caType = 1
            if(_type is not None):
                if(_type == np.int64):
                    caType = 3
                elif(_type == np.float64):
                    caType = 4
                elif(_type == "Date"):
                    caType = 5
                elif(_type == np.datetime64):
                    caType = 7
            return caType
        
        def __getColumnNames__(self):
            cols = self.__conn_data__.columns.keys()
            return list(cols)
        
        
        def __cleanUpResultSet__(self):
            status = True
            if(self.resultSet is not None):
                try:
                    self.resultSet.close()
                except:
                    print("Unable to clear the resultset")
                    status = False
            return status
        
        def __cursor_to_df__(self,size = None):
            df = None
            if(self.resultSet is not None):
                if(size is not None):
                    df = DataFrame(self.resultSet.fetchmany(size = size))
                else:
                    df = DataFrame(self.resultSet.fetchall())
                
                if( not df.empty):
                    df.columns = [d[0] for d in self.resultSet.description]
                    
            return df
        
        def __getColumnType__(self,val):
            colType = self.dataTypes['NUMERIC']
            if(val == 1):
                colType = self.dataTypes['STRING']
            elif(val == 3):
                colType = self.dataTypes['INTEGER']
            elif(val == 5):   
                 colType = self.dataTypes['DATE']
            elif(val == 6):   
                 colType = self.dataTypes['TIME']
            elif(val == 7):   
                 colType = self.dataTypes['DATETIME']
                 
            return colType
        
        def __handleSimulateUpdate__(self,df = None, label2Col = None, _type = None):
            #identify the newly added column
            oldColumns = list(label2Col.keys())
            newColumns = list(df.keys())
            
            print("old columns:",oldColumns)
            print("new columns:",newColumns)
            
            #get the extra columns added
            extraColumns = np.setdiff1d(newColumns,oldColumns).tolist()
            
            #if there are more than 1 extra column added - throw error
            if(len(extraColumns) > 1):
                raise Exception("More than 1 column added to the dataframe/ headers mismatch in new dataframe - exiting here")
                
            #newly added column in the dataframe
            colName = extraColumns[0]
            
            #check whether the type is passed, else default it to string
            if(_type is None):
                _type = self.dataTypes['STRING']
            
            if(colName is None):
                raise Exception("Additional column added is empty or invalid")
            
            #changing the dataframe headers back to actual factables column names
            try:
                orgNames = list(df.keys())
                for i,e in enumerate(orgNames):
                    if(e != colName):
                        label = label2Col[e]
                        if(label is None):
                            raise Exception("Dataframe has unexpected column")
                        df.columns.values[i] = label
                        print("label:",label)
                    else:
                          #This setting should have been done from the application
                          if (getParam('carriots_analytics_derived_dim_name') is not None):
                              derv_name = getParam('carriots_analytics_derived_dim_name')
                              df.columns.values[i] = derv_name
                              colName = derv_name
            except:
                 print("Error in changing the dataframe headers back to actual factTable column names")
                 raise Exception("Error in changing headers of DF")
            
            jdbc = self.__conn_data__.jdbc
            mapd_cursor = jdbc.cursor()
            #create table
            md5Table = None
            md5Table = self.__createTable__(mapd_cursor,df,colName)
            
            #Add a column first
            self.__addColumn__(mapd_cursor,md5Table,name = colName, _type = _type)
            
            #insert data in to table
            self.__insertData__(mapd_cursor,md5Table,df)
            
            global _caNewDims
            #intialize the placeholder for new Dims
            if(_caNewDims is None):
                _caNewDims = list()
                
            #Add newly created dimensions
            if(_caNewDims is not None):
                myDim = dict()
                myDim['name'] = getParam('carriots_analytics_derived_dim_name')
                myDim['datasource'] = self.__conn_data__.factTable
                myDim['supportTable'] = md5Table
                
                _caNewDims.append(myDim)         
        
        def __handleScoreUpdate__(self, df = None, label2Col = None, modelName = None, modelLabel = None):
            
            if(df is None or label2Col is None or modelName is None or modelLabel is None):
                raise Exception("Unable to perform update for score request as required params missing: All are mandatory params")
            
            #update request is from score 
            # We have to prerequsite some values which user is not set
            # For score we have to set the derived dim name and the factable name
            
            #identify the newly added column
            oldColumns = list(label2Col.keys())
            newColumns = list(df.keys())
            
            print("old columns:",oldColumns)
            print("new columns:",newColumns)
            
            #get the extra columns added
            extraColumns = np.setdiff1d(newColumns,oldColumns).tolist()
            
            if(_caParams.get('TARGET_LABEL') not in extraColumns):
                raise Exception("TARGET COLUMN is not available in the data frame, missing:" , _caParams.get('TARGET_LABEL'))
            
            global _caNewDims
            #intialize the placeholder for new Dims
            if(_caNewDims is None):
                _caNewDims = list()
            
            #Prepare the fact table Name
            temp = self.__conn_data__.factTable
            temp = list(temp.split("."))
            dsName = None
            schema = None
            if(len(temp) > 1):
                schema = temp[0].replace("\"","")
                dsName = temp[1].replace("\"","")
            else:
                dsName = temp[0]
            
            dsName = dsName + "_" + _caParams.get('TARGET_NAME') + "_"  + _caParams.get('MODEL_GROUP_NAME') + "_" + modelName
            print("dsName:",dsName)
            carriots_analytics_fact_table_name = hashlib.md5(dsName.encode("UTF-8")).hexdigest()
            if(schema is not None):
                carriots_analytics_fact_table_name = schema + "." + carriots_analytics_fact_table_name
            
            #First add the target column first
            myDim = dict()
            dimName =  _caParams.get('TARGET_NAME') + "_"  + _caParams.get('MODEL_GROUP_NAME') + "_" + modelName
            print("dimName:", dimName)
            dimName = "0x_" + hashlib.md5(dimName.encode("UTF-8")).hexdigest()
            dimLabel = _caParams.get('TARGET_LABEL') + "_" + modelLabel
            myDim['name'] = dimName
            myDim['label'] = dimLabel
            myDim['supportTable'] = carriots_analytics_fact_table_name
            myDim['dataType'] = _caParams.get('TARGET_TYPE')
            
            #add this mapping to label2colMap
            label2Col[_caParams.get('TARGET_LABEL')] = dimName
            
            #add the dimension
            _caNewDims[len(_caNewDims)] = myDim
            
            #Remove  the target column
            targetColumn = dimName
            newColumns = [x for x in extraColumns if x not in _caParams.get('TARGET_LABEL')]
            
            #prepare the new column now
            for val in newColumns:
                dimName =  _caParams.get('TARGET_NAME') + "_"  + _caParams.get('MODEL_GROUP_NAME') + "_" + _caParams.get('MODEL_NAME')
                print(dimName)
                dimName = "0x_" + hashlib.md5(dimName.encode("UTF-8")).hexdigest()
                dimLabel = val + "_" + modelLabel
                
                #add this mapping to the label2ColMap
                label2Col[val] = dimName
                
                myDim = dict()
                myDim['name'] = dimName
                myDim['label'] = dimLabel
                myDim['supportTable'] = carriots_analytics_fact_table_name
                myDim['dataType'] = self.__getDFtoCATypes__(type(df[val]))
                myDim['base'] = targetColumn
                
                #add the dimension
                _caNewDims[len(_caNewDims)] = myDim
            
            #changing the dataframe headers back to actual fact table column names
            extraColNames = list()
            try:
                orgNames = list(df.columns)
                for i,e in enumerate(orgNames):
                    colName = label2Col[e]
                    if(colName is None):
                        raise Exception("Dataframe has an unexpected column")
                    #Get the extra colNames to omit from table creation
                    if(e in extraColumns):
                        extraColNames[len(extraColNames)] = colName
                    df.columns.values[i] = colName
            except:
                print("Error in changing the dataframe headers back to actual factTable column names")
                raise Exception("Error in changing the dataframe headers")
            
            jdbc = self.__conn_data__.jdbc
            mapd_cursor = jdbc.cursor()
            
            #create table
            md5Table = self.__createTable__(df,mapd_cursor,colName)
            
            #Add the new columns
            for val in extraColumns:
                if(val == targetColumn):
                    _type = self.__getColumnType__(_caParams.get('TARGET_TYPE'))
                else:
                    _type = self.__getColumnType__(self.__getDFtoCATypes__(df[val].dtype))
                
                #Add a column first
                self.__addColumn__(mapd_cursor,md5Table,name = val, _type = _type)
                
            #insert data in to table
            self.__insertData__(mapd_cursor,md5Table,df)
            
        
        def __handleForecastUpdate__(self, df = None, label2Col = None, modelName = None, modelLabel = None):
            if(df is None or label2Col is None or modelName is None or modelLabel is None):
                raise Exception("Unable to perform update for score request as required params missing: All are mandatory params")
            
            #update request is from score 
            # We have to prerequsite some values which user is not set
            # For score we have to set the derived dim name and the factable name            
            #identify the newly added column
            oldColumns = list(label2Col.keys())
            newColumns = list(df.keys())
            
            print("old columns:",oldColumns)
            print("new columns:",newColumns)
            
            #get the extra columns added
            extraColumns = np.setdiff1d(newColumns,oldColumns).tolist()
            
            if(_caParams.get('TARGET_LABEL') not in extraColumns):
                raise Exception("TARGET COLUMN is not available in the data frame, missing:" , _caParams.get('TARGET_LABEL'))
            
            #intialize the placeholder for new Dims
            global _caNewDims
            if(_caNewDims is None):
                _caNewDims = list()
            
            #Prepare the fact table Name
            temp = self.__conn_data__.factTable
            temp = list(temp.split("."))
            dsName = None
            schema = None
            if(len(temp) > 1):
                schema = temp[0].replace("\"","")
                dsName = temp[1].replace("\"","")
            else:
                dsName = temp[0]
            
            dsName = dsName + "_" + _caParams.get('TARGET_NAME') + "_"  + _caParams.get('MODEL_GROUP_NAME') + "_" + modelName
            print("dsName:",dsName)
            carriots_analytics_fact_table_name = hashlib.md5(dsName.encode("UTF-8")).hexdigest()
            if(schema is not None):
                carriots_analytics_fact_table_name = schema + "." + carriots_analytics_fact_table_name
            
            #First add the target column first
            myDim = dict()
            dimName =  _caParams.get('TARGET_NAME') + "_"  + _caParams.get('MODEL_GROUP_NAME') + "_" + modelName + \
            "_" + _caParams.get('TEMPORAL_DIM') + "_" + _caParams.get('FORECAST_STEP')
            print("dimName:", dimName)
            dimName = "0x_" + hashlib.md5(dimName.encode("UTF-8")).hexdigest()
            dimLabel = _caParams.get('TARGET_LABEL') + "_" + modelLabel
            myDim['name'] = dimName
            myDim['label'] = dimLabel
            myDim['supportTable'] = carriots_analytics_fact_table_name
            myDim['dataType'] = _caParams.get('TARGET_TYPE')
            myDim["modelName"] = modelName
            myDim["base"] = _caParams.get('TARGET_NAME')
            
            #add this mapping to label2colMap
            label2Col[_caParams.get('TARGET_LABEL')] = dimName
            
            #add the dimension
            _caNewDims[len(_caNewDims)] = myDim
            
            #Remove  the target column
            targetColumn = dimName
            newColumns = [x for x in extraColumns if x not in _caParams.get('TARGET_LABEL')]
            
            #prepare the new column now
            for val in newColumns:
                dimName =  _caParams.get('TARGET_NAME') + "_"  + _caParams.get('MODEL_GROUP_NAME') + "_" + _caParams.get('MODEL_NAME')
                print(dimName)
                dimName = "0x_" + hashlib.md5(dimName.encode("UTF-8")).hexdigest()
                dimLabel = val + "_" + modelLabel
                
                #add this mapping to the label2ColMap
                label2Col[val] = dimName
                
                myDim = dict()
                myDim['name'] = dimName
                myDim['label'] = dimLabel
                myDim['supportTable'] = carriots_analytics_fact_table_name
                myDim['dataType'] = self.__getDFtoCATypes__(type(df[val]))
                myDim['base'] = _caParams.get('TARGET_NAME')
                
                #add the dimension
                _caNewDims[len(_caNewDims)] = myDim
            
             #changing the dataframe headers back to actual fact table column names
            extraColNames = list()
            try:
                orgNames = list(df.columns)
                for i,e in enumerate(orgNames):
                    colName = label2Col[e]
                    if(colName is None):
                        raise Exception("Dataframe has an unexpected column")
                    #Get the extra colNames to omit from table creation
                    if(e in extraColumns):
                        extraColNames[len(extraColNames)] = colName
                    df.columns.values[i] = colName
            except:
                print("Error in changing the dataframe headers back to actual factTable column names")
                raise Exception("Error in changing the dataframe headers")
            
            jdbc = self.__conn_data__.jdbc
            mapd_cursor = jdbc.cursor()
            
            #create table
            md5Table = self.__createTable__(df,mapd_cursor,colName)
            
            #Add the new columns
            for val in extraColumns:
                if(val == targetColumn):
                    _type = self.__getColumnType__(_caParams.get('TARGET_TYPE'))
                else:
                    _type = self.__getColumnType__(self.__getDFtoCATypes__(df[val].dtype))
                
                #Add a column first
                self.__addColumn__(mapd_cursor,md5Table,name = val, _type = _type)
                
            #insert data in to table
            self.__insertData__(mapd_cursor,md5Table,df)
        
        def load(self,columns = None, nrows = None):
            
            #build the lod query to prepare data frame
            dimData = dict()
            try:
                #boolean will set if there are not predictors from app/ user defined prection list is empty
                load_all = False
                if(columns is None):
                    if(_caParams is not None):
                         #For learn/forecast selected columns in the dialog is the predictor names
                         if(_caParams.get('REQ_TYPE') == "LEARN" or _caParams.get('REQ_TYPE') == "FORECAST"):
                             if(_caParams.get('REQ_TYPE') == "FORECAST"):
                                 columns = _caParams.get('CARDINAL_DIMS')
                             else:
                                columns = _caParams.get('predictors')
                        
                         if(columns is None):
                             columns = list()
                                
                         if( _caParams.get('REQ_TYPE') == "FORECAST"):
                                temporal =  _caParams.get('TEMPORAL_DIM')
                                forecast =  _caParams.get('TARGET_NAME')

                                if(not (temporal in columns)):
                                    columns.append(temporal)

                                #Remove the Forecast dim if exixts and pass it as measure always
                                columns = np.setdiff1d(columns,forecast).tolist()
                                #add forecast dim as measure
                                dimData['measure'] = [forecast]

                                #Add the temporal dim as a first element in the list
                                columns = np.setdiff1d(columns,temporal).tolist()
                                columns.insert(0,temporal)
                            
                                
                         elif( _caParams.get('REQ_TYPE') == 'SCORE'):
                            columns = list(_ca_modelMap.keys())
                        
                         if(columns is None or len(columns) < 1):
                           print("Failed to set the predictors from application, hence loading whole datasource")
                           load_all = True
                         else:
                            #Driven through the application
                            dimData['coldim'] = columns
                    
                else:
                     #user defined column names
                     dimData['coldim'] = __getColumns__(self.__conn_data__,colSelected = columns)
                     
                if(load_all):
                     #load all the columns in the datasource
                     dimData['coldim'] = __getColumns__(self.__conn_data__,colSelected = None)
                
            except:
                 print("Error in preparing load query for the data frame")
                 raise Exception("Error in preparing load query for the data frame")
            
            print(dimData)
            
            params = {}
            params['dstoken'] = token
            params['dim'] = dumps(dimData)
            params['type'] =  _caParams.get('REQ_TYPE')
            
            if(_caParams.get("REQ_TYPE") == "FORECAST"):
                params['advancedModelGroup'] =  _caParams.get('MODEL_GROUP_NAME')
                
            headerParams = {}
            headerParams['X-CA-apiKey'] = apikey
            
            res = None
            query = None            
            try:
                res = __doHttpCall__(url,"dsExtConnect",params,headerParams)
                if(res["sql"] is None):
                    raise Exception("Unable to retrieve the SQL from CA server, Exiting!!!")
                    
                query = res["sql"]
            except:
                print("Error in datasource connect info")
                raise Exception("Error in datasource connect info")
                
            #Load datasource in to dataframe
            df = None
            
            #Ensure the previous results set was cleares
            if(self.resultSet is not None):
                self.resultSet.close()
            
            jdbc = self.__conn_data__.jdbc
            try:
                self.resultSet = jdbc.cursor()
                self.resultSet.execute(query)
                exitLoop = False
                rowsToFetch = nrows
                size = CA_DEFAULT_PAGE_SIZE
                
                if((rowsToFetch is not None) and rowsToFetch < CA_DEFAULT_PAGE_SIZE):
                    size = rowsToFetch
                    
                while(not exitLoop):
                    dfLimit = self.__cursor_to_df__(size = size)
                    if(nrows is not None):
                        rowsToFetch = rowsToFetch - size
                    
                        #to get the loft over rows
                        if(rowsToFetch < CA_DEFAULT_PAGE_SIZE):
                            size = rowsToFetch
                    
                        exitLoop = rowsToFetch <= 0
                    
                    #break if we exceed the no.of rows limit/there is no results to fetch
                    if(exitLoop or (dfLimit is not None and dfLimit.shape[0] < 1)):
                        exitLoop = True
                        if(nrows is not None):
                            pass
                        else:
                            self.__cleanUpResultSet__()
                    
                    df = concat([df,dfLimit])
            
            except:
                print("Unable to get the dataframe")
                raise Exception("Unable to get the data frame")
                
            df = self.__updateDataFrameHeaders__(df)                            
            return df
               
        def nextSet(self,nrows = None):
            df = None
            if(self.resultSet is None):
                print("No results to fetch/ no active db connections")
            else:
                try:
                    exitLoop = False
                    rowsToFetch = nrows
                    size = CA_DEFAULT_PAGE_SIZE
                    
                    #if no.of rows provided less than default page size
                    if( not (rowsToFetch is None) and rowsToFetch < CA_DEFAULT_PAGE_SIZE):
                        size = rowsToFetch
                    dfLimit = None
                    while( not exitLoop):
                        dfLimit = self.__cursor_to_df__(size = size)
                        if(nrows is not None):
                            rowsToFetch = rowsToFetch - size                        
                            #to get left over rows(multiple iterations case)
                            if(rowsToFetch < CA_DEFAULT_PAGE_SIZE):
                                size = rowsToFetch
                        
                            exitLoop = rowsToFetch <= 0
                    
                        #break if we exceed the no.of rows limit/ there is no result to fetch
                        if(exit or not (dfLimit is None) and dfLimit.shape[0]):
                            exitLoop = True
                            if(not(nrows is None)):
                                pass
                            else:
                                self.__cleanUpResultSet__()
                    
                        df = concat([df,dfLimit])
                except:
                     print("Error in Getting dataframe")
                     raise Exception("Error in Getting dataframe")
                
                df = self.__updateDataFrameHeaders__(df)
                return df               
            
        def update(self,df=None, _type = None, modelName = None, modelLabel = None):
            if(df is None or df.empty):
                raise Exception("Required parameters were missing")
            
            isScore = False
            isForecast = False
            
            label2Col = self.__conn_data__.columns
            
            #check whether the update call is from score
            if (_caParams is not None):
                if(_caParams.get("REQ_TYPE") == "SCORE"):
                    #Get label to column mapping
                    try:
                        if (_ca_modelMap is not None):
                            raise Exception("CA model map not available exiting now")
                        
                        label2Col = __getColumn2Label__(_ca_modelMap)
                        isScore = True
                    except:
                        print("Error in Getting label to column mapping")
                        raise Exception("Error in Getting label to column mapping")
                elif(_caParams.get("REQ_TYPE") == "FORECAST"):
                    newMapping = dict()
                    #get the name to label mapping
                    col2Label = __getColumn2Label__(label2Col)
                    
                    #insert mapping only for the cardinal dims, temporal and forecasting dims selected
                    #here predictors list is a collection of above 3
                    predictors = _caParams.get('CARDINAL_DIMS')
                    
                    target = _caParams.get('TARGET_NAME')
                    predictors = [x for x in predictors if x not in target]
                    for p in predictors:
                        newMapping[col2Label[p]] = p
                    
                    label2Col = newMapping
                    isForecast = True
            
            if(isScore):
                # handle score
                self.__handleScoreUpdate__(self,df = df, label2Col = label2Col, modelName = modelName, modelLabel = modelLabel)
            elif(isForecast):
                #handleForecast
                self.__handleForecastUpdate(self,df = df, label2Col = label2Col, modelName = modelName, modelLabel = modelLabel)
            else:
                #handle simulate
                self.__handleSimulateUpdate__(df = df, label2Col = label2Col, _type = _type)           
    
    # //////////////////////////////////////////////////////
    # Call the CA REST API to get the connection data
    # Based on the engine type, get the appropriate JDBC driver
    
    #check URL,token and API key set in the the App
    global _caParams
    if (_caParams is not None):
        if(url is None):
            url = _caParams.get('URL')
        
        if(token is None):
            token = _caParams.get('DSTOKEN')
            
        if(apikey is None):
            apikey = _caParams.get('APIKEY')
            
    data = __getDatasourceConnection__(url,token,apikey,tunnelHost)
    jdbc = data['jdbc']
    factTable = data['ftable']
    columns = data['columns']
    colTypes = data['colDataTypes']
    quot = data['quot']
    ba2DBTypes = __getDataTypes__(data['engineType'])
    
    connect_data = BAConnectionData(factTable, jdbc, columns, colTypes, ba2DBTypes, quot, data['username'])
    conn = BAConnection(connect_data)
    return conn

def setCAParams(caParams = None):
    global _caParams
    if(caParams is not None):
        _caParams = caParams

def getParam(key = None):
    val = None
    global _caParams
    if(key is not None and _caParams is not None):
        val = _caParams.get(key)
    return val

def cleanUpParams():
    global _caParams,_caNewDims,_ca_modelMap
    _caParams = None
    _caNewDims = None
    _ca_modelMap = None
    

def setcaModelMap(modelMap = None):
    global _ca_modelMap
    if(modelMap is not None):
        _ca_modelMap = modelMap
    
def  __getDatasourceConnection__(baseUrl,token,apikey,tunnelHost = None):
    data = __getDatasourceMetaData__(baseUrl,token,apikey)
    if (data is not None) and (data['connect_data'] is not None):
        connect_data = data['connect_data']
        jdbcDetails = __getDriverDetails__(connect_data,tunnelHost)
        if(jdbcDetails['driveClass'] is None or jdbcDetails['driver'] is None
           or jdbcDetails['connString'] is None):
            raise Exception("Unable to create JDBC connection- required info missing")
        
        password = ""
        enc_pwd = connect_data['password']
       
        for c in enc_pwd:
           password += chr(ord(c) - 4)
        
        password = password[::-1]
        password = b64decode(password)
        
        password = str(password.decode('utf-8'))
        
        driver_path = path.dirname(modules['caDB'].__file__) + sep +'extdata'+ sep + jdbcDetails['driver']
        #driver_path = getcwd() + sep +'extdata'+ sep + jdbcDetails['driver']
        print(driver_path)
        if(jpype.isJVMStarted()):
            print("JVM already started")
            jpype.detachThreadFromJVM()
            print("successfully detached")
        else:
            print("JVM not started")
            #jpype.startJVM(jpype.getDefaultJVMPath(),"-Djava.class.path=" + driver_path)
            #jpype.attachThreadToJVM()
            
        
        conn = monet.connect(username=connect_data['username'], password=password, hostname=connect_data['hostname'], database=connect_data['dbName'])
        #conn = jaydebeapi.connect(jclassname=jdbcDetails['driveClass'], url=jdbcDetails['connString'], driver_args=[connect_data['username'],password], jars=[driver_path])
        #conn = jaydebeapi.connect(jclassname=jdbcDetails['driveClass'], url = jdbcDetails['connString'], driver_args=[connect_data['username'],password], jars=[driver_path])
        
        print("after connection")
        ftable = connect_data['ftable']
        data = {}
        data['ftable'] = ftable
        data['username'] = connect_data['user_login_name']
        data['jdbc'] = conn
        data['quot'] = jdbcDetails['quot']
        data['columns'] = connect_data['columns']
        data['colDataTypes'] = connect_data['colDataTypes']
        data['engineType'] = connect_data['engine_type']
    else:
        raise Exception("Connect data of the datasource not available")
    
    return data

def __getDataTypes__(engineType):
    dataTypes = {'STRING' : 'VARCHAR(256)',
      'NUMERIC' : 'DOUBLE',
      'INTEGER' : 'INT',
      'DATE' : 'DATE',
      'DATETIME' : 'TIMESTAMP',
      'TIME' : 'TIME'}

    if(engineType.upper() == 'ORACLE'):
        dataTypes['NUMERIC'] = "NUMBER(20,4)"
        dataTypes['INTEGER'] = "NUMBER(20)"
    elif(engineType.upper() == 'MYSQL'):
        dataTypes['NUMERIC'] = "DECIMAL(20,4)"
        dataTypes['DATETIME'] = "DATETIME"
    elif(engineType.upper() == 'REDSHIFT'):
        dataTypes['NUMERIC'] = "DOUBLE PRECISION"
        dataTypes['INTEGER'] = "BIGINT"
    
    return dataTypes

def __getColumns__(conn,colSelected = None):
    cols = conn.columns
    colNames = None
    if( colSelected is not None and len(colSelected) > 0):
        for i,e in range(colSelected):
            if(cols[e] is not None):
                colNames.append(colSelected[e])
            else:
                raise Exception("ColName:" + colSelected[i] + "doesn't exist in the table")
    else:
        colNames = list(__getColumn2Label__(cols).keys())
    
    return colNames

def __getColumn2Label__(colList):
    col2Label =  dict((v,k) for k,v in colList.items())
    return col2Label

def __getDatasourceMetaData__(baseUrl,token,apikey):
    headerParams = {'X-CA-apiKey':apikey}
    queryParams = {'dstoken':token}
    res = __doHttpCall__(baseUrl,'dsExtConnect',queryParams,headerParams)
    return res
            

def __doHttpCall__(baseUrl,identifier,queryParams,headerParams):
    if baseUrl == None:
        raise Exception('Invalid base url')
    
    if baseUrl[-1:] == '/':
        baseUrl = baseUrl[:-1]
    
    baseUrl = baseUrl + '/datasource.do?action='+ identifier
    
    res = post(baseUrl,data = queryParams,headers= headerParams, verify=False)
    
    data = res.json()
    
    return data

def __getDriverDetails__(connect_data,tunnelHost):
    if connect_data['engine_type'] is None:
       raise Exception("Engine Type Not Found")
        
    engineType = connect_data['engine_type']
    
    if tunnelHost is not None:
        host_port = tunnelHost
    else:
        if(connect_data['hostname'] is None):
           raise Exception("Host Name Not found")
        host_port = connect_data['hostname']
        
        if(connect_data['port'] is not None):
            host_port = host_port + ":" + str(connect_data['port'])
    
    if(connect_data['dbName'] is None):
       raise Exception("DBName not Found, Exiting!!!")
        
    jdbcDetails = {}
    
    if(engineType.upper() == "MONETDB"):
        jdbcDetails['driveClass'] = "nl.cwi.monetdb.jdbc.MonetDriver"
        jdbcDetails['driver'] = "monetdb-jdbc-2.28.jar"
        jdbcDetails['connString'] = "jdbc:monetdb://" + host_port + "/" + connect_data['dbName']
        jdbcDetails['quot'] = "\""
    elif(engineType.upper() == "POSTGRESQL" or engineType.upper() == "REDSHIFT"):
        jdbcDetails['driveClass'] = "org.postgresql.Driver"
        jdbcDetails['driver'] = "postgresql-9.2-1002.jdbc4.jar"
        jdbcDetails['connString'] = "jdbc:postgresql://" + host_port + "/" + connect_data['dbName']
        jdbcDetails['quot'] = "\""
    elif(engineType.upper() == "SQLSERVER"):
        jdbcDetails['driveClass'] = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        jdbcDetails['driver'] = "sqljdbc4.jar"
        jdbcDetails['connString'] = "jdbc:sqlserver://" + __getSQLServerConnString__(connect_data)
        jdbcDetails['quot'] = "\""
    elif(engineType.upper() == "ORACLE"):
        jdbcDetails['driveClass'] = "oracle.jdbc.driver.OracleDriver"
        jdbcDetails['driver'] = "ojdbc6.jar"
        jdbcDetails['connString'] = "jdbc:oracle:thin:@" + host_port + "/" + connect_data['dbName']
        jdbcDetails['quot'] = "\""
    elif(engineType.upper() == "SUNDB"):
        jdbcDetails['driveClass'] = "sunje.sundb.jdbc.SundbDriver"
        jdbcDetails['driver'] = "sundb6.jar"
        jdbcDetails['connString'] = "jdbc:sundb://" + host_port + "/" + connect_data['dbName']
        jdbcDetails['quot'] = "\""
    elif(engineType.upper() == "MARIADB"):
        jdbcDetails['driveClass'] = "org.mariadb.jdbc.Driver"
        jdbcDetails['driver'] = "mariadb-java-client-1.3.3.jar"
        jdbcDetails['connString'] = "jdbc:mariadb://" + host_port + "/" + connect_data['dbName']
        jdbcDetails['quot'] = "\""
    
    return jdbcDetails

def __getSQLServerConnString__(connect_data):
    conn_string = connect_data['hostname']+"\\"+connect_data['dbName']
    if(connect_data['port'] is not None):
        conn_string = conn_string + ":" + connect_data['port']
    
    return conn_string
