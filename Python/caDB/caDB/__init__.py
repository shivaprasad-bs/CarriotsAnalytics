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
from pandas import DataFrame
import jaydebeapi

def connect_ca(url,token,apikey,tunnelHost = None):
    
    class BAConnectionData:
        factTable = ""
        jdbc = None
        columns = None
        engineType = None
        ba2DBTypes = None
        qVar = None
        username = None
        
        # The class "constructor" - It's actually an initializer 
        def __init__(self, factTable,jdbc,columns,ba2DBTypes,qVar,uname):
            self.factTable = factTable
            self.jdbc = jdbc
            self.columns = columns
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
        
        def __init__(self,conn_data):
            self.__conn_data__ = conn_data
            self.dataTypes = {"STRING" : "STRING",
                              "NUMERIC" : "NUMERIC",
                              "INTEGER" : "INTEGER",
                              "DATE" :"DATE",
                              "DATETIME" : "DATETIME",
                              "TIME" : "TIME"}
            
        def __createTable__(self,mapd_cursor,df,colname,_type, default = None):
            if colname is None:
                exit("Invalid colName")
            
            #Facttable name is already set in the CA App
            if not ('carriots_analytics_fact_table_name' in locals() or 'carriots.analytics.fact_table_name' in globals()):
                exit("Fact table name not set, exiting here")
                
            temp = carriots_analytics_fact_table_name
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
            colNames.remove(colname)
                
            
            if(not(set(colNames) < set(self.getColumnNames()))):
                raise Exception("Data Frame has more than one new column compared to fact table")
            
            query = "CREATE TABLE "
            query = query + _md5table + " AS" 
            colString = ",".join(str(self.__conn_data__.quot(x)) for x in colNames)
            
            orgQuery = "(SELECT " + colString + " FROM " + self.__conn_data__.factTable + " WHERE 1=2 " + ")"
            query = query + orgQuery
            
            print (query)
            mapd_cursor.execute(query)
                
            #add column
            self.__addColumn__(mapd_cursor,_md5table,colname,_type,default)
                               
            return _md5table
        
        def __addColumn__(self,mapd_cursor,_md5table,name = None,
                          _type = None, default = None):
            if(name is None or _type is None):
                raise Exception("Column name or Type is empty")
            
            if(not(_type in self.__conn_data__.ba2DBTypes.keys())):
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
                values = values + "(" + (",".join("'"+str(x) + "'" for x in _row))+ ")"
            
            query = query + values
            print(query)
            
            #excecute query
            mapd_cursor.execute(query)                
                
            
        def load(self,columns = None):
            query = "SELECT "+ __getColumns__(self.__conn_data__,columns)
            query = query + " FROM "+ self.__conn_data__.factTable
            jdbc = self.__conn_data__.jdbc
            mapd_cursor = None
            try:
                mapd_cursor = jdbc.cursor()
                mapd_cursor.execute(query)
                df = cursor_to_df(mapd_cursor)
                col2Label = __getColumn2Label__(self.__conn_data__.columns)
                for i in range(len(df.columns)):
                    df.columns.values[i] = col2Label[df.columns.values[i]]
            finally:
                if(mapd_cursor is not None):
                    mapd_cursor.close()
            
            return df
        
        def getColumnNames(self):
            cols = self.__conn_data__.columns.keys()
            return cols
                
        def update(self,df=None, colName=None, _type = None):
            if(df is None or df.empty or colName is None):
                raise Exception("DataFrame and colname is empty")
            
            if(not(colName in df.columns.values)):
                raise Exception("Provided column doent exists in data frame")
            
            label2Col = self.__conn_data__.columns
            orgNames = list(df.columns.values)
            for i in range(len(orgNames)):
                if(orgNames[i]!= colName):
                    label = label2Col[orgNames[i]]
                    if(label == None):
                        raise Exception("DataFrame has an unexpected column")
                
                    df.columns.values[i] = label
                else:
                   if ('carriots_analytics_dervived_dim_name' in locals() or 'carriots.analytics.dervived_dim_name' in globals()): 
                       df.columns.values[i] = carriots_analytics_dervived_dim_name
                       colName = carriots_analytics_dervived_dim_name
            
            mapd_cursor = None
            try:
                mapd_cursor = jdbc.cursor()                
                #create table
                _md5Table = self.__createTable__(mapd_cursor,df,colName,_type)            
                #insert into new table
                self.__insertData__(mapd_cursor,_md5Table,df)
                
                #fire event to reload the table in the carriotsAnalytice App
                
                params = {}
                params['dstoken'] = token
                params['dim'] = carriots_analytics_dervived_dim_name
                params['support_table'] = _md5Table
                
                headerParams = {}
                headerParams['X-CA-apiKey'] = apikey
                
                res = __doHttpCall__(url,"reloadext",params,headerParams)
                
            finally:
                if(mapd_cursor is not None):
                    mapd_cursor.close()
            
    
    # //////////////////////////////////////////////////////
    # Call the CA REST API to get the connection data
    # Based on the engine type, get the appropriate JDBC driver
    data = __getDatasourceConnection__(url,token,apikey,tunnelHost)
    jdbc = data['jdbc']
    factTable = data['ftable']
    columns = data['columns']
    quot = data['quot']
    ba2DBTypes = __getDataTypes__(data['engineType'])
    
    connect_data = BAConnectionData(factTable, jdbc, columns, ba2DBTypes, quot, data['username'])
    conn = BAConnection(connect_data)
    return conn
    

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
        
        password = str(password.decode("utf-8"))
        
        driver_path = path.dirname(modules["CarriotsAnalytics_py"].__file__) + sep +'extdata'+ sep + jdbcDetails['driver']
        #driver_path = getcwd() + sep +'extdata'+ sep + jdbcDetails['driver']
        print(driver_path)
        conn = jaydebeapi.connect(jclassname=jdbcDetails['driveClass'],url=jdbcDetails['connString'],driver_args=[connect_data['username'],password],
                                  jars=[driver_path])
        ftable = connect_data['ftable']
        data = {}
        data['ftable'] = ftable
        data['username'] = connect_data['user_login_name']
        data['jdbc'] = conn
        data['quot'] = jdbcDetails['quot']
        data['columns'] = connect_data['columns']
        data['engineType'] = connect_data['engine_type']
    else:
        raise Exception("Connect data of the datasource not available")
    
    return data

def cursor_to_df(cursor):
    df = DataFrame(cursor.fetchall())
    df.columns = [d[0] for d in cursor.description]
    return df

def __getDataTypes__(engineType):
    dataTypes = {"STRING" : "VARCHAR(256)",
      "NUMERIC" : "DOUBLE",
      "INTEGER" : "INT",
      "DATE" :"DATE",
      "DATETIME" : "TIMESTAMP",
      "TIME" : "TIME"}

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
    colString = ""
    if( not(colSelected == None) and len(colSelected) > 0):
        length = len(colSelected)
        for i in range(colSelected):
            if(i > 0 and i< length -1):
                colString = colString + ","
            if( not (cols[colSelected[i]] == None)):
                colString = colString + conn.quot(cols[colSelected[i]])
            else:
                raise Exception("ColName:" + colSelected[i] + "doesn't exist in the table")
            i+=1
    else:
        colString = "*"
    
    return colString

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
        jdbcDetails['driver'] = "monetdb-jdbc-2.8.jar"
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