import jpype
import jaydebeapi as jdbc
import json
import jpype
from pymysql import *
import pandas.io.sql as sql
from jpype

class DatabricksConnection:

    def getConnection(self):
        connection_string = "jdbc:spark://detst.databricks.onetakeda.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/0/0727-153836-awls60;AuthMech=3;UID=token;PWD=dapi424a1718b7c53e23dfe29b4e01c2c245"
        connection_string = "jdbc:spark://detst.databricks.onetakeda.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/0/0917-194420-shrug952;AuthMech=3;UID=token;PWD=dapi424a1718b7c53e23dfe29b4e01c2c245"
        if jpype.isJVMStarted():
            print("Jvm already Started")
        else:
            jpype.startJVM(jpype.getDefaultJVMPath(), '-Djava.class.path=./SparkJDBC41.jar',
                           '-Djavax.net.ssl.trustStore=./databricks.jks',
                           '-Djavax.net.ssl.trustStorePassword=123456')

        driver_class = 'com.simba.spark.jdbc41.Driver'
        driver_file = './SparkJDBC41.jar'
        connection_string_test = connection_string
        con_test = jdbc.connect(driver_class, connection_string_test, jars=driver_file)
        query = 'DESC CORP_DE_LAKE.Release_Tracker_Home_Hist'
        db_df = sql.read_sql(query, con_test)
        print(db_df)
        # return con_test


DatabricksConnection.getConnection("")

if __name__ == "__getConnection__":
    getConnection()
