import jpype
import jaydebeapi as jdbc
import json
import jpype


class DatabricksConnection:

    def getConnection(self, connection_string):

        if jpype.isJVMStarted():
            print("Jvm already Started")
        else:
            jpype.startJVM(jpype.getDefaultJVMPath(), '-Djava.class.path=Pyfiles/SparkJDBC41.jar',
                           '-Djavax.net.ssl.trustStore=Pyfiles/databricks.jks',
                           '-Djavax.net.ssl.trustStorePassword=123456')

        driver_class = 'com.simba.spark.jdbc41.Driver'
        driver_file = 'Pyfiles/SparkJDBC41.jar'
        connection_string_test = connection_string
        con_test = jdbc.connect(driver_class, connection_string_test, jars=driver_file)
        return con_test


if __name__ == "__getConnection__":
    getConnection()
