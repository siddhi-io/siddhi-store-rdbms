# siddhi-store-rdbms

======================================
---
##### Latest Released Version v4.0.0-M3.

This is an extension for siddhi RDBMS event table implementation. This extension can be used to persist events to a RDBMS instance of the users choice.

Features Supported
------------------
 - Defining an Event Table
 - Inserting Events
 - Retrieving Events
 - Deleting Events
 - Updating persisted events
 - Filtering Events
 - Insert or Update Events
      
#### Prerequisites for using the feature
 - A RDBMS server instance should be started.
 - User should have the necessary privileges and access rights to connect to the RDBMS data store of choice.
 - Deployment yaml file has to be inserted in the following manner (properties set are optional), if the user wishes to overwrite default RDBMS configuration in the extension.
 <pre>
siddhi: 
  extensions: 
    extension: 
      name: store
      namespace: rdbms
      properties: 
        mysql.batchEnable: true
        mysql.batchSize: 1000
        mysql.indexCreateQuery: "CREATE INDEX {{TABLE_NAME}}_INDEX ON {{TABLE_NAME}} ({{INDEX_COLUMNS}})"
        mysql.maxVersion: ~
        mysql.minVersion: ~
        mysql.recordDeleteQuery: "DELETE FROM {{TABLE_NAME}} {{CONDITION}}"
        mysql.recordExistsQuery: "SELECT 1 FROM {{TABLE_NAME}} {{CONDITION}} LIMIT 1"
        mysql.recordInsertQuery: "INSERT INTO {{TABLE_NAME}} VALUES ({{Q}})"
        mysql.recordSelectQuery: "SELECT * FROM {{TABLE_NAME}} {{CONDITION}}"
        mysql.recordUpdateQuery: "UPDATE {{TABLE_NAME}} SET {{COLUMNS_AND_VALUES}} {{CONDITION}}"
        mysql.stringSize: 254
        mysql.tableCheckQuery: "CREATE TABLE {{TABLE_NAME}} ({{COLUMNS, PRIMARY_KEYS}})"
        mysql.tableCreateQuery: "CREATE TABLE {{TABLE_NAME}} ({{COLUMNS, PRIMARY_KEYS}})"
        mysql.typeMapping.binaryType: BLOB
        mysql.typeMapping.booleanType: TINYINT(1)
        mysql.typeMapping.doubleType: DOUBLE
        mysql.typeMapping.floatType: FLOAT
        mysql.typeMapping.integerType: INTEGER
        mysql.typeMapping.longType: BIGINT
        mysql.typeMapping.stringType: VARCHAR
 </pre>

 
#### Deploying the feature
 Feature can be deploy as a OSGI bundle by putting jar file of the component to DAS_HOME/lib directory of DAS 4.0.0 pack. 
 
##### Example Siddhi Queries
###### Defining an Event Table
 <pre>
 @Store(type="rdbms", jdbc.url="jdbc:mysql://localhost:3306/das",
 username="root", password="root" , jdbc.driver.name="org.h2.Driver",
 field.length="symbol:100")
 @PrimaryKey("symbol")
 @Index("volume")
 define table StockTable (symbol string, price float, volume long);
 </pre>

#### Documentation 
* [https://docs.wso2.com/display/DAS400/Configuring+Event+Tables+to+Store+Data](https://docs.wso2.com/display/DAS400/Configuring+Event+Tables+to+Store+Data)

## How to Contribute
* Please report issues at [Siddhi Github Issue Tacker](https://github.com/wso2-extensions/siddhi-store-rdbms/issues)
* Send your bug fixes pull requests to [master branch](https://github.com/wso2-extensions/siddhi-store-rdbms/tree/master) 

## Contact us 
Siddhi developers can be contacted via the mailing lists:
  * Carbon Developers List : dev@wso2.org
  * Carbon Architecture List : architecture@wso2.org

### We welcome your feedback and contribution.

Siddhi DAS Team