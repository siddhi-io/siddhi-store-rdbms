Siddhi Store RDBMS
===================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-rdbms/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-rdbms/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-store-rdbms.svg)](https://github.com/siddhi-io/siddhi-store-rdbms/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-store-rdbms.svg)](https://github.com/siddhi-io/siddhi-store-rdbms/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-store-rdbms.svg)](https://github.com/siddhi-io/siddhi-store-rdbms/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-store-rdbms.svg)](https://github.com/siddhi-io/siddhi-store-rdbms/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-store-rdbms extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that persist and retrieve events to/from RDBMS databases such as MySQL, MS SQL, PostgreSQL, H2 and Oracle.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 7.x & 6.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.store.rdbms/siddhi-store-rdbms/">here</a>.
* Versions 5.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.store.rdbms/siddhi-store-rdbms">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-store-rdbms/api/7.0.5">7.0.5</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-store-rdbms/api/7.0.5/#cud-stream-processor">cud</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This function performs SQL CUD (INSERT, UPDATE, DELETE) queries on data sources. <br>Note: This function to work data sources should be set at the Siddhi Manager level.<br></p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-store-rdbms/api/7.0.5/#query-stream-processor">query</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This function performs SQL retrieval queries on data sources. <br>Note: This function to work data sources should be set at the Siddhi Manager level.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-store-rdbms/api/7.0.5/#rdbms-store">rdbms</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#store">Store</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This extension assigns data sources and connection instructions to event tables. It also implements read-write operations on connected data sources.</p></p></div>

## Dependencies 

JDBC connector jar should be added to the runtime. Download the JDBC connector jar based on the RDBMS type that is connected through the Siddhi store. 

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Running Integration tests in docker containers(Optional)

The RDBMS functionality are tested with the docker base integration test framework, except the H2 default 
embedded database is not uses external docker container. The test framework initialize the docker container for each 
database according to the given profile before execute the test suit.

**Start integration tests**

1. Install and run docker in daemon mode.

    *  Installing docker on Linux,<br>
       Note:<br>    These commands retrieve content from get.docker.com web in a quiet output-document mode and install.
       
            wget -qO- https://get.docker.com/ | sh

    *  On installing docker on Mac, see <a target="_blank" href="https://docs.docker.com/docker-for-mac/">Get started with Docker for Mac</a>

    *  On installing docker on Windows, see <a target="_blank" href="https://docs.docker.com/docker-for-windows/">Get started with Docker for Windows</a>
   
2. To run the integration test, navigate to the siddhi-store-rdbms/ directory and issue the following commands.

    * H2 default:
    
            mvn clean install
           
         **Note** : h2 is the default activated profile as it is not used docker.

    * MySQL 5.7:
    
            mvn verify -P local-mysql -f /component/pom.xml -Dskip.surefire.test=true -Ddocker.removeVolumes=true
           
    * Postgres 9.6:
    
             mvn verify -P local-postgres -f /component/pom.xml -Dskip.surefire.test=true -Ddocker.removeVolumes=true
           
    * MSSQL CTP 2.0:
    
            mvn verify -P local-mssql -f /component/pom.xml -Dskip.surefire.test=true -Ddocker.removeVolumes=true
            
    * Oracle 11.2.0.2 Express Edition:
            
             mvn verify -P local-oracle -f component/pom.xml -Dskip.surefire.test=true -Ddocker.removeVolumes=true
             
    * Oracle 12.1.0.2 Standard Edition:
         
         * Download Oracle driver version 12.1.0.2 <a target="_blank" href="https://www.oracle.com/technetwork/database/features/jdbc/default-2280470.html">ojdbc7.jar</a>
             
         * To install the JAR file as a Maven plugin, issue the following command:
     
                  mvn install:install-file -Dfile=/tmp/ojdbc7.jar -DgroupId=com.oracle -DartifactId=ojdbc7 -Dversion=12.1.0.2 -Dpackaging=jar
          
         * Uncomment the following test dependency in the /component/pom.xml file as shown below:
     
                  <dependency>
                     <groupId>com.oracle</groupId>
                     <artifactId>ojdbc7</artifactId>
                     <scope>test</scope>
                     <version>12.1.0.2</version>
                  </dependency>
         
         * To run the integration test, navigate to siddhi-store-rdbms/ directory and issue the following commands:
     
                  mvn verify -P local-oracle12 -f component/pom.xml -Dskip.surefire.test=true -Ddocker.removeVolumes=true
             
    * DB2
    
        * Download DB2 driver version 4.19.26 <a target="_blank" href="http://www.ibm.com/eserver/support/fixes/fixcentral/swg/quickorder?brandid=1&productid=IBM+Data+Server+Client+Packages&vrmf=10.5.*&fixes=*jdbc*FP005">db2jcc4.jar</a>
    
        * To install the JAR file as a Maven plugin, issue the following command:
    
                 mvn install:install-file -Dfile=/tmp/db2jcc4.jar -DgroupId=com.ibm.db2 -DartifactId=db2jcc -Dversion=4.19.26 -Dpackaging=jar
         
        * Uncomment the following test dependency in the /component/pom.xml file as shown below:
    
                 <dependency>
                   <groupId>com.ibm.db2</groupId>
                  <artifactId>db2jcc</artifactId>
                   <scope>test</scope>
                   <version>4.19.26</version>
                 </dependency>
        
        * To run the integration test, navigate to siddhi-store-rdbms/ directory and issue the following commands:
    
                 mvn verify -P local-db2 -f /component/pom.xml -Dskip.surefire.test=true -Ddocker.removeVolumes=true

**Start integration tests in debug mode**
```
mvn -P local-mysql -Dmaven.failsafe.debug verify
Note: local-mysql is the profile. Use other profiles accordingly.
```

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.

