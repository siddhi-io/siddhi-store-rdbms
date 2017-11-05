siddhi-store-rdbms
======================================

The **siddhi-store-rdbms extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that  can be used to persist events to a RDBMS instance of the users choice.
Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-rdbms">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-rdbms/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-rdbms/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-store-rdbms/api/4.0.2">4.0.2</a>.

## Prerequisites

 * A RDBMS server instance should be started.
 * User should have the necessary privileges and access rights to connect to the RDBMS data store of choice.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-rdbms/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.store.rdbms</groupId>
        <artifactId>siddhi-store-rdbms</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Running Integration tests in docker containers(Optional)

The RDBMS functionality are tested with the docker base integration test framework, except the H2 default 
embedded database is not uses external docker container. The test framework initialize the docker container for each 
database according to the given profile before execute the test suit.

1. Install and run docker in daemon mode.

    *  How to install docker on Linux
    
    ```  
    Note: These commands retrieve content from get.docker.com web in a quiet output-document mode and install.
          Then we need to stop docker service as it needs to restart docker in daemon mode. After that, we need to
          export docker daemon host.
    wget -qO- https://get.docker.com/ | sh
    sudo service dockerd stop
    export DOCKER_HOST=tcp://172.17.0.1:4326
    docker daemon -H tcp://172.17.0.1:4326
    ```
    *  How to install docker on Mac 
   https://docs.docker.com/docker-for-mac/

    *  How to install docker on Windows
   https://docs.docker.com/docker-for-windows/
2. To run the integration test, navigate to the siddhi-store-rdbms/ directory and issue the following commands.

    * H2 default:
    ```
    mvn clean install
    Note: h2 is the default activated profile as it is not used docker.
    ```
    * MySQL 5.7:
    ```
    mvn verify -P local-mysql -f /component/pom.xml -Dskip.surefire.test=true -Ddocker.removeVolumes=true
    ```
    * Postgres 9.6:
    ```
    mvn verify -P local-postgres -f /component/pom.xml -Dskip.surefire.test=true -Ddocker.removeVolumes=true
    ```
    * MSSQL CTP 2.0:
     ```
    mvn verify -P local-mssql -f /component/pom.xml -Dskip.surefire.test=true
     ```
    * Oracle 11.2.0.2 Express Edition:
     ```
    mvn verify -P local-oracle -f /component/pom.xml -Dskip.surefire.test=true
     ```
4. To run the integration test with DB2, issue the following commands:
    * Download DB2 driver version 4.19.26 <a target="_blank" href="http://www.ibm.com/eserver/support/fixes/fixcentral/swg/quickorder?brandid=1&productid=IBM+Data+Server+Client+Packages&vrmf=10.5.*&fixes=*jdbc*FP005">db2jcc4.jar</a>
    * To install the JAR file as a Maven plugin, issue the following command:
        ```
        mvn install:install-file -Dfile=/tmp/db2jcc4.jar -DgroupId=com.ibm.db2 -DartifactId=db2jcc -Dversion=4.19.26 -Dpackaging=jar
        ```
    * Uncomment the following test dependency in the /component/pom.xml file as shown below:
        ```
         <dependency>
             <groupId>com.ibm.db2</groupId>
             <artifactId>db2jcc</artifactId>
             <scope>test</scope>
             <version>4.19.26</version>
         </dependency>
        ```
    * To run the integration test, navigate to siddhi-store-rdbms/ directory and issue the following commands:
         ```
          mvn verify -P local-db2 -f /component/pom.xml -Dskip.surefire.test=true
         ```

**Start integration tests in debug mode**
```
mvn -P local-mysql -Dmaven.failsafe.debug verify
Note: local-mysql is the profile. Use other profiles accordingly.
```
## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-rdbms/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-rdbms/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-store-rdbms/api/4.0.2/#rdbms-store">rdbms</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#store">(Store)</a>)*<br><div style="padding-left: 1em;"><p>This extension assigns data sources and connection instructions to event tables. It also implements read write operations on connected datasources</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-rdbms/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-rdbms/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
