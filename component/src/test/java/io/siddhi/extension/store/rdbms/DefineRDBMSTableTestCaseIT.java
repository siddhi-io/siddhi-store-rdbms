/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.extension.store.rdbms;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.extension.store.rdbms.util.LoggerAppender;
import io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.SQLException;
import javax.naming.NamingException;

import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.TABLE_NAME;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.driverClassName;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.password;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.url;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.user;

public class DefineRDBMSTableTestCaseIT {
    private static final Logger log = (Logger) LogManager.getLogger(DefineRDBMSTableTestCaseIT.class);
    private static String regexPattern = "will retry in '5 sec'";
    private static String siddhiAppErrorRegex = "Error starting Siddhi App";

    @BeforeClass
    public static void startTest() {
        log.info("== RDBMS Table DEFINITION tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== RDBMS Table DEFINITION tests completed ==");
    }

    @BeforeMethod
    public void init() {
        try {
            RDBMSTableTestUtils.initDatabaseTable(TABLE_NAME);
            log.info("Test init with url: " + url + " and driverClass: " + driverClassName);
        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @Test(testName = "rdbmstabledefinitiontest1", description = "Testing table creation.")
    public void rdbmstabledefinitiontest1() throws InterruptedException, SQLException {
        //Testing table creation
        log.info("rdbmstabledefinitiontest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\", " +
                "pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        log.info(streams + query);

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 3, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest1")
    public void rdbmstabledefinitiontest2() throws InterruptedException, SQLException {
        //Testing table creation with a invalid primary key normal insertion.
        log.info("rdbmstabledefinitiontest2");
        LoggerAppender appender = new LoggerAppender("LoggerAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();


        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\", " +
                "pool.properties=\"maximumPoolSize:1\")\n" +
                "@PrimaryKey(\"testPrimary\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        AssertJUnit.assertTrue(((LoggerAppender) logger.getAppenders().
                get("LoggerAppender")).getMessages().contains(regexPattern));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest2")
    public void rdbmstabledefinitiontest3() throws InterruptedException, SQLException {
        //Testing Defining a RDBMS table without defining a value for jdbc url field
        log.info("rdbmstabledefinitiontest3");
        LoggerAppender appender = new LoggerAppender("LoggerAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\",field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        AssertJUnit.assertTrue(((LoggerAppender) logger.getAppenders().
                get("LoggerAppender")).getMessages().contains(regexPattern));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest3")
    public void rdbmstabledefinitiontest4() throws InterruptedException, SQLException {
        //Testing table creation with no connection URL
        log.info("rdbmstabledefinitiontest4");
        LoggerAppender appender = new LoggerAppender("LoggerAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.url=\"\", jdbc.driver.name=\"" +
                driverClassName + "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        AssertJUnit.assertTrue(((LoggerAppender) logger.getAppenders().
                get("LoggerAppender")).getMessages().contains(regexPattern));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest4")
    public void rdbmstabledefinitiontest5() throws InterruptedException, SQLException {
        //Testing table creation with invalid connection URL
        log.info("rdbmstabledefinitiontest5");
        LoggerAppender appender = new LoggerAppender("LoggerAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", " +
                "username=\"" + user + "\", password=\"" + password + "\", " +
                "jdbc.url=\"jdsbc:h2:repository/database/" +
                "ANALYTICS_EVENT_STORE\", jdbc.driver.name=\"" + driverClassName + "\", " +
                "field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        AssertJUnit.assertTrue(((LoggerAppender) logger.getAppenders().
                get("LoggerAppender")).getMessages().contains(siddhiAppErrorRegex));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest5")
    public void rdbmstabledefinitiontest6() throws InterruptedException, SQLException {
        //Testing table creation with no password
        log.info("rdbmstabledefinitiontest6");
        LoggerAppender appender = new LoggerAppender("LoggerAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        AssertJUnit.assertTrue(((LoggerAppender) logger.getAppenders().
                get("LoggerAppender")).getMessages().contains(siddhiAppErrorRegex));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest6")
    public void rdbmstabledefinitiontest7() throws InterruptedException, SQLException {
        //Defining a RDBMS table without defining a value for password field
        log.info("rdbmstabledefinitiontest7");
        LoggerAppender appender = new LoggerAppender("LoggerAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"\", field.length=\"symbol:100\", " +
                "pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        AssertJUnit.assertTrue(((LoggerAppender) logger.getAppenders().
                get("LoggerAppender")).getMessages().contains(siddhiAppErrorRegex));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest7")
    public void rdbmstabledefinitiontest8() throws InterruptedException, SQLException {
        //Defining a RDBMS table without defining a value for password field
        log.info("rdbmstabledefinitiontest8");
        LoggerAppender appender = new LoggerAppender("LoggerAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"root###\", field.length=\"symbol:100\", " +
                "pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        AssertJUnit.assertTrue(((LoggerAppender) logger.getAppenders().
                get("LoggerAppender")).getMessages().contains(siddhiAppErrorRegex));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest8")
    public void rdbmstabledefinitiontest9() throws InterruptedException, SQLException {
        //Defining a RDBMS table without having an username field.
        log.info("rdbmstabledefinitiontest9");
        LoggerAppender appender = new LoggerAppender("LoggerAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();
        String usernameErrorRegex = "Failed to initialize store for table name 'StockTable'";
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                "password=\"" + password + "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        AssertJUnit.assertTrue(((LoggerAppender) logger.getAppenders().
                get("LoggerAppender")).getMessages().contains(usernameErrorRegex));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest9")
    public void rdbmstabledefinitiontest10() throws InterruptedException, SQLException {
        //Defining a RDBMS table without defining a value for username field.
        log.info("rdbmstabledefinitiontest10");
        LoggerAppender appender = new LoggerAppender("LoggerAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();
        String usernameEmptyRegex = "Failed to initialize store for table name 'StockTable'";
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"\", password=\"" + password + "\", field.length=\"symbol:100\", " +
                "pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        AssertJUnit.assertTrue(((LoggerAppender) logger.getAppenders().
                get("LoggerAppender")).getMessages().contains(usernameEmptyRegex));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest10")
    public void rdbmstabledefinitiontest11() throws InterruptedException, SQLException {
        //Defining a RDBMS table without defining a value for username field.
        log.info("rdbmstabledefinitiontest11");
        LoggerAppender appender = new LoggerAppender("LoggerAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"root####\", password=\"" + password + "\", field.length=\"symbol:100\", " +
                "pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        String tableConnectionErrorRegex = "Error while connecting to Table 'StockTable";
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        AssertJUnit.assertTrue(((LoggerAppender) logger.getAppenders().
                get("LoggerAppender")).getMessages().contains(tableConnectionErrorRegex));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest11")
    public void rdbmstabledefinitiontest12() throws InterruptedException, SQLException {
        //Defining a RDBMS table without having a field length field.
        log.info("rdbmstabledefinitiontest12");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"" + password + "\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        Thread.sleep(1000);
        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 1, "Update failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest12")
    public void rdbmstabledefinitiontest13() throws InterruptedException, SQLException {
        //Defining a RDBMS table with non existing attribute/s to define the field length .
        log.info("rdbmstabledefinitiontest13");
        LoggerAppender appender = new LoggerAppender("LoggerAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\",  field.length=\"length:254\", password=\"" + password + "\", " +
                "pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        AssertJUnit.assertTrue(((LoggerAppender) logger.getAppenders().
                get("LoggerAppender")).getMessages().contains(regexPattern));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest13")
    public void rdbmstabledefinitiontest14() throws InterruptedException, SQLException, NamingException {
        //Defining a RDBMS table with jndi.resource.
        log.info("rdbmstabledefinitiontest14");
        LoggerAppender appender = new LoggerAppender("LoggerAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();
        RDBMSTableTestUtils.setupJNDIDatasource(url, driverClassName);
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"jdbc:h2:./target/testdb###\", jdbc.driver.name=\"" +
                driverClassName + "\"," +
                "username=\"root###\", jndi.resource=\"java:comp/env/jdbc/TestDB\", field.length=\"symbol:100\", " +
                "password=\"root###\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        Thread.sleep(1000);
        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 1, "Update failed");
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest14")
    public void rdbmstabledefinitiontest15() throws InterruptedException, SQLException, NamingException {
        //Defining a RDBMS table with an invalid value for jndi.resource field.
        log.info("rdbmstabledefinitiontest15");
        LoggerAppender appender = new LoggerAppender("LoggerAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();
        RDBMSTableTestUtils.setupJNDIDatasource(url, driverClassName);
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"jdbc:h2:./target/testdb###\", jdbc.driver.name=\"" +
                driverClassName + "\"," +
                "username=\"root###\", jndi.resource=\"jdbc444/444TestDB\", field.length=\"symbol:100\", " +
                "password=\"root###\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        AssertJUnit.assertTrue(((LoggerAppender) logger.getAppenders().
                get("LoggerAppender")).getMessages().contains(regexPattern));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest15")
    public void rdbmstabledefinitiontest16() throws InterruptedException, SQLException {
        // This testcase verified that defining a RDBMS table by including pool.properties field will be
        // successfully create the table.
        log.info("rdbmstabledefinitiontest16");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\"," +
                "pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 3, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest16")
    public void rdbmstabledefinitiontest17() throws InterruptedException, SQLException {
        //  This testcase verified that defining a RDBMS table by including at least one invalid pool.property
        // field will not successfully create the table.
        log.info("rdbmstabledefinitiontest17");
        LoggerAppender appender = new LoggerAppender("LoggerAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\"," +
                "pool.properties=\"maximumPoolSize:5,maxWSO2:60000\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        AssertJUnit.assertTrue(((LoggerAppender) logger.getAppenders().
                get("LoggerAppender")).getMessages().contains(siddhiAppErrorRegex));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest17")
    public void rdbmstabledefinitiontest18() throws InterruptedException, SQLException {
        //Defining a RDBMS table with an invalid value for a pool.property.
        log.info("rdbmstabledefinitiontest18");
        LoggerAppender appender = new LoggerAppender("LoggerAppender", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();
        SiddhiManager siddhiManager = new SiddhiManager();
        String numberFormatErrorRegex = "WSO2";
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\"," +
                "pool.properties=\"maximumPoolSize:5, maxLifetime:WSO2\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        AssertJUnit.assertTrue(((LoggerAppender) logger.getAppenders().
                get("LoggerAppender")).getMessages().contains(numberFormatErrorRegex));
        siddhiAppRuntime.shutdown();
        logger.removeAppender(appender);
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest18")
    public void rdbmstabledefinitiontest19() throws InterruptedException, SQLException {
        //Defining a RDBMS table with table.name.
        log.info("rdbmstabledefinitiontest19");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            RDBMSTableTestUtils.initDatabaseTable("FooTable");
        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\"," +
                "table.name=\"FooTable\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table FooTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable("FooTable");
        Assert.assertEquals(totalRowsInTable, 3, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest19")
    public void rdbmstabledefinitiontest20() throws InterruptedException, SQLException {
        //This testcase verified that defining a RDBMS table without defining a value for table.name
        // will be successfully create the table.
        log.info("rdbmstabledefinitiontest20");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\"," +
                "table.name=\"\", pool.properties=\"maximumPoolSize:1, maxLifetime:60000\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 3, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest20")
    public void rdbmstabledefinitiontest21() throws InterruptedException, SQLException {
        //Defining a RDBMS table with table.name.
        log.info("rdbmstabledefinitiontest21");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            RDBMSTableTestUtils.initDatabaseTable("FooTable");
        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\"," +
                "jdbc.driver.name=\"" + driverClassName + "\", table.name=\"FooTable\", " +
                "pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable("FooTable");
        Assert.assertEquals(totalRowsInTable, 3, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(enabled = false)
    public void rdbmstabledefinitiontest22() throws InterruptedException, SQLException {
        //Defining a RDBMS table with already exist table.name.
        log.info("rdbmstabledefinitiontest22");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            RDBMSTableTestUtils.initDatabaseTable("FooTable");
        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\", " +
                "jdbc.driver.name=\"" + driverClassName + "\", table.name=\"FooTable\", " +
                "pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table FooTable (symbol string, price float, length int, name string);";

        String streams2 = "" +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\"," +
                "jdbc.driver.name=\"" + driverClassName + "\",table.name=\"FooTable\")\n" +
                "@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long);";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + streams2 +
                query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable("FooTable");
        Assert.assertEquals(totalRowsInTable, 3, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest21")
    public void rdbmstabledefinitiontest23() throws InterruptedException, SQLException {
        //Testing table creation
        log.info("rdbmstabledefinitiontest23");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\", " +
                "pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String streams2 = "" +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\"," +
                "table.name=\"StockTable\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable2 (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + streams2 +
                query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 3, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest23")
    public void rdbmstabledefinitiontest24() throws InterruptedException, SQLException {
        //Testing table creation with custom table check query
        log.info("rdbmstabledefinitiontest24");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", table" +
                ".name = \"StockTable\",  table.check.query=\"SELECT 1 FROM StockTable LIMIT 1\", " +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\", " +
                "pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String streams2 = "" +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\"," +
                "table.name=\"StockTable\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable2 (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + streams2 +
                query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 3, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "rdbmstabledefinitiontest24")
    public void rdbmstabledefinitiontest25() throws InterruptedException, SQLException {
        //Testing table creation
        log.info("rdbmstabledefinitiontest25");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\", " +
                "pool.properties=\"maximumPoolSize:1\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "@Index(\"volume\")" +
                "@Index(\"symbol\", \"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        log.info(streams + query);

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
        long totalIndexInTable = RDBMSTableTestUtils.getIndexesInTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 3, "Definition/Insertion failed");
        Assert.assertEquals(totalIndexInTable, 3, "Indices creation failed");
        siddhiAppRuntime.shutdown();
    }
}
