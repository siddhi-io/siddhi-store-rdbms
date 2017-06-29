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
package org.wso2.extension.siddhi.store.rdbms;

import com.zaxxer.hikari.pool.HikariPool;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.store.rdbms.exception.RDBMSTableException;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.sql.SQLException;
import javax.naming.NamingException;

import static org.wso2.extension.siddhi.store.rdbms.RDBMSTableTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.rdbms.RDBMSTableTestUtils.driverClassName;
import static org.wso2.extension.siddhi.store.rdbms.RDBMSTableTestUtils.password;
import static org.wso2.extension.siddhi.store.rdbms.RDBMSTableTestUtils.url;
import static org.wso2.extension.siddhi.store.rdbms.RDBMSTableTestUtils.user;

public class DefineRDBMSTableTestCaseIT {
    private static final Logger log = Logger.getLogger(DefineRDBMSTableTestCaseIT.class);

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
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\")\n" +
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

    @Test(expectedExceptions = RDBMSTableException.class)
    public void rdbmstabledefinitiontest2() throws InterruptedException, SQLException {
        //Testing table creation with a invalid primary key (normal insertion.
        log.info("rdbmstabledefinitiontest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\")\n" +
                "@PrimaryKey(\"testPrimary\")" +
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

    @Test(expectedExceptions = RDBMSTableException.class)
    public void rdbmstabledefinitiontest3() throws InterruptedException, SQLException {
        //Testing Defining a RDBMS table without defining a value for jdbc url field
        log.info("rdbmstabledefinitiontest3");
        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\",field.length=\"symbol:100\")\n" +
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

        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = RDBMSTableException.class)
    public void rdbmstabledefinitiontest4() throws InterruptedException, SQLException {
        //Testing table creation with no connection URL
        log.info("rdbmstabledefinitiontest4");
        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.url=\"\", jdbc.driver.name=\"" +
                driverClassName + "\", field.length=\"symbol:100\")\n" +
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

        siddhiAppRuntime.shutdown();
    }

    //todo : check this and update test link
    @Test(expectedExceptions = RuntimeException.class)
    public void rdbmstabledefinitiontest5() throws InterruptedException, SQLException {
        //Testing table creation with invalid connection URL
        log.info("rdbmstabledefinitiontest5");
        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", " +
                "username=\"" + user + "\", password=\"" + password + "\", " +
                "jdbc.url=\"eerrrjdbc:h222:repository/database/" +
                "ANALYTICS_EVENT_STORE\", jdbc.driver.name=\"" + driverClassName + "\", " +
                "field.length=\"symbol:100\")\n" +
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

        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = RDBMSTableException.class)
    public void rdbmstabledefinitiontest6() throws InterruptedException, SQLException {
        //Testing table creation with no password
        log.info("rdbmstabledefinitiontest6");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", field.length=\"symbol:100\")\n" +
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

        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = RDBMSTableException.class)
    public void rdbmstabledefinitiontest7() throws InterruptedException, SQLException {
        //Defining a RDBMS table without defining a value for password field
        log.info("rdbmstabledefinitiontest7");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"\", field.length=\"symbol:100\")\n" +
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

        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = HikariPool.PoolInitializationException.class)
    public void rdbmstabledefinitiontest8() throws InterruptedException, SQLException {
        //Defining a RDBMS table without defining a value for password field
        log.info("rdbmstabledefinitiontest8");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"root###\", field.length=\"symbol:100\")\n" +
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

        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = RDBMSTableException.class)
    public void rdbmstabledefinitiontest9() throws InterruptedException, SQLException {
        //Defining a RDBMS table without having an username field.
        log.info("rdbmstabledefinitiontest9");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                "password=\"" + password + "\", field.length=\"symbol:100\")\n" +
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

        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = RDBMSTableException.class)
    public void rdbmstabledefinitiontest10() throws InterruptedException, SQLException {
        //Defining a RDBMS table without defining a value for username field.
        log.info("rdbmstabledefinitiontest10");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"\", password=\"" + password + "\", field.length=\"symbol:100\")\n" +
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

        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = HikariPool.PoolInitializationException.class)
    public void rdbmstabledefinitiontest11() throws InterruptedException, SQLException {
        //Defining a RDBMS table without defining a value for username field.
        log.info("rdbmstabledefinitiontest11");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"root####\", password=\"" + password + "\", field.length=\"symbol:100\")\n" +
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

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void rdbmstabledefinitiontest12() throws InterruptedException, SQLException {
        //Defining a RDBMS table without having a field length field.
        log.info("rdbmstabledefinitiontest12");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"" + password + "\")\n" +
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

    @Test(expectedExceptions = RDBMSTableException.class)
    public void rdbmstabledefinitiontest13() throws InterruptedException, SQLException {
        //Defining a RDBMS table with non existing attribute/s to define the field length .
        log.info("rdbmstabledefinitiontest13");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\",  field.length=\"length:254\", password=\"" + password + "\")\n" +
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

    @Test
    public void rdbmstabledefinitiontest14() throws InterruptedException, SQLException, NamingException {
        //Defining a RDBMS table with jndi.resource.
        log.info("rdbmstabledefinitiontest14");
        SiddhiManager siddhiManager = new SiddhiManager();
        RDBMSTableTestUtils.setupJNDIDatasource(url, driverClassName);
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"jdbc:h2:./target/dasdb###\", jdbc.driver.name=\"" +
                driverClassName + "\"," +
                "username=\"root###\", jndi.resource=\"java:comp/env/jdbc/TestDB\", field.length=\"symbol:100\", " +
                "password=\"root###\")\n" +
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

    @Test(expectedExceptions = RDBMSTableException.class)
    public void rdbmstabledefinitiontest15() throws InterruptedException, SQLException, NamingException {
        //Defining a RDBMS table with an invalid value for jndi.resource field.
        log.info("rdbmstabledefinitiontest15");
        SiddhiManager siddhiManager = new SiddhiManager();
        RDBMSTableTestUtils.setupJNDIDatasource(url, driverClassName);
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"jdbc:h2:./target/dasdb###\", jdbc.driver.name=\"" +
                driverClassName + "\"," +
                "username=\"root###\", jndi.resource=\"jdbc444/444TestDB\", field.length=\"symbol:100\", " +
                "password=\"root###\")\n" +
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

    @Test
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

    @Test(expectedExceptions = RuntimeException.class)
    public void rdbmstabledefinitiontest17() throws InterruptedException, SQLException {
        //  This testcase verified that defining a RDBMS table by including at least one invalid pool.property
        // field will not successfully create the table.
        log.info("rdbmstabledefinitiontest17");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\"," +
                "pool.properties=\"maximumPoolSize:50,maxWSO2:60000\")\n" +
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

    @Test(expectedExceptions = RuntimeException.class)
    public void rdbmstabledefinitiontest18() throws InterruptedException, SQLException {
        //Defining a RDBMS table with an invalid value for a pool.property.
        log.info("rdbmstabledefinitiontest18");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\"," +
                "pool.properties=\"maximumPoolSize:50, maxLifetime:WSO2\")\n" +
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

    @Test
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
                "table.name=\"FooTable\")\n" +
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

    @Test
    public void rdbmstabledefinitiontest20() throws InterruptedException, SQLException {
        //This testcase verified that defining a RDBMS table without defining a value for table.name
        // will be successfully create the table.
        log.info("rdbmstabledefinitiontest20");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\"," +
                "table.name=\"\")\n" +
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

    @Test
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
                "jdbc.driver.name=\"" + driverClassName + "\", table.name=\"FooTable\")\n" +
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
                "jdbc.driver.name=\"" + driverClassName + "\", table.name=\"FooTable\")\n" +
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

    @Test(testName = "rdbmstabledefinitiontest23", description = "Testing table creation.")
    public void rdbmstabledefinitiontest23() throws InterruptedException, SQLException {
        //Testing table creation
        log.info("rdbmstabledefinitiontest23");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\")\n" +
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
    /*
    @Test
    public void rdbmstabledefinitiontest3() throws InterruptedException, SQLException {
        //Testing table creation with a primary key (normal insertion)
        log.info("rdbmstabledefinitiontest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            RDBMSTableTestUtils.initDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                    "username="+ user +", password="+ password +",field.length=\"symbol:100\")\n" +
                    "@PrimaryKey(\"symbol\")" +
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
            Assert.assertEquals("Definition/Insertion failed", 3, totalRowsInTable);
            siddhiAppRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'rdbmstabledefinitiontest2' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expected = RDBMSTableException.class)
    public void rdbmstabledefinitiontest4() throws InterruptedException, SQLException {
        //Testing table creation with a primary key (purposeful duplicate insertion)
        log.info("rdbmstabledefinitiontest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            RDBMSTableTestUtils.initDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                    "username="+ user +", password="+ password +",field.length=\"symbol:100\")\n" +
                    "@PrimaryKey(\"symbol\")" +
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
            stockStream.send(new Object[]{"WSO2", 57.1F, 100L});
            Thread.sleep(1000);

            siddhiAppRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'rdbmstabledefinitiontest3' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void rdbmstabledefinitiontest5() throws InterruptedException, SQLException {
        //Testing table creation with a compound primary key (normal insertion)
        log.info("rdbmstabledefinitiontest4");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            RDBMSTableTestUtils.initDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                    "username="+ user +", password="+ password +",field.length=\"symbol:100\")\n" +
                    "@PrimaryKey(\"symbol, price\")" +
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
            stockStream.send(new Object[]{"WSO2", 58.6F, 100L});
            Thread.sleep(1000);

            long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
            Assert.assertEquals("Definition/Insertion failed", 4, totalRowsInTable);
            siddhiAppRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'rdbmstabledefinitiontest4' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void rdbmstabledefinitiontest6() throws InterruptedException, SQLException {
        //Testing table creation with index
        log.info("rdbmstabledefinitiontest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            RDBMSTableTestUtils.initDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                    "username="+ user +", password="+ password +",field.length=\"symbol:100\")\n" +
                    //"@PrimaryKey(\"symbol\")" +
                    "@Index(\"volume\")" +
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
            Thread.sleep(1000);

            long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
            Assert.assertEquals("Definition/Insertion failed", 2, totalRowsInTable);
            siddhiAppRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'rdbmstabledefinitiontest5' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expected = RDBMSTableException.class)
    public void rdbmstabledefinitiontest7() throws InterruptedException, SQLException {
        //Testing table creation with no username
        log.info("rdbmstabledefinitiontest6");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            RDBMSTableTestUtils.initDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                    "password="+ password +",field.length=\"symbol:100\")\n" +
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

            siddhiAppRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'rdbmstabledefinitiontest6' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expected = RDBMSTableException.class)
    public void rdbmstabledefinitiontest8() throws InterruptedException, SQLException {
        //Testing table creation with no password
        log.info("rdbmstabledefinitiontest7");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            RDBMSTableTestUtils.initDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                    "username="+ user +", field.length=\"symbol:100\")\n" +
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

            siddhiAppRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'rdbmstabledefinitiontest7' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expected = RDBMSTableException.class)
    public void rdbmstabledefinitiontest9() throws InterruptedException, SQLException {
        //Testing table creation with no connection URL
        log.info("rdbmstabledefinitiontest8");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            RDBMSTableTestUtils.initDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", " +
                    "username="+ user +", password="+ password +",field.length=\"symbol:100\")\n" +
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

            siddhiAppRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'rdbmstabledefinitiontest8' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Ignore
    @Test
    public void deleteRDBMSTableTest1() throws InterruptedException {
        log.info("deleteTableTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream DeleteStockStream (deleteSymbol string); " +
                "@store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username="+ user +", password="+ password +",field.length=\"symbol:100\")\n" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == deleteSymbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        deleteStockStream.send(new Object[]{"IBM"});
        deleteStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Ignore
    @Test
    public void updateRDBMSTableTest1() throws InterruptedException {
        log.info("updateTableTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +

                "@store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username="+ user +", password="+ password +",field.length=\"symbol:100\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                "@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update StockTable " +
                "   on StockTable.symbol == symbol;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2_2", 57.6F, 100L});
        updateStockStream.send(new Object[]{"IBM", 56.6F, 100L});

        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();

    }

    @Ignore
    @Test
    public void updateOrInsertRDBMSTableTest1() throws InterruptedException {
        log.info("updateOrInsertTableTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +

                "@store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username="+ user +", password="+ password +",field.length=\"symbol:100\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                "@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "UPDATE OR INSERT INTO StockTable " +
                "   on StockTable.symbol == symbol;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2_2", 57.6F, 100L});
        updateStockStream.send(new Object[]{"IBM", 56.6F, 100L});
        updateStockStream.send(new Object[]{"MSFT", 47.1F, 100L});

        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();

    }

    @Ignore
    @Test
    public void testTableJoinQuery1() throws InterruptedException {
        log.info("testTableJoinQuery1 - OUT 2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +

                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username="+ user +", password="+ password +",field.length=\"symbol:100\")\n" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream#window.length(1) join StockTable " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, StockTable.volume as " +
                "volume " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertArrayEquals(new Object[]{"WSO3", "WSO3", 100L}, event.getData());
                                break;
                            case 2:
                                Assert.assertArrayEquals(new Object[]{"WSO3", "MSFT", 10L}, event.getData());
                                break;
                            default:
                                Assert.assertSame(2, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO3", 55.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 75.6F, 10L});
        checkStockStream.send(new Object[]{"WSO3"});

        Thread.sleep(1000);

        Assert.assertEquals("Number of success events", 2, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }
    */
}
