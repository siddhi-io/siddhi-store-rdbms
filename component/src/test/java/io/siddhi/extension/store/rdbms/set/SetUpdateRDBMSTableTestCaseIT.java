/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.extension.store.rdbms.set;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.TABLE_NAME;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.driverClassName;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.password;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.url;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.user;

public class SetUpdateRDBMSTableTestCaseIT {
    private static final Logger log = Logger.getLogger(SetUpdateRDBMSTableTestCaseIT.class);

    @BeforeMethod
    public void init() {
        try {
            RDBMSTableTestUtils.initDatabaseTable(TABLE_NAME);
            log.info("Test init with url: " + url + " and driverClass: " + driverClassName);
        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @BeforeClass
    public static void startTest() {
        log.info("== SET tests for RDBMS Table - update cases, started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== SET tests for RDBMS Table - update cases, completed ==");
    }

    @Test(testName = "setUpdateRDBMSTableTestCase1", description = "setting all columns")
    public void setUpdateRDBMSTableTestCase1() throws InterruptedException, SQLException {
        log.info("setUpdateRDBMSTableTestCase1: setting all columns");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                    "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\")\n" +
                    //"@PrimaryKey(\"symbol\")" +
                    //"@Index(\"volume\")" +
                    "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into StockTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from UpdateStockStream " +
                    "update StockTable " +
                    "set StockTable.price = price, StockTable.symbol = symbol, " +
                    "   StockTable.volume = volume  " +
                    "   on StockTable.symbol == symbol ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            Thread.sleep(1000);

            long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
            Assert.assertEquals(totalRowsInTable, 3, "Update failed");
            siddhiAppRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'setUpdateRDBMSTableTestCase1' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(testName = "setUpdateRDBMSTableTestCase2", description = "setting a subset of columns")
    public void setUpdateRDBMSTableTestCase2() throws InterruptedException, SQLException {
        log.info("setUpdateRDBMSTableTestCase2: setting a subset of columns");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                    "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\")\n" +
                    //"@PrimaryKey(\"symbol\")" +
                    //"@Index(\"volume\")" +
                    "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into StockTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from UpdateStockStream " +
                    "update StockTable " +
                    "set StockTable.price = price, StockTable.symbol = symbol " +
                    "   on StockTable.symbol == symbol ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            Thread.sleep(1000);

            long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
            Assert.assertEquals(totalRowsInTable, 3, "Update failed");
            siddhiAppRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'setUpdateRDBMSTableTestCase2' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(testName = "setUpdateRDBMSTableTestCase3",
            description = "using a constant value as the assigment expression")
    public void setUpdateRDBMSTableTestCase3() throws InterruptedException, SQLException {
        log.info("setUpdateRDBMSTableTestCase3: using a constant value as the assigment expression.");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                    "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\")\n" +
                    //"@PrimaryKey(\"symbol\")" +
                    //"@Index(\"volume\")" +
                    "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into StockTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from UpdateStockStream " +
                    "update StockTable " +
                    "set StockTable.price = 10 " +
                    "   on StockTable.symbol == symbol ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            Thread.sleep(1000);

            long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
            Assert.assertEquals(totalRowsInTable, 3, "Update failed");
            siddhiAppRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'setUpdateRDBMSTableTestCase3' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(testName = "setUpdateRDBMSTableTestCase4",
            description = "using one of the output attribute values in the " +
                    "select clause as the assignment expression.")
    public void setUpdateRDBMSTableTestCase4() throws InterruptedException, SQLException {
        log.info("setUpdateRDBMSTableTestCase4: using one of the output attribute values in the " +
                "select clause as the assignment expression.");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                    "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\")\n" +
                    //"@PrimaryKey(\"symbol\")" +
                    //"@Index(\"volume\")" +
                    "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into StockTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from UpdateStockStream " +
                    "select price + 100 as newPrice , symbol " +
                    "update StockTable " +
                    "set StockTable.price = newPrice " +
                    "   on StockTable.symbol == symbol ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            Thread.sleep(1000);

            long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
            Assert.assertEquals(totalRowsInTable, 3, "Update failed");
            siddhiAppRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'setUpdateRDBMSTableTestCase4' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(testName = "setUpdateRDBMSTableTestCase5",
            description = "assignment expression containing an output attribute " +
                    "with a basic arithmatic operation.")
    public void setUpdateRDBMSTableTestCase5() throws InterruptedException, SQLException {
        log.info("setUpdateRDBMSTableTestCase5: assignment expression containing an output attribute " +
                "with a basic arithmatic operation.");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                    "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\")\n" +
                    //"@PrimaryKey(\"symbol\")" +
                    //"@Index(\"volume\")" +
                    "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into StockTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from UpdateStockStream " +
                    "select price + 100 as newPrice , symbol " +
                    "update StockTable " +
                    "set StockTable.price = newPrice + 100 " +
                    "   on StockTable.symbol == symbol ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            Thread.sleep(1000);

            long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
            Assert.assertEquals(totalRowsInTable, 3, "Update failed");
            siddhiAppRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'setUpdateRDBMSTableTestCase5' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(testName = "setUpdateRDBMSTableTestCase6",
            description = "Omitting table name from the LHS of set assignment")
    public void setUpdateRDBMSTableTestCase6() throws InterruptedException, SQLException {
        log.info("setUpdateRDBMSTableTestCase6: Omitting table name from the LHS of set assignment.");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                    "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\")\n" +
                    //"@PrimaryKey(\"symbol\")" +
                    //"@Index(\"volume\")" +
                    "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into StockTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from UpdateStockStream " +
                    "update StockTable " +
                    "set price = 100 " +
                    "   on StockTable.symbol == symbol ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            Thread.sleep(1000);

            long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
            Assert.assertEquals(totalRowsInTable, 3, "Update failed");
            siddhiAppRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'setUpdateRDBMSTableTestCase6' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(testName = "setUpdateRDBMSTableTestCase7",
            description = "Set clause should be optional")
    public void setUpdateRDBMSTableTestCase7() throws InterruptedException, SQLException {
        log.info("setUpdateRDBMSTableTestCase7: Set clause should be optional.");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                    "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\")\n" +
                    //"@PrimaryKey(\"symbol\")" +
                    //"@Index(\"volume\")" +
                    "define table StockTable (symbol string, price float, volume long); ";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into StockTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from UpdateStockStream " +
                    "update StockTable " +
                    "   on StockTable.symbol == symbol ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            updateStockStream.send(new Object[]{"IBM", 100f, 100L});
            Thread.sleep(1000);

            long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
            Assert.assertEquals(totalRowsInTable, 3, "Update failed");
            siddhiAppRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'setUpdateRDBMSTableTestCase7' ignored due to " + e.getMessage());
            throw e;
        }
    }
}
