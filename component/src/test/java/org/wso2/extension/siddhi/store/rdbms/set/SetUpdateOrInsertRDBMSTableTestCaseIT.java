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

package org.wso2.extension.siddhi.store.rdbms.set;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableTestUtils;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.sql.SQLException;

import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableTestUtils.driverClassName;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableTestUtils.url;

public class SetUpdateOrInsertRDBMSTableTestCaseIT {
    private static final Logger log = Logger.getLogger(SetUpdateOrInsertRDBMSTableTestCaseIT.class);


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
        log.info("== SET tests for RDBMS Table - update or insert cases, started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== SET tests for RDBMS Table - update or insert cases, completed ==");
    }

    @Test(testName = "setUpdateOrInsertRDBMSTableTestCase1", description = "setting all columns")
    public void setUpdateOrInsertRDBMSTableTestCase1() throws InterruptedException, SQLException {
        log.info("setUpdateOrInsertRDBMSTableTestCase1: setting all columns");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
//            RDBMSTableTestUtils.clearDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                    "jdbc.driver.name=\"" + driverClassName + "\"," +
                    "username=\"root\", password=\"root\",field.length=\"symbol:100\")\n" +
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
                    "update or insert into StockTable " +
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
            log.info("Test case 'setUpdateOrInsertRDBMSTableTestCase1' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(testName = "setUpdateOrInsertRDBMSTableTestCase2", description = "setting a subset of columns")
    public void setUpdateOrInsertRDBMSTableTestCase2() throws InterruptedException, SQLException {
        log.info("setUpdateOrInsertRDBMSTableTestCase2: setting a subset of columns");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
//            RDBMSTableTestUtils.clearDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                    "jdbc.driver.name=\"" + driverClassName + "\"," +
                    "username=\"root\", password=\"root\",field.length=\"symbol:100\")\n" +
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
                    "update or insert into StockTable " +
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
            log.info("Test case 'setUpdateOrInsertRDBMSTableTestCase2' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(testName = "setUpdateOrInsertRDBMSTableTestCase3",
            description = "using a constant value as the assigment expression")
    public void setUpdateOrInsertRDBMSTableTestCase3() throws InterruptedException, SQLException {
        log.info("setUpdateOrInsertRDBMSTableTestCase3: using a constant value as the assigment expression.");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
//            RDBMSTableTestUtils.clearDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                    "jdbc.driver.name=\"" + driverClassName + "\"," +
                    "username=\"root\", password=\"root\",field.length=\"symbol:100\")\n" +
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
                    "update or insert into StockTable " +
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
            log.info("Test case 'setUpdateOrInsertRDBMSTableTestCase3' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(testName = "setUpdateOrInsertRDBMSTableTestCase4",
            description = "using one of the output attribute values in the " +
                    "select clause as the assignment expression.")
    public void setUpdateOrInsertRDBMSTableTestCase4() throws InterruptedException, SQLException {
        log.info("setUpdateOrInsertRDBMSTableTestCase4: using one of the output attribute values in the " +
                "select clause as the assignment expression.");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
//            RDBMSTableTestUtils.clearDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                    "jdbc.driver.name=\"" + driverClassName + "\"," +
                    "username=\"root\", password=\"root\",field.length=\"symbol:100\")\n" +
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
//                    "select price, symbol, volume " +
                    "update or insert into StockTable " +
                    "set StockTable.price = price + 100  " +
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
            log.info("Test case 'setUpdateOrInsertRDBMSTableTestCase4' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(testName = "setUpdateOrInsertRDBMSTableTestCase5",
            description = "assignment expression containing an output attribute")
    public void setUpdateOrInsertRDBMSTableTestCase5() throws InterruptedException, SQLException {
        log.info("setUpdateOrInsertRDBMSTableTestCase5: assignment expression containing an output attribute " +
                "with a basic arithmatic operation.");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
//            RDBMSTableTestUtils.clearDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                    "jdbc.driver.name=\"" + driverClassName + "\"," +
                    "username=\"root\", password=\"root\",field.length=\"symbol:100\")\n" +
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
                    "update or insert into StockTable " +
                    "set StockTable.price = price + 100 " +
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
            log.info("Test case 'setUpdateOrInsertRDBMSTableTestCase5' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(testName = "setUpdateOrInsertRDBMSTableTestCase6",
            description = "Omitting table name from the LHS of set assignment")
    public void setUpdateOrInsertRDBMSTableTestCase6() throws InterruptedException, SQLException {
        log.info("setUpdateOrInsertRDBMSTableTestCase6: Omitting table name from the LHS of set assignment.");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
//            RDBMSTableTestUtils.clearDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                    "jdbc.driver.name=\"" + driverClassName + "\"," +
                    "username=\"root\", password=\"root\",field.length=\"symbol:100\")\n" +
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
                    "update or insert into StockTable " +
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
            log.info("Test case 'setUpdateOrInsertRDBMSTableTestCase6' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(testName = "setUpdateOrInsertRDBMSTableTestCase7",
            description = "Set clause should be optional")
    public void setUpdateOrInsertRDBMSTableTestCase7() throws InterruptedException, SQLException {
        log.info("setUpdateOrInsertRDBMSTableTestCase7: Set clause should be optional.");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
//            RDBMSTableTestUtils.clearDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                    "jdbc.driver.name=\"" + driverClassName + "\"," +
                    "username=\"root\", password=\"root\",field.length=\"symbol:100\")\n" +
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
                    "update or insert into StockTable " +
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
            log.info("Test case 'setUpdateOrInsertRDBMSTableTestCase7' ignored due to " + e.getMessage());
            throw e;
        }
    }
}
