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
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils;
import org.apache.log4j.Logger;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.TABLE_NAME;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.driverClassName;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.password;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.url;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.user;

public class SetUpdateOrInsertRDBMSTableTestCaseIT {
    private static final Logger log = Logger.getLogger(SetUpdateOrInsertRDBMSTableTestCaseIT.class);
    private static AtomicInteger actualEventCount = new AtomicInteger(0);

    @BeforeMethod
    public void init() {
        try {
            actualEventCount.set(0);
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
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                    "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\", " +
                    "pool.properties=\"maximumPoolSize:1\")\n" +
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
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                    "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\", " +
                    "pool.properties=\"maximumPoolSize:1\")\n" +
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
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                    "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\", " +
                    "pool.properties=\"maximumPoolSize:1\")\n" +
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
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                    "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\", " +
                    "pool.properties=\"maximumPoolSize:1\")\n" +
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
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                    "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\", " +
                    "pool.properties=\"maximumPoolSize:1\")\n" +
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
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                    "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\", " +
                    "pool.properties=\"maximumPoolSize:1\")\n" +
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
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\", " +
                    "username=\"" + user + "\", password=\"" + password + "\",field.length=\"symbol:100\", " +
                    "pool.properties=\"maximumPoolSize:1\")\n" +
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

    @Test(testName = "setUpdateOrInsertRDBMSTableTestCase8",
            description = "Record batch update or insert with set operation")
    public void setUpdateOrInsertRDBMSTableTestCase8() throws InterruptedException, SQLException {
        log.info("setUpdateOrInsertRDBMSTableTestCase8: Set update or insert event batch.");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream SearchStream (symbol string); " +
                "define stream UpdateStockStream (symbol string, price int, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price int, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from UpdateStockStream " +
                "select symbol, price, volume " +
                "update or insert into StockTable " +
                "set StockTable.price = price " +
                "   on StockTable.symbol == symbol;" +
                "@info(name = 'query2') " +
                "from SearchStream#window.length(1) join StockTable on StockTable.symbol == SearchStream.symbol " +
                "select StockTable.symbol as symbol, price, volume " +
                "insert into OutStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        InputHandler searchStream = siddhiAppRuntime.getInputHandler("SearchStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        int inEventCount = actualEventCount.incrementAndGet();
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 155, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 155, 100L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 2);
                        }
                    }
                }
            }

        });
        siddhiAppRuntime.start();
        Event[] events = new Event[4];
        events[0] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55, 100L});
        events[1] = new Event(System.currentTimeMillis(), new Object[]{"IBM", 55, 100L});
        events[2] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", 155, 0L});
        events[3] = new Event(System.currentTimeMillis(), new Object[]{"IBM", 155, 0L});
        updateStockStream.send(events);
        searchStream.send(new Object[]{"WSO2"});
        searchStream.send(new Object[]{"IBM"});
        waitTillVariableCountMatches(2, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
    }

    private static void waitTillVariableCountMatches(long expected, Duration duration) {
        Awaitility.await().atMost(duration).until(() -> {
            return actualEventCount.get() == expected;
        });
    }
}
