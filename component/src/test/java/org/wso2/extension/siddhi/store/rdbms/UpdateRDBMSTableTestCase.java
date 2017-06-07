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

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.sql.SQLException;
import java.util.List;

import static org.wso2.extension.siddhi.store.rdbms.RDBMSTableTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.rdbms.RDBMSTableTestUtils.url;

public class UpdateRDBMSTableTestCase {
    private static final Logger log = Logger.getLogger(UpdateRDBMSTableTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        try {
            RDBMSTableTestUtils.clearDatabaseTable(TABLE_NAME);
        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @BeforeClass
    public static void startTest() {
        log.info("== RDBMS Table UPDATE tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== RDBMS Table UPDATE tests completed ==");
    }

    @Test
    public void updateFromTableTest1() throws InterruptedException, SQLException {
        log.info("updateFromTableTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price double, volume long); " +
                "define stream UpdateStockStream (symbol string, price double, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"root\", password=\"root\",field.length=\"symbol:100\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price double, volume long); ";

        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2') " +
                "from UpdateStockStream\n" +
                "select symbol, price, volume\n" +
                "update StockTable\n" +
                "on (StockTable.symbol == symbol);";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");
        executionPlanRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6, 100L});
        stockStream.send(new Object[]{"IBM", 75.6, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6, 100L});
        updateStockStream.send(new Object[]{"IBM", 57.6, 100L});
        Thread.sleep(1000);

        List<List<Object>> recordsInTable = RDBMSTableTestUtils.getRecordsInTable(TABLE_NAME);
        Assert.assertEquals(recordsInTable.get(0).toArray(), new Object[]{"WSO2", 55.6, 100L}, "Update failed");
        Assert.assertEquals(recordsInTable.get(1).toArray(), new Object[]{"WSO2", 57.6, 100L}, "Update failed");
        Assert.assertEquals(recordsInTable.get(2).toArray(), new Object[]{"IBM", 57.6, 100L}, "Update failed");
        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 3, "Update failed");
        executionPlanRuntime.shutdown();
    }

    @Test
    public void updateFromTableTest2() throws InterruptedException, SQLException {
        //Check for update event data in RDBMS table when multiple conditions are true.
        log.info("updateFromTableTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            RDBMSTableTestUtils.clearDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price double, volume long); " +
                    "define stream UpdateStockStream (symbol string, price double, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                    "username=\"root\", password=\"root\",field.length=\"symbol:100\")\n" +
                    //"@PrimaryKey(\"symbol\")" +
                    //"@Index(\"volume\")" +
                    "define table StockTable (symbol string, price double, volume long); ";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into StockTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from UpdateStockStream " +
                    "update StockTable " +
                    "   on (StockTable.symbol == symbol and StockTable.volume > volume) ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6, 50L});
            stockStream.send(new Object[]{"IBM", 75.6, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6, 200L});
            updateStockStream.send(new Object[]{"WSO2", 85.6, 100L});
            Thread.sleep(1000);

            List<List<Object>> recordsInTable = RDBMSTableTestUtils.getRecordsInTable(TABLE_NAME);
            Assert.assertEquals(recordsInTable.get(0).toArray(), new Object[]{"WSO2", 55.6, 50L}, "Update failed");
            Assert.assertEquals(recordsInTable.get(1).toArray(), new Object[]{"IBM", 75.6, 100L}, "Update failed");
            Assert.assertEquals(recordsInTable.get(2).toArray(), new Object[]{"WSO2", 85.6, 100L}, "Update failed");
            long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
            Assert.assertEquals(3, totalRowsInTable, "Update failed");
            executionPlanRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'updateFromTableTest2' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void updateFromTableTest3() throws InterruptedException, SQLException {
        log.info("updateFromTableTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            RDBMSTableTestUtils.clearDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream CheckStockStream (symbol string, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
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
                    "from CheckStockStream[(StockTable.symbol==symbol) in StockTable] " +
                    "insert into OutStream;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            executionPlanRuntime.addCallback("query2", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        for (Event event : inEvents) {
                            inEventCount++;
                        }
                        eventArrived = true;
                    }

                }

            });

            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 55.6f, 100L});
            checkStockStream.send(new Object[]{"BSD", 100L});
            checkStockStream.send(new Object[]{"WSO2", 100L});
            checkStockStream.send(new Object[]{"IBM", 100L});
            Thread.sleep(1000);

            Assert.assertEquals(inEventCount, 2, "Number of success events");
            Assert.assertEquals(eventArrived, true, "Event arrived");
            executionPlanRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'updateFromTableTest5' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void updateFromTableTest4() throws InterruptedException, SQLException {
        log.info("updateFromTableTest4");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            RDBMSTableTestUtils.clearDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream CheckStockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (comp string, prc float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
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
                    "select comp as symbol, prc as price, volume " +
                    "update StockTable " +
                    "on StockTable.symbol==symbol; " +
                    "" +
                    "@info(name = 'query3') " +
                    "from CheckStockStream[(symbol==StockTable.symbol and volume==StockTable.volume " +
                    "and price<StockTable.price) in StockTable] " +
                    "insert into OutStream;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            executionPlanRuntime.addCallback("query3", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        for (Event event : inEvents) {
                            inEventCount++;
                            switch (inEventCount) {
                                case 1:
                                    Assert.assertEquals(event.getData(), new Object[]{"IBM", 150.6f, 100L});
                                    break;
                                case 2:
                                    Assert.assertEquals(event.getData(), new Object[]{"IBM", 190.6f, 100L});
                                    break;
                                default:
                                    Assert.assertSame(inEventCount, 2);
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

            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
            InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 185.6f, 100L});
            checkStockStream.send(new Object[]{"IBM", 150.6f, 100L});
            checkStockStream.send(new Object[]{"WSO2", 175.6f, 100L});
            updateStockStream.send(new Object[]{"IBM", 200f, 100L});
            checkStockStream.send(new Object[]{"IBM", 190.6f, 100L});
            checkStockStream.send(new Object[]{"WSO2", 155.6f, 100L});
            Thread.sleep(2000);

            Assert.assertEquals(inEventCount, 2, "Number of success events");
            Assert.assertEquals(removeEventCount, 0, "Number of remove events");
            Assert.assertEquals(eventArrived, true, "Event arrived");
            executionPlanRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'updateFromTableTest7' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void updateFromTableTest5() throws InterruptedException, SQLException {
        log.info("updateFromTableTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            RDBMSTableTestUtils.clearDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
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
                    "update StockTable " +
                    "   on StockTable.volume == 100 ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
            updateStockStream.send(new Object[]{"IBM", 57.6f, 100L});
            Thread.sleep(1000);

            long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
            Assert.assertEquals(totalRowsInTable, 3, "Update failed");
            executionPlanRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'updateFromTableTest8' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(enabled = false)
    public void updateFromTableTest6() throws InterruptedException, SQLException {
        //Check for update event data in RDBMS table when multiple conditions are true.
        log.info("updateFromTableTest6");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            RDBMSTableTestUtils.clearDatabaseTable(TABLE_NAME);
            String streams = "" +
                    "define stream StockStream (symbol string, price double, volume long); " +
                    "define stream UpdateStockStream (symbol string, price double, volume long); " +
                    "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                    "username=\"root\", password=\"root\",field.length=\"symbol:100\")\n" +
                    //"@PrimaryKey(\"symbol\")" +
                    //"@Index(\"volume\")" +
                    "define table StockTable (symbol string, price double, volume long); ";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into StockTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from UpdateStockStream " +
                    "select symbol, price, volume " +
                    "update StockTable " +
                    "   on (StockTable.symbol == symbol and StockTable.volume > volume) ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6, 50L});
            stockStream.send(new Object[]{"IBM", 75.6, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6, 200L});
            updateStockStream.send(new Object[]{"WSO2", 85.6, 100L});
            Thread.sleep(1000);

            List<List<Object>> recordsInTable = RDBMSTableTestUtils.getRecordsInTable(TABLE_NAME);
            Assert.assertEquals(recordsInTable.get(0).toArray(), new Object[]{"WSO2", 55.6, 50L}, "Update failed");
            Assert.assertEquals(recordsInTable.get(1).toArray(), new Object[]{"IBM", 75.6, 100L}, "Update failed");
            Assert.assertEquals(recordsInTable.get(2).toArray(), new Object[]{"WSO2", 57.6, 100L}, "Update failed");
            long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
            Assert.assertEquals(totalRowsInTable, 3, "Update failed");
            executionPlanRuntime.shutdown();
        } catch (SQLException e) {
            log.info("Test case 'updateFromTableTest2' ignored due to " + e.getMessage());
            throw e;
        }
    }
}
