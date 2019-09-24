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
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils;
import io.siddhi.query.api.exception.DuplicateDefinitionException;
import org.apache.log4j.Logger;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.testng.Assert;
import org.testng.AssertJUnit;
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

public class UpdateOrInsertRDBMSTableTestCaseIT {
    private static final Logger log = Logger.getLogger(UpdateOrInsertRDBMSTableTestCaseIT.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    private static AtomicInteger actualEventCount = new AtomicInteger(0);

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        actualEventCount.set(0);
        try {
            RDBMSTableTestUtils.initDatabaseTable(TABLE_NAME);
        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @BeforeClass
    public static void startTest() {
        log.info("== RDBMS Table UPDATE/INSERT tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== RDBMS Table UPDATE/INSERT tests completed ==");
    }

    @Test
    public void updateOrInsertTableTest1() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream#window.timeBatch(1 sec) " +
                "update or insert into StockTable " +
                "   on StockTable.symbol=='IBM' and StockTable.symbol=='GOOG';";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        updateStockStream.send(new Object[]{"GOOG", 10.6F, 100L});
        Thread.sleep(3000);

        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest1")
    public void updateOrInsertTableTest2() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query2') " +
                "from StockStream " +
                "update or insert into StockTable " +
                "   on StockTable.symbol==symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 10F, 100L});
        stockStream.send(new Object[]{"WSO2", 101F, 100L});
        stockStream.send(new Object[]{"WSO2", 102F, 100L});
        stockStream.send(new Object[]{"WSO2", 103F, 100L});
        stockStream.send(new Object[]{"WSO2", 104F, 100L});
        Thread.sleep(500);

        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest2")
    public void updateOrInsertTableTest3() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
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
                "   on StockTable.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", 100L}, event.getData());
                                break;
                            case 2:
                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2", 100L}, event.getData());
                                break;
                            case 3:
                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2", 100L}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(3, inEventCount);
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
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        updateStockStream.send(new Object[]{"IBM", 77.6F, 200L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        Thread.sleep(500);

        AssertJUnit.assertEquals("Number of success events", 3, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest3")
    public void updateOrInsertTableTest4() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest4");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query2') " +
                "from StockStream " +
                "update or insert into StockTable " +
                "   on StockTable.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", 100L}, event.getData());
                                break;
                            case 2:
                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2", 100L}, event.getData());
                                break;
                            case 3:
                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2", 100L}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(3, inEventCount);
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

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        stockStream.send(new Object[]{"IBM", 77.6F, 200L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        Thread.sleep(500);

        AssertJUnit.assertEquals("Number of success events", 3, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = DuplicateDefinitionException.class, enabled = false)
    public void updateOrInsertTableTest5() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest5");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "define stream UpdateStockStream (comp string, vol long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
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
                "select comp as symbol, vol as volume " +
                "update or insert into StockTable " +
                "   on StockTable.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", 100L}, event.getData());
                                break;
                            case 2:
                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2", 100L}, event.getData());
                                break;
                            case 3:
                                AssertJUnit.assertArrayEquals(new Object[]{"FB", 300L}, event.getData());
                                break;
                            case 4:
                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2", 100L}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(4, inEventCount);
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
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        updateStockStream.send(new Object[]{"FB", 300L});
        checkStockStream.send(new Object[]{"FB", 300L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        Thread.sleep(500);

        AssertJUnit.assertEquals("Number of success events", 4, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", false, eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest4")
    public void updateOrInsertTableTest6() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest6");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "define stream UpdateStockStream (comp string, vol long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "update or insert into StockTable " +
                "   on StockTable.symbol==symbol;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "select comp as symbol, 0f as price, vol as volume " +
                "update or insert into StockTable " +
                "   on StockTable.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", 100L}, event.getData());
                                break;
                            case 2:
                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2", 100L}, event.getData());
                                break;
                            case 3:
                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2", 100L}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(3, inEventCount);
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
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        updateStockStream.send(new Object[]{"IBM", 200L});
        updateStockStream.send(new Object[]{"FB", 300L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        Thread.sleep(500);

        AssertJUnit.assertEquals("Number of success events", 3, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }


    @Test(dependsOnMethods = "updateOrInsertTableTest6")
    public void updateOrInsertTableTest7() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest7");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long, price float); " +
                "define stream UpdateStockStream (comp string, vol long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
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
                "select comp as symbol,  5f as price, vol as volume " +
                "update or insert into StockTable " +
                "   on StockTable.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol and volume==StockTable.volume and price " +
                "< StockTable.price) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", 100L, 56.6f}, event.getData());
                                break;
                            case 2:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", 200L, 0f}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(2, inEventCount);
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
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 155.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L, 56.6f});
        checkStockStream.send(new Object[]{"WSO2", 100L, 155.6f});
        updateStockStream.send(new Object[]{"IBM", 200L});
        checkStockStream.send(new Object[]{"IBM", 200L, 0f});
        checkStockStream.send(new Object[]{"WSO2", 100L, 155.6f});
        Thread.sleep(2000);

        AssertJUnit.assertEquals("Number of success events", 2, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest7")
    public void updateOrInsertTableTest8() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest8");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long, price float); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query2') " +
                "from StockStream " +
                "select symbol, price, volume " +
                "update or insert into StockTable " +
                "   on StockTable.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol and volume==StockTable.volume and price " +
                "< StockTable.price) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", 100L, 55.6f}, event.getData());
                                break;
                            case 2:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", 200L, 55.6f}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(2, inEventCount);
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

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 155.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L, 55.6f});
        checkStockStream.send(new Object[]{"WSO2", 100L, 155.6f});
        stockStream.send(new Object[]{"IBM", 155.6F, 200L});
        checkStockStream.send(new Object[]{"IBM", 200L, 55.6f});
        checkStockStream.send(new Object[]{"WSO2", 100L, 155.6f});
        Thread.sleep(500);

        AssertJUnit.assertEquals("Number of success events", 2, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest8")
    public void updateOrInsertTableTest9() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest9");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long, price float); " +
                "define stream UpdateStockStream (comp string, vol long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream left outer join StockTable " +
                "   on UpdateStockStream.comp == StockTable.symbol " +
                "select  symbol, ifThenElse(price is nulL,0F,price) as price, vol as volume " +
                "update or insert into StockTable " +
                "   on StockTable.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(CheckStockStream.symbol==StockTable.symbol and " +
                "CheckStockStream.volume==StockTable.volume and " +
                "CheckStockStream.price < StockTable.price) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", 100L, 55.6f}, event.getData());
                                break;
                            case 2:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", 200L, 55.6f}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(2, inEventCount);
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
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 155.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L, 55.6f});
        checkStockStream.send(new Object[]{"WSO2", 100L, 155.6f});
        updateStockStream.send(new Object[]{"IBM", 200L});
        checkStockStream.send(new Object[]{"IBM", 200L, 55.6f});
        checkStockStream.send(new Object[]{"WSO2", 100L, 155.6f});
        Thread.sleep(1000);

        AssertJUnit.assertEquals("Number of success events", 2, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest9")
    public void updateOrInsertTableTest10() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest10");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long, price float); " +
                "define stream UpdateStockStream (comp string, vol long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream left outer join StockTable " +
                "   on UpdateStockStream.comp == StockTable.symbol " +
                "select comp as symbol, ifThenElse(price is nulL,5F,price) as price, vol as volume " +
                "update or insert into StockTable " +
                "   on StockTable.symbol==symbol;" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol and volume == StockTable.volume and " +
                "price < StockTable.price) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", 200L, 0f}, event.getData());
                                break;
                            case 2:
                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2", 300L, 4.6f}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(2, inEventCount);
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
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L, 155.6f});
        checkStockStream.send(new Object[]{"WSO2", 100L, 155.6f});
        updateStockStream.send(new Object[]{"IBM", 200L});
        updateStockStream.send(new Object[]{"WSO2", 300L});
        checkStockStream.send(new Object[]{"IBM", 200L, 0f});
        checkStockStream.send(new Object[]{"WSO2", 300L, 4.6f});
        Thread.sleep(1000);

        AssertJUnit.assertEquals("Number of success events", 2, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest10")
    public void updateOrInsertTableTest11() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest11");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
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
                "   on StockTable.volume==volume ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        updateStockStream.send(new Object[]{"GOOG", 10.6F, 100L});
        Thread.sleep(500);

        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
        AssertJUnit.assertEquals("Update failed", 3, totalRowsInTable);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest11")
    public void updateOrInsertTableTest12() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest12");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
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
                "   on StockTable.volume == volume ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        updateStockStream.send(new Object[]{"GOOG", 10.6F, 200L});
        Thread.sleep(500);

        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
        AssertJUnit.assertEquals("Update failed", 4, totalRowsInTable);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest12")
    public void updateOrInsertTableTest13() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest13");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream UpdateStockStream (symbol string, price int, volume long); " +
                "define stream SearchStream (symbol string); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price int, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "   on StockTable.symbol == symbol ;" +
                "" +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 155, 200L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 155, 200L});
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
        events[2] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", 155, 200L});
        events[3] = new Event(System.currentTimeMillis(), new Object[]{"IBM", 155, 200L});
        updateStockStream.send(events);
        searchStream.send(new Object[]{"WSO2"});
        searchStream.send(new Object[]{"IBM"});
        waitTillVariableCountMatches(2, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest13")
    public void updateOrInsertTableTest14() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest14");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream UpdateStockStream (symbol string, price int, volume long); " +
                "define stream SearchStream (symbol string); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@PrimaryKey(\"symbol\", \"volume\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price int, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "   on StockTable.symbol == symbol ;" +
                "" +
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
        events[2] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", 155, 100L});
        events[3] = new Event(System.currentTimeMillis(), new Object[]{"IBM", 155, 100L});
        updateStockStream.send(events);
        searchStream.send(new Object[]{"WSO2"});
        searchStream.send(new Object[]{"IBM"});
        waitTillVariableCountMatches(2, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest14")
    public void updateOrInsertTableTest15() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest15");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream UpdateStockStream (symbol string, price int, volume long); " +
                "define stream SearchStream (symbol string); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price int, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from UpdateStockStream#window.lengthBatch(1000) " +
                "select * " +
                "update or insert into StockTable " +
                "   on StockTable.symbol == symbol ;" +
                "" +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55, 999L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
                        }
                    }
                }
            }

        });
        siddhiAppRuntime.start();
        Event[] events = new Event[1000];
        for (int i = 0; i < 1000; i++) {
            events[i] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55, (long) i});
        }
        updateStockStream.send(events);
        searchStream.send(new Object[]{"WSO2"});
        waitTillVariableCountMatches(1, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest15")
    public void updateOrInsertTableTest16() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest16");
        SiddhiManager siddhiManager = new SiddhiManager();
        int eventCount = 3500;
        String streams = "" +
                "define stream UpdateAPIMStream1 (apiContext string, apiName string , apiVersion string, " +
                "apiCreator string , apiCreatorTenantDomain string , applicationOwner string , lastAccessTime long); " +
                "define stream SearchStream (apiName string); " +
                "define stream UpdateAPIMStream2 (apiContext string, apiName string , apiVersion string, " +
                "apiCreator string , apiCreatorTenantDomain string , applicationOwner string , lastAccessTime long); " +
                "define stream SearchStream (apiName string); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"apiName:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@PrimaryKey(\'apiName\',\'apiCreatorTenantDomain\',\'apiCreator\')\n" +
                "define table ApiLastAccessSummary (apiName string, apiCreatorTenantDomain string, " +
                "apiContext string, apiVersion string, applicationOwner string, lastAccessTime long," +
                " apiCreator string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from UpdateAPIMStream1#window.lengthBatch(" + eventCount + ")\n" +
                "select apiName, apiCreatorTenantDomain, apiContext, apiVersion, applicationOwner, lastAccessTime, " +
                "apiCreator\n" +
                "update or insert into ApiLastAccessSummary\n" +
                "set ApiLastAccessSummary.apiContext = apiContext, ApiLastAccessSummary.apiVersion = apiVersion, " +
                "ApiLastAccessSummary.applicationOwner = applicationOwner, " +
                "ApiLastAccessSummary.lastAccessTime = lastAccessTime\n" +
                "on ApiLastAccessSummary.apiName == apiName and " +
                "ApiLastAccessSummary.apiCreatorTenantDomain == apiCreatorTenantDomain and " +
                "ApiLastAccessSummary.apiCreator == apiCreator;" +
                "@info(name = 'query2') " +
                "from UpdateAPIMStream2#window.lengthBatch(" + eventCount + ")\n" +
                "select apiName, apiCreatorTenantDomain, apiContext, apiVersion, applicationOwner, lastAccessTime, " +
                "apiCreator\n" +
                "update or insert into ApiLastAccessSummary\n" +
                "set ApiLastAccessSummary.apiContext = apiContext, ApiLastAccessSummary.apiVersion = apiVersion, " +
                "ApiLastAccessSummary.applicationOwner = applicationOwner, " +
                "ApiLastAccessSummary.lastAccessTime = lastAccessTime\n" +
                "on ApiLastAccessSummary.apiName == apiName and " +
                "ApiLastAccessSummary.apiCreatorTenantDomain == apiCreatorTenantDomain and " +
                "ApiLastAccessSummary.apiCreator == apiCreator;" +
                "" +
                "@info(name = 'query3') " +
                "from SearchStream#window.length(1) join ApiLastAccessSummary on ApiLastAccessSummary.apiName == " +
                "SearchStream.apiName " +
                "select apiContext, SearchStream.apiName, apiVersion, apiCreator, " +
                "apiCreatorTenantDomain, applicationOwner, lastAccessTime " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler updateAPIMStream1 = siddhiAppRuntime.getInputHandler("UpdateAPIMStream1");
        InputHandler updateAPIMStream2 = siddhiAppRuntime.getInputHandler("UpdateAPIMStream2");
        InputHandler searchStream = siddhiAppRuntime.getInputHandler("SearchStream");
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        int inEventCount = actualEventCount.incrementAndGet();
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"order", "pizzashack", "1.0.0",
                                        "user1", "carbon.super", "app_owner", (long) (eventCount - 1)});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
                        }
                    }
                }
            }

        });
        siddhiAppRuntime.start();
        Event[] events = new Event[eventCount];
        for (int i = 0; i < eventCount; i++) {
            events[i] = new Event(System.currentTimeMillis(), new Object[]{"order", "pizzashack", "1.0.0", "user1",
                    "carbon.super", "app_owner", (long) i});
        }
        updateAPIMStream1.send(events);
        updateAPIMStream2.send(events);
        searchStream.send(new Object[]{"pizzashack"});
        waitTillVariableCountMatches(1, Duration.TWO_MINUTES);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest16")
    public void updateOrInsertTableTest17() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest17");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream UpdateStockStream (symbol string, price int, volume long); " +
                "define stream SearchStream (symbol string); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price int, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from UpdateStockStream#window.lengthBatch(10) " +
                "select * " +
                "update or insert into StockTable " +
                "   on StockTable.symbol == symbol ;" +
                "" +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55, 4L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 55, 2L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"APPLE", 55, 3L});
                                break;
                            case 4:
                                Assert.assertEquals(event.getData(), new Object[]{"GOOGLE", 55, 7L});
                                break;
                            case 5:
                                Assert.assertEquals(event.getData(), new Object[]{"Amazon", 55, 8L});
                                break;
                            case 6:
                                Assert.assertEquals(event.getData(), new Object[]{"INTEL", 55, 9L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 6);
                        }
                    }
                }
            }

        });
        siddhiAppRuntime.start();
        Event[] eventSet1 = new Event[10];
        eventSet1[0] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55, (long) 0});
        eventSet1[1] = new Event(System.currentTimeMillis(), new Object[]{"IBM", 55, (long) 1});
        eventSet1[2] = new Event(System.currentTimeMillis(), new Object[]{"IBM", 55, (long) 2});
        eventSet1[3] = new Event(System.currentTimeMillis(), new Object[]{"APPLE", 55, (long) 3});
        eventSet1[4] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55, (long) 4});
        eventSet1[5] = new Event(System.currentTimeMillis(), new Object[]{"GOOGLE", 55, (long) 5});
        eventSet1[6] = new Event(System.currentTimeMillis(), new Object[]{"GOOGLE", 55, (long) 6});
        eventSet1[7] = new Event(System.currentTimeMillis(), new Object[]{"GOOGLE", 55, (long) 7});
        eventSet1[8] = new Event(System.currentTimeMillis(), new Object[]{"Amazon", 55, (long) 8});
        eventSet1[9] = new Event(System.currentTimeMillis(), new Object[]{"INTEL", 55, (long) 9});

        updateStockStream.send(eventSet1);
        searchStream.send(new Object[]{"WSO2"});
        searchStream.send(new Object[]{"IBM"});
        searchStream.send(new Object[]{"APPLE"});
        searchStream.send(new Object[]{"GOOGLE"});
        searchStream.send(new Object[]{"Amazon"});
        searchStream.send(new Object[]{"INTEL"});
        waitTillVariableCountMatches(6, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void updateOrInsertTableTest18() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest18");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream UpdateStockStream (symbol string, price int, volume long); " +
                "define stream SearchStream (symbol string); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price int, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from UpdateStockStream#window.lengthBatch(10) " +
                "select * " +
                "update or insert into StockTable " +
                "   on StockTable.symbol == symbol ;" +
                "" +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55, 4L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 55, 3L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"APPLE", 55, 3L});
                                break;
                            case 4:
                                Assert.assertEquals(event.getData(), new Object[]{"GOOGLE", 55, 5L});
                                break;
                            case 5:
                                Assert.assertEquals(event.getData(), new Object[]{"Amazon", 55, 8L});
                                break;
                            case 6:
                                Assert.assertEquals(event.getData(), new Object[]{"INTEL", 55, 9L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 6);
                        }
                    }
                }
            }

        });
        siddhiAppRuntime.start();
        Event[] eventSet1 = new Event[5];
        eventSet1[0] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55, (long) 0});
        eventSet1[1] = new Event(System.currentTimeMillis(), new Object[]{"APPLE", 55, (long) 3});
        eventSet1[2] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55, (long) 4});
        eventSet1[3] = new Event(System.currentTimeMillis(), new Object[]{"GOOGLE", 55, (long) 5});
        eventSet1[4] = new Event(System.currentTimeMillis(), new Object[]{"INTEL", 55, (long) 9});
        Event[] eventSet2 = new Event[5];
        eventSet2[0] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55, (long) 4});
        eventSet2[1] = new Event(System.currentTimeMillis(), new Object[]{"IBM", 55, (long) 1});
        eventSet2[2] = new Event(System.currentTimeMillis(), new Object[]{"IBM", 55, (long) 3});
        eventSet2[3] = new Event(System.currentTimeMillis(), new Object[]{"Amazon", 55, (long) 8});
        eventSet2[4] = new Event(System.currentTimeMillis(), new Object[]{"INTEL", 55, (long) 9});

        updateStockStream.send(eventSet1);
        updateStockStream.send(eventSet2);
        searchStream.send(new Object[]{"WSO2"});
        searchStream.send(new Object[]{"IBM"});
        searchStream.send(new Object[]{"APPLE"});
        searchStream.send(new Object[]{"GOOGLE"});
        searchStream.send(new Object[]{"Amazon"});
        searchStream.send(new Object[]{"INTEL"});
        waitTillVariableCountMatches(6, Duration.ONE_MINUTE);
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void updateOrInsertTableTest19() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest19");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream UpdateStockStream (symbol string, price int, volume long); " +
                "define stream SearchStream (symbol string); " +
                "" +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                "define table StockTable (symbol string, price int, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "   on StockTable.symbol == symbol ;" +
                "" +
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
                eventArrived = true;
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 155, 200L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 155, 200L});
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
        events[2] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", 155, 200L});
        events[3] = new Event(System.currentTimeMillis(), new Object[]{"IBM", 155, 200L});
        updateStockStream.send(events);
        searchStream.send(new Object[]{"WSO2"});
        searchStream.send(new Object[]{"IBM"});
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }


    @Test
    public void updateOrInsertTableTest20() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest20");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream UpdateStockStream (symbol string, price int, volume long); " +
                "define stream SearchStream (symbol string); " +
                "" +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price int, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "   on StockTable.symbol == symbol ;" +
                "" +
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
                eventArrived = true;
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 155, 200L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 155, 200L});
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
        events[2] = new Event(System.currentTimeMillis(), new Object[]{"WSO2", 155, 200L});
        events[3] = new Event(System.currentTimeMillis(), new Object[]{"IBM", 155, 200L});
        updateStockStream.send(events);
        searchStream.send(new Object[]{"WSO2"});
        searchStream.send(new Object[]{"IBM"});
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();
    }

    private static void waitTillVariableCountMatches(long expected, Duration duration) {
        Awaitility.await().atMost(duration).until(() -> {
            return actualEventCount.get() == expected;
        });
    }
}
