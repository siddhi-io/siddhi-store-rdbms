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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

public class JoinRDBMSTableTestCaseIT {
    private static final Logger log = LogManager.getLogger(JoinRDBMSTableTestCaseIT.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        try {
            RDBMSTableTestUtils.initDatabaseTable(TABLE_NAME);
        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @BeforeClass
    public static void startTest() {
        log.info("== RDBMS Table JOIN tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== RDBMS Table JOIN tests completed ==");
    }

    @Test
    public void testTableJoinQuery1() throws InterruptedException, SQLException {
        log.info("testTableJoinQuery1 - OUT 2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
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
                "from CheckStockStream#window.length(1) join StockTable " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "IBM", 10L});
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTableJoinQuery2() throws InterruptedException, SQLException {
        log.info("testTableJoinQuery2 - OUT 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
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
                "from CheckStockStream#window.length(1) join StockTable " +
                " on CheckStockStream.symbol==StockTable.symbol " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
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

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(true, eventArrived, "Event arrived");
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testTableJoinQuery3() throws InterruptedException, SQLException {
        log.info("testTableJoinQuery3 - OUT 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol1 string); " +
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
                "from CheckStockStream#window.length(1) join StockTable " +
                " on symbol1== symbol " +
                "select symbol1 as checkSymbol, symbol as symbol, volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
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

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testTableJoinQuery4() throws InterruptedException, SQLException {
        log.info("testTableJoinQuery4 - OUT 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
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
                "from CheckStockStream#window.length(1) as c join StockTable as s " +
                " on c.symbol==s.symbol " +
                "select c.symbol as checkSymbol, s.symbol as symbol, s.volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
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

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTableJoinQuery5() throws InterruptedException, SQLException {
        log.info("testTableJoinQuery5 - OUT 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
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
                "from CheckStockStream#window.length(1) join StockTable as s " +
                " on CheckStockStream.symbol==s.symbol " +
                "select CheckStockStream.symbol as checkSymbol, s.symbol as symbol, s.volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
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

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testTableJoinQuery6() throws InterruptedException, SQLException {
        log.info("testTableJoinQuery6 - OUT 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
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
                "from CheckStockStream#window.length(1) join StockTable as s " +
                " on CheckStockStream.symbol==s.symbol " +
                "select CheckStockStream.symbol as checkSymbol, s.symbol as symbol, s.volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
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

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTableJoinQuery7() throws InterruptedException, SQLException {
        log.info("testTableJoinQuery7 - OUT 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
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
                "from CheckStockStream#window.length(1) join StockTable " +
                " on StockTable.symbol==CheckStockStream.symbol " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
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

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testTableJoinQuery8() throws InterruptedException, SQLException {
        log.info("testTableJoinQuery8 - OUT 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float); " +
                "define stream CheckStockStream (symbol string); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream#window.length(1) join StockTable " +
                " on StockTable.symbol==CheckStockStream.symbol " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2"});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
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

        stockStream.send(new Object[]{"WSO2", 55.6f});
        stockStream.send(new Object[]{"IBM", 75.6f});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTableJoinQuery9() throws InterruptedException, SQLException {
        log.info("testTableJoinQuery9 - OUT 2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "@connection(maxWait = '4000')" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream#window.length(1) join StockTable " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "IBM", 10L});
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTableLeftOuterJoinQueryWithContainCondition() throws InterruptedException {
        log.info("testTableLeftOuterJoinQueryWithContainCondition");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "@connection(maxWait = '4000')" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream#window.length(1) left outer join StockTable " +
                "on str:contains(StockTable.symbol, CheckStockStream.symbol) " +
                "and StockTable.volume == CheckStockStream.volume " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"CSC", null, null});
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"CSC", 20L});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTableRightOuterJoinQueryWithContainCondition() throws InterruptedException {
        log.info("testTableRightOuterJoinQueryWithContainCondition");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "@connection(maxWait = '4000')" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream#window.length(1) right outer join StockTable " +
                "on str:contains(StockTable.symbol, CheckStockStream.symbol) " +
                "and CheckStockStream.volume == StockTable.volume " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
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

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"CSC", 60L});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTableFullOuterJoinQueryWithContainCondition() throws InterruptedException {
        log.info("testTableFullOuterJoinQueryWithContainCondition");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "@connection(maxWait = '4000')" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream#window.length(1) full outer join StockTable " +
                "on str:contains(StockTable.symbol, CheckStockStream.symbol) " +
                "and CheckStockStream.volume == StockTable.volume " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"CSC", null, null});
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"CSC", 50L});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTableJoinQueryWithContainConditionAndConstantAsContainsValue() throws InterruptedException,
            SQLException {
        log.info("testTableJoinQueryWithContainConditionAndConstantAsContainsValue");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "@connection(maxWait = '4000')" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream#window.length(1) join StockTable " +
                "on str:contains(StockTable.symbol, 'WSO2') " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", "WSO2", 100L});
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2", 55.6f});
        checkStockStream.send(new Object[]{"IBM", 75.6f});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTableJoinQueryWithContainConditionAndFieldWithMultipleValues() throws InterruptedException,
            SQLException {
        log.info("testTableJoinQueryWithContainConditionAndFieldWithMultipleValues");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "@connection(maxWait = '4000')" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream#window.length(1) join StockTable " +
                "on str:contains(StockTable.symbol, CheckStockStream.symbol) " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"Colombo",
                                        "WSO2, Palm Grove, Colombo", 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"United States",
                                        "IBM, Armonk, New York, United States", 10L});
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2, Palm Grove, Colombo", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM, Armonk, New York, United States", 75.6f, 10L});
        checkStockStream.send(new Object[]{"Colombo", 55.6f});
        checkStockStream.send(new Object[]{"United States", 75.6f});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTableJoinQueryWithMultipleConditionsIncludingContainCondition1() throws InterruptedException {
        log.info("testTableJoinQueryWithMultipleConditionsIncludingContainCondition");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream#window.length(1) join StockTable " +
                "on (StockTable.volume >= CheckStockStream.volume AND str:contains(StockTable.symbol, 'WSO2')) " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO", "WSO2", 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", "WSO2", 100L});
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM2", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO", 50L});
        checkStockStream.send(new Object[]{"IBM", 1L});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testTableJoinQueryWithMultipleConditionsIncludingContainCondition2() throws InterruptedException {
        log.info("testTableJoinQueryWithMultipleConditionsIncludingContainCondition");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream#window.length(1) join StockTable " +
                "on (str:contains(StockTable.symbol, 'WSO2') AND StockTable.volume >= CheckStockStream.volume) " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO", "WSO2", 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", "WSO2", 100L});
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM2", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO", 50L});
        checkStockStream.send(new Object[]{"IBM", 1L});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testIsNullInHavingClause() throws InterruptedException {
        log.info("testIsNullInHavingClause");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream left outer join StockTable " +
                "on StockTable.symbol == CheckStockStream.symbol " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as nullableSymbol " +
                "having nullableSymbol is null " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", null});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO22", null});
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        checkStockStream.send(new Object[]{"WSO2", 50L});
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        checkStockStream.send(new Object[]{"WSO2", 50L});
        checkStockStream.send(new Object[]{"WSO22", 50L});
        Thread.sleep(1000);

        Assert.assertTrue(eventArrived, "Event arrived");
        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTableJoinQueryWithMultipleContainCondition() throws InterruptedException {
        log.info("testTableJoinQueryWithContainConditionAndFieldWithMultipleValues");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, symbol2 string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@connection(maxWait = '4000')" +
                "define table StockTable (symbol string, symbol2 string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream join StockTable " +
                "on (" +
                "   str:contains(StockTable.symbol, CheckStockStream.symbol) AND " +
                "   str:contains(StockTable.symbol2, CheckStockStream.symbol)" +
                ")" +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"Colombo",
                                        "WSO2, Palm Grove, Colombo", 100L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
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

        stockStream.send(new Object[]{"WSO2, Palm Grove, Colombo", "WSO2, Palm Grove, Colombo", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM, Armonk, New York, United States", "IBM, Armonk, New York", 75.6f, 10L});
        checkStockStream.send(new Object[]{"Colombo", 55.6f});
        checkStockStream.send(new Object[]{"United States", 75.6f});
        Thread.sleep(1000);

        Assert.assertTrue(eventArrived, "Event arrived");
        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

}
