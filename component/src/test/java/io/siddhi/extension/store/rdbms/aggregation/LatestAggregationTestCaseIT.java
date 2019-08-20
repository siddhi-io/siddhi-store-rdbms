/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.extension.store.rdbms.aggregation;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.driverClassName;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.password;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.url;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.user;

public class LatestAggregationTestCaseIT {

    private static final Logger LOG = Logger.getLogger(LatestAggregationTestCaseIT.class);
    private AtomicInteger inEventCount;
    private AtomicInteger removeEventCount;
    private boolean eventArrived;
    private List<Object[]> inEventsList;
    private List<Object[]> removeEventsList;

    @BeforeMethod
    public void init() {
        inEventCount = new AtomicInteger(0);
        removeEventCount = new AtomicInteger(0);
        eventArrived = false;
        inEventsList = new ArrayList<>();
        removeEventsList = new ArrayList<>();

        try {
            RDBMSTableTestUtils.initDatabaseTable("stockAggregation_SECONDS");
            RDBMSTableTestUtils.initDatabaseTable("stockAggregation_MINUTES");
        } catch (SQLException e) {
            LOG.error("Test case ignored due to " + e.getMessage());
        }

    }

    @Test
    public void latestTestCase1() throws InterruptedException {
        LOG.info("latestTestCase: testing latest incremental aggregator");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@purge(enable='false')" +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice, (price * quantity) as latestPrice " +
                "aggregate by timestamp every sec...min ;" +

                "define stream inputStream (symbol string); " +

                "@info(name = 'query1') " +
                "from inputStream as i join stockAggregation as s " +
                "within 1496200000000L, 1596535449000L " +
                "per \"seconds\" " +
                "select AGG_TIMESTAMP, s.symbol, s.latestPrice " +
                "order by AGG_TIMESTAMP " +
                "insert all events into outputStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    if (inEvents != null) {
                        EventPrinter.print(timestamp, inEvents, removeEvents);
                        for (Event event : inEvents) {
                            inEventsList.add(event.getData());
                            inEventCount.incrementAndGet();
                        }
                        eventArrived = true;
                    }
                    if (removeEvents != null) {
                        EventPrinter.print(timestamp, inEvents, removeEvents);
                        for (Event event : removeEvents) {
                            removeEventsList.add(event.getData());
                            removeEventCount.incrementAndGet();
                        }
                    }
                    eventArrived = true;
                }
            });
            InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
            InputHandler inputStreamInputHandler = siddhiAppRuntime.getInputHandler("inputStream");
            siddhiAppRuntime.start();

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO22", 75f, null, 40L, 10, 1496289950100L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO23", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO24", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:50 AM - Out of order event
            stockStreamInputHandler.send(new Object[]{"WSO23", 70f, null, 40L, 10, 1496289950090L});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 101f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM1", 102f, null, 200L, 100, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM1", 500f, null, 200L, 7, 1496289956000L});

            Thread.sleep(100);

            inputStreamInputHandler.send(new Object[]{"IBM"});
            Thread.sleep(100);

            List<Object[]> expected = Arrays.asList(
                    new Object[]{1496289950000L, "WSO22", 750f},
                    new Object[]{1496289952000L, "WSO24", 1600f},
                    new Object[]{1496289954000L, "IBM1", 10200f},
                    new Object[]{1496289956000L, "IBM1", 3500f}
            );
            SiddhiTestHelper.waitForEvents(100, 4, inEventCount, 10000);

            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 4, inEventCount.get());
            AssertJUnit.assertTrue("In events matched",
                                                SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));

            AssertJUnit.assertEquals("Number of remove events", 4, removeEventCount.get());
            AssertJUnit.assertTrue("Remove events matched",
                                                SiddhiTestHelper.isUnsortedEventsMatch(removeEventsList, expected));

             } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "latestTestCase1")
    public void latestTestCase2() throws InterruptedException {
        LOG.info("latestTestCase2: testing latest incremental aggregator - different group by");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@purge(enable='false')" +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice, (price * quantity) as latestPrice " +
                "aggregate by timestamp every sec...min ;" +

                "define stream inputStream (symbol string); " +

                "@info(name = 'query1') " +
                "from inputStream as i join stockAggregation as s " +
                "within 1496200000000L, 1596535449000L " +
                "per \"seconds\" " +
                "select s.symbol, s.latestPrice " +
                "group by s.symbol " +
                "order by AGG_TIMESTAMP " +
                "insert all events into outputStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    if (inEvents != null) {
                        EventPrinter.print(timestamp, inEvents, removeEvents);
                        for (Event event : inEvents) {
                            inEventsList.add(event.getData());
                            inEventCount.incrementAndGet();
                        }
                        eventArrived = true;
                    }
                    if (removeEvents != null) {
                        EventPrinter.print(timestamp, inEvents, removeEvents);
                        for (Event event : removeEvents) {
                            removeEventsList.add(event.getData());
                            removeEventCount.incrementAndGet();
                        }
                    }
                    eventArrived = true;
                }
            });
            InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
            InputHandler inputStreamInputHandler = siddhiAppRuntime.getInputHandler("inputStream");
            siddhiAppRuntime.start();

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO22", 75f, null, 40L, 10, 1496289950100L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO23", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO24", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:50 AM - Out of order event
            stockStreamInputHandler.send(new Object[]{"WSO23", 70f, null, 40L, 10, 1496289950090L});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 101f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM1", 102f, null, 200L, 100, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM1", 500f, null, 200L, 7, 1496289956000L});

            Thread.sleep(100);

            inputStreamInputHandler.send(new Object[]{"IBM"});
            Thread.sleep(100);

            List<Object[]> expected = Arrays.asList(
                    new Object[]{"WSO22", 750f},
                    new Object[]{"WSO24", 1600f},
                    new Object[]{"IBM1", 3500f}
            );
            SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 10000);

            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 3, inEventCount.get());
            AssertJUnit.assertTrue("In events matched",
                    SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));

            AssertJUnit.assertEquals("Number of remove events", 3, removeEventCount.get());
            AssertJUnit.assertTrue("Remove events matched",
                    SiddhiTestHelper.isUnsortedEventsMatch(removeEventsList, expected));

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "latestTestCase2")
    public void latestTestCase3() throws InterruptedException {
        LOG.info("latestTestCase3: testing latest incremental aggregator with another agg");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@purge(enable='false')" +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice, (price * quantity) as latestPrice " +
                "aggregate by timestamp every sec...min ;" +

                "define stream inputStream (symbol string); " +

                "@info(name = 'query1') " +
                "from inputStream as i join stockAggregation as s " +
                "within 1496200000000L, 1596535449000L " +
                "per \"seconds\" " +
                "select AGG_TIMESTAMP, s.symbol, s.latestPrice, s.avgPrice " +
                "order by AGG_TIMESTAMP " +
                "insert all events into outputStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    if (inEvents != null) {
                        EventPrinter.print(timestamp, inEvents, removeEvents);
                        for (Event event : inEvents) {
                            inEventsList.add(event.getData());
                            inEventCount.incrementAndGet();
                        }
                        eventArrived = true;
                    }
                    if (removeEvents != null) {
                        EventPrinter.print(timestamp, inEvents, removeEvents);
                        for (Event event : removeEvents) {
                            removeEventsList.add(event.getData());
                            removeEventCount.incrementAndGet();
                        }
                    }
                    eventArrived = true;
                }
            });
            InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
            InputHandler inputStreamInputHandler = siddhiAppRuntime.getInputHandler("inputStream");
            siddhiAppRuntime.start();

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO22", 75f, null, 40L, 10, 1496289950100L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO23", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO24", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:50 AM - Out of order event
            stockStreamInputHandler.send(new Object[]{"WSO23", 70f, null, 40L, 10, 1496289950090L});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 101f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM1", 102f, null, 200L, 100, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM1", 500f, null, 200L, 7, 1496289956000L});

            Thread.sleep(100);

            inputStreamInputHandler.send(new Object[]{"IBM"});
            Thread.sleep(100);

            List<Object[]> expected = Arrays.asList(
                    new Object[]{1496289950000L, "WSO22", 750f, 65.0},
                    new Object[]{1496289952000L, "WSO24", 1600f, 80.0},
                    new Object[]{1496289954000L, "IBM1", 10200f, 101.5},
                    new Object[]{1496289956000L, "IBM1", 3500f, 700.0}
            );
            SiddhiTestHelper.waitForEvents(100, 4, inEventCount, 10000);

            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 4, inEventCount.get());
            AssertJUnit.assertTrue("In events matched",
                    SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));

            AssertJUnit.assertEquals("Number of remove events", 4, removeEventCount.get());
            AssertJUnit.assertTrue("Remove events matched",
                    SiddhiTestHelper.isUnsortedEventsMatch(removeEventsList, expected));

        } finally {

            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "latestTestCase3", enabled = false)
    public void latestTestCase4() throws InterruptedException {
        LOG.info("latestTestCase4: testing latest incremental aggregator - different group by");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@purge(enable='false')" +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice, (price * quantity) as latestPrice " +
                "aggregate by timestamp every sec...min ;" +

                "define stream inputStream (symbol string); " +

                "@info(name = 'query1') " +
                "from inputStream as i join stockAggregation as s " +
                "within 1496200000000L, 1596535449000L " +
                "per \"seconds\" " +
                "select s.symbol, s.latestPrice, sum(s.avgPrice) as totalAvg " +
                "group by s.symbol " +
                "order by AGG_TIMESTAMP " +
                "insert all events into outputStream; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                    if (inEvents != null) {
                        EventPrinter.print(timestamp, inEvents, removeEvents);
                        for (Event event : inEvents) {
                            inEventsList.add(event.getData());
                            inEventCount.incrementAndGet();
                        }
                        eventArrived = true;
                    }
                }
            });
            InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
            InputHandler inputStreamInputHandler = siddhiAppRuntime.getInputHandler("inputStream");
            siddhiAppRuntime.start();

            // Thursday, June 1, 2017 4:05:50 AM
            stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
            stockStreamInputHandler.send(new Object[]{"WSO22", 75f, null, 40L, 10, 1496289950100L});

            // Thursday, June 1, 2017 4:05:52 AM
            stockStreamInputHandler.send(new Object[]{"WSO23", 60f, 44f, 200L, 56, 1496289952000L});
            stockStreamInputHandler.send(new Object[]{"WSO24", 100f, null, 200L, 16, 1496289952000L});

            // Thursday, June 1, 2017 4:05:50 AM - Out of order event
            stockStreamInputHandler.send(new Object[]{"WSO23", 70f, null, 40L, 10, 1496289950090L});

            // Thursday, June 1, 2017 4:05:54 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 101f, null, 200L, 26, 1496289954000L});
            stockStreamInputHandler.send(new Object[]{"IBM1", 102f, null, 200L, 100, 1496289954000L});

            // Thursday, June 1, 2017 4:05:56 AM
            stockStreamInputHandler.send(new Object[]{"IBM", 900f, null, 200L, 60, 1496289956000L});
            stockStreamInputHandler.send(new Object[]{"IBM1", 500f, null, 200L, 7, 1496289956000L});

            Thread.sleep(100);

            inputStreamInputHandler.send(new Object[]{"IBM"});
            Thread.sleep(100);

            List<Object[]> expected = Arrays.asList(
                    new Object[]{"WSO22", 750f, 65.0},
                    new Object[]{"WSO24", 1600f, 80.0},
                    new Object[]{"IBM1", 3500f, 801.5}
            );
            SiddhiTestHelper.waitForEvents(100, 3, inEventCount, 10000);

            AssertJUnit.assertTrue("Event arrived", eventArrived);
            AssertJUnit.assertEquals("Number of success events", 3, inEventCount.get());
            AssertJUnit.assertTrue("In events matched",
                    SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = {"latestTestCase3"})
    public void latestTestCase5() throws InterruptedException {
        LOG.info("latestTestCase5");
        SiddhiManager siddhiManager = new SiddhiManager();
        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int, timestamp long);";
        String query =
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", username=\"" + user + "\", password=\"" + password +
                "\",jdbc.driver.name=\"" + driverClassName + "\")\n" +
                "@purge(enable='false')" +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice, sum(price) as totalPrice, " +
                "(price * quantity) as lastTradeValue  " +
                "group by symbol " +
                "aggregate every sec...min ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
        siddhiAppRuntime.start();

        stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
        stockStreamInputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10, 1496289950000L});
        Thread.sleep(2000);

        stockStreamInputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56, 1496289952000L});
        stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16, 1496289952000L});
        Thread.sleep(2000);

        stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26, 1496289954000L});
        stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96, 1496289954000L});
        Thread.sleep(5000);

        LocalDate currentDate = LocalDate.now();
        String year = String.valueOf(currentDate.getYear());
        String month = String.valueOf(currentDate.getMonth().getValue());
        if (month.length() == 1) {
            month = "0".concat(month);
        }

        Event[] events = siddhiAppRuntime.query("from stockAggregation " +
                "within \"" + year + "-" + month + "-** **:**:** +05:30\" " +
                "per \"seconds\" " +
                "select symbol, sum(totalPrice) as totalPrice, lastTradeValue " +
                "group by symbol; ");

        EventPrinter.print(events);
        AssertJUnit.assertNotNull(events);
        AssertJUnit.assertEquals(2, events.length);

        List<Object[]> actual = new ArrayList<>();
        for (Event event : events) {
            actual.add(event.getData());
        }

        List<Object[]> expected = Arrays.asList(
                new Object[]{"IBM", 200.0, 9600f},
                new Object[]{"WSO2", 280.0, 1600f}
        );
        AssertJUnit.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(actual, expected));

        siddhiAppRuntime.shutdown();
    }

}
