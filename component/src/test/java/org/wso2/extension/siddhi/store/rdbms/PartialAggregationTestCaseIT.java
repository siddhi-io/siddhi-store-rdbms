package org.wso2.extension.siddhi.store.rdbms;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.config.InMemoryConfigManager;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableTestUtils;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableTestUtils.driverClassName;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableTestUtils.password;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableTestUtils.url;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableTestUtils.user;

// The following test cases are for Partial Aggregations in Siddhi Core. If any of the test cases fail, Siddhi Core
// should be analyzed in Partial Aggregation context.
public class PartialAggregationTestCaseIT {

    private static final Logger log = Logger.getLogger(PartialAggregationTestCaseIT.class);

    @BeforeMethod
    public void init() {

        try {
            RDBMSTableTestUtils.initDatabaseTable("stockAggregation_SECONDS");
            RDBMSTableTestUtils.initDatabaseTable("stockAggregation_MINUTES");
            RDBMSTableTestUtils.initDatabaseTable("stockAggregation_HOURS");
            RDBMSTableTestUtils.initDatabaseTable("stockAggregation_DAYS");
            RDBMSTableTestUtils.initDatabaseTable("stockAggregation_MONTHS");
            RDBMSTableTestUtils.initDatabaseTable("stockAggregation_YEARS");
        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @BeforeClass
    public static void startTest() {
        log.info("== Partial Aggregation tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Partial Aggregation tests completed ==");
    }

    @Test
    public void partialAggregationTest1() throws InterruptedException {
        log.info("partialAggregationTest1 - Checking minute granularity");
        SiddhiManager siddhiManager1 = new SiddhiManager();
        Map<String, String> systemConfigs1 = new HashMap<>();
        systemConfigs1.put("shardId", "node1");
        InMemoryConfigManager inMemoryConfigManager1 =
                new InMemoryConfigManager(null, null, systemConfigs1);
        siddhiManager1.setConfigManager(inMemoryConfigManager1);

        SiddhiManager siddhiManager2 = new SiddhiManager();
        Map<String, String> systemConfigs2 = new HashMap<>();
        systemConfigs2.put("shardId", "node2");
        InMemoryConfigManager inMemoryConfigManager2 =
                new InMemoryConfigManager(null, null, systemConfigs2);
        siddhiManager2.setConfigManager(inMemoryConfigManager2);

        String streams = "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long,"
                + " quantity int, timestamp long); ";
        String query = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" + "@partitionbyid \n" +
                "@purge(enable='false')\n" +
                "define aggregation stockAggregation\n" +
                "from stockStream \n" +
                "select symbol,sum(price) as totalPrice,avg(price) as avgPrice \n" +
                "aggregate by timestamp every sec...year; ";

        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(streams + query);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(streams + query);

        InputHandler stockStreamInputHandler1 = siddhiAppRuntime1.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler2 = siddhiAppRuntime2.getInputHandler("stockStream");

        siddhiAppRuntime1.start();
        siddhiAppRuntime2.start();

        // Thursday, June 1, 2017 4:05:50 AM
        stockStreamInputHandler1.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 70f, 60f, 90L, 6, 1496289950000L});

        // Thursday, June 1, 2017 4:05:51 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 100f, null, 200L, 16, 1496289951011L});
        stockStreamInputHandler2.send(new Object[]{"IBM", 110f, null, 200L, 16, 1496289951011L});

        // Thursday, June 1, 2017 4:06:12 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 300f, null, 200L, 9, 1496289972000L});
        stockStreamInputHandler2.send(new Object[]{"IBM", 500f, null, 200L, 9, 1496289972000L});

        // Thursday, June 1, 2017 4:06:22 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 1000f, null, 200L, 60, 1496289982000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 400f, null, 200L, 7, 1496289982005L});

        // Thursday, June 1, 2017 4:20:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 150f, null, 200L, 26, 1496290801000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 50f, null, 200L, 96, 1496290801001L});

        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime2.query("from stockAggregation within 0L, 1543664151000L per " +
                "'minutes' select AGG_TIMESTAMP, symbol, totalPrice, avgPrice");

        List<Object[]> eventsList = new ArrayList<>();
        for (Event event : events) {
            eventsList.add(event.getData());
        }

        List<Object[]> expected = Arrays.asList(
                new Object[]{1496290800000L, "WSO2", 200.0, 100.0},
                new Object[]{1496289900000L, "IBM", 330.0, 82.5},
                new Object[]{1496289960000L, "WSO2", 2200.0, 550.0}
        );

        Assert.assertEquals(events.length, 3, "Number of success events");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(eventsList, expected), "Data Matched");
        siddhiAppRuntime1.shutdown();
        siddhiAppRuntime2.shutdown();
    }

    @Test(dependsOnMethods = "partialAggregationTest1")
    public void partialAggregationTest2() throws InterruptedException {
        log.info("partialAggregationTest2 - Checking seconds granularity");
        SiddhiManager siddhiManager1 = new SiddhiManager();
        Map<String, String> systemConfigs1 = new HashMap<>();
        systemConfigs1.put("shardId", "node1");
        InMemoryConfigManager inMemoryConfigManager1 =
                new InMemoryConfigManager(null, null, systemConfigs1);
        siddhiManager1.setConfigManager(inMemoryConfigManager1);

        SiddhiManager siddhiManager2 = new SiddhiManager();
        Map<String, String> systemConfigs2 = new HashMap<>();
        systemConfigs2.put("shardId", "node2");
        InMemoryConfigManager inMemoryConfigManager2 =
                new InMemoryConfigManager(null, null, systemConfigs2);
        siddhiManager2.setConfigManager(inMemoryConfigManager2);

        SiddhiManager siddhiManager3 = new SiddhiManager();
        Map<String, String> systemConfigs3 = new HashMap<>();
        systemConfigs3.put("shardId", "node3");
        InMemoryConfigManager inMemoryConfigManager3 =
                new InMemoryConfigManager(null, null, systemConfigs3);
        siddhiManager3.setConfigManager(inMemoryConfigManager3);

        String streams = "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long,"
                + " quantity int, timestamp long); ";
        String query = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:2" +
                "" +
                "" +
                "" +
                ", maxLifetime:60000\")\n" + "@partitionbyid \n" +
                "@purge(enable='false')\n" +
                "define aggregation stockAggregation\n" +
                "from stockStream \n" +
                "select symbol,sum(price) as totalPrice,avg(price) as avgPrice \n" +
                "aggregate by timestamp every sec...year; ";

        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(streams + query);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(streams + query);
        SiddhiAppRuntime siddhiAppRuntime3 = siddhiManager3.createSiddhiAppRuntime(streams + query);

        InputHandler stockStreamInputHandler1 = siddhiAppRuntime1.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler2 = siddhiAppRuntime2.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler3 = siddhiAppRuntime3.getInputHandler("stockStream");

        siddhiAppRuntime1.start();
        siddhiAppRuntime2.start();
        siddhiAppRuntime3.start();

        // Thursday, June 1, 2017 4:05:50 AM
        stockStreamInputHandler1.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 70f, 60f, 90L, 6, 1496289950000L});
        stockStreamInputHandler3.send(new Object[]{"WSO2", 10f, 65f, 30L, 3, 1496289950010L});

        // Thursday, June 1, 2017 4:05:51 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 100f, null, 200L, 16, 1496289951011L});
        stockStreamInputHandler1.send(new Object[]{"IBM", 110f, null, 200L, 16, 1496289951050L});

        // Thursday, June 1, 2017 4:06:12 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 300f, null, 200L, 9, 1496289972000L});
        stockStreamInputHandler2.send(new Object[]{"IBM", 500f, null, 200L, 9, 1496289972000L});

        // Thursday, June 1, 2017 4:06:22 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 1000f, null, 200L, 60, 1496289982020L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 400f, null, 200L, 7, 1496289982030L});
        stockStreamInputHandler3.send(new Object[]{"WSO2", 35f, 65f, 30L, 3, 1496289982040L});
        stockStreamInputHandler3.send(new Object[]{"WSO2", 5f, 65f, 30L, 3, 1496289982000L});


        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime1.query("from stockAggregation within 0L, 1543664151000L per " +
                "'seconds' select AGG_TIMESTAMP, totalPrice, avgPrice");

        List<Object[]> eventsList = new ArrayList<>();
        for (Event event : events) {
            eventsList.add(event.getData());
        }

        List<Object[]> expected = Arrays.asList(
                new Object[]{1496289972000L, 800.0, 400.0},
                new Object[]{1496289950000L, 130.0, 43.333333333333336},
                new Object[]{1496289982000L, 1440.0, 360.0},
                new Object[]{1496289951000L, 210.0, 105.0}
        );

        Assert.assertEquals(events.length, 4, "Number of success events");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(eventsList, expected), "Data Matched");
        siddhiAppRuntime1.shutdown();
        siddhiAppRuntime2.shutdown();
        siddhiAppRuntime3.shutdown();
    }

    @Test(dependsOnMethods = "partialAggregationTest2")
    public void partialAggregationTest3() throws InterruptedException {
        log.info("partialAggregationTest3 - Checking hours granularity");
        SiddhiManager siddhiManager1 = new SiddhiManager();
        Map<String, String> systemConfigs1 = new HashMap<>();
        systemConfigs1.put("shardId", "node1");
        InMemoryConfigManager inMemoryConfigManager1 =
                new InMemoryConfigManager(null, null, systemConfigs1);
        siddhiManager1.setConfigManager(inMemoryConfigManager1);

        SiddhiManager siddhiManager2 = new SiddhiManager();
        Map<String, String> systemConfigs2 = new HashMap<>();
        systemConfigs2.put("shardId", "node2");
        InMemoryConfigManager inMemoryConfigManager2 =
                new InMemoryConfigManager(null, null, systemConfigs2);
        siddhiManager2.setConfigManager(inMemoryConfigManager2);

        String streams = "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long,"
                + " quantity int, timestamp long); ";
        String query = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" + "@partitionbyid \n" +
                "@purge(enable='false')\n" +
                "define aggregation stockAggregation\n" +
                "from stockStream \n" +
                "select symbol,sum(price) as totalPrice,avg(price) as avgPrice \n" +
                "aggregate by timestamp every sec...year; ";

        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(streams + query);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(streams + query);

        InputHandler stockStreamInputHandler1 = siddhiAppRuntime1.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler2 = siddhiAppRuntime2.getInputHandler("stockStream");

        siddhiAppRuntime1.start();
        siddhiAppRuntime2.start();

        // Thursday, June 1, 2017 4:05:50 AM
        stockStreamInputHandler1.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 70f, 60f, 90L, 6, 1496289950000L});

        // Thursday, June 1, 2017 4:05:51 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 100f, null, 200L, 16, 1496289951011L});
        stockStreamInputHandler2.send(new Object[]{"IBM", 110f, null, 200L, 16, 1496289951011L});

        // Thursday, June 1, 2017 4:06:12 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 300f, null, 200L, 9, 1496289972000L});
        stockStreamInputHandler2.send(new Object[]{"IBM", 500f, null, 200L, 9, 1496289972000L});

        // Thursday, June 1, 2017 4:06:22 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 1000f, null, 200L, 60, 1496289982000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 400f, null, 200L, 7, 1496289982000L});

        // Thursday, June 1, 2017 5:20:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 150f, null, 200L, 26, 1496294401000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 50f, null, 200L, 96, 1496294401000L});

        // Friday, June 2, 2017 3:50:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 250f, null, 200L, 26, 1496375401000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 10f, null, 200L, 96, 1496375401500L});

        // Friday, June 2, 2017 23:50:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 155f, null, 200L, 26, 1496447401000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 5f, null, 200L, 96, 1496447401000L});

        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime2.query("from stockAggregation within 0L, 1543664151000L per " +
                "'hours' select AGG_TIMESTAMP, totalPrice, avgPrice");

        List<Object[]> eventsList = new ArrayList<>();
        for (Event event : events) {
            eventsList.add(event.getData());
        }

        List<Object[]> expected = Arrays.asList(
                new Object[]{1496444400000L, 160.0, 80.0},
                new Object[]{1496372400000L, 260.0, 130.0},
                new Object[]{1496289600000L, 2530.0, 316.25},
                new Object[]{1496293200000L, 200.0, 100.0}
        );

        Assert.assertEquals(events.length, 4, "Number of success events");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(eventsList, expected), "Data Matched");
        siddhiAppRuntime1.shutdown();
        siddhiAppRuntime2.shutdown();
    }

    @Test(dependsOnMethods = "partialAggregationTest3")
    public void partialAggregationTest4() throws InterruptedException {
        log.info("partialAggregationTest4 - Checking days granularity");
        SiddhiManager siddhiManager1 = new SiddhiManager();
        Map<String, String> systemConfigs1 = new HashMap<>();
        systemConfigs1.put("shardId", "node1");
        InMemoryConfigManager inMemoryConfigManager1 =
                new InMemoryConfigManager(null, null, systemConfigs1);
        siddhiManager1.setConfigManager(inMemoryConfigManager1);

        SiddhiManager siddhiManager2 = new SiddhiManager();
        Map<String, String> systemConfigs2 = new HashMap<>();
        systemConfigs2.put("shardId", "node2");
        InMemoryConfigManager inMemoryConfigManager2 =
                new InMemoryConfigManager(null, null, systemConfigs2);
        siddhiManager2.setConfigManager(inMemoryConfigManager2);

        String streams = "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long,"
                + " quantity int, timestamp long); ";
        String query = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" + "@partitionbyid \n" +
                "@purge(enable='false')\n" +
                "define aggregation stockAggregation\n" +
                "from stockStream \n" +
                "select symbol,sum(price) as totalPrice,avg(price) as avgPrice, " +
                "(price*quantity) as lastTradeValue \n" +
                "aggregate by timestamp every sec...year; ";

        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(streams + query);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(streams + query);

        InputHandler stockStreamInputHandler1 = siddhiAppRuntime1.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler2 = siddhiAppRuntime2.getInputHandler("stockStream");

        siddhiAppRuntime1.start();
        siddhiAppRuntime2.start();

        // Thursday, June 1, 2017 4:05:50 AM
        stockStreamInputHandler1.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 70f, 60f, 90L, 6, 1496289950000L});

        // Thursday, June 1, 2017 4:05:51 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 100f, null, 200L, 16, 1496289951011L});
        stockStreamInputHandler2.send(new Object[]{"IBM", 110f, null, 200L, 16, 1496289951011L});

        // Thursday, June 1, 2017 5:20:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 150f, null, 200L, 26, 1496294401000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 50f, null, 200L, 96, 1496294401003L});

        // Friday, June 2, 2017 3:50:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 250f, null, 200L, 26, 1496375401000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 10f, null, 200L, 96, 1496375401500L});

        // Friday, June 2, 2017 23:50:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 155f, null, 200L, 26, 1496447401000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 5f, null, 200L, 96, 1496447401005L});

        // July 19, 2017 04:20:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 7f, null, 200L, 26, 1500438001000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 3f, null, 200L, 96, 1500438001500L});


        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime2.query("from stockAggregation within 0L, 1543664151000L per " +
                "'days' select AGG_TIMESTAMP, totalPrice, avgPrice, lastTradeValue");

        List<Object[]> eventsList = new ArrayList<>();
        for (Event event : events) {
            eventsList.add(event.getData());
        }

        List<Object[]> expected = Arrays.asList(
                new Object[]{1496361600000L, 420.0, 105.0, 480.0f},
                new Object[]{1496275200000L, 530.0, 88.33333333333333, 4800.0f},
                new Object[]{1500422400000L, 10.0, 5.0, 288.0f}
        );

        Assert.assertEquals(events.length, 3, "Number of success events");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(eventsList, expected), "Data Matched");
        siddhiAppRuntime1.shutdown();
        siddhiAppRuntime2.shutdown();
    }

    @Test(dependsOnMethods = "partialAggregationTest4")
    public void partialAggregationTest5() throws InterruptedException {
        log.info("partialAggregationTest5 - Checking months granularity");
        SiddhiManager siddhiManager1 = new SiddhiManager();
        Map<String, String> systemConfigs1 = new HashMap<>();
        systemConfigs1.put("shardId", "node1");
        InMemoryConfigManager inMemoryConfigManager1 =
                new InMemoryConfigManager(null, null, systemConfigs1);
        siddhiManager1.setConfigManager(inMemoryConfigManager1);

        SiddhiManager siddhiManager2 = new SiddhiManager();
        Map<String, String> systemConfigs2 = new HashMap<>();
        systemConfigs2.put("shardId", "node2");
        InMemoryConfigManager inMemoryConfigManager2 =
                new InMemoryConfigManager(null, null, systemConfigs2);
        siddhiManager2.setConfigManager(inMemoryConfigManager2);

        String streams = "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long,"
                + " quantity int, timestamp long); ";
        String query = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" + "@partitionbyid \n" +
                "@purge(enable='false')\n" +
                "define aggregation stockAggregation\n" +
                "from stockStream \n" +
                "select symbol,sum(price) as totalPrice,avg(price) as avgPrice, " +
                "(price*quantity) as lastTradeValue \n" +
                "aggregate by timestamp every sec...year; ";

        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(streams + query);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(streams + query);

        InputHandler stockStreamInputHandler1 = siddhiAppRuntime1.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler2 = siddhiAppRuntime2.getInputHandler("stockStream");

        siddhiAppRuntime1.start();
        siddhiAppRuntime2.start();

        // Thursday, June 1, 2017 4:05:50 AM
        stockStreamInputHandler1.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 70f, 60f, 90L, 6, 1496289950000L});

        // Thursday, June 1, 2017 4:05:51 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 100f, null, 200L, 16, 1496289951011L});
        stockStreamInputHandler2.send(new Object[]{"IBM", 110f, null, 200L, 16, 1496289951011L});

        // Thursday, June 1, 2017 5:20:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 150f, null, 200L, 26, 1496294401000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 50f, null, 200L, 96, 1496294401003L});

        // Friday, June 2, 2017 3:50:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 250f, null, 200L, 26, 1496375401000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 10f, null, 200L, 96, 1496375401500L});

        // Friday, June 2, 2017 23:50:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 155f, null, 200L, 26, 1496447401000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 5f, null, 200L, 96, 1496447401005L});

        // July 19, 2017 04:20:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 7f, null, 200L, 26, 1500438001000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 3f, null, 200L, 96, 1500438001500L});

        // August 29, 2017 04:20:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 12f, null, 200L, 26, 1503980401000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 8f, null, 200L, 96, 1503980401500L});


        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime2.query("from stockAggregation within 0L, 1543664151000L per " +
                "'months' select AGG_TIMESTAMP, totalPrice, avgPrice, lastTradeValue");

        List<Object[]> eventsList = new ArrayList<>();
        for (Event event : events) {
            eventsList.add(event.getData());
        }

        List<Object[]> expected = Arrays.asList(
                new Object[]{1501545600000L, 20.0, 10.0, 768.0f},
                new Object[]{1496275200000L, 950.0, 95.0, 480.0f},
                new Object[]{1498867200000L, 10.0, 5.0, 288.0f}
        );

        Assert.assertEquals(events.length, 3, "Number of success events");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(eventsList, expected), "Data Matched");
        siddhiAppRuntime1.shutdown();
        siddhiAppRuntime2.shutdown();
    }

    @Test(dependsOnMethods = "partialAggregationTest5")
    public void partialAggregationTest6() throws InterruptedException {
        log.info("partialAggregationTest6 - Checking years granularity");
        SiddhiManager siddhiManager1 = new SiddhiManager();
        Map<String, String> systemConfigs1 = new HashMap<>();
        systemConfigs1.put("shardId", "node1");
        InMemoryConfigManager inMemoryConfigManager1 =
                new InMemoryConfigManager(null, null, systemConfigs1);
        siddhiManager1.setConfigManager(inMemoryConfigManager1);

        SiddhiManager siddhiManager2 = new SiddhiManager();
        Map<String, String> systemConfigs2 = new HashMap<>();
        systemConfigs2.put("shardId", "node2");
        InMemoryConfigManager inMemoryConfigManager2 =
                new InMemoryConfigManager(null, null, systemConfigs2);
        siddhiManager2.setConfigManager(inMemoryConfigManager2);

        SiddhiManager siddhiManager3 = new SiddhiManager();
        Map<String, String> systemConfigs3 = new HashMap<>();
        systemConfigs3.put("shardId", "node3");
        InMemoryConfigManager inMemoryConfigManager3 =
                new InMemoryConfigManager(null, null, systemConfigs3);
        siddhiManager3.setConfigManager(inMemoryConfigManager3);

        String streams = "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long,"
                + " quantity int, timestamp long); ";
        String query = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" + "@partitionbyid \n" +
                "@purge(enable='false')\n" +
                "define aggregation stockAggregation\n" +
                "from stockStream \n" +
                "select symbol,sum(price) as totalPrice,avg(price) as avgPrice, " +
                "(price*quantity) as lastTradeValue \n" +
                "aggregate by timestamp every sec...year; ";

        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(streams + query);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(streams + query);
        SiddhiAppRuntime siddhiAppRuntime3 = siddhiManager3.createSiddhiAppRuntime(streams + query);

        InputHandler stockStreamInputHandler1 = siddhiAppRuntime1.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler2 = siddhiAppRuntime2.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler3 = siddhiAppRuntime3.getInputHandler("stockStream");

        siddhiAppRuntime1.start();
        siddhiAppRuntime2.start();
        siddhiAppRuntime3.start();

        // Thursday, June 1, 2017 4:05:50 AM
        stockStreamInputHandler1.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 70f, 60f, 90L, 6, 1496289950000L});

        // Friday, June 2, 2017 23:50:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 155f, null, 200L, 26, 1496447401000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 5f, null, 200L, 96, 1496447401005L});

        // July 19, 2017 04:20:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 7f, null, 200L, 26, 1500438001000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 3f, null, 200L, 96, 1500438001500L});

        // August 29, 2017 04:20:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 12f, null, 200L, 26, 1503980401000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 8f, null, 200L, 96, 1503980401500L});

        // January 02, 2018 00:00:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 45f, null, 200L, 16, 1514851260000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 10f, null, 200L, 86, 1514851260500L});

        // May 02, 2018 08:07:40 PM
        stockStreamInputHandler3.send(new Object[]{"IBM", 100f, null, 200L, 100, 1517861260000L});

        // April 04, 2019 04:20:00 PM
        stockStreamInputHandler3.send(new Object[]{"IBM", 100f, null, 200L, 100, 1555734000000L});

        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime2.query("from stockAggregation within 0L, 1655734000000L per " +
                "'years' select AGG_TIMESTAMP, totalPrice, avgPrice, lastTradeValue");

        List<Object[]> eventsList = new ArrayList<>();
        for (Event event : events) {
            eventsList.add(event.getData());
        }

        List<Object[]> expected = Arrays.asList(
                new Object[]{1546300800000L, 100.0, 100.0, 10000.0f},
                new Object[]{1514764800000L, 155.0, 51.666666666666664, 10000.0f},
                new Object[]{1483228800000L, 310.0, 38.75, 768.0f}
        );

        Assert.assertEquals(events.length, 3, "Number of success events");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(eventsList, expected), "Data Matched");
        siddhiAppRuntime1.shutdown();
        siddhiAppRuntime2.shutdown();
        siddhiAppRuntime3.shutdown();
    }

    @Test(dependsOnMethods = "partialAggregationTest6")
    public void partialAggregationTest7() throws InterruptedException {
        log.info("partialAggregationTest7 - Checking system timestamp");
        SiddhiManager siddhiManager1 = new SiddhiManager();
        Map<String, String> systemConfigs1 = new HashMap<>();
        systemConfigs1.put("shardId", "node1");
        InMemoryConfigManager inMemoryConfigManager1 =
                new InMemoryConfigManager(null, null, systemConfigs1);
        siddhiManager1.setConfigManager(inMemoryConfigManager1);

        SiddhiManager siddhiManager2 = new SiddhiManager();
        Map<String, String> systemConfigs2 = new HashMap<>();
        systemConfigs2.put("shardId", "node2");
        InMemoryConfigManager inMemoryConfigManager2 =
                new InMemoryConfigManager(null, null, systemConfigs2);
        siddhiManager2.setConfigManager(inMemoryConfigManager2);

        SiddhiManager siddhiManager3 = new SiddhiManager();
        Map<String, String> systemConfigs3 = new HashMap<>();
        systemConfigs3.put("shardId", "node3");
        InMemoryConfigManager inMemoryConfigManager3 =
                new InMemoryConfigManager(null, null, systemConfigs3);
        siddhiManager3.setConfigManager(inMemoryConfigManager3);

        String streams = "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long,"
                + " quantity int); ";
        String query = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" + "@partitionbyid \n" +
                "@purge(enable='false')\n" +
                "define aggregation stockAggregation\n" +
                "from stockStream \n" +
                "select symbol,sum(price) as totalPrice,avg(price) as avgPrice, " +
                "(price*quantity) as lastTradeValue \n" +
                "aggregate every sec...year; ";

        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(streams + query);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(streams + query);
        SiddhiAppRuntime siddhiAppRuntime3 = siddhiManager3.createSiddhiAppRuntime(streams + query);

        InputHandler stockStreamInputHandler1 = siddhiAppRuntime1.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler2 = siddhiAppRuntime2.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler3 = siddhiAppRuntime3.getInputHandler("stockStream");

        siddhiAppRuntime1.start();
        siddhiAppRuntime2.start();
        siddhiAppRuntime3.start();

        stockStreamInputHandler1.send(new Object[]{"WSO2", 50f, 60f, 90L, 6});
        stockStreamInputHandler1.send(new Object[]{"WSO2", 70f, 60f, 90L, 6});

        stockStreamInputHandler2.send(new Object[]{"IBM", 155f, null, 200L, 26});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 5f, null, 200L, 96});

        stockStreamInputHandler1.send(new Object[]{"IBM", 7f, null, 200L, 26});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 3f, null, 200L, 96});
        stockStreamInputHandler3.send(new Object[]{"IBM", 100f, null, 200L, 100});
        Thread.sleep(100);

        stockStreamInputHandler1.send(new Object[]{"WSO2", 50f, 60f, 90L, 6});
        stockStreamInputHandler3.send(new Object[]{"IBM", 100f, null, 200L, 120});
        Thread.sleep(1000);

        stockStreamInputHandler3.send(new Object[]{"IBM", 100f, null, 200L, 180});

        Thread.sleep(1000);

        Event[] events = siddhiAppRuntime1.query("from stockAggregation within 0L, " +
                (System.currentTimeMillis() + 1000000) + "L per 'years' select avgPrice, totalPrice as sumPrice, " +
                "lastTradeValue");

        List<Object[]> eventsList = new ArrayList<>();
        for (Event event : events) {
            eventsList.add(event.getData());
        }

        List<Object[]> expected = new ArrayList<>();
        expected.add(new Object[]{64.0, 640.0, 18000.0f});

        Assert.assertEquals(events.length, 1, "Number of success events");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(eventsList, expected), "Data Matched");
        siddhiAppRuntime1.shutdown();
        siddhiAppRuntime2.shutdown();
        siddhiAppRuntime3.shutdown();
    }

    @Test(dependsOnMethods = "partialAggregationTest7")
    public void partialAggregationTest8() throws InterruptedException {
        log.info("partialAggregationTest8 - Checking system timestamp using join");
        SiddhiManager siddhiManager1 = new SiddhiManager();
        Map<String, String> systemConfigs1 = new HashMap<>();
        systemConfigs1.put("shardId", "node1");
        InMemoryConfigManager inMemoryConfigManager1 =
                new InMemoryConfigManager(null, null, systemConfigs1);
        siddhiManager1.setConfigManager(inMemoryConfigManager1);

        SiddhiManager siddhiManager2 = new SiddhiManager();
        Map<String, String> systemConfigs2 = new HashMap<>();
        systemConfigs2.put("shardId", "node2");
        InMemoryConfigManager inMemoryConfigManager2 =
                new InMemoryConfigManager(null, null, systemConfigs2);
        siddhiManager2.setConfigManager(inMemoryConfigManager2);

        SiddhiManager siddhiManager3 = new SiddhiManager();
        Map<String, String> systemConfigs3 = new HashMap<>();
        systemConfigs3.put("shardId", "node3");
        InMemoryConfigManager inMemoryConfigManager3 =
                new InMemoryConfigManager(null, null, systemConfigs3);
        siddhiManager3.setConfigManager(inMemoryConfigManager3);

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int);";
        String query = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" +
                "@partitionbyid " +
                "@purge(enable='false')\n" +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select avg(price) as avgPrice, sum(price) as totalPrice, (price * quantity) " +
                "as lastTradeValue  " +
                "aggregate every sec...year; " +

                "define stream inputStream (symbol string, value int, startTime long, " +
                "endTime long, perValue string); " +

                "@info(name = 'query1') " +
                "from inputStream join stockAggregation " +
                "within startTime, endTime " +
                "per perValue " +
                "select avgPrice, totalPrice as sumPrice,lastTradeValue " +
                "insert all events into outputStream; ";

        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(stockStream + query);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(stockStream + query);
        SiddhiAppRuntime siddhiAppRuntime3 = siddhiManager3.createSiddhiAppRuntime(stockStream + query);
        List<Object[]> inEventsList = new ArrayList<>();
        AtomicInteger inEventCount = new AtomicInteger(0);
        siddhiAppRuntime1.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                }
            }
        });

        InputHandler stockStreamInputHandler1 = siddhiAppRuntime1.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler2 = siddhiAppRuntime2.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler3 = siddhiAppRuntime3.getInputHandler("stockStream");
        InputHandler inputStreamInputHandler = siddhiAppRuntime1.getInputHandler("inputStream");

        siddhiAppRuntime1.start();
        siddhiAppRuntime2.start();
        siddhiAppRuntime3.start();

        stockStreamInputHandler1.send(new Object[]{"WSO2", 50f, 60f, 90L, 6});
        stockStreamInputHandler1.send(new Object[]{"WSO2", 70f, 60f, 90L, 6});

        stockStreamInputHandler2.send(new Object[]{"IBM", 155f, null, 200L, 26});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 5f, null, 200L, 96});

        stockStreamInputHandler1.send(new Object[]{"IBM", 7f, null, 200L, 26});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 3f, null, 200L, 96});
        stockStreamInputHandler3.send(new Object[]{"IBM", 100f, null, 200L, 100});

        stockStreamInputHandler1.send(new Object[]{"WSO2", 50f, 60f, 90L, 6});
        stockStreamInputHandler3.send(new Object[]{"IBM", 100f, null, 200L, 120});
        Thread.sleep(1000);

        stockStreamInputHandler3.send(new Object[]{"IBM", 100f, null, 200L, 180});

        Thread.sleep(1000);
        LocalDate currentDate = LocalDate.now();
        String year = String.valueOf(currentDate.getYear() + 2);
        inputStreamInputHandler.send(new Object[]{"IBM", 1, "2017-06-01 09:35:52 +05:30",
                year + "-06-01 09:35:52 +05:30", "years"});
        Thread.sleep(1000);

        List<Object[]> expected = new ArrayList<>();
        expected.add(new Object[]{64.0, 640.0, 18000.0f});

        Assert.assertEquals(inEventCount.get(), 1, "Number of success events");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected), "Data Matched");
        siddhiAppRuntime1.shutdown();
        siddhiAppRuntime2.shutdown();
        siddhiAppRuntime3.shutdown();
    }

    @Test(dependsOnMethods = "partialAggregationTest8")
    public void partialAggregationTest9() throws InterruptedException {
        log.info("partialAggregationTest9 - Checking aggregation group by");
        SiddhiManager siddhiManager1 = new SiddhiManager();
        Map<String, String> systemConfigs1 = new HashMap<>();
        systemConfigs1.put("shardId", "node1");
        InMemoryConfigManager inMemoryConfigManager1 =
                new InMemoryConfigManager(null, null, systemConfigs1);
        siddhiManager1.setConfigManager(inMemoryConfigManager1);

        SiddhiManager siddhiManager2 = new SiddhiManager();
        Map<String, String> systemConfigs2 = new HashMap<>();
        systemConfigs2.put("shardId", "node2");
        InMemoryConfigManager inMemoryConfigManager2 =
                new InMemoryConfigManager(null, null, systemConfigs2);
        siddhiManager2.setConfigManager(inMemoryConfigManager2);

        SiddhiManager siddhiManager3 = new SiddhiManager();
        Map<String, String> systemConfigs3 = new HashMap<>();
        systemConfigs3.put("shardId", "node3");
        InMemoryConfigManager inMemoryConfigManager3 =
                new InMemoryConfigManager(null, null, systemConfigs3);
        siddhiManager3.setConfigManager(inMemoryConfigManager3);

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long, " +
                        "quantity int, timestamp long); ";
        String query = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" +
                "@partitionbyid " +
                "@purge(enable='false')\n" +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice, sum(price) as totalPrice, count() as count " +
                "group by symbol " +
                "aggregate by timestamp every sec...year; " +

                "define stream inputStream (value int, startTime long, " +
                "endTime long, perValue string); " +

                "@info(name = 'query1') " +
                "from inputStream join stockAggregation " +
                "within startTime, endTime " +
                "per perValue " +
                "select AGG_TIMESTAMP, symbol, totalPrice, avgPrice, count " +
                "insert all events into outputStream; ";

        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(stockStream + query);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(stockStream + query);
        SiddhiAppRuntime siddhiAppRuntime3 = siddhiManager3.createSiddhiAppRuntime(stockStream + query);

        List<Object[]> inEventsList = new ArrayList<>();
        AtomicInteger inEventCount = new AtomicInteger(0);
        siddhiAppRuntime1.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                }
            }
        });

        InputHandler stockStreamInputHandler1 = siddhiAppRuntime1.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler2 = siddhiAppRuntime2.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler3 = siddhiAppRuntime3.getInputHandler("stockStream");
        InputHandler inputStreamInputHandler = siddhiAppRuntime1.getInputHandler("inputStream");

        siddhiAppRuntime1.start();
        siddhiAppRuntime2.start();
        siddhiAppRuntime3.start();

        // Thursday, June 1, 2017 4:05:50 AM
        stockStreamInputHandler1.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 70f, 60f, 90L, 6, 1496289950000L});

        // Friday, June 2, 2017 23:50:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 155f, null, 200L, 26, 1496447401000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 5f, null, 200L, 96, 1496447401005L});

        // July 19, 2017 04:20:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 7f, null, 200L, 26, 1500438001000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 3f, null, 200L, 96, 1500438001500L});

        // August 29, 2017 04:20:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 12f, null, 200L, 26, 1503980401000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 8f, null, 200L, 96, 1503980401500L});

        // January 02, 2018 00:00:01 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 45f, null, 200L, 16, 1514851260000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 10f, null, 200L, 86, 1514851260500L});

        // May 02, 2018 08:07:40 PM
        stockStreamInputHandler3.send(new Object[]{"IBM", 100f, null, 200L, 100, 1517861260000L});

        // April 04, 2019 04:20:00 PM
        stockStreamInputHandler3.send(new Object[]{"IBM", 100f, null, 200L, 100, 1555734000000L});
        stockStreamInputHandler1.send(new Object[]{"IBM", 1f, null, 200L, 100, 1555734000000L});

        Thread.sleep(1000);
        inputStreamInputHandler.send(new Object[]{1, "2015-06-01 09:35:52 +05:30", "2021-06-01 09:35:52 +05:30",
                "years"});
        Thread.sleep(1000);

        List<Object[]> expected = Arrays.asList(
                new Object[]{1483228800000L, "IBM", 174.0, 58.0, 3L},
                new Object[]{1514764800000L, "WSO2", 10.0, 10.0, 1L},
                new Object[]{1483228800000L, "WSO2", 136.0, 27.2, 5L},
                new Object[]{1514764800000L, "IBM", 145.0, 72.5, 2L},
                new Object[]{1546300800000L, "IBM", 101.0, 50.5, 2L}
        );

        Event[] events = siddhiAppRuntime1.query("from stockAggregation within 0L, 1643664151000L per " +
                "'years' select AGG_TIMESTAMP, symbol, totalPrice, avgPrice, count ");

        List<Object[]> eventsList = new ArrayList<>();
        for (Event event : events) {
            eventsList.add(event.getData());
        }
        Assert.assertEquals(events.length, 5, "Number of success events");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(eventsList, expected),
                "Data Matched from Store Query");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected),
                "Data Matched from Join Query");
        siddhiAppRuntime1.shutdown();
        siddhiAppRuntime2.shutdown();
        siddhiAppRuntime3.shutdown();
    }

    @Test(dependsOnMethods = "partialAggregationTest9")
    public void partialAggregationTest10() throws InterruptedException {
        log.info("partialAggregationTest10 - Checking group by using system timestamp");
        SiddhiManager siddhiManager1 = new SiddhiManager();
        Map<String, String> systemConfigs1 = new HashMap<>();
        systemConfigs1.put("shardId", "node1");
        InMemoryConfigManager inMemoryConfigManager1 =
                new InMemoryConfigManager(null, null, systemConfigs1);
        siddhiManager1.setConfigManager(inMemoryConfigManager1);

        SiddhiManager siddhiManager2 = new SiddhiManager();
        Map<String, String> systemConfigs2 = new HashMap<>();
        systemConfigs2.put("shardId", "node2");
        InMemoryConfigManager inMemoryConfigManager2 =
                new InMemoryConfigManager(null, null, systemConfigs2);
        siddhiManager2.setConfigManager(inMemoryConfigManager2);

        SiddhiManager siddhiManager3 = new SiddhiManager();
        Map<String, String> systemConfigs3 = new HashMap<>();
        systemConfigs3.put("shardId", "node3");
        InMemoryConfigManager inMemoryConfigManager3 =
                new InMemoryConfigManager(null, null, systemConfigs3);
        siddhiManager3.setConfigManager(inMemoryConfigManager3);

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long, " +
                        "quantity int); ";
        String query = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" +
                "@partitionbyid " +
                "@purge(enable='false')\n" +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice, sum(price) as totalPrice, count() as count " +
                "group by symbol " +
                "aggregate every sec...year; " +

                "define stream inputStream (value int, startTime long, " +
                "endTime long, perValue string); " +

                "@info(name = 'query1') " +
                "from inputStream join stockAggregation " +
                "within startTime, endTime " +
                "per perValue " +
                "select symbol, totalPrice, avgPrice, count " +
                "insert all events into outputStream; ";

        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(stockStream + query);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(stockStream + query);
        SiddhiAppRuntime siddhiAppRuntime3 = siddhiManager3.createSiddhiAppRuntime(stockStream + query);
        List<Object[]> inEventsList = new ArrayList<>();
        AtomicInteger inEventCount = new AtomicInteger(0);
        siddhiAppRuntime1.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timestamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                }
            }
        });

        InputHandler stockStreamInputHandler1 = siddhiAppRuntime1.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler2 = siddhiAppRuntime2.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler3 = siddhiAppRuntime3.getInputHandler("stockStream");
        InputHandler inputStreamInputHandler = siddhiAppRuntime1.getInputHandler("inputStream");

        siddhiAppRuntime1.start();
        siddhiAppRuntime2.start();
        siddhiAppRuntime3.start();

        stockStreamInputHandler1.send(new Object[]{"WSO2", 50f, 60f, 90L, 6});
        stockStreamInputHandler1.send(new Object[]{"WSO2", 70f, 60f, 90L, 6});

        stockStreamInputHandler2.send(new Object[]{"IBM", 155f, null, 200L, 26});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 5f, null, 200L, 96});

        stockStreamInputHandler1.send(new Object[]{"IBM", 7f, null, 200L, 26});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 3f, null, 200L, 96});
        stockStreamInputHandler3.send(new Object[]{"IBM", 100f, null, 200L, 100});

        stockStreamInputHandler1.send(new Object[]{"WSO2", 50f, 60f, 90L, 6});
        stockStreamInputHandler3.send(new Object[]{"IBM", 100f, null, 200L, 120});
        Thread.sleep(1000);

        stockStreamInputHandler3.send(new Object[]{"IBM", 100f, null, 200L, 180});

        Thread.sleep(1000);
        LocalDate currentDate = LocalDate.now();
        String year = String.valueOf(currentDate.getYear() + 2);
        inputStreamInputHandler.send(new Object[]{1, "2017-06-01 09:35:52 +05:30",
                year + "-06-01 09:35:52 +05:30", "years"});
        Thread.sleep(1000);

        List<Object[]> expected = Arrays.asList(
                new Object[]{"IBM", 462.0, 92.4, 5L},
                new Object[]{"WSO2", 178.0, 35.6, 5L}
        );

        Event[] events = siddhiAppRuntime1.query("from stockAggregation within 0L, " +
                (System.currentTimeMillis() + 1000000) + "L per 'years' select symbol, totalPrice, avgPrice, count ");

        List<Object[]> eventsList = new ArrayList<>();
        for (Event event : events) {
            eventsList.add(event.getData());
        }

        Assert.assertEquals(inEventCount.get(), 2, "Number of success events");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected),
                "Data Matched using Join");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(eventsList, expected),
                "Data Matched using Store Query");
        siddhiAppRuntime1.shutdown();
        siddhiAppRuntime2.shutdown();
        siddhiAppRuntime3.shutdown();
    }

    @Test(dependsOnMethods = "partialAggregationTest10")
    public void partialAggregationTest11() throws InterruptedException {
        log.info("partialAggregationTest11 - Checking out of order events");
        SiddhiManager siddhiManager1 = new SiddhiManager();
        Map<String, String> systemConfigs1 = new HashMap<>();
        systemConfigs1.put("shardId", "node1");
        InMemoryConfigManager inMemoryConfigManager1 =
                new InMemoryConfigManager(null, null, systemConfigs1);
        siddhiManager1.setConfigManager(inMemoryConfigManager1);

        SiddhiManager siddhiManager2 = new SiddhiManager();
        Map<String, String> systemConfigs2 = new HashMap<>();
        systemConfigs2.put("shardId", "node2");
        InMemoryConfigManager inMemoryConfigManager2 =
                new InMemoryConfigManager(null, null, systemConfigs2);
        siddhiManager2.setConfigManager(inMemoryConfigManager2);

        SiddhiManager siddhiManager3 = new SiddhiManager();
        Map<String, String> systemConfigs3 = new HashMap<>();
        systemConfigs3.put("shardId", "node3");
        InMemoryConfigManager inMemoryConfigManager3 =
                new InMemoryConfigManager(null, null, systemConfigs3);
        siddhiManager3.setConfigManager(inMemoryConfigManager3);

        String streams = "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long,"
                + " quantity int, timestamp long); ";
        String query = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" + "@partitionbyid \n" +
                "@purge(enable='false')\n" +
                "define aggregation stockAggregation\n" +
                "from stockStream \n" +
                "select symbol,sum(price) as totalPrice,avg(price) as avgPrice \n" +
                "aggregate by timestamp every sec...year; ";

        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(streams + query);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(streams + query);
        SiddhiAppRuntime siddhiAppRuntime3 = siddhiManager3.createSiddhiAppRuntime(streams + query);

        InputHandler stockStreamInputHandler1 = siddhiAppRuntime1.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler2 = siddhiAppRuntime2.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler3 = siddhiAppRuntime3.getInputHandler("stockStream");

        siddhiAppRuntime1.start();
        siddhiAppRuntime2.start();
        siddhiAppRuntime3.start();

        // Thursday, June 1, 2017 4:05:50 AM
        stockStreamInputHandler1.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 70f, 60f, 90L, 6, 1496289950000L});
        stockStreamInputHandler3.send(new Object[]{"WSO2", 10f, 65f, 30L, 3, 1496289950010L});

        // Thursday, June 1, 2017 4:05:51 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 100f, null, 200L, 16, 1496289951011L});
        stockStreamInputHandler1.send(new Object[]{"IBM", 110f, null, 200L, 16, 1496289951050L});

        // Thursday, June 1, 2017 4:06:12 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 300f, null, 200L, 9, 1496289972000L});
        stockStreamInputHandler2.send(new Object[]{"IBM", 500f, null, 200L, 9, 1496289972000L});

        // Thursday, June 1, 2017 4:06:22 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 1000f, null, 200L, 60, 1496289982020L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 400f, null, 200L, 7, 1496289982030L});
        stockStreamInputHandler3.send(new Object[]{"WSO2", 35f, 65f, 30L, 3, 1496289982040L});
        stockStreamInputHandler3.send(new Object[]{"WSO2", 5f, 65f, 30L, 3, 1496289982000L});

        // Out of order event - Thursday, June 1, 2017 4:05:50 AM
        stockStreamInputHandler1.send(new Object[]{"WSO2", 135f, 60f, 90L, 11, 1496289950020L});
        stockStreamInputHandler1.send(new Object[]{"WSO2", 135f, 60f, 90L, 11, 1496289950000L});

        // Out of order event - Thursday, June 1, 2017 4:06:12 AM
        stockStreamInputHandler3.send(new Object[]{"IBM", 111f, null, 200L, 9, 1496289972000L});

        // Out of order event - Thursday, June 1, 2017 4:06:22 AM
        stockStreamInputHandler2.send(new Object[]{"IBM", 99f, null, 200L, 10, 1496289982000L});

        // June 24, 2017 7:39:42 AM
        stockStreamInputHandler3.send(new Object[]{"IBM", 11f, null, 200L, 10, 1498289982000L});
        stockStreamInputHandler3.send(new Object[]{"IBM", 13f, null, 200L, 10, 1498289982000L});

        // Thursday, June 1, 2017 1:35:50 AM
        stockStreamInputHandler1.send(new Object[]{"WSO2", 6f, 60f, 90L, 6, 1496280950000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 7f, 60f, 90L, 6, 1496280950000L});
        stockStreamInputHandler3.send(new Object[]{"WSO2", 3f, 65f, 30L, 3, 1496280950010L});

        Thread.sleep(1000);

        Event[] eventsSeconds = siddhiAppRuntime1.query("from stockAggregation within 0L, " +
                "1543664151000L per 'seconds' select AGG_TIMESTAMP, totalPrice, avgPrice");

        List<Object[]> eventsListSeconds = new ArrayList<>();
        for (Event event : eventsSeconds) {
            eventsListSeconds.add(event.getData());
        }

        Event[] eventsMinutes = siddhiAppRuntime1.query("from stockAggregation within 0L, " +
                "1543664151000L per 'minutes' select AGG_TIMESTAMP, totalPrice, avgPrice");

        List<Object[]> eventsListMinutes = new ArrayList<>();
        for (Event event : eventsMinutes) {
            eventsListMinutes.add(event.getData());
        }


        List<Object[]> expectedSeconds = Arrays.asList(
                new Object[]{1496289972000L, 911.0, 303.6666666666667},
                new Object[]{1496289950000L, 400.0, 80.0},
                new Object[]{1496289982000L, 1539.0, 307.8},
                new Object[]{1496280950000L, 16.0, 5.333333333333333},
                new Object[]{1498289982000L, 24.0, 12.0},
                new Object[]{1496289951000L, 210.0, 105.0}

        );

        List<Object[]> expectedMinutes = Arrays.asList(
                new Object[]{1496280900000L, 16.0, 5.333333333333333},
                new Object[]{1496289900000L, 610.0, 87.14285714285714},
                new Object[]{1496289960000L, 2450.0, 306.25},
                new Object[]{1498289940000L, 24.0, 12.0}
        );

        Assert.assertEquals(eventsSeconds.length, 6, "Number of success events for seconds");
        Assert.assertEquals(eventsMinutes.length, 4, "Number of success events for minutes");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(eventsListSeconds, expectedSeconds),
                "Seconds granularity Data Matched");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(eventsListMinutes, expectedMinutes),
                "Minutes granularity Data Matched");
        siddhiAppRuntime1.shutdown();
        siddhiAppRuntime2.shutdown();
        siddhiAppRuntime3.shutdown();
    }

    @Test(dependsOnMethods = "partialAggregationTest11")
    public void partialAggregationTest12() throws InterruptedException {
        try {
            RDBMSTableTestUtils.initDatabaseTable("stockSumAggregation_SECONDS");
            RDBMSTableTestUtils.initDatabaseTable("stockSumAggregation_MINUTES");
            RDBMSTableTestUtils.initDatabaseTable("stockSumAggregation_HOURS");
            RDBMSTableTestUtils.initDatabaseTable("stockSumAggregation_DAYS");
            RDBMSTableTestUtils.initDatabaseTable("stockSumAggregation_MONTHS");
            RDBMSTableTestUtils.initDatabaseTable("stockSumAggregation_YEARS");
            RDBMSTableTestUtils.initDatabaseTable("stockAvgAggregation_SECONDS");
            RDBMSTableTestUtils.initDatabaseTable("stockAvgAggregation_MINUTES");
            RDBMSTableTestUtils.initDatabaseTable("stockAvgAggregation_HOURS");
            RDBMSTableTestUtils.initDatabaseTable("stockAvgAggregation_DAYS");
            RDBMSTableTestUtils.initDatabaseTable("stockAvgAggregation_MONTHS");
            RDBMSTableTestUtils.initDatabaseTable("stockAvgAggregation_YEARS");
        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
        log.info("partialAggregationTest12 - Checking grouped by out of order events with one app having two partial "
                + "aggregations");
        SiddhiManager siddhiManager1 = new SiddhiManager();
        Map<String, String> systemConfigs1 = new HashMap<>();
        systemConfigs1.put("shardId", "node1");
        InMemoryConfigManager inMemoryConfigManager1 =
                new InMemoryConfigManager(null, null, systemConfigs1);
        siddhiManager1.setConfigManager(inMemoryConfigManager1);

        SiddhiManager siddhiManager2 = new SiddhiManager();
        Map<String, String> systemConfigs2 = new HashMap<>();
        systemConfigs2.put("shardId", "node2");
        InMemoryConfigManager inMemoryConfigManager2 =
                new InMemoryConfigManager(null, null, systemConfigs2);
        siddhiManager2.setConfigManager(inMemoryConfigManager2);

        SiddhiManager siddhiManager3 = new SiddhiManager();
        Map<String, String> systemConfigs3 = new HashMap<>();
        systemConfigs3.put("shardId", "node3");
        InMemoryConfigManager inMemoryConfigManager3 =
                new InMemoryConfigManager(null, null, systemConfigs3);
        siddhiManager3.setConfigManager(inMemoryConfigManager3);

        String streams = "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long,"
                + " quantity int, timestamp long); ";
        String query = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" + "@partitionbyid \n" +
                "@purge(enable='false')\n" +
                "define aggregation stockSumAggregation\n" +
                "from stockStream \n" +
                "select symbol,sum(price) as totalPrice \n" +
                "group by symbol " +
                "aggregate by timestamp every sec...hour; ";

        String query2 = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:1, maxLifetime:60000\")\n" + "@partitionbyid \n" +
                "@purge(enable='false')\n" +
                "define aggregation stockAvgAggregation\n" +
                "from stockStream \n" +
                "select symbol,avg(price) as avgPrice \n" +
                "group by symbol " +
                "aggregate by timestamp every sec...hour; ";


        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(streams + query + query2);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(streams + query + query2);
        SiddhiAppRuntime siddhiAppRuntime3 = siddhiManager3.createSiddhiAppRuntime(streams + query + query2);

        InputHandler stockStreamInputHandler1 = siddhiAppRuntime1.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler2 = siddhiAppRuntime2.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler3 = siddhiAppRuntime3.getInputHandler("stockStream");

        siddhiAppRuntime1.start();
        siddhiAppRuntime2.start();
        siddhiAppRuntime3.start();

        // Thursday, June 1, 2017 4:05:50 AM
        stockStreamInputHandler1.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 70f, 60f, 90L, 6, 1496289950000L});
        stockStreamInputHandler3.send(new Object[]{"WSO2", 10f, 65f, 30L, 3, 1496289950010L});

        // Thursday, June 1, 2017 4:05:51 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 100f, null, 200L, 16, 1496289951011L});
        stockStreamInputHandler1.send(new Object[]{"IBM", 110f, null, 200L, 16, 1496289951050L});

        // Thursday, June 1, 2017 4:06:12 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 300f, null, 200L, 9, 1496289972000L});
        stockStreamInputHandler2.send(new Object[]{"IBM", 500f, null, 200L, 9, 1496289972000L});

        // Thursday, June 1, 2017 4:06:22 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 1000f, null, 200L, 60, 1496289982020L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 400f, null, 200L, 7, 1496289982030L});
        stockStreamInputHandler3.send(new Object[]{"WSO2", 35f, 65f, 30L, 3, 1496289982040L});
        stockStreamInputHandler3.send(new Object[]{"WSO2", 5f, 65f, 30L, 3, 1496289982000L});

        // Out of order event - Thursday, June 1, 2017 4:05:50 AM
        stockStreamInputHandler1.send(new Object[]{"WSO2", 135f, 60f, 90L, 11, 1496289950020L});
        stockStreamInputHandler1.send(new Object[]{"WSO2", 135f, 60f, 90L, 11, 1496289950000L});

        // Out of order event - Thursday, June 1, 2017 4:06:12 AM
        stockStreamInputHandler3.send(new Object[]{"IBM", 111f, null, 200L, 9, 1496289972000L});

        // Out of order event - Thursday, June 1, 2017 4:06:22 AM
        stockStreamInputHandler2.send(new Object[]{"IBM", 99f, null, 200L, 10, 1496289982000L});

        // June 24, 2017 7:39:42 AM
        stockStreamInputHandler3.send(new Object[]{"IBM", 11f, null, 200L, 10, 1498289982000L});
        stockStreamInputHandler3.send(new Object[]{"IBM", 13f, null, 200L, 10, 1498289982000L});

        // Thursday, June 1, 2017 1:35:50 AM
        stockStreamInputHandler1.send(new Object[]{"WSO2", 6f, 60f, 90L, 6, 1496280950000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 7f, 60f, 90L, 6, 1496280950000L});
        stockStreamInputHandler3.send(new Object[]{"WSO2", 3f, 65f, 30L, 3, 1496280950010L});

        Thread.sleep(1000);

        Event[] sumAggEvents = siddhiAppRuntime1.query("from stockSumAggregation within 0L, " +
                "1543664151000L per 'hours' select AGG_TIMESTAMP, symbol, totalPrice");

        List<Object[]> eventsListSumAgg = new ArrayList<>();
        for (Event event : sumAggEvents) {
            eventsListSumAgg.add(event.getData());
        }

        Event[] avgAggEvents = siddhiAppRuntime1.query("from stockAvgAggregation within 0L, " +
                "1543664151000L per 'hours' select AGG_TIMESTAMP, symbol, avgPrice");

        List<Object[]> eventsListAvgAgg = new ArrayList<>();
        for (Event event : avgAggEvents) {
            eventsListAvgAgg.add(event.getData());
        }


        List<Object[]> expectedSumAgg = Arrays.asList(
                new Object[]{1496289600000L, "WSO2", 840.0},
                new Object[]{1498287600000L, "IBM", 24.0},
                new Object[]{1496278800000L, "WSO2", 16.0},
                new Object[]{1496289600000L, "IBM", 2220.0}

        );

        List<Object[]> expectedAvgAgg = Arrays.asList(
                new Object[]{1496289600000L, "WSO2", 105.0},
                new Object[]{1498287600000L, "IBM", 12.0},
                new Object[]{1496278800000L, "WSO2", 5.333333333333333},
                new Object[]{1496289600000L, "IBM", 317.14285714285717}
        );

        Assert.assertEquals(sumAggEvents.length, 4, "Number of success events for seconds");
        Assert.assertEquals(avgAggEvents.length, 4, "Number of success events for minutes");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(eventsListSumAgg, expectedSumAgg),
                "Sum Agg Data Matched");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(eventsListAvgAgg, expectedAvgAgg),
                "Avg Agg Data Matched");
        siddhiAppRuntime1.shutdown();
        siddhiAppRuntime2.shutdown();
        siddhiAppRuntime3.shutdown();
    }

    @Test(dependsOnMethods = "partialAggregationTest12")
    public void partialAggregationTest13() throws InterruptedException {
        try {
            RDBMSTableTestUtils.initDatabaseTable("stockNormalAggregation_SECONDS");
            RDBMSTableTestUtils.initDatabaseTable("stockNormalAggregation_MINUTES");
            RDBMSTableTestUtils.initDatabaseTable("stockNormalAggregation_HOURS");
            RDBMSTableTestUtils.initDatabaseTable("stockNormalAggregation_DAYS");
            RDBMSTableTestUtils.initDatabaseTable("stockNormalAggregation_MONTHS");
            RDBMSTableTestUtils.initDatabaseTable("stockNormalAggregation_YEARS");
        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }

        log.info("partialAggregationTest13 - Checking normal and partitionbyid aggregations together");
        SiddhiManager siddhiManager1 = new SiddhiManager();
        Map<String, String> systemConfigs1 = new HashMap<>();
        systemConfigs1.put("shardId", "node1");
        InMemoryConfigManager inMemoryConfigManager1 =
                new InMemoryConfigManager(null, null, systemConfigs1);
        siddhiManager1.setConfigManager(inMemoryConfigManager1);

        SiddhiManager siddhiManager2 = new SiddhiManager();
        Map<String, String> systemConfigs2 = new HashMap<>();
        systemConfigs2.put("shardId", "node2");
        InMemoryConfigManager inMemoryConfigManager2 =
                new InMemoryConfigManager(null, null, systemConfigs2);
        siddhiManager2.setConfigManager(inMemoryConfigManager2);

        SiddhiManager siddhiManager3 = new SiddhiManager();
        Map<String, String> systemConfigs3 = new HashMap<>();
        systemConfigs3.put("shardId", "node3");
        InMemoryConfigManager inMemoryConfigManager3 =
                new InMemoryConfigManager(null, null, systemConfigs3);
        siddhiManager3.setConfigManager(inMemoryConfigManager3);

        String streams = "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long,"
                + " quantity int, timestamp long); ";
        String query = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" + "@partitionbyid \n" +
                "@purge(enable='false')\n" +
                "define aggregation stockAggregation\n" +
                "from stockStream \n" +
                "select symbol,sum(price) as totalPrice,avg(price) as avgPrice \n" +
                "aggregate by timestamp every sec...year; ";

        String query2 = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" +
                "@purge(enable='false')\n" +
                "define aggregation stockNormalAggregation\n" +
                "from stockStream \n" +
                "select symbol,sum(price) as totalPrice,avg(price) as avgPrice \n" +
                "aggregate by timestamp every sec...year; ";

        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager1.createSiddhiAppRuntime(streams + query + query2);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager2.createSiddhiAppRuntime(streams + query);
        SiddhiAppRuntime siddhiAppRuntime3 = siddhiManager3.createSiddhiAppRuntime(streams + query);

        InputHandler stockStreamInputHandler1 = siddhiAppRuntime1.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler2 = siddhiAppRuntime2.getInputHandler("stockStream");
        InputHandler stockStreamInputHandler3 = siddhiAppRuntime3.getInputHandler("stockStream");

        siddhiAppRuntime1.start();
        siddhiAppRuntime2.start();
        siddhiAppRuntime3.start();

        // Thursday, June 1, 2017 4:05:50 AM
        stockStreamInputHandler1.send(new Object[]{"WSO2", 50f, 60f, 90L, 6, 1496289950000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 70f, 60f, 90L, 6, 1496289950000L});
        stockStreamInputHandler3.send(new Object[]{"WSO2", 10f, 65f, 30L, 3, 1496289950010L});

        // Thursday, June 1, 2017 4:05:51 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 100f, null, 200L, 16, 1496289951011L});
        stockStreamInputHandler1.send(new Object[]{"IBM", 110f, null, 200L, 16, 1496289951050L});

        // Thursday, June 1, 2017 4:06:12 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 300f, null, 200L, 9, 1496289972000L});
        stockStreamInputHandler2.send(new Object[]{"IBM", 500f, null, 200L, 9, 1496289972000L});

        // Thursday, June 1, 2017 4:06:22 AM
        stockStreamInputHandler1.send(new Object[]{"IBM", 1000f, null, 200L, 60, 1496289982020L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 400f, null, 200L, 7, 1496289982030L});
        stockStreamInputHandler3.send(new Object[]{"WSO2", 35f, 65f, 30L, 3, 1496289982040L});
        stockStreamInputHandler3.send(new Object[]{"WSO2", 5f, 65f, 30L, 3, 1496289982000L});

        // Out of order event - Thursday, June 1, 2017 4:05:50 AM
        stockStreamInputHandler1.send(new Object[]{"WSO2", 135f, 60f, 90L, 11, 1496289950020L});
        stockStreamInputHandler1.send(new Object[]{"WSO2", 135f, 60f, 90L, 11, 1496289950000L});

        // Out of order event - Thursday, June 1, 2017 4:06:12 AM
        stockStreamInputHandler3.send(new Object[]{"IBM", 111f, null, 200L, 9, 1496289972000L});

        // Out of order event - Thursday, June 1, 2017 4:06:22 AM
        stockStreamInputHandler2.send(new Object[]{"IBM", 99f, null, 200L, 10, 1496289982000L});

        // June 24, 2017 7:39:42 AM
        stockStreamInputHandler3.send(new Object[]{"IBM", 11f, null, 200L, 10, 1498289982000L});
        stockStreamInputHandler3.send(new Object[]{"IBM", 13f, null, 200L, 10, 1498289982000L});

        // Thursday, June 1, 2017 1:35:50 AM
        stockStreamInputHandler1.send(new Object[]{"WSO2", 6f, 60f, 90L, 6, 1496280950000L});
        stockStreamInputHandler2.send(new Object[]{"WSO2", 7f, 60f, 90L, 6, 1496280950000L});
        stockStreamInputHandler3.send(new Object[]{"WSO2", 3f, 65f, 30L, 3, 1496280950010L});

        Thread.sleep(1000);

        Event[] aggEvents = siddhiAppRuntime1.query("from stockAggregation within 0L, 1543664151000L per "
                + "'days' select AGG_TIMESTAMP, symbol, totalPrice");

        List<Object[]> eventsListAgg = new ArrayList<>();
        for (Event event : aggEvents) {
            eventsListAgg.add(event.getData());
        }

        Event[] normalAggEvents = siddhiAppRuntime1.query("from stockNormalAggregation within 0L, " +
                "1543664151000L per 'days' select AGG_TIMESTAMP, symbol, totalPrice");

        List<Object[]> eventsListNormalAgg = new ArrayList<>();
        for (Event event : normalAggEvents) {
            eventsListNormalAgg.add(event.getData());
        }

        List<Object[]> expectedAgg = Arrays.asList(
                new Object[]{1498262400000L, "IBM", 24.0},
                new Object[]{1496275200000L, "WSO2", 3076.0}
        );

        List<Object[]> expectedNormalAgg = new ArrayList<>();
        expectedNormalAgg.add(new Object[]{1496275200000L, "IBM", 1836.0});

        Assert.assertEquals(aggEvents.length, 2, "Number of success events for seconds");
        Assert.assertEquals(normalAggEvents.length, 1, "Number of success events for minutes");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(eventsListAgg, expectedAgg),
                "Partitionbyid Agg Data Matched");
        Assert.assertTrue(SiddhiTestHelper.isUnsortedEventsMatch(eventsListNormalAgg, expectedNormalAgg),
                "Normal Agg Data Matched");
        siddhiAppRuntime1.shutdown();
        siddhiAppRuntime2.shutdown();
        siddhiAppRuntime3.shutdown();
    }
}
