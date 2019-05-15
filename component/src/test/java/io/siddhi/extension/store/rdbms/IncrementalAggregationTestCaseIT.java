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
package io.siddhi.extension.store.rdbms;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.extension.store.rdbms.DistributedAggregationTestCaseIT;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableTestUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableTestUtils.driverClassName;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableTestUtils.password;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableTestUtils.url;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableTestUtils.user;

public class IncrementalAggregationTestCaseIT {
    private static final Logger log = Logger.getLogger(DistributedAggregationTestCaseIT.class);

    @BeforeClass
    public static void startTest() {
        log.info("Test init with url: " + url + " and driverClass: " + driverClassName);
        log.info("== Incremental Aggregation tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Incremental Aggregation tests completed ==");
    }

    @BeforeMethod
    public void init() {

        try {
            RDBMSTableTestUtils.initDatabaseTable("stockAggregation_SECONDS");
            RDBMSTableTestUtils.initDatabaseTable("stockAggregation_MINUTES");
            RDBMSTableTestUtils.initDatabaseTable("stockAggregation_HOURS");
            RDBMSTableTestUtils.initDatabaseTable("stockAggregation_DAYS");
            RDBMSTableTestUtils.initDatabaseTable("stockAggregation_MONTHS");
            RDBMSTableTestUtils.initDatabaseTable("stockAggregation_YEARS");

            //Create Aggregation tables
            SiddhiManager siddhiManager = new SiddhiManager();

            String streams = "define stream stockStream (symbol string, price float, timestamp long); ";
            String query = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                    "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                    "\", pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" +
                    "@purge(enable='false')\n" +
                    "define aggregation stockAggregation\n" +
                    "from stockStream \n" +
                    "select symbol, sum(price) as totalPrice,avg(price) as avgPrice \n" +
                    "group by symbol " +
                    "aggregate by timestamp every sec...year; ";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            siddhiAppRuntime.start();
            siddhiAppRuntime.shutdown();
            siddhiManager.shutdown();
        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @Test
    public void incrementalAggregationTest1() throws InterruptedException {

        log.info("incrementalAggregationTest1 - Recreate in-memory after seconds of server shutdown");
        RDBMSTableTestUtils.runStatements("INSERT INTO stockAggregation_SECONDS VALUES  " +
                "(1525786020000, 1525786020000, 'WSO2', 100, 1), (1525786021000, 1525786021000, 'IBM', 100, 1)," +
                "(1525786081000, 1525786081000, 'IBM', 100, 1);");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "define stream stockStream (symbol string, price float, timestamp long); ";
        String query = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" +
                "@purge(enable='false')\n" +
                "define aggregation stockAggregation\n" +
                "from stockStream \n" +
                "select symbol, sum(price) as totalPrice,avg(price) as avgPrice \n" +
                "group by symbol " +
                "aggregate by timestamp every sec...year; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler inputStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");

        siddhiAppRuntime.start();
        inputStreamInputHandler.send(new Object[]{"WSO2", 100f, System.currentTimeMillis()});
        Thread.sleep(5000);
        Event[] query1 = siddhiAppRuntime
                .query("from stockAggregation within '2018-**-** **:**:**' per 'minutes' select *");
        Assert.assertNotNull(query1);
        Assert.assertEquals(query1.length, 3);
        List<Object[]> outputDataList = new ArrayList<>();
        for (Event event : query1) {
            outputDataList.add(event.getData());
        }
        List<Object[]> expected = Arrays.asList(
                new Object[]{1525786080000L, "IBM", 100.0, 100.0},
                new Object[]{1525786020000L, "WSO2", 100.0, 100.0},
                new Object[]{1525786020000L, "IBM", 100.0, 100.0}
        );
        SiddhiTestHelper.isUnsortedEventsMatch(expected, outputDataList);
        Thread.sleep(65000);

        Event[] query2 = siddhiAppRuntime
                .query("from stockAggregation within '" + Calendar.getInstance().get(Calendar.YEAR)
                        + "-**-** **:**:**' per 'minutes' select *");
        Assert.assertNotNull(query2);
        Assert.assertEquals(query2.length, 1);

        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "incrementalAggregationTest1")
    public void incrementalAggregationTest2() throws InterruptedException {

        log.info("incrementalAggregationTest1 - Recreate in-memory after seconds of server shutdown");
        RDBMSTableTestUtils.runStatements("INSERT INTO stockAggregation_SECONDS VALUES  " +
                "(1525786020000, 1525786020000, 'WSO2', 100, 1), (1525786021000, 1525786021000, 'IBM', 100, 1)," +
                "(1525786081000, 1525786081000, 'IBM', 100, 1);", "INSERT INTO stockAggregation_MINUTES VALUES  " +
                "(1494253173000, 1494253173000, 'WSO2', 100, 1);"
        );

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "define stream stockStream (symbol string, price float, timestamp long); ";
        String query = "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", pool.properties=\"maximumPoolSize:2, maxLifetime:60000\")\n" +
                "@purge(enable='false')\n" +
                "define aggregation stockAggregation\n" +
                "from stockStream \n" +
                "select symbol, sum(price) as totalPrice,avg(price) as avgPrice \n" +
                "group by symbol " +
                "aggregate by timestamp every sec...year; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler inputStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");

        siddhiAppRuntime.start();
        inputStreamInputHandler.send(new Object[]{"WSO2", 100f, System.currentTimeMillis()});
        Thread.sleep(5000);
        Event[] query1 = siddhiAppRuntime
                .query("from stockAggregation within '2018-**-** **:**:**' per 'minutes' select *");
        Assert.assertNotNull(query1);
        Assert.assertEquals(query1.length, 3);
        List<Object[]> outputDataList = new ArrayList<>();
        for (Event event : query1) {
            outputDataList.add(event.getData());
        }
        List<Object[]> expected = Arrays.asList(
                new Object[]{1525786080000L, "IBM", 100.0, 100.0},
                new Object[]{1525786020000L, "WSO2", 100.0, 100.0},
                new Object[]{1525786020000L, "IBM", 100.0, 100.0}
        );
        SiddhiTestHelper.isUnsortedEventsMatch(expected, outputDataList);
        Thread.sleep(65000);

        Event[] query2 = siddhiAppRuntime
                .query("from stockAggregation within '" + Calendar.getInstance().get(Calendar.YEAR)
                        + "-**-** **:**:**' per 'minutes' select *");
        Assert.assertNotNull(query2);
        Assert.assertEquals(query2.length, 1);

        siddhiAppRuntime.shutdown();
    }

}
