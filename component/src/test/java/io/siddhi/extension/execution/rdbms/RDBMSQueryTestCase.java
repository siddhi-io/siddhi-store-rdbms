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
package io.siddhi.extension.execution.rdbms;

import com.zaxxer.hikari.HikariDataSource;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.sql.DataSource;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.TABLE_NAME;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.driverClassName;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.password;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.url;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.user;

public class RDBMSQueryTestCase {
    private static final Logger log = Logger.getLogger(RDBMSQueryTestCase.class);
    private boolean isEventArrived;
    private AtomicInteger eventCount;
    private List<Object[]> actualData;

    @BeforeClass
    public static void startTest() {
        log.info("== RDBMS Query tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== RDBMS Query completed ==");
    }

    @BeforeMethod
    public void init() {
        try {
            RDBMSTableTestUtils.initDatabaseTable(TABLE_NAME);
            log.info("Test init with url: " + url + " and driverClass: " + driverClassName);
            isEventArrived = false;
            eventCount = new AtomicInteger();
            actualData = new ArrayList<>();
        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"" + password + "\", pool.properties=\"maximumPoolSize:1\")" +
                "define table " + TABLE_NAME + " (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();

        siddhiAppRuntime.query("select 'WSO2' as symbol, 80f as price, 100L as volume insert into " + TABLE_NAME);
        siddhiAppRuntime.query("select 'IBM' as symbol, 180f as price, 200L as volume insert into " + TABLE_NAME);

        siddhiAppRuntime.shutdown();
    }

    @Test()
    public void rdbmsQuery1() throws InterruptedException {
        //Testing table query
        log.info("rdbmsQuery1 - Test Select");
        SiddhiManager siddhiManager = new SiddhiManager();
        DataSource dataSource = RDBMSTableTestUtils.initDataSource();
        siddhiManager.setDataSource("TEST_DATASOURCE", dataSource);

        String streams = "" +
                "define stream StockStream (checkSymbol string); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream#rdbms:query(\"TEST_DATASOURCE\", \"symbol string, price float, volume long\", " +
                "\"select * from " + TABLE_NAME + "\") " +
                "select symbol, price, volume " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    actualData.add(event.getData());
                    isEventArrived = true;
                    eventCount.incrementAndGet();
                }
            }
        });

        stockStream.send(new Object[]{"WSO2"});
        SiddhiTestHelper.waitForEvents(2000, 2, eventCount, 60000);
        siddhiAppRuntime.shutdown();
        ((HikariDataSource) dataSource).close();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"WSO2", 80f, 100L},
                new Object[]{"IBM", 180f, 200L}
        );

        Assert.assertTrue(isEventArrived, "Event Not Arrived");
        Assert.assertTrue(SiddhiTestHelper.isEventsMatch(actualData, expected), "Event output does not match");

    }

    @Test(dependsOnMethods = "rdbmsQuery1")
    public void rdbmsQuery2() throws InterruptedException {
        //Testing table query
        log.info("rdbmsQuery1 - Test Select dynamic query");
        SiddhiManager siddhiManager = new SiddhiManager();
        DataSource dataSource = RDBMSTableTestUtils.initDataSource();
        siddhiManager.setDataSource("TEST_DATASOURCE", dataSource);

        String streams = "" +
                "define stream StockStream (checkSymbol string, query String); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream#rdbms:query(\"TEST_DATASOURCE\", \"symbol string, price float, volume long\", " +
                "query) " +
                "select symbol, price, volume " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    actualData.add(event.getData());
                    isEventArrived = true;
                    eventCount.incrementAndGet();
                }
            }
        });

        stockStream.send(new Object[]{"WSO2", "select * from " + TABLE_NAME + ""});
        SiddhiTestHelper.waitForEvents(2000, 2, eventCount, 60000);
        siddhiAppRuntime.shutdown();
        ((HikariDataSource) dataSource).close();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"WSO2", 80f, 100L},
                new Object[]{"IBM", 180f, 200L}
        );

        Assert.assertTrue(isEventArrived, "Event Not Arrived");
        Assert.assertTrue(SiddhiTestHelper.isEventsMatch(actualData, expected), "Event output does not match");
    }


    @Test(dependsOnMethods = "rdbmsQuery2")
    public void rdbmsQuery3() throws InterruptedException {
        //Testing table query
        log.info("rdbmsQuery1 - Test Select parameterised query");
        SiddhiManager siddhiManager = new SiddhiManager();
        DataSource dataSource = RDBMSTableTestUtils.initDataSource();
        siddhiManager.setDataSource("TEST_DATASOURCE", dataSource);

        String streams = "" +
                "define stream StockStream (checkSymbol string, query String); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream#rdbms:query(\"TEST_DATASOURCE\", \"symbol string, price float, volume long\", " +
                "\"select * from " + TABLE_NAME + " where symbol=?\", 'WSO2') " +
                "select symbol, price, volume " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    actualData.add(event.getData());
                    isEventArrived = true;
                    eventCount.incrementAndGet();
                }
            }
        });

        stockStream.send(new Object[]{"WSO2", "select * from " + TABLE_NAME + ""});
        SiddhiTestHelper.waitForEvents(2000, 1, eventCount, 60000);
        siddhiAppRuntime.shutdown();
        ((HikariDataSource) dataSource).close();

        List<Object[]> expected = new ArrayList<>();
        expected.add(new Object[]{"WSO2", 80f, 100L});

        Assert.assertTrue(isEventArrived, "Event Not Arrived");
        Assert.assertTrue(SiddhiTestHelper.isEventsMatch(actualData, expected), "Event output does not match.");
    }



    @Test(dependsOnMethods = "rdbmsQuery3")
    public void rdbmsQuery4() throws InterruptedException {
        //Testing table query
        log.info("rdbmsQuery1 - Test Select parameterised query");
        SiddhiManager siddhiManager = new SiddhiManager();
        DataSource dataSource = RDBMSTableTestUtils.initDataSource();
        siddhiManager.setDataSource("TEST_DATASOURCE", dataSource);

        String streams = "" +
                "define stream StockStream (checkSymbol string, query String); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream#rdbms:query(\"TEST_DATASOURCE\", \"symbol string, price float, volume long\", " +
                "\"select * from " + TABLE_NAME + " where symbol=?\", checkSymbol) " +
                "select symbol, price, volume " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    actualData.add(event.getData());
                    isEventArrived = true;
                    eventCount.incrementAndGet();
                }
            }
        });

        stockStream.send(new Object[]{"WSO2", "select * from " + TABLE_NAME + ""});
        SiddhiTestHelper.waitForEvents(2000, 1, eventCount, 60000);
        siddhiAppRuntime.shutdown();
        ((HikariDataSource) dataSource).close();

        List<Object[]> expected = new ArrayList<>();
        expected.add(new Object[]{"WSO2", 80f, 100L});

        Assert.assertTrue(isEventArrived, "Event Not Arrived");
        Assert.assertTrue(SiddhiTestHelper.isEventsMatch(actualData, expected), "Event output does not match.");
    }

}
