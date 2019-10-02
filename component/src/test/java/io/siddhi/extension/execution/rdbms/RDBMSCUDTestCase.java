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
import io.siddhi.core.util.config.InMemoryConfigManager;
import io.siddhi.core.util.config.InMemoryConfigReader;
import io.siddhi.core.util.config.YAMLConfigManager;
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

public class RDBMSCUDTestCase {
    private static final Logger log = Logger.getLogger(RDBMSCUDTestCase.class);
    private boolean isEventArrived;
    private AtomicInteger eventCount;
    private List<Object[]> actualData;

    @BeforeClass
    public static void startTest() {
        log.info("== RDBMS CUD tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== RDBMS CUD completed ==");
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
    public void rdbmsCUD1() throws InterruptedException {
        //Testing table query
        log.info("rdbmsCUD1 - Test Update");

        YAMLConfigManager yamlConfigManager = new YAMLConfigManager(
                "extensions: \n"+
                "  - extension: \n" +
                "      namespace: rdbms\n" +
                "      name: cud\n" +
                "      properties:\n" +
                "        perform.CUD.operations: true" );

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setConfigManager(yamlConfigManager);

        DataSource dataSource = RDBMSTableTestUtils.initDataSource();
        siddhiManager.setDataSource("TEST_DATASOURCE", dataSource);

        String streams = "" +
                "define stream StockStream (checkSymbol string); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream#rdbms:cud(\"TEST_DATASOURCE\", \"UPDATE " + TABLE_NAME + " SET " +
                "`symbol` = 'WSO22' WHERE `symbol` = 'WSO2';\") " +
                "select numRecords " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    isEventArrived = true;
                    eventCount.incrementAndGet();
                    actualData.add(event.getData());
                }
            }
        });

        stockStream.send(new Object[]{"WSO2"});
        SiddhiTestHelper.waitForEvents(2000, 1, eventCount, 60000);
        siddhiAppRuntime.shutdown();
        ((HikariDataSource) dataSource).close();

        Assert.assertTrue(isEventArrived, "Event Not Arrived");
        Assert.assertEquals(1, eventCount.get(), "Event count did not match");
        Assert.assertEquals(1, actualData.get(0)[0]);

    }
}
