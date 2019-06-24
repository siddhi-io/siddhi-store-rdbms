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
import org.apache.log4j.Logger;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.TABLE_NAME;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.driverClassName;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.password;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.url;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.user;

public class InsertIntoRDBMSTableTestCaseIT {
    private static final Logger log = Logger.getLogger(InsertIntoRDBMSTableTestCaseIT.class);
    private static AtomicInteger actualEventCount = new AtomicInteger(0);

    @BeforeClass
    public static void startTest() {
        log.info("== RDBMS Table INSERT tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== RDBMS Table INSERT tests completed ==");
    }

    @BeforeMethod
    public void init() {
        try {
            actualEventCount.set(0);
            RDBMSTableTestUtils.initDatabaseTable(TABLE_NAME);
        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @Test
    public void insertIntoRDBMSTableTest1() throws InterruptedException, SQLException {
        //Configure siddhi to insert events data to RDBMS table only from specific fields of the stream.
        log.info("insertIntoRDBMSTableTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName
                + "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, volume long); ";

        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 2, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void insertIntoRDBMSTableTest2() throws InterruptedException, SQLException {
        //Testing table creation with a compound primary key (normal insertion)
        log.info("insertIntoRDBMSTableTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@PrimaryKey(\"symbol, price\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 58.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 4, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void insertIntoRDBMSTableTest3() throws InterruptedException, SQLException {
        //Testing table creation with a compound primary key (normal insertion)
        log.info("insertIntoRDBMSTableTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long);\n";

        String table = "" +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", " +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@PrimaryKey(\"symbol, price\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long);\n" +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", table.name=\"StockTable\"," +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100\")\n" +
                "@PrimaryKey(\"symbol, price\")" +
                //"@Index(\"volume\")" +
                "define table MyTable (symbol string, price float);\n";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "select symbol, price " +
                "insert into MyTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + table + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F});
        stockStream.send(new Object[]{"IBM", 75.6F});
        stockStream.send(new Object[]{"MSFT", 57.6F});
        stockStream.send(new Object[]{"WSO2", 58.6F});
        Thread.sleep(1000);

        long totalRowsInTable = RDBMSTableTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 4, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void insertIntoRDBMSTableTest4() throws InterruptedException, SQLException, IOException {
        //Testing large data message insertion
        log.info("insertIntoRDBMSTableTest4");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "define stream StockStream (symbol string, message string);\n" +
                "define stream JoinStream (symbol string);\n";
        File filePath = new File("src" + File.separator + "test" + File.separator + "resources"
                + File.separator + "insertInto" + File.separator + "big-message.log");
        //chars length 5000
        String bigMessage = RDBMSTableTestUtils.readFile(filePath.getAbsolutePath());
        String table = "" +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", table.name=\"StockTable\"," +
                "username=\"" + user + "\", password=\"" + password + "\", jdbc.driver.name=\"" + driverClassName +
                "\", field.length=\"symbol:100, message:5000\", pool.properties=\"maximumPoolSize:1\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table " + TABLE_NAME + " (symbol string, message string);\n";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "select symbol, message " +
                "insert into " + TABLE_NAME + " ;" +
                "@info(name = 'query2') " +
                "from JoinStream join " + TABLE_NAME + " on JoinStream.symbol == " + TABLE_NAME + ".symbol " +
                "select JoinStream.symbol, message " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + table + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler joinStream = siddhiAppRuntime.getInputHandler("JoinStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        int inEventCount = actualEventCount.incrementAndGet();
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"wso2", bigMessage});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
                        }
                    }
                }
            }

        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"wso2", bigMessage});
        joinStream.send(new Object[]{"wso2"});
        waitTillVariableCountMatches(1, Duration.ONE_MINUTE);
        Assert.assertEquals(actualEventCount.get(), 1, "Number of success events");
        siddhiAppRuntime.shutdown();
    }

    private static void waitTillVariableCountMatches(long expected, Duration duration) {
        Awaitility.await().atMost(duration).until(() -> {
            return actualEventCount.get() == expected;
        });
    }
}
