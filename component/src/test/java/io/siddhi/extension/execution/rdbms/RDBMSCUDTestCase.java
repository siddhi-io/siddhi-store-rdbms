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
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.config.YAMLConfigManager;
import io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.TABLE_NAME;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.driverClassName;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.password;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.url;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableTestUtils.user;

public class RDBMSCUDTestCase {
    private static final Logger log = LogManager.getLogger(RDBMSCUDTestCase.class);
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

    @BeforeTest
    public void validateParameters() {
        log.info("validateParameters - Validate RDBMS CUD parameters");

        String databaseType = System.getenv("DATABASE_TYPE");
        if (databaseType == null) {
            databaseType = RDBMSTableTestUtils.TestType.H2.toString();
        }
        RDBMSTableTestUtils.TestType type = RDBMSTableTestUtils.TestType.valueOf(databaseType);

        YAMLConfigManager yamlConfigManager = new YAMLConfigManager(
                "extensions: \n" +
                        "  - extension: \n" +
                        "      namespace: rdbms\n" +
                        "      name: cud\n" +
                        "      properties:\n" +
                        "        perform.CUD.operations: true");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setConfigManager(yamlConfigManager);

        DataSource dataSource = RDBMSTableTestUtils.initDataSource();
        siddhiManager.setDataSource("TEST_DATASOURCE", dataSource);

        String definitions = "" +
                "define stream InsertStream(symbol string, price float, volume long);\n" +
                "\n" +
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"" + password + "\", pool.properties=\"maximumPoolSize:1\")" +
                "define table " + TABLE_NAME + " (symbol string, price float, volume long); " +
                "\n";

        String parameterizedSqlQuery = "INSERT INTO " + TABLE_NAME + "(symbol, price, volume) VALUES (?,?,?)";
        String nonParameterizedSqlQuery = "INSERT INTO " + TABLE_NAME + "(symbol, price, volume) VALUES ('a',10.0,1)";
        if (!type.equals(RDBMSTableTestUtils.TestType.ORACLE)) {
            parameterizedSqlQuery = parameterizedSqlQuery.concat(";");
            nonParameterizedSqlQuery = nonParameterizedSqlQuery.concat(";");
        }

        // Test non-parameterized queries in CUD operations [BEGIN]

        String invalidNonParameterizedCud =
                "from InsertStream#rdbms:cud(\"TEST_DATASOURCE\", \"" + nonParameterizedSqlQuery +
                        "\", volume, 't1') " +
                        "select numRecords " +
                        "insert into ignoreStream ;" +
                        "\n";
        String validNonParameterizedCud1 =
                "from InsertStream#rdbms:cud(\"TEST_DATASOURCE\", \"" + nonParameterizedSqlQuery + "\", 't1') " +
                        "select numRecords " +
                        "insert into ignoreStream ;" +
                        "\n";
        String validNonParameterizedCud2 =
                "from InsertStream#rdbms:cud(\"TEST_DATASOURCE\", \"" + nonParameterizedSqlQuery + "\") " +
                        "select numRecords " +
                        "insert into ignoreStream ;" +
                        "\n";

        boolean isCreationSuccessful;
        try {
            siddhiManager.createSiddhiAppRuntime(definitions + invalidNonParameterizedCud);
            isCreationSuccessful = true;
        } catch (SiddhiAppCreationException e) {
            isCreationSuccessful = false;
        }
        Assert.assertFalse(isCreationSuccessful,
                "Creating a Siddhi app with the following INVALID CUD operation did NOT fail:\n" +
                        invalidNonParameterizedCud);

        try {
            siddhiManager.createSiddhiAppRuntime(definitions + validNonParameterizedCud1);
            isCreationSuccessful = true;
        } catch (SiddhiAppCreationException e) {
            isCreationSuccessful = false;
        }
        Assert.assertTrue(isCreationSuccessful,
                "Creating a Siddhi app with the following valid CUD operation failed:\n" +
                        validNonParameterizedCud1);

        try {
            siddhiManager.createSiddhiAppRuntime(definitions + validNonParameterizedCud2);
            isCreationSuccessful = true;
        } catch (SiddhiAppCreationException e) {
            isCreationSuccessful = false;
        }
        Assert.assertTrue(isCreationSuccessful,
                "Creating a Siddhi app with the following valid CUD operation failed:\n" +
                        validNonParameterizedCud2);

        // Test non-parameterized queries in CUD operations [END]

        // Test parameterized queries in CUD operations [BEGIN]

        String invalidParameterizedCud = "" +
                "from InsertStream#rdbms:cud(\"TEST_DATASOURCE\", \"" + parameterizedSqlQuery + "\", 't1') " +
                "select numRecords " +
                "insert into ignoreStream ;" +
                "\n";

        String validParameterizedCud1 = "" +
                "from InsertStream#rdbms:cud(\"TEST_DATASOURCE\", \"" + parameterizedSqlQuery +
                "\", symbol, price, volume) " +
                "select numRecords " +
                "insert into ignoreStream ;" +
                "\n";

        String validParameterizedCud2 = "" +
                "from InsertStream#rdbms:cud(\"TEST_DATASOURCE\", \"" + parameterizedSqlQuery +
                "\", symbol, price, volume, 't1') " +
                "select numRecords " +
                "insert into ignoreStream ;" +
                "\n";

        try {
            siddhiManager.createSiddhiAppRuntime(definitions + invalidParameterizedCud);
            isCreationSuccessful = true;
        } catch (SiddhiAppCreationException e) {
            isCreationSuccessful = false;
        }
        Assert.assertFalse(isCreationSuccessful,
                "Creating a Siddhi app with the following INVALID CUD operation did NOT fail:\n" +
                        invalidParameterizedCud);

        try {
            siddhiManager.createSiddhiAppRuntime(definitions + validParameterizedCud1);
            isCreationSuccessful = true;
        } catch (SiddhiAppCreationException e) {
            isCreationSuccessful = false;
        }
        Assert.assertTrue(isCreationSuccessful,
                "Creating a Siddhi app with the following valid CUD operation failed:\n" +
                        validParameterizedCud1);

        try {
            siddhiManager.createSiddhiAppRuntime(definitions + validParameterizedCud2);
            isCreationSuccessful = true;
        } catch (SiddhiAppCreationException e) {
            isCreationSuccessful = false;
        }
        Assert.assertTrue(isCreationSuccessful,
                "Creating a Siddhi app with the following valid CUD operation failed:\n" +
                        validParameterizedCud2);

        // Test parameterized queries in CUD operations [BEGIN]
    }

    @BeforeMethod
    public void init() {
        isEventArrived = false;
        eventCount = new AtomicInteger();
        actualData = new ArrayList<>();

        try {
            RDBMSTableTestUtils.initDatabaseTable(TABLE_NAME);
            log.info("Test init with url: " + url + " and driverClass: " + driverClassName);
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

        String databaseType = System.getenv("DATABASE_TYPE");
        if (databaseType == null) {
            databaseType = RDBMSTableTestUtils.TestType.H2.toString();
        }
        RDBMSTableTestUtils.TestType type = RDBMSTableTestUtils.TestType.valueOf(databaseType);

        boolean isOracle11 = false;
        String sqlQuery;
        if (type.equals(RDBMSTableTestUtils.TestType.ORACLE)) {
            sqlQuery = "UPDATE " + TABLE_NAME + " SET symbol = 'WSO22' WHERE symbol = 'WSO2'";
            isOracle11 = Boolean.parseBoolean(System.getenv("IS_ORACLE_11"));
        } else {
            sqlQuery = "UPDATE " + TABLE_NAME + " SET symbol = 'WSO22' WHERE symbol = 'WSO2';";
        }

        YAMLConfigManager yamlConfigManager = new YAMLConfigManager(
                "extensions: \n" +
                "  - extension: \n" +
                "      namespace: rdbms\n" +
                "      name: cud\n" +
                "      properties:\n" +
                "        perform.CUD.operations: true");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setConfigManager(yamlConfigManager);

        DataSource dataSource = RDBMSTableTestUtils.initDataSource();
        siddhiManager.setDataSource("TEST_DATASOURCE", dataSource);

        String streams = "" +
                "define stream StockStream (checkSymbol string); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream#rdbms:cud(\"TEST_DATASOURCE\", \"" + sqlQuery + "\") " +
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

        if (isOracle11) {
            Assert.assertEquals(-2, actualData.get(0)[0]);
        } else {
            Assert.assertEquals(1, actualData.get(0)[0]);
        }
    }

    @Test()
    public void rdbmsCudTransactionTest1() throws InterruptedException {
        log.info("rdbmsCudTransactionTest1 - Test insert, commit and rollback");

        String databaseType = System.getenv("DATABASE_TYPE");
        if (databaseType == null) {
            databaseType = RDBMSTableTestUtils.TestType.H2.toString();
        }
        RDBMSTableTestUtils.TestType type = RDBMSTableTestUtils.TestType.valueOf(databaseType);

        String sqlQuery = "INSERT INTO " + TABLE_NAME + "(symbol, price, volume) VALUES (?,?,?)";
        if (!type.equals(RDBMSTableTestUtils.TestType.ORACLE)) {
            sqlQuery = sqlQuery.concat(";");
        }

        YAMLConfigManager yamlConfigManager = new YAMLConfigManager(
                "extensions: \n" +
                        "  - extension: \n" +
                        "      namespace: rdbms\n" +
                        "      name: cud\n" +
                        "      properties:\n" +
                        "        perform.CUD.operations: true");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setConfigManager(yamlConfigManager);

        DataSource dataSource = RDBMSTableTestUtils.initDataSource();
        siddhiManager.setDataSource("TEST_DATASOURCE", dataSource);

        String streams = "" +
                "define stream InsertStream(symbol string, price float, volume long);\n" +
                "define stream CommitStream(name string);\n" +
                "define stream RollbackStream(name string);\n" +
                "define stream ListTableContentStream(dummy string);\n" +
                "define stream TableContentOutputStream(dummy string, symbol string, price float, volume long);\n" +
                "\n";

        String table =
                "@Store(type=\"rdbms\", jdbc.url=\"" + url + "\", jdbc.driver.name=\"" + driverClassName + "\"," +
                "username=\"" + user + "\", password=\"" + password + "\", pool.properties=\"maximumPoolSize:1\")" +
                "define table " + TABLE_NAME + " (symbol string, price float, volume long); " +
                "\n";

        String queries = "" +
                "from InsertStream#rdbms:cud(\"TEST_DATASOURCE\", \"" + sqlQuery + "\", symbol, price, volume, 't1') " +
                "select numRecords " +
                "insert into ignoreStream ;" +
                "\n" +
                "from CommitStream#rdbms:cud(\"TEST_DATASOURCE\", \"COMMIT\", 't1')\n" +
                "select numRecords\n" +
                "insert into ignoreStream;" +
                "\n" +
                "from RollbackStream#rdbms:cud(\"TEST_DATASOURCE\", \"ROLLBACK\", 't1')\n" +
                "select numRecords\n" +
                "insert into ignoreStream;" +
                "\n" +
                "from ListTableContentStream left outer join " + TABLE_NAME + "\n" +
                "select ListTableContentStream.dummy as dummy, " + TABLE_NAME + ".symbol as symbol, " +
                TABLE_NAME + ".price as price, " + TABLE_NAME + ".volume as volume\n" +
                "insert into TableContentOutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + table + queries);
        InputHandler insertStream = siddhiAppRuntime.getInputHandler("InsertStream");
        InputHandler commitStream = siddhiAppRuntime.getInputHandler("CommitStream");
        InputHandler rollbackStream = siddhiAppRuntime.getInputHandler("RollbackStream");
        InputHandler listTableContentStream = siddhiAppRuntime.getInputHandler("ListTableContentStream");
        siddhiAppRuntime.start();

        siddhiAppRuntime.addCallback("TableContentOutputStream", new StreamCallback() {
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

        insertStream.send(new Object[]{"A", 1.0f, 1L});
        insertStream.send(new Object[]{"B", 2.0f, 2L});
        commitStream.send(new Object[]{"commit"});

        insertStream.send(new Object[]{"C", 3.0f, 3L});
        insertStream.send(new Object[]{"D", 4.0f, 4L});
        rollbackStream.send(new Object[]{"rollback"});

        insertStream.send(new Object[]{"E", 5.0f, 5L});
        insertStream.send(new Object[]{"F", 6.0f, 6L});
        commitStream.send(new Object[]{"commit"});

        listTableContentStream.send(new Object[]{"dummy"});

        SiddhiTestHelper.waitForEvents(2000, 1, eventCount, 60000);
        siddhiAppRuntime.shutdown();
        ((HikariDataSource) dataSource).close();

        Assert.assertTrue(isEventArrived, "No events arrived");
        Assert.assertEquals(eventCount.get(), 6, "Event count did not match");

        List<Object[]> expected = Arrays.asList(
                new Object[]{"dummy", "WSO2", 80.0f, 100L},
                new Object[]{"dummy", "IBM", 180.0f, 200L},
                new Object[]{"dummy", "A", 1.0f, 1L},
                new Object[]{"dummy", "B", 2.0f, 2L},
                new Object[]{"dummy", "E", 5.0f, 5L},
                new Object[]{"dummy", "F", 6.0f, 6L}
        );
        Assert.assertTrue(SiddhiTestHelper.isEventsMatch(actualData, expected),
                "Received events do not match with the expected ones");
    }
}
