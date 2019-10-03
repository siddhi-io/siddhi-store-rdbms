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

package io.siddhi.extension.store.rdbms.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

public class RDBMSTableTestUtils {

    public static final String TABLE_NAME = "StockTable";
    private static final Logger log = Logger.getLogger(RDBMSTableTestUtils.class);
    private static String connectionUrlMysql = "jdbc:mysql://{{container.ip}}:{{container.port}}/testdb?useSSL=false";
    private static String connectionUrlPostgres = "jdbc:postgresql://{{container.ip}}:{{container.port}}/testdb";
    private static String connectionUrlOracle = "jdbc:oracle:thin:@{{container.ip}}:{{container.port}}/XE";
    private static String connectionUrlMsSQL = "jdbc:sqlserver://{{container.ip}}:{{container.port}};" +
            "databaseName=tempdb";
    private static final String WRONG_USER_NAME_REGEX_MYSQL = "Access denied for user";
    private static final String WRONG_USER_NAME_REGEX_ORACLE = "invalid username/password; logon denied";
    private static final String WRONG_USER_NAME_REGEX_H2 = "Wrong user name or password";
    private static final String WRONG_USER_NAME_REGEX_POSTGRES = "password authentication failed";
    private static final String WRONG_USER_NAME_REGEX_MSSQL = "Login failed for user";
    private static final String WRONG_USER_NAME_REGEX_DB2 = "Connection authorization failure occurred";
    private static String connectionUrlDB2 = "jdbc:db2://{{container.ip}}:{{container.port}}/SAMPLE";
    private static final String CONNECTION_URL_H2 = "jdbc:h2:./target/testdb";
    private static final String JDBC_DRIVER_CLASS_NAME_H2 = "org.h2.Driver";
    private static final String JDBC_DRIVER_CLASS_NAME_MYSQL = "com.mysql.jdbc.Driver";
    private static final String JDBC_DRIVER_CLASS_NAME_ORACLE = "oracle.jdbc.driver.OracleDriver";
    private static final String JDBC_DRIVER_CLASS_POSTGRES = "org.postgresql.Driver";
    private static final String JDBC_DRIVER_CLASS_MSSQL = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String JDBC_DRIVER_CLASS_DB2 = "com.ibm.db2.jcc.DB2Driver";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";
    private static final String JNDI_RESOURCE = "java:comp/env/jdbc/TestDB";
    public static String url = CONNECTION_URL_H2;
    public static String driverClassName = JDBC_DRIVER_CLASS_NAME_H2;
    public static String user = USERNAME;
    public static String password = PASSWORD;
    public static RDBMSTableTestUtils.TestType testDatabaseType = TestType.H2;
    private static DataSource testDataSource;
    public static String wrongUsernameRegex = WRONG_USER_NAME_REGEX_H2;

    private RDBMSTableTestUtils() {

    }

    public static DataSource getTestDataSource() {
        return getDataSource();
    }

    public static DataSource getDataSource() {
        if (testDataSource == null) {
            testDataSource = initDataSource();
        }
        return testDataSource;
    }

    public static DataSource initDataSource() {
        String databaseType = System.getenv("DATABASE_TYPE");
        if (databaseType == null) {
            databaseType = TestType.H2.toString();
        }
        TestType type = RDBMSTableTestUtils.TestType.valueOf(databaseType);
        user = System.getenv("DATABASE_USER");
        password = System.getenv("DATABASE_PASSWORD");
        String port = System.getenv("PORT");
        Properties connectionProperties = new Properties();
        switch (type) {
            case MySQL:
                testDatabaseType = TestType.MySQL;
                url = connectionUrlMysql.replace("{{container.ip}}", getIpAddressOfContainer()).
                        replace("{{container.port}}", port);
                driverClassName = JDBC_DRIVER_CLASS_NAME_MYSQL;
                wrongUsernameRegex = WRONG_USER_NAME_REGEX_MYSQL;
                break;
            case H2:
                testDatabaseType = TestType.H2;
                url = CONNECTION_URL_H2;
                driverClassName = JDBC_DRIVER_CLASS_NAME_H2;
                user = USERNAME;
                password = PASSWORD;
                wrongUsernameRegex = WRONG_USER_NAME_REGEX_H2;
                break;
            case POSTGRES:
                testDatabaseType = TestType.POSTGRES;
                url = connectionUrlPostgres.replace("{{container.ip}}", getIpAddressOfContainer()).
                        replace("{{container.port}}", port);
                driverClassName = JDBC_DRIVER_CLASS_POSTGRES;
                wrongUsernameRegex = WRONG_USER_NAME_REGEX_POSTGRES;
                break;
            case ORACLE:
                testDatabaseType = TestType.ORACLE;
                url = connectionUrlOracle.replace("{{container.ip}}", getIpAddressOfContainer()).
                        replace("{{container.port}}", port);
                driverClassName = JDBC_DRIVER_CLASS_NAME_ORACLE;
                wrongUsernameRegex = WRONG_USER_NAME_REGEX_ORACLE;
                break;
            case MSSQL:
                testDatabaseType = TestType.MSSQL;
                url = connectionUrlMsSQL.replace("{{container.ip}}", getIpAddressOfContainer()).
                        replace("{{container.port}}", port);
                driverClassName = JDBC_DRIVER_CLASS_MSSQL;
                wrongUsernameRegex = WRONG_USER_NAME_REGEX_MSSQL;
                break;
            case DB2:
                testDatabaseType = TestType.DB2;
                url = connectionUrlDB2.replace("{{container.ip}}", getIpAddressOfContainer()).
                        replace("{{container.port}}", port);
                driverClassName = JDBC_DRIVER_CLASS_DB2;
                wrongUsernameRegex = WRONG_USER_NAME_REGEX_DB2;
                break;
        }
        log.info("URL of docker instance: " + url);
        connectionProperties.setProperty("jdbcUrl", url);
        connectionProperties.setProperty("driverClassName", driverClassName);
        connectionProperties.setProperty("dataSource.user", user);
        connectionProperties.setProperty("dataSource.password", password);
        connectionProperties.setProperty("poolName", "Test_Pool");
        connectionProperties.setProperty("maximumPoolSize", "4");
        HikariConfig config = new HikariConfig(connectionProperties);
        return new HikariDataSource(config);
    }

    public static void initDatabaseTable(String tableName) throws SQLException {
        PreparedStatement stmt = null;
        Connection con = null;
        try {
            con = getTestDataSource().getConnection();
            con.setAutoCommit(false);
            stmt = con.prepareStatement("DROP TABLE " + tableName);
            stmt.execute();
            con.commit();
        } catch (SQLException e) {
            log.error("Clearing DB table failed due to " + e.getMessage(), e);
            throw e;
        } finally {
            RDBMSTableUtils.cleanupConnection(null, stmt, con);
        }
    }

    public static long getRowsInTable(String tableName) throws SQLException {
        PreparedStatement stmt = null;
        Connection con = null;
        try {
            con = getTestDataSource().getConnection();
            stmt = con.prepareStatement("SELECT count(*) FROM " + tableName + "");
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
            return 0;
        } catch (SQLException e) {
            log.error("Getting rows in DB table failed due to " + e.getMessage(), e);
            throw e;
        } finally {
            RDBMSTableUtils.cleanupConnection(null, stmt, con);
        }
    }

    public static long getIndexesInTable(String tableName) throws SQLException {

        String indexCountQuery;
        switch (testDatabaseType) {
            case H2:
                indexCountQuery = "SELECT count(distinct(INDEX_NAME)) as indexCount " +
                        "FROM information_schema.INDEXES where TABLE_NAME = '" + tableName.toUpperCase() + "'";
                break;
            case MySQL:
                indexCountQuery = "select count(distinct(INDEX_NAME)) as indexCount " +
                        "from information_schema.statistics where TABLE_NAME='" + tableName + "';";
                break;
            case MSSQL:
                indexCountQuery = "select count(distinct(name)) as indexCount " +
                        "from sys.indexes where object_id= OBJECT_ID('dbo." + tableName + "');";
                break;
            case POSTGRES:
                indexCountQuery = "select count(distinct(indexname)) as indexCount " +
                        "from pg_indexes where TABLENAME='" + tableName.toLowerCase() + "';";
                break;
            case ORACLE:
                indexCountQuery = "select count(distinct(index_name)) as indexCount " +
                        "from SYS.ALL_INDEXES where TABLE_NAME ='" + tableName.toUpperCase() + "'";
                break;
            default:
                indexCountQuery = "SELECT count(distinct(INDEX_NAME)) as indexCount " +
                        "FROM information_schema.INDEXES where TABLE_NAME = '" + tableName + "'";
        }

        PreparedStatement stmt = null;
        Connection con = null;
        try {
            con = getTestDataSource().getConnection();
            stmt = con.prepareStatement(indexCountQuery);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
            return 0;
        } catch (SQLException e) {
            log.error("Getting rows in DB table failed due to " + e.getMessage(), e);
            throw e;
        } finally {
            RDBMSTableUtils.cleanupConnection(null, stmt, con);
        }
    }


    public enum TestType {
        MySQL, H2, ORACLE, MSSQL, DB2, POSTGRES
    }

    public static void setupJNDIDatasource(String url, String driverClassName) {
        try {
            System.setProperty(Context.INITIAL_CONTEXT_FACTORY,
                    "org.apache.naming.java.javaURLContextFactory");
            System.setProperty(Context.URL_PKG_PREFIXES,
                    "org.apache.naming");
            InitialContext context = new InitialContext();
            context.createSubcontext("java:");
            context.createSubcontext("java:comp");
            context.createSubcontext("java:comp/env");
            context.createSubcontext("java:comp/env/jdbc");
            Properties connectionProperties = new Properties();
            connectionProperties.setProperty("jdbcUrl", url);
            connectionProperties.setProperty("dataSource.user", user);
            connectionProperties.setProperty("dataSource.password", password);
            connectionProperties.setProperty("driverClassName", driverClassName);
            connectionProperties.setProperty("poolName", "JNDI_Pool");
            HikariConfig config = new HikariConfig(connectionProperties);
            DataSource testDataSourceJNDI = new HikariDataSource(config);
            context.bind(JNDI_RESOURCE, testDataSourceJNDI);
        } catch (NamingException e) {
            log.error("Error while bind the datasource as JNDI resource." + e.getMessage(), e);
        }
    }

    /**
     * Utility for get Docker running host
     *
     * @return docker host
     * @throws URISyntaxException if docker Host url is malformed this will throw
     */
    public static String getIpAddressOfContainer() {
        String ip = System.getenv("DOCKER_HOST_IP");
        String dockerHost = System.getenv("DOCKER_HOST");
        if (!StringUtils.isEmpty(dockerHost)) {
            try {
                URI uri = new URI(dockerHost);
                ip = uri.getHost();
            } catch (URISyntaxException e) {
                log.error("Error while getting the docker Host url." + e.getMessage(), e);
            }
        }
        return ip;
    }

    public static String readFile(String file) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
            }
            return stringBuilder.toString();
        }
    }

    public static void runStatements(String... queries) {
        PreparedStatement stmt = null;
        Connection con = null;
        try {
            con = getTestDataSource().getConnection();
            con.setAutoCommit(true);
            for (String query : queries) {
                stmt = con.prepareStatement(query);
                stmt.execute();
            }
        } catch (SQLException e) {
            log.error("Insert DB table failed due to " + e.getMessage(), e);
        } finally {
            RDBMSTableUtils.cleanupConnection(null, stmt, con);
        }
    }

}
