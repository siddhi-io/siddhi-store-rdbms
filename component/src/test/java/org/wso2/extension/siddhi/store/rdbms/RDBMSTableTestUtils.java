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

package org.wso2.extension.siddhi.store.rdbms;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableUtils;

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
    private static String connectionUrlMysql = "jdbc:mysql://{{container.ip}}:3306/dasdb";
    private static String connectionUrlPostgres = "jdbc:postgresql://{{container.ip}}:5432/dasdb";
    private static String connectionUrlOracle = "jdbc:oracle:thin:@{{container.ip}}:1521/XE";
    private static String connectionUrlMsSQL = "jdbc:sqlserver//{{container.ip}}:1433/dasdb";
    private static final String CONNECTION_URL_H2 = "jdbc:h2:./target/dasdb";
    private static final String JDBC_DRIVER_CLASS_NAME_H2 = "org.h2.Driver";
    private static final String JDBC_DRIVER_CLASS_NAME_MYSQL = "com.mysql.jdbc.Driver";
    private static final String JDBC_DRIVER_CLASS_NAME_ORACLE = "oracle.jdbc.driver.OracleDriver";
    private static final String JDBC_DRIVER_CLASS_POSTGRES = "org.postgresql.Driver";
    private static final String JDBC_DRIVER_CLASS_MSSQL = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";
    private static final String JNDI_RESOURCE = "java:comp/env/jdbc/TestDB";
    public static String url = CONNECTION_URL_H2;
    public static String driverClassName = JDBC_DRIVER_CLASS_NAME_H2;
    public static String user = USERNAME;
    public static String password = PASSWORD;
    private static DataSource testDataSource;

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

    private static DataSource initDataSource() {
        TestType type = RDBMSTableTestUtils.TestType.valueOf(System.getenv("DATABASE_TYPE"));
        user = System.getenv("DATABASE_USER");
        password = System.getenv("DATABASE_PASSWORD");
        Properties connectionProperties = new Properties();
        switch (type) {
            case MySQL:
                url = connectionUrlMysql.replace("{{container.ip}}", getIpAddressOfContainer());
                driverClassName = JDBC_DRIVER_CLASS_NAME_MYSQL;
                break;
            case H2:
                url = CONNECTION_URL_H2;
                driverClassName = JDBC_DRIVER_CLASS_NAME_H2;
                user = USERNAME;
                password = PASSWORD;
                break;
            case POSTGRES:
                url = connectionUrlPostgres.replace("{{container.ip}}", getIpAddressOfContainer());
                driverClassName = JDBC_DRIVER_CLASS_POSTGRES;
                break;
            case ORACLE:
                url = connectionUrlOracle.replace("{{container.ip}}", getIpAddressOfContainer());
                driverClassName = JDBC_DRIVER_CLASS_NAME_ORACLE;
                break;
            case MSSQL:
                url = connectionUrlMsSQL.replace("{{container.ip}}", getIpAddressOfContainer());
                driverClassName = JDBC_DRIVER_CLASS_MSSQL;
                break;
        }
        log.info("URL : " + url);
        connectionProperties.setProperty("jdbcUrl", url);
        connectionProperties.setProperty("driverClassName", driverClassName);
        connectionProperties.setProperty("dataSource.user", user);
        connectionProperties.setProperty("dataSource.password", password);
        connectionProperties.setProperty("poolName", "Test_Pool");
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
            log.debug("Clearing DB table failed due to " + e.getMessage(), e);
        } finally {
            RDBMSTableUtils.cleanupConnection(null, stmt, con);
        }
    }

    public static void initDatabaseTable(String tableName, TestType testType, String user, String password)
            throws SQLException {
        PreparedStatement stmt = null;
        Connection con = null;
        try {
            con = getDataSource().getConnection();
            con.setAutoCommit(false);
            stmt = con.prepareStatement("DROP TABLE " + tableName);
            stmt.execute();
            con.commit();
        } catch (SQLException e) {
            log.debug("Clearing DB table failed due to " + e.getMessage(), e);
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

    public enum TestType {
        MySQL, H2, ORACLE, MSSQL, DB2, POSTGRES
    }

    protected static void setupJNDIDatasource(String url, String driverClassName) {
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
        if (!StringUtils.isEmpty(ip)) {
            return ip;
        }
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
}
