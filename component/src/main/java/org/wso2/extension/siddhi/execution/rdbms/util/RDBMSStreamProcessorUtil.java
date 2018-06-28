/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.extension.siddhi.execution.rdbms.util;

import com.zaxxer.hikari.HikariDataSource;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.datasource.core.api.DataSourceService;
import org.wso2.carbon.datasource.core.exception.DataSourceException;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Util class.
 */
public class RDBMSStreamProcessorUtil {
    private static final Logger LOG = LoggerFactory.getLogger(RDBMSStreamProcessorUtil.class);
    private static final String[] OPERATIONS = new String[]{"DROP", "ALTER"};
    private static final String[] MANIPULATION_OPERATIONS = new String[]{"INSERT", "UPDATE", "DELETE", "DROP", "ALTER"};

    /**
     * Method which can be used to clear up and ephemeral SQL connectivity artifacts.
     *
     * @param rs   {@link ResultSet} instance (can be null)
     * @param stmt {@link Statement} instance (can be null)
     * @param conn {@link Connection} instance (can be null)
     */
    public static void cleanupConnection(ResultSet rs, Statement stmt, Connection conn, String siddhiAppName) {
        if (rs != null) {
            try {
                rs.close();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(siddhiAppName + " - Closed ResultSet");
                }
            } catch (SQLException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(siddhiAppName + " - Error in closing ResultSet: " + e.getMessage(), e);
                }
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(siddhiAppName + " - Closed PreparedStatement");
                }
            } catch (SQLException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(siddhiAppName + " - Error in closing PreparedStatement: " + e.getMessage(), e);
                }
            }
        }
        if (conn != null) {
            try {
                conn.close();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(siddhiAppName + " - Closed Connection");
                }
            } catch (SQLException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(siddhiAppName + " - Error in closing Connection: " + e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Convert a record in result set to object array.
     *
     * @param attributeList List of attributes
     * @param rs Result set
     *
     * @return Object array
     * @throws SQLException this is thrown when an error occurs when accessing the result set.
     */
    public static Object[] processRecord(List<Attribute> attributeList, ResultSet rs) throws SQLException {
        List<Object> result = new ArrayList<>();
        for (int i = 0; i < attributeList.size(); i++) {
            switch (attributeList.get(i).getType()) {
                case BOOL:
                    result.add(rs.getBoolean(i + 1));
                    break;
                case DOUBLE:
                    result.add(rs.getDouble(i + 1));
                    break;
                case FLOAT:
                    result.add(rs.getFloat(i + 1));
                    break;
                case INT:
                    result.add(rs.getInt(i + 1));
                    break;
                case LONG:
                    result.add(rs.getLong(i + 1));
                    break;
                case OBJECT:
                    result.add(rs.getObject(i + 1));
                    break;
                case STRING:
                    result.add(rs.getString(i + 1));
                    break;
            }
        }
        return result.toArray();
    }

    /**
     * Utility function for validating the query.
     *
     * @param isRetrievalQuery Is it a retrieval query
     * @param query Query that is to be validated
     * @return true when query has unauthorised operations
     */
    public static boolean queryContainsCheck(boolean isRetrievalQuery, String query) {
        if (isRetrievalQuery) {
            return Arrays.stream(RDBMSStreamProcessorUtil.MANIPULATION_OPERATIONS)
                    .parallel().anyMatch(query.toUpperCase(Locale.getDefault())::contains);
        } else {
            return Arrays.stream(RDBMSStreamProcessorUtil.OPERATIONS)
                    .parallel().anyMatch(query.toUpperCase(Locale.getDefault())::contains);
        }
    }

    /**
     * The datasource parameter is validated
     * @param attributeExpressionExecutor Function parameter Attribute Expression Executor
     * @param siddhiAppName Siddhi App name
     * @return Datasource name
     */
    public static String validateDatasourceName(ExpressionExecutor attributeExpressionExecutor, String siddhiAppName) {
        if ((attributeExpressionExecutor instanceof ConstantExpressionExecutor)) {
            String dataSourceName = ((ConstantExpressionExecutor) attributeExpressionExecutor)
                    .getValue().toString();
            if (dataSourceName.trim().length() != 0) {
                return dataSourceName;
            } else {
                throw new SiddhiAppValidationException(siddhiAppName + " - The parameter 'datasource.name' cannot " +
                        "be empty in rdbms query function.");
            }
        } else {
            throw new SiddhiAppValidationException(siddhiAppName + " - The parameter 'datasource.name' in rdbms " +
                    "query function should be a constant, but found a dynamic parameter of type " +
                    attributeExpressionExecutor.getClass().getCanonicalName() + "'.");
        }

    }

    /**
     * Utility class to get the datasource service
     *
     * @param dataSourceName The datasource name
     * @param siddhiAppName Siddhi App name
     * @return Hikari Data Source
     */
    public static HikariDataSource getDataSourceService(String dataSourceName, String siddhiAppName) {
        try {
            BundleContext bundleContext = FrameworkUtil.getBundle(DataSourceService.class)
                    .getBundleContext();
            ServiceReference serviceRef = bundleContext.getServiceReference(DataSourceService.class
                    .getName());
            if (serviceRef == null) {
                throw new SiddhiAppRuntimeException(siddhiAppName + " - DatasourceService : '" +
                        DataSourceService.class.getCanonicalName() + "' cannot be found.");
            } else {
                DataSourceService dataSourceService = (DataSourceService) bundleContext
                        .getService(serviceRef);
                if (LOG.isDebugEnabled()) {
                    LOG.debug(siddhiAppName + " - Lookup for datasource '" + dataSourceName + "' completed through " +
                            "DataSource Service lookup.");
                }
                return (HikariDataSource) dataSourceService.getDataSource(dataSourceName);
            }
        } catch (DataSourceException e) {
            throw new SiddhiAppRuntimeException(siddhiAppName + " - Datasource '" + dataSourceName + "' cannot be " +
                    "connected.", e);
        }
    }
}
