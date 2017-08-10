/*
*  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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
package org.wso2.extension.siddhi.store.rdbms;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.extension.siddhi.store.rdbms.config.RDBMSQueryConfigurationEntry;
import org.wso2.extension.siddhi.store.rdbms.config.RDBMSTypeMapping;
import org.wso2.extension.siddhi.store.rdbms.exception.RDBMSTableException;
import org.wso2.extension.siddhi.store.rdbms.util.Constant;
import org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableUtils;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.SystemParameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.exception.CannotLoadConfigurationException;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.table.record.AbstractRecordTable;
import org.wso2.siddhi.core.table.record.ExpressionBuilder;
import org.wso2.siddhi.core.table.record.RecordIterator;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.CompiledExpression;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.annotation.Element;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.ANNOTATION_DRIVER_CLASS_NAME;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.ANNOTATION_ELEMENT_FIELD_LENGTHS;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.ANNOTATION_ELEMENT_JNDI_RESOURCE;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.ANNOTATION_ELEMENT_PASSWORD;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.ANNOTATION_ELEMENT_POOL_PROPERTIES;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.ANNOTATION_ELEMENT_TABLE_NAME;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.ANNOTATION_ELEMENT_URL;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.ANNOTATION_ELEMENT_USERNAME;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.BATCH_ENABLE;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.BATCH_SIZE;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.BINARY_TYPE;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.BOOLEAN_TYPE;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.CLOSE_PARENTHESIS;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.DOUBLE_TYPE;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.FLOAT_TYPE;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.INDEX_CREATE_QUERY;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.INTEGER_TYPE;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.LONG_TYPE;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.OPEN_PARENTHESIS;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_COLUMNS;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_COLUMNS_VALUES;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_CONDITION;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_INDEX;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_Q;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_TABLE_NAME;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.PROPERTY_SEPARATOR;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.QUESTION_MARK;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.RECORD_DELETE_QUERY;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.RECORD_EXISTS_QUERY;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.RECORD_INSERT_QUERY;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.RECORD_SELECT_QUERY;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.RECORD_UPDATE_QUERY;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.SEPARATOR;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.SQL_NOT_NULL;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.SQL_PRIMARY_KEY_DEF;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.STRING_SIZE;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.STRING_TYPE;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.TABLE_CHECK_QUERY;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.TABLE_CREATE_QUERY;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.TYPE_MAPPING;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.WHITESPACE;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;

/**
 * Class representing the RDBMS Event Table implementation.
 */
@Extension(
        name = "rdbms",
        namespace = "store",
        description = "This extension assigns data sources and connection instructions to event tables. It also " +
                "implements read write operations on connected datasources",
        parameters = {
                @Parameter(name = "jdbc.url",
                        description = "The JDBC URL via which the RDBMS data store is accessed.",
                        type = {DataType.STRING}),
                @Parameter(name = "username",
                        description = "The username to be used to access the RDBMS data store.",
                        type = {DataType.STRING}),
                @Parameter(name = "password",
                        description = "The password to be used to access the RDBMS data store.",
                        type = {DataType.STRING}),
                @Parameter(name = "jdbc.driver.name",
                        description = "The driver class name for connecting the RDBMS data store.",
                        type = {DataType.STRING}),
                @Parameter(name = "pool.properties",
                        description = "Any pool parameters for the database connection must be specified as key value" +
                                " pairs.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "null"),
                @Parameter(name = "jndi.resource",
                        description = "The name of the JNDI resource through which the connection is attempted. " +
                                "If this is found, the pool properties described above are not taken into account, " +
                                "and the connection is attempted via JNDI lookup instead.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "null"),
                @Parameter(name = "table.name",
                        description = "The name with which the event table should be persisted in the store. If no " +
                                "name is specified via this parameter, the event table is persisted with the same " +
                                "name as the Siddhi table.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "The table name defined in the Siddhi App query."),
                @Parameter(name = "field.length",
                        description = "The number of characters that the values for fields of the `STRING` type in " +
                                "the table definition must contain. If this is not specified, the default number of " +
                                "characters specific to the database type is considered.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "null")
        },
        examples = {
                @Example(
                        syntax = "@Store(type=\"rdbms\", jdbc.url=\"jdbc:mysql://localhost:3306/das\", " +
                                "username=\"root\", password=\"root\" , jdbc.driver.name=\"org.h2.Driver\"," +
                                "field.length=\"symbol:100\")\n" +
                                "@PrimaryKey(\"symbol\")" +
                                "@Index(\"volume\")" +
                                "define table StockTable (symbol string, price float, volume long);",
                        description = "The above example creates an event table named `StockTable` on the DB if " +
                                "it does not already exist (with 3 attributes named `symbol`, `price`, and `volume` " +
                                "of the types types `string`, `float` and `long` respectively). The connection is " +
                                "made as specified by the parameters configured for the '@Store' annotation. The " +
                                "`symbol` attribute is considered a unique field, and a DB index is created for it."
                )
        },
        systemParameter = {
                @SystemParameter(
                        name = "{{RDBMS-Name}}.maxVersion",
                        description = "The latest version supported for {{RDBMS-Name}}.",
                        defaultValue = "0",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.minVersion",
                        description = "The earliest version supported for {{RDBMS-Name}}.",
                        defaultValue = "0",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.tableCheckQuery",
                        description = "The template query for the `check table` operation in {{RDBMS-Name}}.",
                        defaultValue = "<b>H2</b>: CREATE TABLE {{TABLE_NAME}} ({{COLUMNS, PRIMARY_KEYS}})<br>" +
                                "<b>MySQL</b>: CREATE TABLE {{TABLE_NAME}} ({{COLUMNS, PRIMARY_KEYS}})<br>" +
                                "<b>Oracle</b>: CREATE TABLE {{TABLE_NAME}} ({{COLUMNS, PRIMARY_KEYS}})<br>" +
                                "<b>Microsoft SQL Server</b>: CREATE TABLE {{TABLE_NAME}} ({{COLUMNS, " +
                                "PRIMARY_KEYS}})<br>" +
                                "<b>PostgreSQL</b>: CREATE TABLE {{TABLE_NAME}} ({{COLUMNS, PRIMARY_KEYS}})<br>" +
                                "<b>DB2.*</b>: CREATE TABLE {{TABLE_NAME}} ({{COLUMNS, PRIMARY_KEYS}})",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.tableCreateQuery",
                        description = "The template query for the `create table` operation in {{RDBMS-Name}}.",
                        defaultValue = "<b>H2</b>: SELECT 1 FROM {{TABLE_NAME}} LIMIT 1<br>" +
                                "<b>MySQL</b>: SELECT 1 FROM {{TABLE_NAME}} LIMIT 1<br>" +
                                "<b>Oracle</b>: SELECT 1 FROM {{TABLE_NAME}} WHERE rownum=1<br>" +
                                "<b>Microsoft SQL Server</b>: SELECT TOP 1 1 from {{TABLE_NAME}}<br>" +
                                "<b>PostgreSQL</b>: SELECT 1 FROM {{TABLE_NAME}} LIMIT 1<br>" +
                                "<b>DB2.*</b>: SELECT 1 FROM {{TABLE_NAME}} FETCH FIRST 1 ROWS ONLY",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.indexCreateQuery",
                        description = "The template query for the `create index` operation in {{RDBMS-Name}}.",
                        defaultValue = "<b>H2</b>: CREATE INDEX {{TABLE_NAME}}_INDEX ON {{TABLE_NAME}} " +
                                "({{INDEX_COLUMNS}})" +
                                "<br>" +
                                "<b>MySQL</b>: CREATE INDEX {{TABLE_NAME}}_INDEX ON {{TABLE_NAME}} " +
                                "({{INDEX_COLUMNS}})<br>" +
                                "<b>Oracle</b>: CREATE INDEX {{TABLE_NAME}}_INDEX ON {{TABLE_NAME}} " +
                                "({{INDEX_COLUMNS}})<br>" +
                                "<b>Microsoft SQL Server</b>: CREATE INDEX {{TABLE_NAME}}_INDEX ON {{TABLE_NAME}} " +
                                "({{INDEX_COLUMNS}}) {{TABLE_NAME}} ({{INDEX_COLUMNS}})<br>" +
                                "<b>PostgreSQL</b>: CREATE INDEX {{TABLE_NAME}}_INDEX ON {{TABLE_NAME}} " +
                                "({{INDEX_COLUMNS}})<br>" +
                                "<b>DB2.*</b>: CREATE INDEX {{TABLE_NAME}}_INDEX ON {{TABLE_NAME}} ({{INDEX_COLUMNS}})",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.recordInsertQuery",
                        description = "The template query for the `insert record` operation in {{RDBMS-Name}}.",
                        defaultValue = "<b>H2</b>: INSERT INTO {{TABLE_NAME}} VALUES ({{Q}})<br>" +
                                "<b>MySQL</b>: INSERT INTO {{TABLE_NAME}} VALUES ({{Q}})<br>" +
                                "<b>Oracle</b>: INSERT INTO {{TABLE_NAME}} VALUES ({{Q}})<br>" +
                                "<b>Microsoft SQL Server</b>: INSERT INTO {{TABLE_NAME}} VALUES ({{Q}})<br>" +
                                "<b>PostgreSQL</b>: INSERT INTO {{TABLE_NAME}} VALUES ({{Q}})<br>" +
                                "<b>DB2.*</b>: INSERT INTO {{TABLE_NAME}} VALUES ({{Q}})",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.recordUpdateQuery",
                        description = "The template query for the `update record` operation in {{RDBMS-Name}}.",
                        defaultValue = "<b>H2</b>: UPDATE {{TABLE_NAME}} SET {{COLUMNS_AND_VALUES}} {{CONDITION}}<br>" +
                                "<b>MySQL</b>: UPDATE {{TABLE_NAME}} SET {{COLUMNS_AND_VALUES}} {{CONDITION}}<br>" +
                                "<b>Oracle</b>: UPDATE {{TABLE_NAME}} SET {{COLUMNS_AND_VALUES}} {{CONDITION}}<br>" +
                                "<b>Microsoft SQL Server</b>: UPDATE {{TABLE_NAME}} SET {{COLUMNS_AND_VALUES}} " +
                                "{{CONDITION}}<br>" +
                                "<b>PostgreSQL</b>: UPDATE {{TABLE_NAME}} SET {{COLUMNS_AND_VALUES}} " +
                                "{{CONDITION}}<br>" +
                                "<b>DB2.*</b>: UPDATE {{TABLE_NAME}} SET {{COLUMNS_AND_VALUES}} {{CONDITION}}",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.recordSelectQuery",
                        description = "The template query for the `select record` operation in {{RDBMS-Name}}.",
                        defaultValue = "<b>H2</b>: SELECT * FROM {{TABLE_NAME}} {{CONDITION}}<br>" +
                                "<b>MySQL</b>: SELECT * FROM {{TABLE_NAME}} {{CONDITION}}<br>" +
                                "<b>Oracle</b>: SELECT * FROM {{TABLE_NAME}} {{CONDITION}}<br>" +
                                "<b>Microsoft SQL Server</b>: SELECT * FROM {{TABLE_NAME}} {{CONDITION}}<br>" +
                                "<b>PostgreSQL</b>: SELECT * FROM {{TABLE_NAME}} {{CONDITION}}<br>" +
                                "<b>DB2.*</b>: SELECT * FROM {{TABLE_NAME}} {{CONDITION}}",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.recordExistsQuery",
                        description = "The template query for the `check record existence` operation in " +
                                "{{RDBMS-Name}}.",
                        defaultValue = "<b>H2</b>: SELECT TOP 1 1 FROM {{TABLE_NAME}} {{CONDITION}}<br>" +
                                "<b>MySQL</b>: SELECT 1 FROM {{TABLE_NAME}} {{CONDITION}}<br>" +
                                "<b>Oracle</b>: SELECT COUNT(1) INTO existence FROM {{TABLE_NAME}} {{CONDITION}}<br>" +
                                "<b>Microsoft SQL Server</b>: SELECT TOP 1 FROM {{TABLE_NAME}} {{CONDITION}}<br>" +
                                "<b>PostgreSQL</b>: SELECT 1 FROM {{TABLE_NAME}} {{CONDITION}} LIMIT 1<br>" +
                                "<b>DB2.*</b>: SELECT 1 FROM {{TABLE_NAME}} {{CONDITION}} FETCH FIRST 1 ROWS ONLY",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.recordDeleteQuery",
                        description = "The query for the `delete record` operation in {{RDBMS-Name}}.",
                        defaultValue = "<b>H2</b>: DELETE FROM {{TABLE_NAME}} {{CONDITION}}<br>" +
                                "<b>MySQL</b>: DELETE FROM {{TABLE_NAME}} {{CONDITION}}<br>" +
                                "<b>Oracle</b>: DELETE FROM {{TABLE_NAME}} {{CONDITION}}<br>" +
                                "<b>Microsoft SQL Server</b>: DELETE FROM {{TABLE_NAME}} {{CONDITION}}<br>" +
                                "<b>PostgreSQL</b>: DELETE FROM {{TABLE_NAME}} {{CONDITION}}<br>" +
                                "<b>DB2.*</b>: DELETE FROM {{TABLE_NAME}} {{CONDITION}}",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.stringSize",
                        description = "This defines the length for the string fields in {{RDBMS-Name}}.",
                        defaultValue = "<b>H2</b>: 254<br>" +
                                "<b>MySQL</b>: 254<br>" +
                                "<b>Oracle</b>: 254<br>" +
                                "<b>Microsoft SQL Server</b>: 254<br>" +
                                "<b>PostgreSQL</b>: 254<br>" +
                                "<b>DB2.*</b>: 254",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.batchSize",
                        description = "This defines the batch size when operations are performed for batches of " +
                                "events.",
                        defaultValue = "<b>H2</b>: 1000<br>" +
                                "<b>MySQL</b>: 1000<br>" +
                                "<b>Oracle</b>: 1000<br>" +
                                "<b>Microsoft SQL Server</b>: 1000<br>" +
                                "<b>PostgreSQL</b>: 1000<br>" +
                                "<b>DB2.*</b>: 1000",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.batchEnable",
                        description = "This specifies whether `Update` and `Insert` operations can be performed for" +
                                " batches of events or not.",
                        defaultValue = "<b>H2</b>: true<br>" +
                                "<b>MySQL</b>: true<br>" +
                                "<b>Oracle</b>: true<br>" +
                                "<b>Microsoft SQL Server</b>: true<br>" +
                                "<b>PostgreSQL</b>: true<br>" +
                                "<b>DB2.*</b>: true",
                        possibleParameters = "N/A"
                )
        }
)
public class RDBMSEventTable extends AbstractRecordTable {

    private static final Log log = LogFactory.getLog(RDBMSEventTable.class);
    private RDBMSQueryConfigurationEntry queryConfigurationEntry;
    private HikariDataSource dataSource;
    private String tableName;
    private List<Attribute> attributes;
    private ConfigReader configReader;
    private String jndiResourceName;
    private Annotation storeAnnotation;
    private Annotation primaryKeys;
    private Annotation indices;
    private String selectQuery;
    private String containsQuery;
    private String deleteQuery;
    private String insertQuery;
    private String recordUpdateQuery;
    private String tableCheckQuery;
    private String createQuery;
    private String indexQuery;
    private int batchSize;
    private boolean batchEnable;
    private String binaryType;
    private String booleanType;
    private String doubleType;
    private String floatType;
    private String integerType;
    private String longType;
    private String stringType;
    private String stringSize;

    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        attributes = tableDefinition.getAttributeList();
        storeAnnotation = AnnotationHelper.getAnnotation(ANNOTATION_STORE, tableDefinition.getAnnotations());
        primaryKeys = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_PRIMARY_KEY,
                tableDefinition.getAnnotations());
        indices = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_INDEX,
                tableDefinition.getAnnotations());
        RDBMSTableUtils.validateAnnotation(primaryKeys);
        RDBMSTableUtils.validateAnnotation(indices);
        jndiResourceName = storeAnnotation.getElement(ANNOTATION_ELEMENT_JNDI_RESOURCE);
        if (null != configReader) {
            this.configReader = configReader;
        } else {
            this.configReader = new DefaultConfigReader();
        }
        String tableName = storeAnnotation.getElement(ANNOTATION_ELEMENT_TABLE_NAME);
        this.tableName = RDBMSTableUtils.isEmpty(tableName) ? tableDefinition.getId() : tableName;
    }

    @Override
    protected void add(List<Object[]> records) {
        String sql = this.composeInsertQuery();
        try {
            this.batchExecuteQueriesWithRecords(sql, records, false);
        } catch (SQLException e) {
            throw new RDBMSTableException("Error in adding events to '" + this.tableName + "' store: "
                    + e.getMessage(), e);
        }
    }

    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition) {
        String condition = ((RDBMSCompiledCondition) compiledCondition).getCompiledQuery();
        //Some databases does not support single condition on where clause.
        //(atomic condition on where clause: SELECT * FROM TABLE WHERE true)
        //If the compile condition is resolved for '?', atomicCondition boolean value
        // will be used for ignore condition resolver.
        boolean atomicCondition = false;
        if (condition.equals(QUESTION_MARK)) {
            atomicCondition = true;
            if (log.isDebugEnabled()) {
                log.debug("Ignore the condition resolver in 'find()' method for compile " +
                        "condition: '" + QUESTION_MARK + "'");
            }
        }
        Connection conn = this.getConnection();
        PreparedStatement stmt = null;
        ResultSet rs;
        try {
            stmt = RDBMSTableUtils.isEmpty(condition) | atomicCondition ?
                    conn.prepareStatement(selectQuery.replace(PLACEHOLDER_CONDITION, "")) :
                    conn.prepareStatement(RDBMSTableUtils.formatQueryWithCondition(selectQuery, condition));
            if (!atomicCondition) {
                RDBMSTableUtils.resolveCondition(stmt, (RDBMSCompiledCondition) compiledCondition,
                        findConditionParameterMap, 0);
            }
            rs = stmt.executeQuery();
            //Passing all java.sql artifacts to the iterator to ensure everything gets cleaned up at once.
            return new RDBMSIterator(conn, stmt, rs, this.attributes, this.tableName);
        } catch (SQLException e) {
            RDBMSTableUtils.cleanupConnection(null, stmt, conn);
            throw new RDBMSTableException("Error retrieving records from table '" + this.tableName + "': "
                    + e.getMessage(), e);
        }
    }

    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap,
                               CompiledCondition compiledCondition) {
        String condition = ((RDBMSCompiledCondition) compiledCondition).getCompiledQuery();
        Connection conn = this.getConnection();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = RDBMSTableUtils.isEmpty(condition) ?
                    conn.prepareStatement(containsQuery.replace(PLACEHOLDER_CONDITION, "")) :
                    conn.prepareStatement(RDBMSTableUtils.formatQueryWithCondition(containsQuery, condition));
            RDBMSTableUtils.resolveCondition(stmt, (RDBMSCompiledCondition) compiledCondition,
                    containsConditionParameterMap, 0);
            rs = stmt.executeQuery();
            return rs.next();
        } catch (SQLException e) {
            throw new RDBMSTableException("Error performing a contains check on table '" + this.tableName
                    + "': " + e.getMessage(), e);
        } finally {
            RDBMSTableUtils.cleanupConnection(rs, stmt, conn);
        }
    }

    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps,
                          CompiledCondition compiledCondition) {
        this.batchProcessDelete(deleteConditionParameterMaps, compiledCondition);
    }

    private void batchProcessDelete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition
            compiledCondition) {
        String condition = ((RDBMSCompiledCondition) compiledCondition).getCompiledQuery();
        Connection conn = this.getConnection();
        PreparedStatement stmt = null;
        try {
            stmt = RDBMSTableUtils.isEmpty(condition) ?
                    conn.prepareStatement(deleteQuery.replace(PLACEHOLDER_CONDITION, "")) :
                    conn.prepareStatement(RDBMSTableUtils.formatQueryWithCondition(deleteQuery, condition));
            int counter = 0;
            for (Map<String, Object> deleteConditionParameterMap : deleteConditionParameterMaps) {
                RDBMSTableUtils.resolveCondition(stmt, (RDBMSCompiledCondition) compiledCondition,
                        deleteConditionParameterMap, 0);
                stmt.addBatch();
                counter++;
                if (counter == batchSize) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                    counter = 0;
                }
            }
            if (counter > 0) {
                stmt.executeBatch();
            }
        } catch (SQLException e) {
            throw new RDBMSTableException("Error performing record deletion on table '" + this.tableName
                    + "': " + e.getMessage(), e);
        } finally {
            RDBMSTableUtils.cleanupConnection(null, stmt, conn);
        }
    }

    @Override
    protected void update(CompiledCondition compiledCondition, List<Map<String, Object>> updateConditionParameterMaps,
                          Map<String, CompiledExpression> updateSetExpressions,
                          List<Map<String, Object>> updateValues)
            throws ConnectionUnavailableException {
        String sql = this.composeUpdateQuery(compiledCondition, updateSetExpressions);
        this.batchProcessSQLUpdates(sql, updateConditionParameterMaps, compiledCondition,
                updateSetExpressions, updateValues);
    }


    /**
     * Method for processing update operations in a batched manner. This assumes that all update operations will be
     * accepted by the database.
     *  @param sql                          the SQL update operation as string.
     * @param updateConditionParameterMaps the runtime parameters that should be populated to the condition.
     * @param compiledCondition            the condition that was built during compile time.
     * @param updateSetExpressions
     * @param updateSetParameterMaps       the runtime parameters that should be populated to the update statement.
     */
    private void batchProcessSQLUpdates(String sql, List<Map<String, Object>> updateConditionParameterMaps,
                                        CompiledCondition compiledCondition,
                                        Map<String, CompiledExpression> updateSetExpressions,
                                        List<Map<String, Object>> updateSetParameterMaps) {
        int counter = 0;
        Connection conn = this.getConnection();
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(sql);
            Iterator<Map<String, Object>> conditionParamIterator = updateConditionParameterMaps.iterator();
            Iterator<Map<String, Object>> updateSetParameterMapsIterator = updateSetParameterMaps.iterator();
            while (conditionParamIterator.hasNext() && updateSetParameterMapsIterator.hasNext()) {
                Map<String, Object> conditionParameters = conditionParamIterator.next();
                Map<String, Object> updateSetMap = updateSetParameterMapsIterator.next();
                int ordinal = 1;
                for (Map.Entry<String, CompiledExpression> assignmentEntry :
                        updateSetExpressions.entrySet()) {
                    for (Map.Entry<Integer, Object> parameterEntry :
                            ((RDBMSCompiledCondition) assignmentEntry.getValue()).getParameters().entrySet()) {
                        Object parameter = parameterEntry.getValue();
                        if (parameter instanceof Constant) {
                            Constant constant = (Constant) parameter;
                            RDBMSTableUtils.populateStatementWithSingleElement(stmt, ordinal, constant.getType(),
                                    constant.getValue());
                        } else {
                            Attribute variable = (Attribute) parameter;
                            RDBMSTableUtils.populateStatementWithSingleElement(stmt, ordinal, variable.getType(),
                                    updateSetMap.get(variable.getName()));
                        }
                        ordinal++;
                    }
                }

                //Incrementing the ordinals of the conditions in the statement with the # of variables to be updated
                RDBMSTableUtils.resolveCondition(stmt, (RDBMSCompiledCondition) compiledCondition,
                        conditionParameters, ordinal - 1);
                stmt.addBatch();
                counter++;
                if (counter == batchSize) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                    counter = 0;
                }
            }
            if (counter > 0) {
                stmt.executeBatch();
            }
        } catch (SQLException e) {
            throw new RDBMSTableException("Error performing record update operations on table '" + this.tableName
                    + "': " + e.getMessage(), e);
        } finally {
            RDBMSTableUtils.cleanupConnection(null, stmt, conn);
        }
    }

    @Override
    protected void updateOrAdd(CompiledCondition compiledCondition,
                               List<Map<String, Object>> updateConditionParameterMaps,
                               Map<String, CompiledExpression> updateSetExpressions,
                               List<Map<String, Object>> updateSetParameterMaps, List<Object[]> addingRecords)
            throws ConnectionUnavailableException {
        List<Integer> recordInsertIndexList;
        if (batchEnable) {
            recordInsertIndexList = batchProcessUpdate(updateConditionParameterMaps, compiledCondition,
                    updateSetExpressions, updateSetParameterMaps);
        } else {
            recordInsertIndexList = sequentialProcessUpdate(updateConditionParameterMaps, compiledCondition,
                    updateSetExpressions, updateSetParameterMaps);
        }
        batchProcessInsert(addingRecords, recordInsertIndexList);
    }

    private List<Integer> batchProcessUpdate(List<Map<String, Object>> updateConditionParameterMaps,
                                             CompiledCondition compiledCondition,
                                             Map<String, CompiledExpression> updateSetExpressions,
                                             List<Map<String, Object>> updateSetParameterMaps) {
        int counter = 0;
        Connection conn = this.getConnection();
        PreparedStatement updateStmt = null;
        List<Integer> recordInsertIndexList = new ArrayList<>();
        try {
            updateStmt = conn.prepareStatement(this.composeUpdateQuery(compiledCondition,
                    updateSetExpressions));
            Iterator<Map<String, Object>> conditionParamIterator = updateConditionParameterMaps.iterator();
            Iterator<Map<String, Object>> updateSetMapIterator = updateSetParameterMaps.iterator();
            while (conditionParamIterator.hasNext() && updateSetMapIterator.hasNext()) {
                Map<String, Object> conditionParameters = conditionParamIterator.next();
                Map<String, Object> updateSetMap = updateSetMapIterator.next();

                int ordinal = 1;
                for (Map.Entry<String, CompiledExpression> assignmentEntry :
                        updateSetExpressions.entrySet()) {
                    for (Map.Entry<Integer, Object> parameterEntry :
                            ((RDBMSCompiledCondition) assignmentEntry.getValue()).getParameters().entrySet()) {
                        Object parameter = parameterEntry.getValue();
                        if (parameter instanceof Constant) {
                            Constant constant = (Constant) parameter;
                            RDBMSTableUtils.populateStatementWithSingleElement(updateStmt, ordinal, constant.getType(),
                                    constant.getValue());
                        } else {
                            Attribute variable = (Attribute) parameter;
                            RDBMSTableUtils.populateStatementWithSingleElement(updateStmt, ordinal, variable.getType(),
                                    updateSetMap.get(variable.getName()));
                        }
                        ordinal++;
                    }
                }

                //Incrementing the ordinals of the conditions in the statement with the # of variables to be updated
                RDBMSTableUtils.resolveCondition(updateStmt, (RDBMSCompiledCondition) compiledCondition,
                        conditionParameters, ordinal - 1);
                updateStmt.addBatch();
                if (counter % batchSize == batchSize - 1) {
                    recordInsertIndexList.addAll(this.filterRequiredInsertIndex(updateStmt.executeBatch(),
                            (counter - batchSize)));
                    updateStmt.clearBatch();
                }
                counter++;
            }
            if (counter % batchSize > 0) {
                recordInsertIndexList.addAll(this.filterRequiredInsertIndex(updateStmt.executeBatch(),
                        (counter - (counter % batchSize))));
            }
        } catch (SQLException e) {
            throw new RDBMSTableException("Error performing update/insert operation (update) on table '"
                    + this.tableName + "': " + e.getMessage(), e);
        } finally {
            RDBMSTableUtils.cleanupConnection(null, updateStmt, null);
        }
        return recordInsertIndexList;
    }

    private List<Integer> sequentialProcessUpdate(List<Map<String, Object>> updateConditionParameterMaps,
                                                  CompiledCondition compiledCondition,
                                                  Map<String, CompiledExpression> updateSetExpressions,
                                                  List<Map<String, Object>> updateSetParameterMaps) {
        int counter = 0;
        final int seed = this.attributes.size();
        Connection conn = this.getConnection(false);
        PreparedStatement updateStmt = null;
        List<Integer> updateResultList = new ArrayList<>();
        try {
            updateStmt = conn.prepareStatement(this.composeUpdateQuery(compiledCondition,
                    updateSetExpressions));
            while (counter < updateSetParameterMaps.size()) {
                Map<String, Object> conditionParameters = updateConditionParameterMaps.get(counter);
                Map<String, Object> updateSetParameterMap = updateSetParameterMaps.get(counter);

                int ordinal = 1;
                for (Map.Entry<String, CompiledExpression> assignmentEntry :
                        updateSetExpressions.entrySet()) {
                    for (Map.Entry<Integer, Object> parameterEntry :
                            ((RDBMSCompiledCondition) assignmentEntry.getValue()).getParameters().entrySet()) {
                        Object parameter = parameterEntry.getValue();
                        if (parameter instanceof Constant) {
                            Constant constant = (Constant) parameter;
                            RDBMSTableUtils.populateStatementWithSingleElement(updateStmt, ordinal, constant.getType(),
                                    constant.getValue());
                        } else {
                            Attribute variable = (Attribute) parameter;
                            RDBMSTableUtils.populateStatementWithSingleElement(updateStmt, ordinal, variable.getType(),
                                    updateSetParameterMap.get(variable.getName()));
                        }
                        ordinal++;
                    }
                }

                //Incrementing the ordinals of the conditions in the statement with the # of variables to be updated
                RDBMSTableUtils.resolveCondition(updateStmt, (RDBMSCompiledCondition) compiledCondition,
                        conditionParameters, seed);
                int isUpdate = updateStmt.executeUpdate();
                conn.commit();
                if (isUpdate < 1) {
                    updateResultList.add(counter);
                }
                counter++;
            }
        } catch (SQLException e) {
            throw new RDBMSTableException("Error performing update/insert operation (update) on table '"
                    + this.tableName
                    + "': " + e.getMessage(), e);
        } finally {
            RDBMSTableUtils.cleanupConnection(null, updateStmt, null);
        }
        return updateResultList;
    }

    private void batchProcessInsert(List<Object[]> addingRecords, List<Integer> recordInsertIndexList) {
        int counter = 0;
        Connection conn = this.getConnection(false);
        PreparedStatement insertStmt = null;
        try {
            insertStmt = conn.prepareStatement(this.composeInsertQuery());
            while (counter < recordInsertIndexList.size()) {
                if (recordInsertIndexList.get(counter) == counter) {
                    Object[] record = addingRecords.get(counter);
                    this.populateStatement(record, insertStmt);
                    try {
                        insertStmt.addBatch();
                        if (counter % batchSize == (batchSize - 1)) {
                            insertStmt.executeBatch();
                            conn.commit();
                            insertStmt.clearBatch();
                        }
                    } catch (SQLException e2) {
                        RDBMSTableUtils.rollbackConnection(conn);
                        throw new RDBMSTableException("Error performing update/insert operation (insert) on table '"
                                + this.tableName + "': " + e2.getMessage(), e2);
                    }
                }
                counter++;
            }
            if (counter % batchSize > 0) {
                insertStmt.executeBatch();
                conn.commit();
            }
        } catch (SQLException e) {
            throw new RDBMSTableException("Error performing update/insert operation (update) on table '"
                    + this.tableName
                    + "': " + e.getMessage(), e);
        } finally {
            RDBMSTableUtils.cleanupConnection(null, insertStmt, null);
        }
    }

    private List<Integer> filterRequiredInsertIndex(int[] updateResultIndex, int lastUpdatedRecordIndex) {
        List<Integer> insertIndexList = new ArrayList<Integer>();
        int currentRecodeIndex = lastUpdatedRecordIndex;
        for (int i = 0; i < updateResultIndex.length; i++) {
            //Filter update result index and adding to list.
            currentRecodeIndex += i;
            if (updateResultIndex[i] < 1) {
                insertIndexList.add(currentRecodeIndex);
            }
        }
        return insertIndexList;
    }

    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        RDBMSConditionVisitor visitor = new RDBMSConditionVisitor(this.tableName);
        expressionBuilder.build(visitor);
        return new RDBMSCompiledCondition(visitor.returnCondition(), visitor.getParameters());
    }


    @Override
    protected CompiledCondition compileSetAttribute(ExpressionBuilder expressionBuilder) {
        return compileCondition(expressionBuilder);
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        try {
            if (dataSource == null) {
                if (!RDBMSTableUtils.isEmpty(jndiResourceName)) {
                    this.lookupDatasource(jndiResourceName);
                } else {
                    this.initializeDatasource(storeAnnotation);
                }
                if (this.queryConfigurationEntry == null) {
                    this.queryConfigurationEntry = RDBMSTableUtils.lookupCurrentQueryConfigurationEntry(this.dataSource,
                            this.configReader);
                    selectQuery = this.resolveTableName(configReader.readConfig(
                            this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR + RECORD_SELECT_QUERY,
                            this.queryConfigurationEntry.getRecordSelectQuery()));
                    containsQuery = this.resolveTableName(configReader.readConfig(
                            this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR + RECORD_EXISTS_QUERY,
                            this.queryConfigurationEntry.getRecordExistsQuery()));
                    deleteQuery = this.resolveTableName(configReader.readConfig(
                            this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR + RECORD_DELETE_QUERY,
                            this.queryConfigurationEntry.getRecordDeleteQuery()));
                    batchSize = Integer.parseInt(configReader.readConfig(
                            this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR + BATCH_SIZE,
                            String.valueOf(this.queryConfigurationEntry.getBatchSize())));
                    insertQuery = this.resolveTableName(configReader.readConfig(
                            this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR + RECORD_INSERT_QUERY,
                            this.queryConfigurationEntry.getRecordInsertQuery()));
                    recordUpdateQuery = this.resolveTableName(configReader.readConfig(
                            this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR + RECORD_UPDATE_QUERY,
                            this.queryConfigurationEntry.getRecordUpdateQuery()));
                    tableCheckQuery = this.resolveTableName(configReader.readConfig(
                            this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR + TABLE_CHECK_QUERY,
                            this.queryConfigurationEntry.getTableCheckQuery()));
                    createQuery = this.resolveTableName(configReader.readConfig(
                            this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR + TABLE_CREATE_QUERY,
                            this.queryConfigurationEntry.getTableCreateQuery()));
                    indexQuery = this.resolveTableName(configReader.readConfig(
                            this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR + INDEX_CREATE_QUERY,
                            this.queryConfigurationEntry.getIndexCreateQuery()));
                    batchEnable = Boolean.parseBoolean(configReader.readConfig(
                            this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR +
                                    BATCH_ENABLE, String.valueOf(this.queryConfigurationEntry.getBatchEnable())));
                    RDBMSTypeMapping typeMapping = this.queryConfigurationEntry.getRdbmsTypeMapping();
                    booleanType = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                    PROPERTY_SEPARATOR + TYPE_MAPPING + PROPERTY_SEPARATOR + BOOLEAN_TYPE,
                            typeMapping.getBooleanType());
                    doubleType = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                    PROPERTY_SEPARATOR + TYPE_MAPPING + PROPERTY_SEPARATOR + DOUBLE_TYPE,
                            typeMapping.getDoubleType());
                    floatType = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                    PROPERTY_SEPARATOR + TYPE_MAPPING + PROPERTY_SEPARATOR + FLOAT_TYPE,
                            typeMapping.getFloatType());
                    integerType = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                    PROPERTY_SEPARATOR + TYPE_MAPPING + PROPERTY_SEPARATOR + INTEGER_TYPE,
                            typeMapping.getIntegerType());
                    longType = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                    PROPERTY_SEPARATOR + TYPE_MAPPING + PROPERTY_SEPARATOR + LONG_TYPE,
                            typeMapping.getLongType());
                    binaryType = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                    PROPERTY_SEPARATOR + TYPE_MAPPING + PROPERTY_SEPARATOR + BINARY_TYPE,
                            typeMapping.getBinaryType());
                    stringType = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                    PROPERTY_SEPARATOR + TYPE_MAPPING + PROPERTY_SEPARATOR + STRING_TYPE,
                            typeMapping.getStringType());
                    stringSize = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                    PROPERTY_SEPARATOR + STRING_SIZE,
                            this.queryConfigurationEntry.getStringSize());
                }
            }
            if (!this.tableExists()) {
                this.createTable(storeAnnotation, primaryKeys, indices);
                log.info("A table: " + this.tableName + " is created with the provided information.");
            }
        } catch (CannotLoadConfigurationException | NamingException | HikariPool.PoolInitializationException |
                RDBMSTableException e) {
            this.destroy();
            throw new ConnectionUnavailableException("Failed to initialize store for table name '" +
                    this.tableName + "': " + e.getMessage(), e);
        }
    }

    @Override
    public void disconnect() {

    }

    @Override
    public void destroy() {
        if (dataSource != null) {
            dataSource.close();
            if (log.isDebugEnabled()) {
                log.debug("Closing the pool name: " + dataSource.getPoolName());
            }
        }
    }

    /**
     * Method for looking up a datasource instance through JNDI.
     *
     * @param resourceName the name of the resource to be looked up.
     * @throws NamingException if the lookup fails.
     */
    private void lookupDatasource(String resourceName) throws NamingException {
        this.dataSource = InitialContext.doLookup(resourceName);
    }

    /**
     * Method for composing the SQL query for INSERT operations with proper placeholders.
     *
     * @return the composed SQL query in string form.
     */
    private String composeInsertQuery() {
        StringBuilder params = new StringBuilder();
        int fieldsLeft = this.attributes.size();
        while (fieldsLeft > 0) {
            params.append(QUESTION_MARK);
            if (fieldsLeft > 1) {
                params.append(SEPARATOR);
            }
            fieldsLeft = fieldsLeft - 1;
        }
        return insertQuery.replace(PLACEHOLDER_Q, params.toString());
    }

    /**
     * Method for composing the SQL query for UPDATE operations with proper placeholders.
     *
     * @return the composed SQL query in string form.
     */
    private String composeUpdateQuery(CompiledCondition compiledCondition,
                                      Map<String, CompiledExpression> updateSetExpressions) {
        String condition = ((RDBMSCompiledCondition) compiledCondition).getCompiledQuery();
        String result = updateSetExpressions.entrySet().stream().map(e -> e.getKey()
                + " = " + ((RDBMSCompiledCondition) e.getValue()).getCompiledQuery())
                .collect(Collectors.joining(", "));
        recordUpdateQuery = recordUpdateQuery.replace(PLACEHOLDER_COLUMNS_VALUES, result);

        recordUpdateQuery = RDBMSTableUtils.isEmpty(condition) ? recordUpdateQuery.replace(PLACEHOLDER_CONDITION, "") :
                RDBMSTableUtils.formatQueryWithCondition(recordUpdateQuery, condition);
        return recordUpdateQuery;
    }

    /**
     * Method for creating and initializing the datasource instance given the "@Store" annotation.
     *
     * @param storeAnnotation the source annotation which contains the needed parameters.
     */
    private void initializeDatasource(Annotation storeAnnotation) {
        Properties connectionProperties = new Properties();
        String poolPropertyString = storeAnnotation.getElement(ANNOTATION_ELEMENT_POOL_PROPERTIES);
        String url = storeAnnotation.getElement(ANNOTATION_ELEMENT_URL);
        String username = storeAnnotation.getElement(ANNOTATION_ELEMENT_USERNAME);
        String password = storeAnnotation.getElement(ANNOTATION_ELEMENT_PASSWORD);
        String driverClassName = storeAnnotation.getElement(ANNOTATION_DRIVER_CLASS_NAME);
        if (RDBMSTableUtils.isEmpty(url)) {
            throw new RDBMSTableException("Required parameter '" + ANNOTATION_ELEMENT_URL + "' for DB " +
                    "connectivity cannot be empty.");
        }
        if (RDBMSTableUtils.isEmpty(username)) {
            throw new RDBMSTableException("Required parameter '" + ANNOTATION_ELEMENT_USERNAME + "' for DB " +
                    "connectivity cannot be empty.");
        }
        if (RDBMSTableUtils.isEmpty(password)) {
            throw new RDBMSTableException("Required parameter '" + ANNOTATION_ELEMENT_PASSWORD + "' for DB " +
                    "connectivity cannot be empty.");
        }
        if (RDBMSTableUtils.isEmpty(driverClassName)) {
            throw new RDBMSTableException("Required parameter '" + ANNOTATION_DRIVER_CLASS_NAME + "' for DB " +
                    "connectivity cannot be empty.");
        }
        connectionProperties.setProperty("jdbcUrl", url);
        connectionProperties.setProperty("dataSource.user", username);
        connectionProperties.setProperty("dataSource.password", password);
        connectionProperties.setProperty("driverClassName", driverClassName);
        if (poolPropertyString != null) {
            List<String[]> poolProps = RDBMSTableUtils.processKeyValuePairs(poolPropertyString);
            poolProps.forEach(pair -> connectionProperties.setProperty(pair[0], pair[1]));
        }
        HikariConfig config = new HikariConfig(connectionProperties);
        this.dataSource = new HikariDataSource(config);
    }

    /**
     * Returns a connection instance assuming that autocommit should be true.
     *
     * @return a new {@link Connection} instance from the datasource.
     */
    private Connection getConnection() {
        return this.getConnection(true);
    }

    /**
     * Returns a connection instance.
     *
     * @param autoCommit whether or not transactions to the connections should be committed automatically.
     * @return a new {@link Connection} instance from the datasource.
     */
    private Connection getConnection(boolean autoCommit) {
        Connection conn;
        try {
            conn = this.dataSource.getConnection();
            conn.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            throw new RDBMSTableException("Error initializing connection: " + e.getMessage(), e);
        }
        return conn;
    }

    /**
     * Method for replacing the placeholder for the table name with the Event Table's name.
     *
     * @param statement the SQL statement in string form.
     * @return the formatted SQL statement.
     */
    private String resolveTableName(String statement) {
        if (statement == null) {
            return null;
        }
        return statement.replace(PLACEHOLDER_TABLE_NAME, this.tableName);
    }

    /**
     * Method for creating a table on the data store in question, if it does not exist already.
     *
     * @param storeAnnotation the "@Store" annotation that contains the connection properties.
     * @param primaryKeys     the unique keys that should be set for the table.
     * @param indices         the DB indices that should be set for the table.
     */
    private void createTable(Annotation storeAnnotation, Annotation primaryKeys, Annotation indices) {
        StringBuilder builder = new StringBuilder();
        List<Element> primaryKeyList = (primaryKeys == null) ? new ArrayList<>() : primaryKeys.getElements();
        List<Element> indexElementList = (indices == null) ? new ArrayList<>() : indices.getElements();
        List<String> queries = new ArrayList<>();
        Map<String, String> fieldLengths = RDBMSTableUtils.processFieldLengths(storeAnnotation.getElement(
                ANNOTATION_ELEMENT_FIELD_LENGTHS));
        this.validateFieldLengths(fieldLengths);
        this.attributes.forEach(attribute -> {
            builder.append(attribute.getName()).append(WHITESPACE);
            switch (attribute.getType()) {
                case BOOL:
                    builder.append(booleanType);
                    break;
                case DOUBLE:
                    builder.append(doubleType);
                    break;
                case FLOAT:
                    builder.append(floatType);
                    break;
                case INT:
                    builder.append(integerType);
                    break;
                case LONG:
                    builder.append(longType);
                    break;
                case OBJECT:
                    builder.append(binaryType);
                    break;
                case STRING:
                    builder.append(stringType);
                    if (null != stringSize) {
                        builder.append(OPEN_PARENTHESIS);
                        builder.append(fieldLengths.getOrDefault(attribute.getName(), stringSize));
                        builder.append(CLOSE_PARENTHESIS);
                    }
                    break;
            }
            if (this.queryConfigurationEntry.isKeyExplicitNotNull()) {
                builder.append(WHITESPACE).append(SQL_NOT_NULL);
            }
            if (this.attributes.indexOf(attribute) != this.attributes.size() - 1 || !primaryKeyList.isEmpty()) {
                builder.append(SEPARATOR);
            }
        });
        if (primaryKeyList != null && !primaryKeyList.isEmpty()) {
            builder.append(SQL_PRIMARY_KEY_DEF)
                    .append(OPEN_PARENTHESIS)
                    .append(RDBMSTableUtils.flattenAnnotatedElements(primaryKeyList))
                    .append(CLOSE_PARENTHESIS);
        }
        queries.add(createQuery.replace(PLACEHOLDER_COLUMNS, builder.toString()));
        if (indexElementList != null && !indexElementList.isEmpty()) {
            queries.add(indexQuery.replace(PLACEHOLDER_INDEX,
                    RDBMSTableUtils.flattenAnnotatedElements(indexElementList)));
        }
        try {
            this.executeDDQueries(queries, false);
        } catch (SQLException e) {
            throw new RDBMSTableException("Unable to initialize table '" + this.tableName + "': " + e.getMessage(), e);
        }
    }

    /**
     * Method used to validate the field length specifications and ensure that the table definition contains them.
     *
     * @param fieldLengths the specified list of custom string field lengths.
     */
    private void validateFieldLengths(Map<String, String> fieldLengths) {
        List<String> attributeNames = new ArrayList<>();
        this.attributes.forEach(attribute -> attributeNames.add(attribute.getName()));
        fieldLengths.keySet().forEach(field -> {
            if (!attributeNames.contains(field)) {
                throw new RDBMSTableException("Field '" + field + "' (for which a size of " + fieldLengths.get(field)
                        + " has been specified) does not exist in the table's list of fields.");
            }
        });
    }

    /**
     * Method for performing data definition queries for the current datasource.
     *
     * @param queries    the list of queries to be executed.
     * @param autocommit whether or not the transactions should automatically be committed.
     * @throws SQLException if the query execution fails.
     */
    private void executeDDQueries(List<String> queries, boolean autocommit) throws SQLException {
        Connection conn = this.getConnection(autocommit);
        boolean committed = autocommit;
        PreparedStatement stmt;
        try {
            for (String query : queries) {
                stmt = conn.prepareStatement(query);
                stmt.execute();
                RDBMSTableUtils.cleanupConnection(null, stmt, null);
            }
            if (!autocommit) {
                conn.commit();
                committed = true;
            }
        } catch (SQLException e) {
            if (!autocommit) {
                RDBMSTableUtils.rollbackConnection(conn);
            }
            throw e;
        } finally {
            if (!committed) {
                RDBMSTableUtils.rollbackConnection(conn);
            }
            RDBMSTableUtils.cleanupConnection(null, null, conn);
        }
    }

    /**
     * Given a set of records and a query, this method performs that query per each record.
     *
     * @param query      the query to be executed.
     * @param records    the records to use.
     * @param autocommit whether or not the transactions should automatically be committed.
     * @throws SQLException if the query execution fails.
     */
    private void batchExecuteQueriesWithRecords(String query, List<Object[]> records, boolean autocommit)
            throws SQLException {
        PreparedStatement stmt = null;
        boolean committed = autocommit;
        Connection conn = this.getConnection(autocommit);
        try {
            stmt = conn.prepareStatement(query);
            for (Object[] record : records) {
                this.populateStatement(record, stmt);
                stmt.addBatch();
            }
            stmt.executeBatch();
            if (!autocommit) {
                conn.commit();
                committed = true;
            }
        } catch (SQLException e) {
            if (log.isDebugEnabled()) {
                log.debug("Attempted execution of query [" + query + "] produced an exception: " + e.getMessage());
            }
            if (!autocommit) {
                RDBMSTableUtils.rollbackConnection(conn);
            }
            throw e;
        } finally {
            if (!committed) {
                RDBMSTableUtils.rollbackConnection(conn);
            }
            RDBMSTableUtils.cleanupConnection(null, stmt, conn);
        }
    }

    /**
     * Method for checking whether or not the given table (which reflects the current event table instance) exists.
     *
     * @return true/false based on the table existence.
     */
    private boolean tableExists() {
        Connection conn = this.getConnection();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(tableCheckQuery);
            rs = stmt.executeQuery();
            return true;
        } catch (SQLException e) {
            if (log.isDebugEnabled()) {
                log.debug("Table '" + this.tableName + "' assumed to not exist since its existence check resulted "
                        + "in exception " + e.getMessage());
            }
            return false;
        } finally {
            RDBMSTableUtils.cleanupConnection(rs, stmt, conn);
        }
    }

    /**
     * Method for populating values to a pre-created SQL prepared statement.
     *
     * @param record the record whose values should be populated.
     * @param stmt   the statement to which the values should be set.
     */
    private void populateStatement(Object[] record, PreparedStatement stmt) {
        Attribute attribute = null;
        try {
            for (int i = 0; i < this.attributes.size(); i++) {
                attribute = this.attributes.get(i);
                Object value = record[i];
                if (value != null || attribute.getType() == Attribute.Type.STRING) {
                    RDBMSTableUtils.populateStatementWithSingleElement(stmt, i + 1, attribute.getType(), value);
                } else {
                    throw new RDBMSTableException("Cannot Execute Insert/Update: null value detected for " +
                            "attribute '" + attribute.getName() + "'");
                }
            }
        } catch (SQLException e) {
            throw new RDBMSTableException("Dropping event since value for attribute name " + attribute.getName() +
                    "cannot be set: " + e.getMessage(), e);
        }
    }

    private static class DefaultConfigReader implements ConfigReader {
        @Override
        public String readConfig(String name, String defaultValue) {
            return defaultValue;
        }

        @Override
        public Map<String, String> getAllConfigs() {
            return new HashMap<>();
        }
    }
}
