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
package io.siddhi.extension.store.rdbms;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.SystemParameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.exception.CannotLoadConfigurationException;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.DatabaseRuntimeException;
import io.siddhi.core.exception.QueryableRecordTableException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.table.record.AbstractQueryableRecordTable;
import io.siddhi.core.table.record.ExpressionBuilder;
import io.siddhi.core.table.record.RecordIterator;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledExpression;
import io.siddhi.core.util.collection.operator.CompiledSelection;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.extension.store.rdbms.config.RDBMSQueryConfigurationEntry;
import io.siddhi.extension.store.rdbms.config.RDBMSSelectQueryTemplate;
import io.siddhi.extension.store.rdbms.config.RDBMSTypeMapping;
import io.siddhi.extension.store.rdbms.exception.RDBMSTableException;
import io.siddhi.extension.store.rdbms.metrics.RDBMSMetrics;
import io.siddhi.extension.store.rdbms.metrics.RDBMSStatus;
import io.siddhi.extension.store.rdbms.util.RDBMSTableConstants;
import io.siddhi.extension.store.rdbms.util.RDBMSTableUtils;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.query.selection.OrderByAttribute;
import io.siddhi.query.api.util.AnnotationHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.wso2.carbon.datasource.core.api.DataSourceService;
import org.wso2.carbon.datasource.core.exception.DataSourceException;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.ANNOTATION_DRIVER_CLASS_NAME;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.ANNOTATION_ELEMENT_DATASOURCE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.ANNOTATION_ELEMENT_FIELD_LENGTHS;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.ANNOTATION_ELEMENT_JNDI_RESOURCE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.ANNOTATION_ELEMENT_PASSWORD;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.ANNOTATION_ELEMENT_POOL_PROPERTIES;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.ANNOTATION_ELEMENT_TABLE_CHECK_QUERY;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.ANNOTATION_ELEMENT_TABLE_NAME;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.ANNOTATION_ELEMENT_URL;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.ANNOTATION_ELEMENT_USERNAME;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.BATCH_ENABLE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.BATCH_SIZE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.BIG_STRING_TYPE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.BINARY_TYPE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.BOOLEAN_TYPE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.CLOSE_PARENTHESIS;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.COLLATION;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.DOUBLE_TYPE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.FIELD_SIZE_LIMIT;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.FLOAT_TYPE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.GROUP_BY_CLAUSE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.HAVING_CLAUSE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.INDEX_CREATE_QUERY;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.INTEGER_TYPE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.IS_LIMIT_BEFORE_OFFSET;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.LIMIT_CLAUSE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.LIMIT_WRAPPER_CLAUSE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.LONG_TYPE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.OFFSET_CLAUSE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.OFFSET_WRAPPER_CLAUSE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.OPEN_PARENTHESIS;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.ORDER_BY_CLAUSE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_COLUMNS;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_COLUMNS_FOR_CREATE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_COLUMNS_VALUES;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_CONDITION;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_INDEX;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_INDEX_NUMBER;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_INNER_QUERY;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_Q;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_SELECTORS;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_TABLE_NAME;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_VALUES;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.PROPERTY_SEPARATOR;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.QUERY_WRAPPER_CLAUSE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.QUESTION_MARK;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.RECORD_CONTAINS_CONDITION;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.RECORD_DELETE_QUERY;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.RECORD_EXISTS_QUERY;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.RECORD_INSERT_QUERY;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.RECORD_SELECT_QUERY;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.RECORD_UPDATE_QUERY;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.SELECT_CLAUSE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.SELECT_QUERY_TEMPLATE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.SELECT_QUERY_WITH_SUB_SELECT_TEMPLATE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.SEPARATOR;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.SQL_AND;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.SQL_AS;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.SQL_COLLATE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.SQL_MAX;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.SQL_NOT_NULL;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.SQL_PRIMARY_KEY_DEF;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.STRING_SIZE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.STRING_TYPE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.SUB_SELECT_QUERY_REF;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.TABLE_CHECK_QUERY;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.TABLE_CREATE_QUERY;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.TRANSACTION_SUPPORTED;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.TYPE_MAPPING;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.WHERE_CLAUSE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableConstants.WHITESPACE;
import static io.siddhi.extension.store.rdbms.util.RDBMSTableUtils.processFindConditionWithContainsConditionTemplate;

/**
 * Class representing the RDBMS Event Table implementation.
 */
@Extension(
        name = "rdbms",
        namespace = "store",
        description = "This extension assigns data sources and connection instructions to event tables. It also " +
                "implements read-write operations on connected data sources. A new improvement is added when running " +
                "with SI / SI Tooling 1.1.0 or higher product pack, where an external configuration file can be " +
                "provided to read supported RDBMS databases. " +
                "Prerequisites - Configuration file needed to be added to [Product_Home]/conf/siddhi/rdbms path with " +
                "the configuration file name as rdbms-table-config.xml , <database name=”[Database_Name]”> for each " +
                "database name should be the equivalent database product name returned from java sql " +
                "Connection.getMetaData().getDatabaseProductName() as shown in API documentation  " +
                "https://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html#getDatabaseProductName())." +
                "Sample Configuration for one of the databases can be as follows," +
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<rdbms-table-configuration>\n" +
                "<database name=\"Teradata\">\n" +
                "        <tableCreateQuery>CREATE TABLE {{TABLE_NAME}} ({{COLUMNS, PRIMARY_KEYS}})" +
                "</tableCreateQuery>\n" +
                "        <tableCheckQuery>SELECT 1 FROM {{TABLE_NAME}} SAMPLE 1</tableCheckQuery>\n" +
                "        <indexCreateQuery>CREATE INDEX {{TABLE_NAME}}_INDEX_{{INDEX_NUM}} ({{INDEX_COLUMNS}}) ON " +
                "{{TABLE_NAME}}\n" +
                "        </indexCreateQuery>\n" +
                "        <recordExistsQuery>SELECT 1 FROM {{TABLE_NAME}} {{CONDITION}} SAMPLE 1</recordExistsQuery>\n" +
                "        <recordSelectQuery>SELECT * FROM {{TABLE_NAME}} {{CONDITION}}</recordSelectQuery>\n" +
                "        <recordInsertQuery>INSERT INTO {{TABLE_NAME}} ({{COLUMNS}}) VALUES ({{Q}})" +
                "</recordInsertQuery>\n" +
                "        <recordUpdateQuery>UPDATE {{TABLE_NAME}} SET {{COLUMNS_AND_VALUES}} {{CONDITION}}" +
                "</recordUpdateQuery>\n" +
                "        <recordDeleteQuery>DELETE FROM {{TABLE_NAME}} {{CONDITION}}</recordDeleteQuery>\n" +
                "        <recordContainsCondition>({{COLUMNS}} LIKE {{VALUES}})</recordContainsCondition>\n" +
                "        <selectQueryTemplate>\n" +
                "            <selectClause>SELECT {{SELECTORS}} FROM {{TABLE_NAME}}</selectClause>\n" +
                "            <selectQueryWithSubSelect>SELECT {{SELECTORS}} FROM {{TABLE_NAME}}, ( {{INNER_QUERY}} ) " +
                "AS t2\n" +
                "            </selectQueryWithSubSelect>\n" +
                "            <whereClause>WHERE {{CONDITION}}</whereClause>\n" +
                "            <groupByClause>GROUP BY {{COLUMNS}}</groupByClause>\n" +
                "            <havingClause>HAVING {{CONDITION}}</havingClause>\n" +
                "            <orderByClause>ORDER BY {{COLUMNS}}</orderByClause>\n" +
                "            <limitClause>SAMPLE {{Q}}</limitClause>\n" +
                "        </selectQueryTemplate>\n" +
                "        <stringSize>254</stringSize>\n" +
                "        <batchEnable>true</batchEnable>\n" +
                "        <batchSize>1000</batchSize>\n" +
                "        <typeMapping>\n" +
                "            <binaryType>\n" +
                "                <typeName>BLOB</typeName>\n" +
                "                <typeValue>2004</typeValue>\n" +
                "            </binaryType>\n" +
                "            <booleanType>\n" +
                "                <typeName>SMALLINT</typeName>\n" +
                "                <typeValue>5</typeValue>\n" +
                "            </booleanType>\n" +
                "            <doubleType>\n" +
                "                <typeName>FLOAT</typeName>\n" +
                "                <typeValue>8</typeValue>\n" +
                "            </doubleType>\n" +
                "            <floatType>\n" +
                "                <typeName>FLOAT</typeName>\n" +
                "                <typeValue>6</typeValue>\n" +
                "            </floatType>\n" +
                "            <integerType>\n" +
                "                <typeName>INTEGER</typeName>\n" +
                "                <typeValue>4</typeValue>\n" +
                "            </integerType>\n" +
                "            <longType>\n" +
                "                <typeName>BIGINT</typeName>\n" +
                "                <typeValue>-5</typeValue>\n" +
                "            </longType>\n" +
                "            <stringType>\n" +
                "                <typeName>VARCHAR</typeName>\n" +
                "                <typeValue>12</typeValue>\n" +
                "            </stringType>\n" +
                "        </typeMapping>\n" +
                "    </database>\n" +
                "</rdbms-table-configuration>",
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
                        description = "Any pool parameters for the database connection must be specified as key-value" +
                                " pairs.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "null"),
                @Parameter(name = "jndi.resource",
                        description = "The name of the JNDI resource through which the connection is attempted. " +
                                "If this is found, the pool properties described above are not taken into account " +
                                "and the connection is attempted via JNDI lookup instead.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "null"),
                @Parameter(name = "datasource",
                        description = "The name of the Carbon datasource that should be used for creating the " +
                                "connection with the database. If this is found, neither the pool properties nor the " +
                                "JNDI resource name described above are taken into account and the connection " +
                                "is attempted via Carbon datasources instead. Only works in Siddhi Distribution ",
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
                        description = "The number of characters that the values for fields of the 'STRING' type in " +
                                "the table definition must contain. Each required field must be provided as a " +
                                "comma-separated list of key-value pairs in the '<field.name>:<length>' format. If " +
                                "this is not specified, the default number of characters specific to the database " +
                                "type is considered.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "null"),
                @Parameter(name = "table.check.query",
                        description = "This query will be used to check whether the table is exist in the given " +
                                "database. But the provided query should return an SQLException if the table does " +
                                "not exist in the database. Furthermore if the provided table is a database view, " +
                                "and it is not exists in the database a table from given name will be created in the " +
                                "database",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "The tableCheckQuery which define in store rdbms configs"
                ),
                @Parameter(name = "use.collation",
                        description = "This property allows users to use collation for string attributes. By " +
                                "default it's false and binary collation is not used. Currently 'latin1_bin' and " +
                                "'SQL_Latin1_General_CP1_CS_AS' are used as collations for MySQL and " +
                                "Microsoft SQL database types respectively.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false"
                ),
                @Parameter(name = "allow.null.values",
                        description = "This property allows users to insert null values to the numeric columns. ",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false")
        },
        examples = {
                @Example(
                        syntax = "@Store(type=\"rdbms\", jdbc.url=\"jdbc:mysql://localhost:3306/stocks\", " +
                                "username=\"root\", password=\"root\", jdbc.driver.name=\"com.mysql.jdbc.Driver\"," +
                                "field.length=\"symbol:100\")\n" +
                                "@PrimaryKey(\"id\", \"symbol\")\n" +
                                "@Index(\"volume\")\n" +
                                "define table StockTable (id string, symbol string, price float, volume long);",
                        description = "The above example creates an event table named 'StockTable' in the database if" +
                                " it does not already exist (with four attributes named `id`, `symbol`, `price`, and " +
                                "`volume` of the types 'string', 'string', 'float', and 'long' respectively). " +
                                "The connection is made as specified by the parameters configured for the " +
                                "'@Store' annotation.\n\n " +
                                "The @PrimaryKey() and @Index() annotations can be used to define primary keys or " +
                                "indexes for the table and they follow Siddhi query syntax. RDBMS store supports " +
                                "having more than one `attributes` in the @PrimaryKey or @Index annotations.\n " +
                                "In this example a composite Primary key of both attributes `id` and `symbol` " +
                                "will be created."
                ),
                @Example(
                        syntax = "@Store(type=\"rdbms\", jdbc.url=\"jdbc:mysql://localhost:3306/das\", " +
                                "username=\"root\", password=\"root\" , jdbc.driver.name=\"org.h2.Driver\"," +
                                "field.length=\"symbol:100\")\n" +
                                "@PrimaryKey(\"symbol\")\n" +
                                "@Index(\"symbol\")\n" +
                                "define table StockTable (symbol string, price float, volume long);\n" +
                                "define stream InputStream (symbol string, volume long);\n" +
                                "from InputStream as a join StockTable as b on str:contains(b.symbol, a.symbol)\n" +
                                "select a.symbol as symbol, b.volume as volume\n" +
                                "insert into FooStream;",
                        description = "The above example creates an event table named 'StockTable' in the database if" +
                                " it does not already exist (with three attributes named 'symbol', 'price', and " +
                                "'volume' of the types 'string', 'float' and 'long' respectively). Then the table is " +
                                "joined with a stream named 'InputStream' based on a condition. The following " +
                                "operations are included in the condition:\n" +
                                "[ AND, OR, Comparisons( <  <=  >  >=  == !=), IS NULL, " +
                                "NOT, str:contains(Table<Column>, Stream<Attribute> or Search.String)]"
                ),
                @Example(
                        syntax = "@Store(type=\"rdbms\", jdbc.url=\"jdbc:mysql://localhost:3306/das\", " +
                                "table.name=\"StockTable\", username=\"root\", password=\"root\" , jdbc.driver" +
                                ".name=\"org.h2.Driver\", field.length=\"symbol:100\", table.check" +
                                ".query=\"SELECT 1 FROM StockTable LIMIT 1\")\n" +
                                "@PrimaryKey(\"symbol\")\n" +
                                "@Index(\"symbol\")\n" +
                                "define table StockTable (symbol string, price float, volume long);\n" +
                                "define stream InputStream (symbol string, volume long);\n" +
                                "from InputStream as a join StockTable as b on str:contains(b.symbol, a.symbol)\n" +
                                "select a.symbol as symbol, b.volume as volume\n" +
                                "insert into FooStream;",
                        description = "The above example creates an event table named 'StockTable' in the database if" +
                                " it does not already exist (with three attributes named 'symbol', 'price', and " +
                                "'volume' of the types 'string', 'float' and 'long' respectively). Then the table is " +
                                "joined with a stream named 'InputStream' based on a condition. The following " +
                                "operations are included in the condition:\n" +
                                "[ AND, OR, Comparisons( <  <=  >  >=  == !=), IS NULL, " +
                                "NOT, str:contains(Table<Column>, Stream<Attribute> or Search.String)]"
                )
        },
        //system parameter is used to override any existing default values(eg:- tableCheckQuery , tableCreateQuery etc)
        // of a particular RDBMS type
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
                        description = "The template query for the 'check table' operation in {{RDBMS-Name}}.",
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
                        description = "The template query for the 'create table' operation in {{RDBMS-Name}}.",
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
                        description = "The template query for the 'create index' operation in {{RDBMS-Name}}.",
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
                        description = "The template query for the 'insert record' operation in {{RDBMS-Name}}.",
                        defaultValue = "<b>H2</b>: INSERT INTO {{TABLE_NAME}} ({{COLUMNS}}) VALUES ({{Q}})<br>" +
                                "<b>MySQL</b>: INSERT INTO {{TABLE_NAME}} ({{COLUMNS}}) VALUES ({{Q}})<br>" +
                                "<b>Oracle</b>: INSERT INTO {{TABLE_NAME}} ({{COLUMNS}}) VALUES ({{Q}})<br>" +
                                "<b>Microsoft SQL Server</b>: INSERT INTO {{TABLE_NAME}} ({{COLUMNS}}) " +
                                "VALUES ({{Q}})<br>" +
                                "<b>PostgreSQL</b>: INSERT INTO {{TABLE_NAME}} ({{COLUMNS}}) VALUES ({{Q}})<br>" +
                                "<b>DB2.*</b>: INSERT INTO {{TABLE_NAME}} ({{COLUMNS}}) VALUES ({{Q}})",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.recordUpdateQuery",
                        description = "The template query for the 'update record' operation in {{RDBMS-Name}}.",
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
                        description = "The template query for the 'select record' operation in {{RDBMS-Name}}.",
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
                        description = "The template query for the 'check record existence' operation in " +
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
                        description = "The query for the 'delete record' operation in {{RDBMS-Name}}.",
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
                        name = "{{RDBMS-Name}}.fieldSizeLimit",
                        description = "This defines the field size limit for select/switch to big string type from " +
                                "the default string type if the 'bigStringType' is available in field type list.",
                        defaultValue = "<b>H2</b>: N/A<br>" +
                                "<b>MySQL</b>: N/A<br>" +
                                "<b>Oracle</b>: 2000<br>" +
                                "<b>Microsoft SQL Server</b>: N/A<br>" +
                                "<b>PostgreSQL</b>: N/A<br>" +
                                "<b>DB2.*</b>: N/A",
                        possibleParameters = "0 =< n =< INT_MAX"
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
                        description = "This specifies whether 'Update' and 'Insert' operations can be performed for" +
                                " batches of events or not.",
                        defaultValue = "<b>H2</b>: true<br>" +
                                "<b>MySQL</b>: true<br>" +
                                "<b>Oracle (versions 12.0 and less)</b>: false<br>" +
                                "<b>Oracle (versions 12.1 and above)</b>: true<br>" +
                                "<b>Microsoft SQL Server</b>: true<br>" +
                                "<b>PostgreSQL</b>: true<br>" +
                                "<b>DB2.*</b>: true",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.transactionSupported",
                        description = "This is used to specify whether the JDBC connection that is used supports JDBC" +
                                " transactions or not.",
                        defaultValue = "<b>H2</b>: true<br>" +
                                "<b>MySQL</b>: true<br>" +
                                "<b>Oracle</b>: true<br>" +
                                "<b>Microsoft SQL Server</b>: true<br>" +
                                "<b>PostgreSQL</b>: true<br>" +
                                "<b>DB2.*</b>: true",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.typeMapping.binaryType",
                        description = "This is used to specify the binary data type. An attribute defines as " +
                                "'object' type in Siddhi stream will be stored into RDBMS with this type.",
                        defaultValue = "<b>H2</b>: BLOB<br>" +
                                "<b>MySQL</b>: BLOB<br>" +
                                "<b>Oracle</b>: BLOB<br>" +
                                "<b>Microsoft SQL Server</b>: VARBINARY(max)<br>" +
                                "<b>PostgreSQL</b>: BYTEA<br>" +
                                "<b>DB2.*</b>: BLOB(64000)",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.typeMapping.booleanType",
                        description = "This is used to specify the boolean data type. An attribute defines as " +
                                "'bool' type in Siddhi stream will be stored into RDBMS with this type.",
                        defaultValue = "<b>H2</b>: TINYINT(1)<br>" +
                                "<b>MySQL</b>: TINYINT(1)<br>" +
                                "<b>Oracle</b>: NUMBER(1)<br>" +
                                "<b>Microsoft SQL Server</b>: BIT<br>" +
                                "<b>PostgreSQL</b>: BOOLEAN<br>" +
                                "<b>DB2.*</b>: SMALLINT",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.typeMapping.doubleType",
                        description = "This is used to specify the double data type. An attribute defines as " +
                                "'double' type in Siddhi stream will be stored into RDBMS with this type.",
                        defaultValue = "<b>H2</b>: DOUBLE<br>" +
                                "<b>MySQL</b>: DOUBLE<br>" +
                                "<b>Oracle</b>: NUMBER(19,4)<br>" +
                                "<b>Microsoft SQL Server</b>: FLOAT(32)<br>" +
                                "<b>PostgreSQL</b>: DOUBLE PRECISION<br>" +
                                "<b>DB2.*</b>: DOUBLE",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.typeMapping.floatType",
                        description = "This is used to specify the float data type. An attribute defines as " +
                                "'float' type in Siddhi stream will be stored into RDBMS with this type.",
                        defaultValue = "<b>H2</b>: FLOAT<br>" +
                                "<b>MySQL</b>: FLOAT<br>" +
                                "<b>Oracle</b>: NUMBER(19,4)<br>" +
                                "<b>Microsoft SQL Server</b>: REAL<br>" +
                                "<b>PostgreSQL</b>: REAL<br>" +
                                "<b>DB2.*</b>: REAL",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.typeMapping.integerType",
                        description = "This is used to specify the integer data type. An attribute defines as " +
                                "'int' type in Siddhi stream will be stored into RDBMS with this type.",
                        defaultValue = "<b>H2</b>: INTEGER<br>" +
                                "<b>MySQL</b>: INTEGER<br>" +
                                "<b>Oracle</b>: NUMBER(10)<br>" +
                                "<b>Microsoft SQL Server</b>: INTEGER<br>" +
                                "<b>PostgreSQL</b>: INTEGER<br>" +
                                "<b>DB2.*</b>: INTEGER",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.typeMapping.longType",
                        description = "This is used to specify the long data type. An attribute defines as " +
                                "'long' type in Siddhi stream will be stored into RDBMS with this type.",
                        defaultValue = "<b>H2</b>: BIGINT<br>" +
                                "<b>MySQL</b>: BIGINT<br>" +
                                "<b>Oracle</b>: NUMBER(19)<br>" +
                                "<b>Microsoft SQL Server</b>: BIGINT<br>" +
                                "<b>PostgreSQL</b>: BIGINT<br>" +
                                "<b>DB2.*</b>: BIGINT",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.typeMapping.stringType",
                        description = "This is used to specify the string data type. An attribute defines as " +
                                "'string' type in Siddhi stream will be stored into RDBMS with this type.",
                        defaultValue = "<b>H2</b>: VARCHAR(stringSize)<br>" +
                                "<b>MySQL</b>: VARCHAR(stringSize)<br>" +
                                "<b>Oracle</b>: VARCHAR(stringSize)<br>" +
                                "<b>Microsoft SQL Server</b>: VARCHAR(stringSize)<br>" +
                                "<b>PostgreSQL</b>: VARCHAR(stringSize)<br>" +
                                "<b>DB2.*</b>: VARCHAR(stringSize)",
                        possibleParameters = "N/A"
                ),
                @SystemParameter(
                        name = "{{RDBMS-Name}}.typeMapping.bigStringType",
                        description = "This is used to specify the big string data type. An attribute defines as " +
                                "'string' type in Siddhi stream and field.length define in the annotation is " +
                                "greater than the fieldSizeLimit, will be stored into RDBMS with this type.",
                        defaultValue = "<b>H2</b>: N/A<br>" +
                                "<b>MySQL</b>: N/A" +
                                "<b>Oracle</b>: CLOB" +
                                "<b>Microsoft SQL Server</b>: N/A<br>" +
                                "<b>PostgreSQL</b>: N/A<br>" +
                                "<b>DB2.*</b>: N/A",
                        possibleParameters = "N/A"
                )
        }
)
public class RDBMSEventTable extends AbstractQueryableRecordTable {

    private static final Log log = LogFactory.getLog(RDBMSEventTable.class);
    private static final String SELECT_NULL = "(SELECT NULL)";
    private static final String ZERO = "0";
    private RDBMSQueryConfigurationEntry queryConfigurationEntry;
    private HikariDataSource dataSource;
    private boolean isLocalDatasource;
    private String dataSourceName;
    private String tableName;
    private List<Attribute> attributes;
    private ConfigReader configReader;
    private String jndiResourceName;
    private Annotation storeAnnotation;
    private Annotation primaryKeys;
    private List<Annotation> indices;
    private String selectQuery;
    private String containsQuery;
    private String deleteQuery;
    private String insertQuery;
    private String recordUpdateQuery;
    private String tableCheckQuery;
    private String createQuery;
    private String indexQuery;
    private int batchSize;
    private int fieldSizeLimit;
    private boolean batchEnable;
    private boolean transactionSupported;
    private String binaryType;
    private String booleanType;
    private String doubleType;
    private String floatType;
    private String integerType;
    private String longType;
    private String stringType;
    private String bigStringType;
    private String stringSize;
    private String recordContainsConditionTemplate;
    private RDBMSSelectQueryTemplate rdbmsSelectQueryTemplate;
    private boolean useCollation = false;
    private boolean allowNullValues = false;
    private String collation;
    private RDBMSMetrics metrics;
    private RDBMSTypeMapping typeMapping;

    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        attributes = tableDefinition.getAttributeList();
        storeAnnotation = AnnotationHelper.getAnnotation(ANNOTATION_STORE, tableDefinition.getAnnotations());
        useCollation = Boolean.parseBoolean(storeAnnotation.getElement(RDBMSTableConstants
                .USE_COLLATION));
        allowNullValues = Boolean.parseBoolean(storeAnnotation.getElement(RDBMSTableConstants.ALLOW_NULL));
        primaryKeys = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_PRIMARY_KEY,
                tableDefinition.getAnnotations());
        indices = AnnotationHelper.getAnnotations(SiddhiConstants.ANNOTATION_INDEX,
                tableDefinition.getAnnotations());
        RDBMSTableUtils.validateAnnotation(primaryKeys);
        indices.forEach(RDBMSTableUtils::validateAnnotation);
        jndiResourceName = storeAnnotation.getElement(ANNOTATION_ELEMENT_JNDI_RESOURCE);
        dataSourceName = storeAnnotation.getElement(ANNOTATION_ELEMENT_DATASOURCE);
        if (null != configReader) {
            this.configReader = configReader;
        } else {
            this.configReader = new DefaultConfigReader();
        }
        String tableName = storeAnnotation.getElement(ANNOTATION_ELEMENT_TABLE_NAME);
        this.tableName = RDBMSTableUtils.isEmpty(tableName) ? tableDefinition.getId() : tableName;
        String tableCheckQuery = storeAnnotation.getElement(ANNOTATION_ELEMENT_TABLE_CHECK_QUERY);
        this.tableCheckQuery = RDBMSTableUtils.isEmpty(tableCheckQuery) ? null : tableCheckQuery;
    }

    @Override
    protected void add(List<Object[]> records) throws ConnectionUnavailableException {
        String sql = this.composeInsertQuery();
        // Setting autocommit to true if the JDBC connection does not support transactions.
        try {
            this.batchExecuteQueriesWithRecords(sql, records, !this.transactionSupported);
        } catch (ConnectionUnavailableException e) {
            throw new ConnectionUnavailableException("Failed to add records to store: '" + this.tableName
                    + "' in the datasource: " + this.dataSourceName, e);
        } catch (RDBMSTableException e) {
            throw new DatabaseRuntimeException("Failed to add records to the store: '" + this.tableName
                    + "' in the datasource: " + this.dataSourceName, e);
        }
    }

    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        RDBMSCompiledCondition rdbmsCompiledCondition = (RDBMSCompiledCondition) compiledCondition;
        String findCondition;
        if (rdbmsCompiledCondition.isContainsConditionExist()) {
            findCondition = processFindConditionWithContainsConditionTemplate(
                    rdbmsCompiledCondition.getCompiledQuery(), this.recordContainsConditionTemplate);
        } else {
            findCondition = rdbmsCompiledCondition.getCompiledQuery();
        }
        //Some databases does not support single condition on where clause.
        //(atomic condition on where clause: SELECT * FROM TABLE WHERE true)
        //If the compile condition is resolved for '?', atomicCondition boolean value
        // will be used for ignore condition resolver.
        boolean atomicCondition = false;
        if (findCondition.equals(QUESTION_MARK)) {
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
            stmt = RDBMSTableUtils.isEmpty(findCondition) | atomicCondition ?
                    conn.prepareStatement(selectQuery.replace(PLACEHOLDER_CONDITION, "")) :
                    conn.prepareStatement(RDBMSTableUtils.formatQueryWithCondition(selectQuery, findCondition));
            if (!atomicCondition) {
                if (rdbmsCompiledCondition.isContainsConditionExist()) {

                    RDBMSTableUtils.resolveConditionForContainsCheck(stmt, (RDBMSCompiledCondition) compiledCondition,
                            findConditionParameterMap, 0, typeMapping);
                } else {
                    RDBMSTableUtils.resolveCondition(stmt, (RDBMSCompiledCondition) compiledCondition,
                            findConditionParameterMap, 0, typeMapping);
                }
            }
            rs = stmt.executeQuery();
            //Passing all java.sql artifacts to the iterator to ensure everything gets cleaned up at once.
            return new RDBMSIterator(conn, stmt, rs, this.attributes, this.tableName, allowNullValues);
        } catch (SQLException e) {
            try {
                boolean isConnValid = conn.isValid(0);
                RDBMSTableUtils.cleanupConnection(null, stmt, conn);
                if (!isConnValid) {
                    throw new ConnectionUnavailableException("Connection closed. Error retrieving records from store '"
                            + this.tableName + "'", e);
                } else {
                    throw new RDBMSTableException("Error retrieving records from store '" + this.tableName
                            + "' in the datasource:" + this.dataSourceName, e);
                }
            } catch (SQLException e1) {
                throw new RDBMSTableException("Error retrieving records " + "from store '" + this.tableName +
                        "' in the datasource:" + this.dataSourceName + ". Failed to close the connection.", e1);
            }
        }
    }

    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        String condition = ((RDBMSCompiledCondition) compiledCondition).getCompiledQuery();
        Connection conn = this.getConnection();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = RDBMSTableUtils.isEmpty(condition) ?
                    conn.prepareStatement(containsQuery.replace(PLACEHOLDER_CONDITION, "")) :
                    conn.prepareStatement(RDBMSTableUtils.formatQueryWithCondition(containsQuery, condition));
            RDBMSTableUtils.resolveCondition(stmt, (RDBMSCompiledCondition) compiledCondition,
                    containsConditionParameterMap, 0, typeMapping);
            rs = stmt.executeQuery();
            return rs.next();
        } catch (SQLException e) {
            try {
                if (!conn.isValid(0)) {
                    throw new ConnectionUnavailableException("Error performing contains check. Connection is closed " +
                            "for store: '" + tableName + "'", e);
                } else {
                    throw new RDBMSTableException("Error performing contains check for store '" +
                            this.tableName + "' in the datasource: " + this.dataSourceName, e);
                }
            } catch (SQLException e1) {
                throw new RDBMSTableException("Error performing contains check for store: '" + tableName
                        + "' in the datasource: " + this.dataSourceName, e1);
            }
        } finally {
            RDBMSTableUtils.cleanupConnection(rs, stmt, conn);
        }
    }

    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        this.batchProcessDelete(deleteConditionParameterMaps, compiledCondition);
    }

    private void batchProcessDelete(List<Map<String, Object>> deleteConditionParameterMaps,
                                    CompiledCondition compiledCondition) throws ConnectionUnavailableException {
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
                        deleteConditionParameterMap, 0, typeMapping);
                stmt.addBatch();
                counter++;
                if (counter == batchSize) {
                    int[] results = stmt.executeBatch();
                    stmt.clearBatch();
                    counter = 0;
                    if (metrics != null) {
                        int count = executedRowsCount(results);
                        if (count > 0) {
                            metrics.getTotalWriteMetrics().inc(count);
                            metrics.getDeleteCountMetric().inc(count);
                            metrics.getWritesCountMetrics().inc(count);
                        }
                        metrics.setRDBMSStatus(RDBMSStatus.PROCESSING);
                    }
                }
            }
            if (counter > 0) {
                int[] results = stmt.executeBatch();
                if (metrics != null) {
                    int count = executedRowsCount(results);
                    if (count > 0) {
                        metrics.getDeleteCountMetric().inc(count);
                        metrics.getWritesCountMetrics().inc(count);
                        metrics.getTotalWriteMetrics().inc(count);
                    }
                    metrics.setRDBMSStatus(RDBMSStatus.PROCESSING);
                }
            }
        } catch (SQLException e) {
            if (metrics != null) {
                metrics.setRDBMSStatus(RDBMSStatus.ERROR);
            }
            try {
                if (!conn.isValid(0)) {
                    throw new ConnectionUnavailableException("Error performing record deletion. Connection is closed " +
                            "for store: '" + tableName + "'", e);
                } else {
                    throw new DatabaseRuntimeException("Error performing record deletion for store '"
                            + this.tableName + "'", e);
                }
            } catch (SQLException e1) {
                throw new RDBMSTableException("Error performing record deletion for store: '" + tableName
                        + "' in the datasource: " + this.dataSourceName, e1);
            }
        } finally {
            RDBMSTableUtils.cleanupConnection(null, stmt, conn);
        }
    }

    @Override
    protected void update(CompiledCondition compiledCondition, List<Map<String, Object>> updateConditionParameterMaps,
                          Map<String, CompiledExpression> updateSetExpressions, List<Map<String, Object>> updateValues)
            throws ConnectionUnavailableException {
        String sql = this.composeUpdateQuery(compiledCondition, updateSetExpressions);
        this.batchProcessSQLUpdates(sql, updateConditionParameterMaps, compiledCondition,
                updateSetExpressions, updateValues);
    }


    /**
     * Method for processing update operations in a batched manner. This assumes that all update operations will be
     * accepted by the database.
     *
     * @param sql                          the SQL update operation as string.
     * @param updateConditionParameterMaps the runtime parameters that should be populated to the condition.
     * @param compiledCondition            the condition that was built during compile time.
     * @param updateSetExpressions         the expressions that are used in the SET operation
     * @param updateSetParameterMaps       the runtime parameters that should be populated to the update statement.
     */
    private void batchProcessSQLUpdates(String sql, List<Map<String, Object>> updateConditionParameterMaps,
                                        CompiledCondition compiledCondition,
                                        Map<String, CompiledExpression> updateSetExpressions,
                                        List<Map<String, Object>> updateSetParameterMaps)
            throws ConnectionUnavailableException {
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
                int ordinal = RDBMSTableUtils.enumerateUpdateSetEntries(updateSetExpressions, stmt, updateSetMap,
                        typeMapping);
                //Incrementing the ordinals of the conditions in the statement with the # of variables to be updated
                RDBMSTableUtils.resolveCondition(stmt, (RDBMSCompiledCondition) compiledCondition,
                        conditionParameters, ordinal - 1, typeMapping);
                stmt.addBatch();
                counter++;
                if (counter == batchSize) {
                    int[] updateResultIndex = stmt.executeBatch();
                    stmt.clearBatch();
                    counter = 0;
                    if (metrics != null) {
                        int count = executedRowsCount(updateResultIndex);
                        if (count > 0) {
                            metrics.getUpdateCountMetric().inc(count);
                            metrics.getWritesCountMetrics().inc(count);
                            metrics.getTotalWriteMetrics().inc(count);
                        }
                        metrics.setRDBMSStatus(RDBMSStatus.PROCESSING);
                    }
                }
            }
            if (counter > 0) {
                int[] updateResultIndex = stmt.executeBatch();
                if (metrics != null) {
                    int count = executedRowsCount(updateResultIndex);
                    if (count > 0) {
                        metrics.getUpdateCountMetric().inc(count);
                        metrics.getWritesCountMetrics().inc(count);
                        metrics.getTotalWriteMetrics().inc(count);
                    }
                    metrics.setRDBMSStatus(RDBMSStatus.PROCESSING);
                }
            }
        } catch (SQLException e) {
            if (metrics != null) {
                metrics.setRDBMSStatus(RDBMSStatus.ERROR);
            }
            try {
                if (!conn.isValid(0)) {
                    throw new ConnectionUnavailableException("Error performing record update operations. " +
                            "Connection is closed for store: '" + tableName + "'", e);
                } else {
                    throw new DatabaseRuntimeException("Error performing record update operations for store '"
                            + this.tableName + "'", e);
                }
            } catch (SQLException e1) {
                throw new DatabaseRuntimeException("Error performing record update operations for store: '" +
                        tableName + "'", e1);
            }
        } finally {
            RDBMSTableUtils.cleanupConnection(null, stmt, conn);
        }
    }

    @Override
    protected synchronized void updateOrAdd(CompiledCondition compiledCondition,
                                            List<Map<String, Object>> updateConditionParameterMaps,
                                            Map<String, CompiledExpression> updateSetExpressions,
                                            List<Map<String, Object>> updateSetParameterMaps,
                                            List<Object[]> addingRecords)
            throws ConnectionUnavailableException {
        List<Integer> recordInsertIndexList;
        //If any existing records already contain the new values supplied for the MATCHING columns,
        //if so, those records are updated. If not, a new record is inserted.
        if (batchEnable) {
            recordInsertIndexList = batchProcessUpdate(updateConditionParameterMaps, compiledCondition,
                    updateSetExpressions, updateSetParameterMaps);
        } else {
            recordInsertIndexList = sequentialProcessUpdate(updateConditionParameterMaps, compiledCondition,
                    updateSetExpressions, updateSetParameterMaps);
        }
        if (!recordInsertIndexList.isEmpty()) {
            List<Object[]> recordInsertList = new LinkedList<>();
            for (Integer ordinal : recordInsertIndexList) {
                recordInsertList.add(addingRecords.get(ordinal));
            }
            Map<String, ExpressionExecutor> inMemorySetExpressionExecutors = new HashMap<>();
            for (Map.Entry<String, CompiledExpression> entry : updateSetExpressions.entrySet()) {
                inMemorySetExpressionExecutors.put(
                        entry.getKey(),
                        ((RDBMSCompiledCondition) entry.getValue()).getInMemorySetExpressionExecutor());
            }
            List<Object[]> reducedRecordInsertList = ((RDBMSCompiledCondition) compiledCondition).
                    getUpdateOrInsertReducer().reduceEventsForInsert(recordInsertList, inMemorySetExpressionExecutors);
            batchProcessInsert(reducedRecordInsertList);
        }
    }

    private List<Integer> batchProcessUpdate(List<Map<String, Object>> updateConditionParameterMaps,
                                             CompiledCondition compiledCondition,
                                             Map<String, CompiledExpression> updateSetExpressions,
                                             List<Map<String, Object>> updateSetParameterMaps)
            throws ConnectionUnavailableException {
        int counter = 0;
        Connection conn = this.getConnection();
        PreparedStatement updateStmt = null;
        List<Integer> recordInsertIndexList = new ArrayList<>();
        String query = this.composeUpdateQuery(compiledCondition, updateSetExpressions);
        try {
            updateStmt = conn.prepareStatement(query);
            Iterator<Map<String, Object>> conditionParamIterator = updateConditionParameterMaps.iterator();
            Iterator<Map<String, Object>> updateSetMapIterator = updateSetParameterMaps.iterator();
            while (conditionParamIterator.hasNext() && updateSetMapIterator.hasNext()) {
                Map<String, Object> conditionParameters = conditionParamIterator.next();
                Map<String, Object> updateSetMap = updateSetMapIterator.next();
                int ordinal = RDBMSTableUtils.enumerateUpdateSetEntries(updateSetExpressions, updateStmt, updateSetMap,
                        typeMapping);
                //Incrementing the ordinals of the conditions in the statement with the # of variables to be updated
                RDBMSTableUtils.resolveCondition(updateStmt, (RDBMSCompiledCondition) compiledCondition,
                        conditionParameters, ordinal - 1, typeMapping);
                updateStmt.addBatch();
                if (counter % batchSize == batchSize - 1) {
                    recordInsertIndexList.addAll(this.filterRequiredInsertIndex(updateStmt.executeBatch(),
                            ((counter / batchSize) * batchSize)));
                    updateStmt.clearBatch();
                }
                counter++;
            }
            if (counter % batchSize > 0) {
                recordInsertIndexList.addAll(this.filterRequiredInsertIndex(updateStmt.executeBatch(),
                        (counter - (counter % batchSize))));
            }
            return recordInsertIndexList;
        } catch (SQLException e) {
            if (metrics != null) {
                metrics.setRDBMSStatus(RDBMSStatus.ERROR);
            }
            try {
                if (!conn.isValid(0)) {
                    throw new ConnectionUnavailableException("Could not execute batch update/insert operation " +
                            "(update). Connection is closed for store: '" + tableName + "'", e);
                } else {
                    throw new DatabaseRuntimeException("Could not execute batch update/insert operation (update) for " +
                            "store '" + this.tableName + "'", e);
                }
            } catch (SQLException e1) {
                throw new DatabaseRuntimeException("Could not execute batch update/insert operation (update) " +
                        "for store: '" + tableName + "'", e1);
            }
        } finally {
            RDBMSTableUtils.cleanupConnection(null, updateStmt, conn);
        }
    }


    private List<Integer> sequentialProcessUpdate(List<Map<String, Object>> updateConditionParameterMaps,
                                                  CompiledCondition compiledCondition,
                                                  Map<String, CompiledExpression> updateSetExpressions,
                                                  List<Map<String, Object>> updateSetParameterMaps)
            throws ConnectionUnavailableException {
        int counter = 0;
        Connection conn = this.getConnection(false);
        PreparedStatement updateStmt = null;
        List<Integer> updateResultList = new ArrayList<>();
        try {
            updateStmt = conn.prepareStatement(this.composeUpdateQuery(compiledCondition,
                    updateSetExpressions));
            while (counter < updateSetParameterMaps.size()) {
                Map<String, Object> conditionParameters = updateConditionParameterMaps.get(counter);
                Map<String, Object> updateSetParameterMap = updateSetParameterMaps.get(counter);
                int ordinal = RDBMSTableUtils.enumerateUpdateSetEntries(
                        updateSetExpressions, updateStmt, updateSetParameterMap, typeMapping);
                //Incrementing the ordinals of the conditions in the statement with the # of variables to be updated
                RDBMSTableUtils.resolveCondition(updateStmt, (RDBMSCompiledCondition) compiledCondition,
                        conditionParameters, ordinal - 1, typeMapping);
                int isUpdate = updateStmt.executeUpdate();
                conn.commit();
                if (isUpdate < 1) {
                    updateResultList.add(counter);
                } else {
                    if (metrics != null) {
                        metrics.getUpdateCountMetric().inc();
                        metrics.getWritesCountMetrics().inc();
                        metrics.getTotalWriteMetrics().inc();
                        metrics.setRDBMSStatus(RDBMSStatus.PROCESSING);
                    }
                }
                counter++;
            }
            return updateResultList;
        } catch (SQLException e) {
            if (metrics != null) {
                metrics.setRDBMSStatus(RDBMSStatus.ERROR);
            }
            try {
                if (!conn.isValid(0)) {
                    throw new ConnectionUnavailableException("Could not execute update/insert operation (update). " +
                            "Connection is closed for store: '" + tableName + "'", e);
                } else {
                    throw new DatabaseRuntimeException("Could not execute update/insert operation (update) for store '"
                            + this.tableName + "'", e);
                }
            } catch (SQLException e1) {
                throw new DatabaseRuntimeException("Could not execute update/insert operation (update) " +
                        "for store: '" + tableName + "'", e1);
            }
        } finally {
            RDBMSTableUtils.cleanupConnection(null, updateStmt, conn);
        }
    }

    private void batchProcessInsert(List<Object[]> addingRecords) throws ConnectionUnavailableException {
        String query = this.composeInsertQuery();
        Connection conn = this.getConnection(false);
        PreparedStatement insertStmt = null;
        try {
            insertStmt = conn.prepareStatement(query);
            int counter = 0;
            for (Object[] record : addingRecords) {
                this.populateStatement(record, insertStmt);
                try {
                    insertStmt.addBatch();
                    if (counter % batchSize == (batchSize - 1)) {
                        int[] result = insertStmt.executeBatch();
                        conn.commit();
                        insertStmt.clearBatch();
                        if (metrics != null) {
                            int count = executedRowsCount(result);
                            if (count > 0) {
                                metrics.getInsertCountMetric().inc(count);
                                metrics.getWritesCountMetrics().inc(count);
                                metrics.getTotalWriteMetrics().inc(count);
                            }
                            metrics.setRDBMSStatus(RDBMSStatus.PROCESSING);
                        }
                    }
                } catch (SQLException e) {
                    if (metrics != null) {
                        metrics.setRDBMSStatus(RDBMSStatus.ERROR);
                    }
                    try {
                        boolean isConnInvalid = conn.isValid(0);
                        RDBMSTableUtils.rollbackConnection(conn);
                        if (!isConnInvalid) {
                            throw new ConnectionUnavailableException("Could not execute insert operation. " +
                                    "Connection is closed for store: '" + tableName + "'", e);
                        } else {
                            throw new RDBMSTableException("Could not execute insert operation " +
                                    "for store '" + this.tableName + "' in the datasource: " + this.dataSourceName, e);
                        }
                    } catch (SQLException e1) {
                        throw new RDBMSTableException("Could not execute insert operation  " +
                                "for store: '" + tableName + "' in the datasource: " + this.dataSourceName, e1);
                    }
                }
                counter++;
                if (counter % batchSize > 0) {
                    int[] result = insertStmt.executeBatch();
                    conn.commit();
                    if (metrics != null) {
                        int count = executedRowsCount(result);
                        if (count > 0) {
                            metrics.getInsertCountMetric().inc(count);
                            metrics.getWritesCountMetrics().inc(count);
                            metrics.getTotalWriteMetrics().inc(count);
                        }
                        metrics.setRDBMSStatus(RDBMSStatus.PROCESSING);
                    }
                }
            }
        } catch (SQLException e) {
            if (metrics != null) {
                metrics.setRDBMSStatus(RDBMSStatus.ERROR);
            }
            try {
                if (!conn.isValid(0)) {
                    throw new ConnectionUnavailableException("Could not execute insert operation. " +
                            "Connection is closed for store: '" + tableName + "'", e);
                } else {
                    throw new RDBMSTableException("Could not execute insert operation " +
                            "for store '" + this.tableName + "' in the datasource: " + this.dataSourceName, e);
                }
            } catch (SQLException e1) {
                throw new RDBMSTableException("Could not execute insert operation  " +
                        "for store: '" + tableName + "' in the datasource: " + this.dataSourceName, e1);
            }
        } finally {
            RDBMSTableUtils.cleanupConnection(null, insertStmt, conn);
        }
    }

    private List<Integer> filterRequiredInsertIndex(int[] updateResultIndex, int lastUpdatedRecordIndex) {
        List<Integer> insertIndexList = new ArrayList<>();
        for (int i = 0; i < updateResultIndex.length; i++) {
            //Filter update result index and adding to list.
            if (updateResultIndex[i] < 1) {
                insertIndexList.add(lastUpdatedRecordIndex + i);
            } else if (metrics != null) {
                int count = updateResultIndex[i];
                metrics.getUpdateCountMetric().inc(count);
                metrics.getWritesCountMetrics().inc(count);
                metrics.getTotalWriteMetrics().inc(count);
                metrics.setRDBMSStatus(RDBMSStatus.PROCESSING);
            }
        }
        return insertIndexList;
    }

    private int executedRowsCount(int[] updateResultIndex) {
        int count = 0;
        for (int resultIndex : updateResultIndex) {
            if (resultIndex >= 1) {
                count += resultIndex;
            }
        }
        return count;
    }

    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        RDBMSConditionVisitor visitor = new RDBMSConditionVisitor(this.tableName, false);
        expressionBuilder.build(visitor);
        return new RDBMSCompiledCondition(visitor.returnCondition(), visitor.getParameters(),
                visitor.isContainsConditionExist(), visitor.getOrdinalOfContainPattern(), false, null, null,
                expressionBuilder.getUpdateOrInsertReducer(), expressionBuilder.getInMemorySetExpressionExecutor());
    }


    @Override
    protected CompiledCondition compileSetAttribute(ExpressionBuilder expressionBuilder) {
        return compileCondition(expressionBuilder);
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        try {
            if (dataSource == null) {
                if (!RDBMSTableUtils.isEmpty(dataSourceName)) {
                    BundleContext bundleContext = FrameworkUtil.getBundle(DataSourceService.class)
                            .getBundleContext();
                    ServiceReference serviceRef = bundleContext.getServiceReference(DataSourceService.class
                            .getName());
                    if (serviceRef == null) {
                        throw new DataSourceException("DatasourceService : '" +
                                DataSourceService.class.getCanonicalName() + "' cannot be found.");
                    } else {
                        DataSourceService dataSourceService = (DataSourceService) bundleContext
                                .getService(serviceRef);
                        this.dataSource = (HikariDataSource) dataSourceService.getDataSource(dataSourceName);
                        this.isLocalDatasource = false;
                        if (log.isDebugEnabled()) {
                            log.debug("Lookup for datasource '" + dataSourceName + "' completed through " +
                                    "DataSource Service lookup.");
                        }
                    }
                } else {
                    if (!RDBMSTableUtils.isEmpty(jndiResourceName)) {
                        this.lookupDatasource(jndiResourceName);
                    } else {
                        this.initializeDatasource(storeAnnotation);
                    }
                }
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
                fieldSizeLimit = Integer.parseInt(configReader.readConfig(
                        this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR + FIELD_SIZE_LIMIT,
                        String.valueOf(this.queryConfigurationEntry.getFieldSizeLimit())));
                insertQuery = this.resolveTableName(configReader.readConfig(
                        this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR + RECORD_INSERT_QUERY,
                        this.queryConfigurationEntry.getRecordInsertQuery()));
                insertQuery = this.insertColumnNames();
                recordUpdateQuery = this.resolveTableName(configReader.readConfig(
                        this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR + RECORD_UPDATE_QUERY,
                        this.queryConfigurationEntry.getRecordUpdateQuery()));
                if (tableCheckQuery == null) {
                    tableCheckQuery = this.resolveTableName(configReader.readConfig(
                            this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR + TABLE_CHECK_QUERY,
                            this.queryConfigurationEntry.getTableCheckQuery()));
                }
                createQuery = this.resolveTableName(configReader.readConfig(
                        this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR + TABLE_CREATE_QUERY,
                        this.queryConfigurationEntry.getTableCreateQuery()));
                indexQuery = this.resolveTableName(configReader.readConfig(
                        this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR + INDEX_CREATE_QUERY,
                        this.queryConfigurationEntry.getIndexCreateQuery()));
                batchEnable = Boolean.parseBoolean(configReader.readConfig(
                        this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR +
                                BATCH_ENABLE, String.valueOf(this.queryConfigurationEntry.getBatchEnable())));
                transactionSupported = Boolean.parseBoolean(configReader.readConfig(
                        this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR +
                                TRANSACTION_SUPPORTED, String.valueOf(
                                this.queryConfigurationEntry.isTransactionSupported())));
                collation = configReader.readConfig(
                        this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR +
                                COLLATION, String.valueOf(this.queryConfigurationEntry.getCollation()));
                typeMapping = this.queryConfigurationEntry.getRdbmsTypeMapping();
                booleanType = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                PROPERTY_SEPARATOR + TYPE_MAPPING + PROPERTY_SEPARATOR + BOOLEAN_TYPE,
                        typeMapping.getBooleanType().getTypeName());
                doubleType = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                PROPERTY_SEPARATOR + TYPE_MAPPING + PROPERTY_SEPARATOR + DOUBLE_TYPE,
                        typeMapping.getDoubleType().getTypeName());
                floatType = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                PROPERTY_SEPARATOR + TYPE_MAPPING + PROPERTY_SEPARATOR + FLOAT_TYPE,
                        typeMapping.getFloatType().getTypeName());
                integerType = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                PROPERTY_SEPARATOR + TYPE_MAPPING + PROPERTY_SEPARATOR + INTEGER_TYPE,
                        typeMapping.getIntegerType().getTypeName());
                longType = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                PROPERTY_SEPARATOR + TYPE_MAPPING + PROPERTY_SEPARATOR + LONG_TYPE,
                        typeMapping.getLongType().getTypeName());
                binaryType = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                PROPERTY_SEPARATOR + TYPE_MAPPING + PROPERTY_SEPARATOR + BINARY_TYPE,
                        typeMapping.getBinaryType().getTypeName());
                stringType = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                PROPERTY_SEPARATOR + TYPE_MAPPING + PROPERTY_SEPARATOR + STRING_TYPE,
                        typeMapping.getStringType().getTypeName());
                bigStringType = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                PROPERTY_SEPARATOR + TYPE_MAPPING + PROPERTY_SEPARATOR + BIG_STRING_TYPE,
                        typeMapping.getBigStringType() != null ? typeMapping.getBigStringType().getTypeName() : null);
                stringSize = configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                PROPERTY_SEPARATOR + STRING_SIZE,
                        this.queryConfigurationEntry.getStringSize());
                recordContainsConditionTemplate = configReader.readConfig(
                        this.queryConfigurationEntry.getDatabaseName() + PROPERTY_SEPARATOR +
                                RECORD_CONTAINS_CONDITION, this.queryConfigurationEntry.
                                getRecordContainsCondition()).replace(PLACEHOLDER_VALUES, QUESTION_MARK);

                RDBMSSelectQueryTemplate rdbmsSelectQueryTemplate =
                        this.queryConfigurationEntry.getRdbmsSelectQueryTemplate();
                this.rdbmsSelectQueryTemplate = new RDBMSSelectQueryTemplate();

                this.rdbmsSelectQueryTemplate.setSelectClause(resolveTableName(
                        configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                PROPERTY_SEPARATOR + SELECT_QUERY_TEMPLATE + PROPERTY_SEPARATOR
                                + SELECT_CLAUSE, rdbmsSelectQueryTemplate.getSelectClause())));
                this.rdbmsSelectQueryTemplate.setSelectQueryWithSubSelect(resolveTableName(
                        configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                        PROPERTY_SEPARATOR + SELECT_QUERY_TEMPLATE + PROPERTY_SEPARATOR
                                        + SELECT_QUERY_WITH_SUB_SELECT_TEMPLATE,
                                rdbmsSelectQueryTemplate.getSelectQueryWithSubSelect())));
                this.rdbmsSelectQueryTemplate.setWhereClause(resolveTableName(
                        configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                PROPERTY_SEPARATOR + SELECT_QUERY_TEMPLATE + PROPERTY_SEPARATOR
                                + WHERE_CLAUSE, rdbmsSelectQueryTemplate.getWhereClause())));
                this.rdbmsSelectQueryTemplate.setGroupByClause(
                        configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                PROPERTY_SEPARATOR + SELECT_QUERY_TEMPLATE + PROPERTY_SEPARATOR
                                + GROUP_BY_CLAUSE, rdbmsSelectQueryTemplate.getGroupByClause()));
                this.rdbmsSelectQueryTemplate.setHavingClause(
                        configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                PROPERTY_SEPARATOR + SELECT_QUERY_TEMPLATE + PROPERTY_SEPARATOR
                                + HAVING_CLAUSE, rdbmsSelectQueryTemplate.getHavingClause()));
                this.rdbmsSelectQueryTemplate.setOrderByClause(
                        configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                PROPERTY_SEPARATOR + SELECT_QUERY_TEMPLATE + PROPERTY_SEPARATOR
                                + ORDER_BY_CLAUSE, rdbmsSelectQueryTemplate.getOrderByClause()));
                this.rdbmsSelectQueryTemplate.setLimitClause(
                        configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                PROPERTY_SEPARATOR + SELECT_QUERY_TEMPLATE + PROPERTY_SEPARATOR
                                + LIMIT_CLAUSE, rdbmsSelectQueryTemplate.getLimitClause()));
                this.rdbmsSelectQueryTemplate.setOffsetClause(
                        configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                PROPERTY_SEPARATOR + SELECT_QUERY_TEMPLATE + PROPERTY_SEPARATOR
                                + OFFSET_CLAUSE, rdbmsSelectQueryTemplate.getOffsetClause()));
                this.rdbmsSelectQueryTemplate.setIsLimitBeforeOffset(
                        configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                        PROPERTY_SEPARATOR + SELECT_QUERY_TEMPLATE + PROPERTY_SEPARATOR
                                        + IS_LIMIT_BEFORE_OFFSET,
                                rdbmsSelectQueryTemplate.getIsLimitBeforeOffset()));
                this.rdbmsSelectQueryTemplate.setQueryWrapperClause(
                        configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                        PROPERTY_SEPARATOR + SELECT_QUERY_TEMPLATE + PROPERTY_SEPARATOR
                                        + QUERY_WRAPPER_CLAUSE,
                                rdbmsSelectQueryTemplate.getQueryWrapperClause()));
                this.rdbmsSelectQueryTemplate.setLimitWrapperClause(
                        configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                        PROPERTY_SEPARATOR + SELECT_QUERY_TEMPLATE + PROPERTY_SEPARATOR
                                        + LIMIT_WRAPPER_CLAUSE,
                                rdbmsSelectQueryTemplate.getLimitWrapperClause()));
                this.rdbmsSelectQueryTemplate.setOffsetWrapperClause(
                        configReader.readConfig(this.queryConfigurationEntry.getDatabaseName() +
                                        PROPERTY_SEPARATOR + SELECT_QUERY_TEMPLATE + PROPERTY_SEPARATOR
                                        + OFFSET_WRAPPER_CLAUSE,
                                rdbmsSelectQueryTemplate.getOffsetWrapperClause()));
            }
            if (!this.tableExists()) {
                this.createTable(storeAnnotation, primaryKeys, indices);
                if (log.isDebugEnabled()) {
                    log.debug("A table: " + this.tableName + " is created with the provided information.");
                }
            }
        } catch (CannotLoadConfigurationException | NamingException | RDBMSTableException e) {
            this.destroy();
            throw new ConnectionUnavailableException("Failed to initialize store for table name '" +
                    this.tableName + "'", e);
        } catch (DataSourceException e) {
            this.destroy();
            throw new RDBMSTableException("Failed to initialize store for table name '" +
                    this.tableName + "' in the datasource: " + this.dataSourceName, e);
        }
        if (metrics != null) {
            metrics.updateTableStatus(siddhiAppContext.getExecutorService(), siddhiAppContext.getName());
        }
    }

    @Override
    public void disconnect() {
        if (dataSource != null && isLocalDatasource) {
            dataSource.close();
            if (log.isDebugEnabled()) {
                log.debug("Closing the pool name: " + dataSource.getPoolName());
            }
        }
    }

    @Override
    public void destroy() {
        this.disconnect();
        if (log.isDebugEnabled()) {
            log.debug("Destroyed RDBMS Store instance");
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
        this.isLocalDatasource = false;
        if (log.isDebugEnabled()) {
            log.debug("Lookup for resource '" + resourceName + "' completed through " +
                    "JNDI lookup.");
        }
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

    private String insertColumnNames() {
        StringBuilder columnNames = new StringBuilder();
        for (int i = 0; i < attributes.size(); i++) {
            columnNames.append(attributes.get(i).getName()).append(WHITESPACE).append(SEPARATOR);
        }
        //Deleting the last two characters to remove the WHITESPACE and SEPARATOR
        columnNames.delete(columnNames.length() - 2, columnNames.length() - 1);
        return insertQuery.replace(PLACEHOLDER_COLUMNS, columnNames.toString());
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
        String localRecordUpdateQuery = recordUpdateQuery.replace(PLACEHOLDER_COLUMNS_VALUES, result);

        localRecordUpdateQuery = RDBMSTableUtils.isEmpty(condition) ? localRecordUpdateQuery.
                replace(PLACEHOLDER_CONDITION, "") :
                RDBMSTableUtils.formatQueryWithCondition(localRecordUpdateQuery, condition);
        return localRecordUpdateQuery;
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
        if (RDBMSTableUtils.isEmpty(driverClassName)) {
            throw new RDBMSTableException("Required parameter '" + ANNOTATION_DRIVER_CLASS_NAME + "' for DB " +
                    "connectivity cannot be empty.");
        }
        connectionProperties.setProperty("jdbcUrl", url);
        connectionProperties.setProperty("dataSource.user", username);
        if (!RDBMSTableUtils.isEmpty(password)) {
            connectionProperties.setProperty("dataSource.password", password);
        }
        connectionProperties.setProperty("driverClassName", driverClassName);
        if (poolPropertyString != null) {
            List<String[]> poolProps = RDBMSTableUtils.processKeyValuePairs(poolPropertyString);
            poolProps.forEach(pair -> connectionProperties.setProperty(pair[0], pair[1]));
        }
        HikariConfig config = new HikariConfig(connectionProperties);
        this.dataSource = new HikariDataSource(config);
        this.isLocalDatasource = true;
        if (log.isDebugEnabled()) {
            log.debug("Database connection for '" + this.tableName + "' created through connection" +
                    " parameters specified in the query.");
        }
        if (MetricsDataHolder.getInstance().getMetricService() != null &&
                MetricsDataHolder.getInstance().getMetricManagementService().isEnabled()) {
            try {
                if (MetricsDataHolder.getInstance().getMetricManagementService().isReporterRunning(
                        "prometheus")) {
                    this.metrics = new RDBMSMetrics(siddhiAppContext.getName(), url, tableName);
                }
            } catch (IllegalArgumentException e) {
                log.debug("Prometheus reporter is not running. Hence store-rdbms metrics will not be initialized.");
            }
        }
    }

    /**
     * Returns a connection instance assuming that autocommit should be true.
     *
     * @return a new {@link Connection} instance from the datasource.
     */
    private Connection getConnection() throws ConnectionUnavailableException {
        return this.getConnection(true);
    }

    /**
     * Returns a connection instance.
     *
     * @param autoCommit whether or not transactions to the connections should be committed automatically.
     * @return a new {@link Connection} instance from the datasource.
     */
    private Connection getConnection(boolean autoCommit) throws ConnectionUnavailableException {
        Connection conn;
        try {
            conn = this.dataSource.getConnection();
            conn.setAutoCommit(autoCommit);
            if (metrics != null) {
                metrics.setDatabaseParams(conn.getMetaData().getURL(), conn.getCatalog(), conn.getMetaData()
                        .getDatabaseProductName());
            }
        } catch (SQLException e) {
            if (metrics != null) {
                metrics.setRDBMSStatus(RDBMSStatus.ERROR);
            }
            throw new ConnectionUnavailableException("Error initializing connection for store: " + tableName, e);
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
    private void createTable(Annotation storeAnnotation, Annotation primaryKeys, List<Annotation> indices)
            throws ConnectionUnavailableException {
        StringBuilder builder = new StringBuilder();
        List<Element> primaryKeyList = (primaryKeys == null) ? new ArrayList<>() : primaryKeys.getElements();
        List<List<Element>> indexElementList = new ArrayList<>();
        if (indices != null) {
            indices.forEach((index) -> indexElementList.add(index.getElements()));
        }

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
                    String fieldLengthAsString = fieldLengths.getOrDefault(attribute.getName(), stringSize);
                    int fieldLength = fieldLengthAsString != null ? Integer.parseInt(fieldLengthAsString) : 0;
                    if (fieldLength > fieldSizeLimit && bigStringType != null) {
                        builder.append(bigStringType);
                    } else {
                        builder.append(stringType);
                        if (null != stringSize) {
                            builder.append(OPEN_PARENTHESIS);
                            builder.append(fieldLengths.getOrDefault(attribute.getName(), stringSize));
                            builder.append(CLOSE_PARENTHESIS);
                        }
                    }
                    if (useCollation && collation != null) {
                        builder.append(WHITESPACE).append(SQL_COLLATE).append(WHITESPACE).append(collation);
                    }
                    break;
            }
            if (this.queryConfigurationEntry.isKeyExplicitNotNull()) {
                builder.append(WHITESPACE).append(SQL_NOT_NULL);
            } else {
                for (Element key : primaryKeyList) {
                    if (attribute.getName().equals(key.getValue())) {
                        builder.append(WHITESPACE).append(SQL_NOT_NULL);
                        break;
                    }
                }
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
        queries.add(createQuery.replace(PLACEHOLDER_COLUMNS_FOR_CREATE, builder.toString()));
        if (!indexElementList.isEmpty()) {
            for (int i = 0; i < indexElementList.size(); i++) {
                if (!indexElementList.get(i).isEmpty()) {
                    String query = indexQuery.replace(PLACEHOLDER_INDEX,
                            RDBMSTableUtils.flattenAnnotatedElements(indexElementList.get(i)));
                    query = query.replace(PLACEHOLDER_INDEX_NUMBER, String.valueOf(i + 1));
                    queries.add(query);
                }
            }
        }
        try {
            // Setting autocommit to true if the JDBC connection does not support transactions.
            this.executeDDQueries(queries, !this.transactionSupported);
            if (log.isDebugEnabled()) {
                log.debug("Table '" + this.tableName + "' created.");
            }
        } catch (RDBMSTableException e) {
            if (e.getCause() != null && e.getCause().getMessage().toLowerCase(Locale.ENGLISH).
                    contains("already exists")) {
                if (log.isDebugEnabled()) {
                    log.debug("Table exist with the name " + tableName + ". Existing table will be used ");
                }
            } else {
                throw new RDBMSTableException("Unable to initialize table '" + this.tableName
                        + "' in the datasource: " + this.dataSourceName, e);
            }
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
            if (!field.equals(SiddhiConstants.AGG_SHARD_ID_COL) && !attributeNames.contains(field)) {
                throw new RDBMSTableException("Field '" + field + "' (for which a size of " + fieldLengths.get(field)
                        + " has been specified) does not exist in the '" + tableName
                        + "' table's list of fields  in the datasource: " + this.dataSourceName);
            }
        });
    }

    /**
     * Method for performing data definition queries for the current datasource.
     *
     * @param queries    the list of queries to be executed.
     * @param autocommit whether or not the transactions should automatically be committed.
     * @throws ConnectionUnavailableException if the query execution fails.
     */
    private void executeDDQueries(List<String> queries, boolean autocommit)
            throws ConnectionUnavailableException {
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
            try {
                boolean isConnValid = conn.isValid(0);
                if (!autocommit) {
                    RDBMSTableUtils.rollbackConnection(conn);
                }
                if (!isConnValid) {
                    throw new ConnectionUnavailableException("Could not execute data definition queries. " +
                            "Connection is closed for store: '" + tableName + "'", e);
                } else {
                    throw new RDBMSTableException("Could not execute data definition queries for store '"
                            + this.tableName + "' in the datasource: " + this.dataSourceName, e);
                }
            } catch (SQLException e1) {
                throw new RDBMSTableException("Could not execute data definition queries for store: '" +
                        tableName + "' in the datasource: " + this.dataSourceName, e1);
            }
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
     * @throws ConnectionUnavailableException if the query execution fails.
     */
    private void batchExecuteQueriesWithRecords(String query, List<Object[]> records, boolean autocommit)
            throws ConnectionUnavailableException {
        PreparedStatement stmt = null;
        boolean committed = autocommit;
        Connection conn = this.getConnection(autocommit);
        try {
            stmt = conn.prepareStatement(query);
            for (Object[] record : records) {
                this.populateStatement(record, stmt);
                stmt.addBatch();
            }
            int[] result = stmt.executeBatch();
            if (!autocommit) {
                conn.commit();
                committed = true;
            }
            if (metrics != null) {
                int count = executedRowsCount(result);
                if (count > 0) {
                    metrics.getInsertCountMetric().inc(count);
                    metrics.getWritesCountMetrics().inc(count);
                    metrics.getTotalWriteMetrics().inc(count);
                }
                metrics.setRDBMSStatus(RDBMSStatus.PROCESSING);
            }
        } catch (SQLException e) {
            if (e.getMessage().contains("try restarting transaction") && stmt != null) {
                log.warn("SQL Exception received instructing to restart the transaction. Hence retrying the query ["
                        + query + "]. " + e.getMessage());
                try {
                    int[] result = stmt.executeBatch();
                    if (!autocommit) {
                        conn.commit();
                        committed = true;
                    }
                    if (metrics != null) {
                        int count = executedRowsCount(result);
                        if (count > 0) {
                            metrics.getInsertCountMetric().inc(count);
                            metrics.getWritesCountMetrics().inc(count);
                            metrics.getTotalWriteMetrics().inc(count);
                        }
                        metrics.setRDBMSStatus(RDBMSStatus.PROCESSING);
                    }
                } catch (SQLException e1) {
                    if (log.isDebugEnabled()) {
                        log.debug("Reattempted execution of query [" + query + "] produced an exception: " +
                                e.getMessage());
                    }
                    try {
                        boolean isConnValid = conn.isValid(0);
                        if (!autocommit) {
                            RDBMSTableUtils.rollbackConnection(conn);
                        }
                        if (!isConnValid) {
                            if (metrics != null) {
                                metrics.setRDBMSStatus(RDBMSStatus.ERROR);
                            }
                            throw new ConnectionUnavailableException("Failed to execute query for store: " + tableName,
                                    e);
                        } else {
                            String errMessage = "Failed to execute query '" + query + "' for store: '" + tableName +  "' in the datasource: " + this.dataSourceName;
                            log.error(errMessage);
                            log.error("Dropped " + records.size() + " records, ");
                            for (int i = 0; i < records.size(); i++) {
                                log.error("Record #" + (i + 1) + " : " + Arrays.toString(records.get(i)));
                            }
                            throw new RDBMSTableException(errMessage, e);
                        }
                    } catch (SQLException e2) {
                        throw new ConnectionUnavailableException("Error occurred when attempting to check whether " +
                                "connection is available for store: " + tableName, e2);
                    }
                }
            } else {
                String errMessage = "Attempted execution of query [" + query + "] on the table: " + tableName
                        + " in the datasource: " + this.dataSourceName + " produced an exception: ";
                if (log.isDebugEnabled()) {
                    log.debug(errMessage + e.getMessage());
                }
                if (metrics != null) {
                    metrics.setRDBMSStatus(RDBMSStatus.ERROR);
                }
                throw new RDBMSTableException(errMessage, e);
            }
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
    private boolean tableExists() throws ConnectionUnavailableException {
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
    private void populateStatement(Object[] record, PreparedStatement stmt) throws ConnectionUnavailableException {
        Attribute attribute = null;
        try {
            for (int i = 0; i < this.attributes.size(); i++) {
                attribute = this.attributes.get(i);
                Object value = record[i];
                if (allowNullValues || value != null || attribute.getType() == Attribute.Type.STRING) {
                    RDBMSTableUtils.populateStatementWithSingleElement(stmt, i + 1, attribute.getType(), value,
                            typeMapping);
                } else {
                    throw new RDBMSTableException("Cannot Execute Insert/Update on the table: '" + tableName
                            + "' in the datasource: " + this.dataSourceName + ". A null value detected for the attribute '"
                            + attribute.getName() + "'");
                }
            }
        } catch (SQLException e) {
            try {
                if (!stmt.getConnection().isValid(0)) {
                    throw new ConnectionUnavailableException("Connection is closed. Could not execute Insert/Update " +
                            " for store: '" + tableName + "'", e);
                } else {
                    throw new RDBMSTableException("Dropping event since value for attribute name " +
                            attribute.getName() + " cannot be set for store: '" + tableName
                            + "' in the datasource: " + this.dataSourceName, e);
                }
            } catch (SQLException e1) {
                throw new RDBMSTableException("Could not execute Insert/Update for store: '" + tableName
                        + "' in the datasource: " + this.dataSourceName, e1);
            }
        }
    }

    @Override
    protected RecordIterator<Object[]> query(Map<String, Object> parameterMap, CompiledCondition compiledCondition,
                                             CompiledSelection compiledSelection, Attribute[] outputAttributes)
            throws ConnectionUnavailableException {
        RDBMSCompiledSelection rdbmsCompiledSelection = (RDBMSCompiledSelection) compiledSelection;
        RDBMSCompiledCondition rdbmsCompiledCondition = (RDBMSCompiledCondition) compiledCondition;
        boolean containsConditionExist = rdbmsCompiledCondition.isContainsConditionExist();
        if (containsConditionExist) {
            rdbmsCompiledCondition.setCompiledQuery(processFindConditionWithContainsConditionTemplate(
                    rdbmsCompiledCondition.getCompiledQuery(), this.recordContainsConditionTemplate));
        }
        if ("?".equals(rdbmsCompiledCondition.getCompiledQuery())) {
            rdbmsCompiledCondition = null;
        }
        Connection conn = this.getConnection();
        PreparedStatement stmt;
        String query = getSelectQuery(rdbmsCompiledCondition, rdbmsCompiledSelection);
        if (log.isDebugEnabled()) {
            log.debug("Store Query SQL Syntax: '" + query + "'");
        }
        try {
            stmt = conn.prepareStatement(query);
            RDBMSTableUtils.resolveQuery(stmt, rdbmsCompiledSelection, rdbmsCompiledCondition, parameterMap, 0,
                    containsConditionExist, typeMapping);
        } catch (SQLException e) {
            try {
                if (!conn.isValid(0)) {
                    throw new ConnectionUnavailableException("Connection is closed when preparing to execute " +
                            "query: '" + query + "' for store: '" + tableName + "'", e);
                } else {
                    throw new RDBMSTableException("Error when preparing to execute query: '" + query
                            + "' on store: '" + this.tableName + "' in the datasource: " + this.dataSourceName, e);
                }
            } catch (SQLException e1) {
                throw new RDBMSTableException("Error when preparing to execute query: '" + query
                        + "' on store: '" + this.tableName + "' in the datasource: " + this.dataSourceName, e1);
            }
        }
        ResultSet rs;
        try {
            rs = stmt.executeQuery();

            //Supporting null to keep backward compatibility.
            // If the outputAttributes are null, it is assumed that all the attributes from the table definition
            // are being selected in the query.
            if (outputAttributes == null) {
                return new RDBMSIterator(conn, stmt, rs, this.attributes, this.tableName, allowNullValues);
            }
            return new RDBMSIterator(conn, stmt, rs, Arrays.asList(outputAttributes), this.tableName, allowNullValues);
        } catch (SQLException e) {
            try {
                boolean isConnValid = conn.isValid(0);
                RDBMSTableUtils.cleanupConnection(null, stmt, conn);
                if (!isConnValid) {
                    throw new ConnectionUnavailableException("Connection is closed when preparing to execute " +
                            "query: '" + query + "' for store: '" + tableName + "'", e);
                } else {
                    throw new RDBMSTableException("Error when preparing to execute query: '" + query
                            + "' on '" + this.tableName + "' in the datasource: " + this.dataSourceName, e);
                }
            } catch (SQLException e1) {
                throw new RDBMSTableException("Error when preparing to execute query: '" + query
                        + "' on store: '" + this.tableName + "' in the datasource: " + this.dataSourceName, e1);
            }
        }
    }

    private String getSelectQuery(RDBMSCompiledCondition rdbmsCompiledCondition,
                                  RDBMSCompiledSelection rdbmsCompiledSelection) {

        boolean isContainsLastFunction = rdbmsCompiledSelection.getCompiledSelectClause().isUseSubSelect();

        String selectors = rdbmsCompiledSelection.getCompiledSelectClause().getCompiledQuery();
        String subSelectQuerySelectors = rdbmsCompiledSelection.getCompiledSelectClause().getSubSelectQuerySelectors();

        String selectClause;
        if (isContainsLastFunction) {
            selectClause = rdbmsSelectQueryTemplate.getSelectClause()
                    .replace(PLACEHOLDER_SELECTORS, subSelectQuerySelectors);
        } else {
            selectClause = rdbmsSelectQueryTemplate.getSelectClause()
                    .replace(PLACEHOLDER_SELECTORS, selectors);
        }

        StringBuilder selectQuery = new StringBuilder(selectClause);

        if (rdbmsCompiledCondition != null) {
            String whereClause = rdbmsSelectQueryTemplate.getWhereClause();
            if (whereClause == null || whereClause.isEmpty()) {
                throw new QueryableRecordTableException("Where clause is present in query but 'whereClause' has not " +
                        "being configured in RDBMS Event Table query configuration, for store: " + tableName);
            }
            whereClause = whereClause.replace(PLACEHOLDER_CONDITION, rdbmsCompiledCondition.getCompiledQuery());
            selectQuery.append(WHITESPACE).append(whereClause);
        }
        RDBMSCompiledCondition compiledGroupByClause = rdbmsCompiledSelection.getCompiledGroupByClause();
        if (compiledGroupByClause != null) {
            String groupByClause = rdbmsSelectQueryTemplate.getGroupByClause();
            if (groupByClause == null || groupByClause.isEmpty()) {
                throw new QueryableRecordTableException("Group by clause is present in query but 'groupByClause' has " +
                        "not being configured in RDBMS Event Table query configuration, for store: " + tableName);
            }
            groupByClause = groupByClause.replace(PLACEHOLDER_COLUMNS, compiledGroupByClause.getCompiledQuery());
            selectQuery.append(WHITESPACE).append(groupByClause);
        }
        RDBMSCompiledCondition compiledHavingClause = rdbmsCompiledSelection.getCompiledHavingClause();
        if (compiledHavingClause != null) {
            String havingClause = rdbmsSelectQueryTemplate.getHavingClause();
            if (havingClause == null || havingClause.isEmpty()) {
                throw new QueryableRecordTableException("Having by clause is present in query but 'havingClause' has " +
                        "not being configured in RDBMS Event Table query configuration, for store: " + tableName);
            }
            havingClause = havingClause.replace(PLACEHOLDER_CONDITION, compiledHavingClause.getCompiledQuery());
            selectQuery.append(WHITESPACE).append(havingClause);
        }
        RDBMSCompiledCondition compiledOrderByClause = rdbmsCompiledSelection.getCompiledOrderByClause();
        if (compiledOrderByClause != null) {
            String orderByClause = rdbmsSelectQueryTemplate.getOrderByClause();
            if (orderByClause == null || orderByClause.isEmpty()) {
                throw new QueryableRecordTableException("Order by clause is present in query but 'orderByClause' has " +
                        "not being configured in RDBMS Event Table query configuration, for store: " + tableName);
            }
            orderByClause = orderByClause.replace(PLACEHOLDER_COLUMNS, compiledOrderByClause.getCompiledQuery());
            selectQuery.append(WHITESPACE).append(orderByClause);
        }
        Long limit = rdbmsCompiledSelection.getLimit();
        Long offset = rdbmsCompiledSelection.getOffset();
        if (rdbmsSelectQueryTemplate.getQueryWrapperClause() != null) {
            String queryWrapperClause = rdbmsSelectQueryTemplate.getQueryWrapperClause().replace(
                    PLACEHOLDER_INNER_QUERY, selectQuery.toString());
            if (limit != null) {
                String limitWrapper = rdbmsSelectQueryTemplate.getLimitWrapperClause();
                if (limitWrapper != null) {
                    if (offset != null) {
                        limitWrapper = limitWrapper.replace(RDBMSTableConstants.PLACEHOLDER_Q, Long.toString(limit
                                + offset));
                    } else {
                        limitWrapper = limitWrapper.replace(RDBMSTableConstants.PLACEHOLDER_Q, Long.toString(limit));
                    }
                    queryWrapperClause = queryWrapperClause.replace(RDBMSTableConstants.PLACEHOLDER_LIMIT_WRAPPER,
                            limitWrapper);
                } else {
                    throw new QueryableRecordTableException("Limit by clause is present in query but " +
                            "'limitWrapperClause' has not being configured in RDBMS Event Table query configuration," +
                            " for store: " + tableName);
                }
            } else {
                queryWrapperClause = queryWrapperClause.replace(RDBMSTableConstants.PLACEHOLDER_LIMIT_WRAPPER, "");
            }
            if (offset != null) {
                String offsetWrapper = rdbmsSelectQueryTemplate.getOffsetWrapperClause();
                if (offsetWrapper != null) {
                    offsetWrapper = offsetWrapper.replace(RDBMSTableConstants.PLACEHOLDER_Q, Long.toString(offset));
                    queryWrapperClause = queryWrapperClause.replace(RDBMSTableConstants.PLACEHOLDER_OFFSET_WRAPPER,
                            offsetWrapper);
                } else {
                    throw new QueryableRecordTableException("Offset by clause is present in query but " +
                            "'OffsetWrapperClause' has not being configured in RDBMS Event Table query configuration," +
                            " for store: " + tableName);
                }
            } else {
                queryWrapperClause = queryWrapperClause.replace(RDBMSTableConstants.PLACEHOLDER_OFFSET_WRAPPER, "");
            }

            if (isContainsLastFunction) {
                return getQueryWithSubSelectors(rdbmsCompiledSelection, selectors, queryWrapperClause);
            }
            return queryWrapperClause;
        } else {
            if (limit != null) {
                String limitClause = rdbmsSelectQueryTemplate.getLimitClause();
                if (limitClause == null || limitClause.isEmpty()) {
                    throw new QueryableRecordTableException("Limit by clause is present in query but 'limitClause' " +
                            "has not being configured in RDBMS Event Table query configuration, for store: " +
                            tableName);
                }
                limitClause = limitClause.replace(RDBMSTableConstants.PLACEHOLDER_Q, Long.toString(limit));
                if (offset != null) {
                    String offsetClause = rdbmsSelectQueryTemplate.getOffsetClause();
                    if (offsetClause == null || offsetClause.isEmpty()) {
                        throw new QueryableRecordTableException("Offset clause is present in query but " +
                                "'offsetClause' has not being configured in RDBMS Event Table query configuration, " +
                                "for store: " + tableName);
                    }
                    offsetClause = offsetClause.replace(RDBMSTableConstants.PLACEHOLDER_Q, Long.toString(offset));
                    Boolean isLimitBeforeOffset = Boolean.parseBoolean(rdbmsSelectQueryTemplate.
                            getIsLimitBeforeOffset());
                    if (isLimitBeforeOffset == null) {
                        throw new QueryableRecordTableException("Offset clause is present in query but " +
                                "'isLimitBeforeOffset' has not being configured in RDBMS Event Table query " +
                                "configuration, for store: " + tableName);
                    }
                    if (queryConfigurationEntry.getDatabaseName().equalsIgnoreCase(
                            RDBMSTableConstants.MICROSOFT_SQL_SERVER_NAME) && compiledOrderByClause == null) {
                        String orderByClause = rdbmsSelectQueryTemplate.getOrderByClause();
                        orderByClause = orderByClause.replace(PLACEHOLDER_COLUMNS, SELECT_NULL);
                        selectQuery.append(WHITESPACE).append(orderByClause);
                    }
                    if (isLimitBeforeOffset) {
                        selectQuery.append(WHITESPACE).append(limitClause).append(WHITESPACE).append(offsetClause);
                    } else {
                        selectQuery.append(WHITESPACE).append(offsetClause).append(WHITESPACE).append(limitClause);
                    }
                } else {
                    if (queryConfigurationEntry.getDatabaseName().equalsIgnoreCase(
                            RDBMSTableConstants.MICROSOFT_SQL_SERVER_NAME)) {
                        if (compiledOrderByClause == null) {
                            String orderByClause = rdbmsSelectQueryTemplate.getOrderByClause();
                            orderByClause = orderByClause.replace(PLACEHOLDER_COLUMNS, SELECT_NULL);
                            selectQuery.append(WHITESPACE).append(orderByClause);
                        }
                        String offsetClause = rdbmsSelectQueryTemplate.getOffsetClause();
                        offsetClause = offsetClause.replace(RDBMSTableConstants.PLACEHOLDER_Q, ZERO);
                        boolean isLimitBeforeOffset = Boolean.parseBoolean(rdbmsSelectQueryTemplate.
                                getIsLimitBeforeOffset());
                        if (isLimitBeforeOffset) {
                            selectQuery.append(WHITESPACE).append(limitClause).append(WHITESPACE).append(offsetClause);
                        } else {
                            selectQuery.append(WHITESPACE).append(offsetClause).append(WHITESPACE).append(limitClause);
                        }
                    } else {
                        selectQuery.append(WHITESPACE).append(limitClause);
                    }
                }
            }

            if (isContainsLastFunction) {
                return getQueryWithSubSelectors(rdbmsCompiledSelection, selectors, selectQuery.toString());
            }

            return selectQuery.toString();
        }
    }


    private String getQueryWithSubSelectors(RDBMSCompiledSelection rdbmsCompiledSelection, String selectors,
                                            String selectQuery) {
        String selectQueryWithSubSelect = rdbmsSelectQueryTemplate.getSelectQueryWithSubSelect();
        if (selectQueryWithSubSelect == null || selectQueryWithSubSelect.isEmpty()) {
            throw new QueryableRecordTableException("incrementalAggregator:last() is used in the query but " +
                    "'selectQueryWithSubSelect' has not being configured in RDBMS Event Table query " +
                    "configuration, for store: " + tableName);
        }
        String whereClause = rdbmsSelectQueryTemplate.getWhereClause();
        if (whereClause == null || whereClause.isEmpty()) {
            throw new QueryableRecordTableException("Where clause is present in query but 'whereClause' " +
                    "has not being configured in RDBMS Event Table query configuration, for store: "
                    + tableName);
        }

        selectQueryWithSubSelect = selectQueryWithSubSelect
                .replace(PLACEHOLDER_SELECTORS, selectors)
                .replace(PLACEHOLDER_INNER_QUERY, selectQuery);

        whereClause = whereClause.replace(PLACEHOLDER_CONDITION,
                rdbmsCompiledSelection.getCompiledSelectClause().getOuterCompiledCondition());

        selectQueryWithSubSelect = selectQueryWithSubSelect + WHITESPACE + whereClause;

        RDBMSCompiledCondition compiledGroupByClause = rdbmsCompiledSelection.getCompiledGroupByClause();
        String groupByClause = rdbmsSelectQueryTemplate.getGroupByClause();
        if (compiledGroupByClause != null) {
            groupByClause = groupByClause.replace(PLACEHOLDER_COLUMNS,
                    compiledGroupByClause.getCompiledQuery());
            selectQueryWithSubSelect = selectQueryWithSubSelect + WHITESPACE + groupByClause;
        }

        return selectQueryWithSubSelect;
    }

    @Override
    protected CompiledSelection compileSelection(List<SelectAttributeBuilder> selectAttributeBuilders,
                                                 List<ExpressionBuilder> groupByExpressionBuilder,
                                                 ExpressionBuilder havingExpressionBuilder,
                                                 List<OrderByAttributeBuilder> orderByAttributeBuilders, Long limit,
                                                 Long offset) {
        return new RDBMSCompiledSelection(
                compileSelectClause(selectAttributeBuilders),
                (groupByExpressionBuilder == null) ? null : compileClause(groupByExpressionBuilder, false),
                (havingExpressionBuilder == null) ? null :
                        compileClause(Collections.singletonList(havingExpressionBuilder), true),
                (orderByAttributeBuilders == null) ? null : compileOrderByClause(orderByAttributeBuilders),
                limit, offset);
    }

    private RDBMSCompiledCondition compileSelectClause(List<SelectAttributeBuilder> selectAttributeBuilders) {
        StringBuilder compiledSelectionList = new StringBuilder();
        StringBuilder compiledSubSelectQuerySelection = new StringBuilder();
        StringBuilder compiledOuterOnCondition = new StringBuilder();

        SortedMap<Integer, Object> paramMap = new TreeMap<>();
        int offset = 0;

        boolean containsLastFunction = false;
        List<RDBMSConditionVisitor> conditionVisitorList = new ArrayList<>();
        for (SelectAttributeBuilder attributeBuilder : selectAttributeBuilders) {
            RDBMSConditionVisitor visitor = new RDBMSConditionVisitor(this.tableName, false);
            attributeBuilder.getExpressionBuilder().build(visitor);
            if (visitor.isLastConditionExist()) {
                containsLastFunction = true;
            }
            conditionVisitorList.add(visitor);
        }

        boolean isLastFunctionEncountered = false;
        for (int i = 0; i < conditionVisitorList.size(); i++) {
            RDBMSConditionVisitor visitor = conditionVisitorList.get(i);
            SelectAttributeBuilder selectAttributeBuilder = selectAttributeBuilders.get(i);

            String compiledCondition = visitor.returnCondition();

            if (containsLastFunction) {
                if (visitor.isLastConditionExist()) {
                    // Add the select columns with function incrementalAggregator:last()
                    compiledSelectionList.append(compiledCondition).append(SQL_AS)
                            .append(selectAttributeBuilder.getRename()).append(SEPARATOR);
                    if (!isLastFunctionEncountered) {
                        //Only add max variable for incrementalAggregator:last() once
                        compiledSubSelectQuerySelection.append(visitor.returnMaxVariableCondition()).append(SEPARATOR);
                        compiledOuterOnCondition.append(visitor.getOuterCompiledCondition()).append(SQL_AND);
                        isLastFunctionEncountered = true;
                    }
                } else if (visitor.isContainsAttributeFunction()) {
                    //Add columns with attributes function such as sum(), max()
                    //Add max(variable) by default since oracle all columns not in group by must have
                    // attribute function
                    compiledSelectionList.append(SQL_MAX).append(OPEN_PARENTHESIS)
                            .append(SUB_SELECT_QUERY_REF).append(".").append(selectAttributeBuilder.getRename())
                            .append(CLOSE_PARENTHESIS).append(SQL_AS)
                            .append(selectAttributeBuilder.getRename()).append(SEPARATOR);
                    compiledSubSelectQuerySelection.append(compiledCondition).append(SQL_AS)
                            .append(selectAttributeBuilder.getRename()).append(SEPARATOR);
                } else {
                    // Add group by column
                    compiledSelectionList.append(compiledCondition).append(SQL_AS)
                            .append(selectAttributeBuilder.getRename()).append(SEPARATOR);
                    compiledSubSelectQuerySelection.append(compiledCondition).append(SQL_AS)
                            .append(selectAttributeBuilder.getRename()).append(SEPARATOR);
                    compiledOuterOnCondition.append(visitor.getOuterCompiledCondition()).append(SQL_AND);
                }
            } else {
                compiledSelectionList.append(compiledCondition);
                if (selectAttributeBuilder.getRename() != null && !selectAttributeBuilder.getRename().isEmpty()) {
                    compiledSelectionList.append(SQL_AS).
                            append(selectAttributeBuilder.getRename());
                }
                compiledSelectionList.append(SEPARATOR);
            }

            Map<Integer, Object> conditionParamMap = visitor.getParameters();
            int maxOrdinal = 0;
            for (Map.Entry<Integer, Object> entry : conditionParamMap.entrySet()) {
                Integer ordinal = entry.getKey();
                paramMap.put(ordinal + offset, entry.getValue());
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            offset = offset + maxOrdinal;
        }

        if (compiledSelectionList.length() > 0) {
            compiledSelectionList.setLength(compiledSelectionList.length() - 2); // Removing the last comma separator.
        }

        if (compiledSubSelectQuerySelection.length() > 0) {
            compiledSubSelectQuerySelection.setLength(compiledSubSelectQuerySelection.length() - 2);
        }

        if (compiledOuterOnCondition.length() > 0) {
            compiledOuterOnCondition.setLength(compiledOuterOnCondition.length() - 4);
        }

        return new RDBMSCompiledCondition(compiledSelectionList.toString(), paramMap, false, new ArrayList<>(),
                containsLastFunction, compiledSubSelectQuerySelection.toString(), compiledOuterOnCondition.toString(),
                null, null);
    }

    private RDBMSCompiledCondition compileClause(List<ExpressionBuilder> expressionBuilders, boolean isHavingClause) {
        StringBuilder compiledSelectionList = new StringBuilder();
        SortedMap<Integer, Object> paramMap = new TreeMap<>();
        int offset = 0;

        for (ExpressionBuilder expressionBuilder : expressionBuilders) {
            RDBMSConditionVisitor visitor = new RDBMSConditionVisitor(this.tableName, isHavingClause);
            expressionBuilder.build(visitor);

            String compiledCondition = visitor.returnCondition();
            compiledSelectionList.append(compiledCondition).append(SEPARATOR);

            Map<Integer, Object> conditionParamMap = visitor.getParameters();
            int maxOrdinal = 0;
            for (Map.Entry<Integer, Object> entry : conditionParamMap.entrySet()) {
                Integer ordinal = entry.getKey();
                paramMap.put(ordinal + offset, entry.getValue());
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            offset = offset + maxOrdinal;
        }

        if (compiledSelectionList.length() > 0) {
            compiledSelectionList.setLength(compiledSelectionList.length() - 2); // Removing the last comma separator.
        }
        return new RDBMSCompiledCondition(compiledSelectionList.toString(), paramMap, false,
                new ArrayList<>(), false, null, null, null, null);
    }

    private RDBMSCompiledCondition compileOrderByClause(List<OrderByAttributeBuilder> orderByAttributeBuilders) {
        StringBuilder compiledSelectionList = new StringBuilder();
        SortedMap<Integer, Object> paramMap = new TreeMap<>();
        int offset = 0;

        for (OrderByAttributeBuilder orderByAttributeBuilder : orderByAttributeBuilders) {
            RDBMSConditionVisitor visitor = new RDBMSConditionVisitor(this.tableName, true);
            orderByAttributeBuilder.getExpressionBuilder().build(visitor);

            String compiledCondition = visitor.returnCondition();
            compiledSelectionList.append(compiledCondition);
            OrderByAttribute.Order order = orderByAttributeBuilder.getOrder();
            if (order == null) {
                compiledSelectionList.append(SEPARATOR);
            } else {
                compiledSelectionList.append(WHITESPACE).append(order.toString()).append(SEPARATOR);
            }

            Map<Integer, Object> conditionParamMap = visitor.getParameters();
            int maxOrdinal = 0;
            for (Map.Entry<Integer, Object> entry : conditionParamMap.entrySet()) {
                Integer ordinal = entry.getKey();
                paramMap.put(ordinal + offset, entry.getValue());
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            offset = offset + maxOrdinal;
        }

        if (compiledSelectionList.length() > 0) {
            compiledSelectionList.setLength(compiledSelectionList.length() - 2); // Removing the last comma separator.
        }
        return new RDBMSCompiledCondition(compiledSelectionList.toString(), paramMap, false,
                new ArrayList<>(), false, null, null, null, null);
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


