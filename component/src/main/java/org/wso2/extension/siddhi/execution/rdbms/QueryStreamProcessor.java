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
package org.wso2.extension.siddhi.execution.rdbms;

import com.zaxxer.hikari.HikariDataSource;
import org.wso2.extension.siddhi.execution.rdbms.util.RDBMSStreamProcessorUtil;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This extension function can be used to perform SQL retrieval queries on a WSO2 datasource.
 */
@Extension(
        name = "query",
        namespace = "rdbms",
        description = "The function can be used to perform SQL retrieval queries on a WSO2 datasource. \n" +
                "Note: This will only work within WSO2 SP.",
        parameters = {
                @Parameter(
                        name = "datasource.name",
                        description = "The name of the WSO2 datasource on which the query should be performed on",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "query",
                        description = "The select query(formatted according to " +
                                "the appropriate database type) that needs to be performed",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "attribute.definition.list",
                        description = "Comma separated list of `<AttributeName AttributeType>`. " +
                                "It is expected that the SQL query will return the attributes in order, as in if one " +
                                "attribute is defined here, the SQL query should return one column result set, " +
                                "if more than one column is returned then the first column will be processed. The " +
                                "Siddhi data types supported will be `STRING`, `INT`, `LONG`, `DOUBLE`, `FLOAT`, " +
                                "`BOOL`. \n Mapping of the Siddhi data type to database data type can be done as " +
                                "follows, \n" +
                                "*Siddhi Datatype*->*Datasource Datatype*\n" +
                                "`STRING`->`CHAR`,`VARCHAR`,`LONGVARCHAR`\n" +
                                "`INT`&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;->`INTEGER`\n" +
                                "`LONG`&nbsp;&nbsp;&nbsp;&nbsp;->`BIGINT`\n" +
                                "`DOUBLE`->`DOUBLE`\n" +
                                "`FLOAT`&nbsp;&nbsp;&nbsp;->`REAL`\n" +
                                "`BOOL`&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;->`BIT`\n",
                        type = DataType.STRING
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "attributeName",
                        description = "The return attributes will be the ones defined in the parameter" +
                                "`attribute.definition.list`.",
                        type = {DataType.STRING, DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT,
                                DataType.BOOL}
                )
        },
        examples = {
                @Example(
                        syntax = "from TriggerStream#rdbms:query('SAMPLE_DB', 'select * from " +
                                "Transactions_Table', 'creditcardno string, country string, transaction string," +
                                " amount int') \n" +
                                "select creditcardno, country, transaction, amount \n" +
                                "insert into recordStream;",
                        description = "Events inserted into recordStream includes all records matched for the query " +
                                "i.e an event will be generated for each record retrieved from the datasource. " +
                                "The event will include as additional attributes, the attributes defined in the " +
                                "`attribute.definition.list`(creditcardno, country, transaction, amount)."
                )
        }
)
public class QueryStreamProcessor extends StreamProcessor {
    private String dataSourceName;
    private HikariDataSource dataSource;
    private ExpressionExecutor queryExpressionExecutor;
    private List<Attribute> attributeList = new ArrayList<>();

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {

        if ((attributeExpressionExecutors.length != 3)) {
            throw new SiddhiAppValidationException("rdbms query function  should have 3 parameters , but found '" +
                    attributeExpressionExecutors.length + "' parameters.");
        }

        this.dataSourceName = RDBMSStreamProcessorUtil.validateDatasourceName(attributeExpressionExecutors[0]);

        if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.STRING) {
            queryExpressionExecutor = attributeExpressionExecutors[1];
        } else {
            throw new SiddhiAppValidationException("The parameter 'query' in rdbms query " +
                    "function should be of type STRING, but found a parameter with type '" +
                    attributeExpressionExecutors[1].getReturnType() + "'.");
        }

        if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
            String attributeDefinition = ((ConstantExpressionExecutor) attributeExpressionExecutors[2])
                    .getValue().toString();
            this.attributeList = Arrays.stream(attributeDefinition.split(","))
                    .map((String attributeExp) -> {
                        String[] splitAttributeDef = attributeExp.trim().split("\\s");
                        if (splitAttributeDef.length != 2) {
                            throw new SiddhiAppValidationException("The parameter 'attribute.definition.list' is " +
                                    "invalid, it should be comma separated list of <AttributeName AttributeType>, " +
                                    "but found '" + attributeDefinition);
                        }
                        Attribute.Type attributeType;
                        switch (splitAttributeDef[1].toLowerCase()) {
                            case "bool":
                                attributeType = Attribute.Type.BOOL;
                                break;
                            case "double":
                                attributeType = Attribute.Type.DOUBLE;
                                break;
                            case "float":
                                attributeType = Attribute.Type.FLOAT;
                                break;
                            case "int":
                                attributeType = Attribute.Type.INT;
                                break;
                            case "long":
                                attributeType = Attribute.Type.LONG;
                                break;
                            case "string":
                                attributeType = Attribute.Type.STRING;
                                break;
                            default:
                                throw new SiddhiAppCreationException("The attribute defined  in the parameter " +
                                        "'attribute.definition.list' should be a valid Siddhi  attribute type, " +
                                        "but found an attribute of type '" + splitAttributeDef[1] + "'.");
                        }
                        return new Attribute(splitAttributeDef[0], attributeType);
                    }).collect(Collectors.toList());
            return this.attributeList;
        } else {
            throw new SiddhiAppValidationException("The parameter 'query' in rdbms query function should be a " +
                    "constant, but found a dynamic attribute of type '" +
                    attributeExpressionExecutors[1].getClass().getCanonicalName() + "'.");
        }
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        Connection conn = this.getConnection();
        PreparedStatement stmt = null;
        ResultSet resultSet = null;

        while (streamEventChunk.hasNext()) {
            try {
                StreamEvent event = streamEventChunk.next();
                String query = ((String) queryExpressionExecutor.execute(event));
                if (RDBMSStreamProcessorUtil.queryContainsCheck(true, query)) {
                    throw new SiddhiAppRuntimeException("Dropping event since the query has unauthorised operations, " +
                            "'" + query + "'. Event: '" + event + "'.");
                }
                stmt = conn.prepareStatement(query);
                resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(event);
                    Object[] data = RDBMSStreamProcessorUtil.processRecord(this.attributeList, resultSet);
                    complexEventPopulater.populateComplexEvent(clonedEvent, data);
                    streamEventChunk.insertBeforeCurrent(clonedEvent);
                }
                streamEventChunk.remove();
            } catch (SQLException e) {
                throw new SiddhiAppRuntimeException("Error in retrieving records from  datasource '"
                        + this.dataSourceName + "': " + e.getMessage(), e);
            } finally {
                RDBMSStreamProcessorUtil.cleanupConnection(resultSet, stmt, conn);
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    private Connection getConnection() {
        Connection conn;
        try {
            conn = this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new SiddhiAppRuntimeException("Error initializing datasource '" + this.dataSourceName +
                    "'connection: " + e.getMessage(), e);
        }
        return conn;
    }

    @Override
    public void start() {
        this.dataSource = RDBMSStreamProcessorUtil.getDataSourceService(this.dataSourceName);
    }

    @Override
    public void stop() {

    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {

    }
}
