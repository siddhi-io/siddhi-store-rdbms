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
package io.siddhi.extension.execution.rdbms;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.extension.execution.rdbms.util.RDBMSStreamProcessorUtil;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.sql.DataSource;

/**
 * This extension function can be used to perform SQL retrieval queries on a datasource.
 */
@Extension(
        name = "query",
        namespace = "rdbms",
        description = "This function performs SQL retrieval queries on data sources. \n" +
                "Note: This function to work data sources should be set at the Siddhi Manager level.",
        parameters = {
                @Parameter(
                        name = "datasource.name",
                        description = "The name of the datasource for which the query should be performed. If Siddhi " +
                                "is used as a Java/Python library the datasource should be explicitly set in the " +
                                "siddhi manager in order for the function to work.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "attribute.definition.list",
                        description = "This is provided as a comma-separated list in the " +
                                "'<AttributeName AttributeType>' format. " +
                                "The SQL query is expected to return the attributes in the given order. e.g., If one " +
                                "attribute is defined here, the SQL query should return one column result set. " +
                                "If more than one column is returned, then the first column is processed. The " +
                                "Siddhi data types supported are 'STRING', 'INT', 'LONG', 'DOUBLE', 'FLOAT', and " +
                                "'BOOL'. \n Mapping of the Siddhi data type to the database data type can be done as " +
                                "follows, \n" +
                                "*Siddhi Datatype* -> *Datasource Datatype*\n" +
                                "`STRING` -> `CHAR`,`VARCHAR`,`LONGVARCHAR`\n" +
                                "`INT`\t-> `INTEGER`\n" +
                                "`LONG`\t-> `BIGINT`\n" +
                                "`DOUBLE`-> `DOUBLE`\n" +
                                "`FLOAT`\t-> `REAL`\n" +
                                "`BOOL`\t-> `BIT`\n",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "query",
                        description = "The select query(formatted according to " +
                                "the relevant database type) that needs to be performed",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(
                        name = "parameter",
                        description = "If the second parameter is a parametrised SQL query, then siddhi attributes " +
                                "can be passed to set the values of the parameters",
                        type = {DataType.STRING, DataType.BOOL, DataType.INT, DataType.DOUBLE, DataType.FLOAT,
                                DataType.LONG},
                        optional = true,
                        defaultValue = "<Empty_String>",
                        dynamic = true
                )
        },
        parameterOverloads = {
                @ParameterOverload(
                        parameterNames = {"datasource.name", "attribute.definition.list", "query"}
                ),
                @ParameterOverload(
                        parameterNames = {"datasource.name", "attribute.definition.list", "query", "parameter"}
                ),
                @ParameterOverload(
                        parameterNames = {"datasource.name", "attribute.definition.list", "query", "parameter", "..."}
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
                        syntax = "from TriggerStream#rdbms:query('SAMPLE_DB', 'creditcardno string, country string, " +
                                "transaction string, amount int', 'select * from Transactions_Table') \n" +
                                "select creditcardno, country, transaction, amount \n" +
                                "insert into recordStream;",
                        description = "Events inserted into recordStream includes all records matched for the query " +
                                "i.e an event will be generated for each record retrieved from the datasource. " +
                                "The event will include as additional attributes, the attributes defined in the " +
                                "`attribute.definition.list`(creditcardno, country, transaction, amount)."
                ),
                @Example(
                        syntax = "from TriggerStream#rdbms:query('SAMPLE_DB', 'creditcardno string, country string," +
                                "transaction string, amount int', 'select * from where country=?', " +
                                "countrySearchWord) " +
                                "select creditcardno, country, transaction, amount \n" +
                                "insert into recordStream;",
                        description = "Events inserted into recordStream includes all records matched for the query " +
                                "i.e an event will be generated for each record retrieved from the datasource. " +
                                "The event will include as additional attributes, the attributes defined in the " +
                                "`attribute.definition.list`(creditcardno, country, transaction, amount). " +
                                "countrySearchWord value from the event will be set in the query when querying the " +
                                "datasource."
                )
        }
)
public class QueryStreamProcessor extends StreamProcessor<State> {
    private SiddhiContext siddhiContext;
    private String dataSourceName;
    private DataSource dataSource;
    private ExpressionExecutor queryExpressionExecutor;
    private List<Attribute> attributeList = new ArrayList<>();
    private boolean isQueryParameterised = false;
    private List<ExpressionExecutor> expressionExecutors = new ArrayList<>();


    @Override
    protected StateFactory<State> init(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                StreamEventClonerHolder streamEventClonerHolder, boolean outputExpectsExpiredEvents,
                                boolean findToBeExecuted, SiddhiQueryContext siddhiQueryContext) {

        int attributesLength = attributeExpressionExecutors.length;
        this.dataSourceName = RDBMSStreamProcessorUtil.validateDatasourceName(attributeExpressionExecutors[0]);
        this.siddhiContext = siddhiQueryContext.getSiddhiAppContext().getSiddhiContext();

        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            String attributeDefinition = ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
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
        } else {
            throw new SiddhiAppValidationException("The parameter 'attribute.definition.list' in rdbms query " +
                    "function should be a constant, but found a dynamic attribute of type '" +
                    queryExpressionExecutor.getClass().getCanonicalName() + "'.");
        }

        this.queryExpressionExecutor = attributeExpressionExecutors[2];

        if (attributesLength > 3) {
            this.isQueryParameterised = true;
            this.expressionExecutors.addAll(
                    Arrays.asList(attributeExpressionExecutors).subList(3, attributeExpressionExecutors.length));

            if (queryExpressionExecutor instanceof ConstantExpressionExecutor) {
                String query = ((ConstantExpressionExecutor) queryExpressionExecutor).getValue().toString();
                long attributeCount = query.chars().filter(ch -> ch == '?').count();
                if (attributeCount != attributesLength - 3) {
                    throw new SiddhiAppValidationException("The parameter 'query' in rdbms query function " +
                            "contains '" + attributeCount + "' ordinals, but found siddhi attributes of count '" +
                            (attributesLength - 3) + "'.");
                }
            } else {
                throw new SiddhiAppValidationException("The parameter 'query' in rdbms query " +
                        "function should be a constant, but found a parameter of instance '" +
                        attributeExpressionExecutors[1].getClass().getName() + "'.");
            }
        }
        return null;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           State state) {
        Connection conn = this.getConnection();
        PreparedStatement stmt = null;
        ResultSet resultSet = null;

        try {
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                String query = ((String) queryExpressionExecutor.execute(event));
                stmt = conn.prepareStatement(query);
                if (isQueryParameterised) {
                    for (int i = 0; i < this.expressionExecutors.size(); i++) {
                        ExpressionExecutor attributeExpressionExecutor = this.expressionExecutors.get(i);
                        RDBMSStreamProcessorUtil.populateStatementWithSingleElement(stmt, i + 1,
                                attributeExpressionExecutor.getReturnType(),
                                attributeExpressionExecutor.execute(event));
                    }
                }
                resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(event);
                    Object[] data = RDBMSStreamProcessorUtil.processRecord(this.attributeList, resultSet);
                    complexEventPopulater.populateComplexEvent(clonedEvent, data);
                    streamEventChunk.insertBeforeCurrent(clonedEvent);
                }
                streamEventChunk.remove();
                RDBMSStreamProcessorUtil.cleanupConnection(resultSet, stmt, null);
            }
            nextProcessor.process(streamEventChunk);
        } catch (SQLException e) {
            throw new SiddhiAppRuntimeException("Error in retrieving records from  datasource '"
                    + this.dataSourceName + "': " + e.getMessage(), e);
        } finally {
            RDBMSStreamProcessorUtil.cleanupConnection(resultSet, stmt, conn);
        }
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
        this.dataSource = siddhiContext.getSiddhiDataSource(this.dataSourceName);
        if (this.dataSource == null) {
            this.dataSource = RDBMSStreamProcessorUtil.getDataSourceService(this.dataSourceName);
        }
    }

    @Override
    public void stop() {

    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return this.attributeList;
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.SLIDE;
    }

}
