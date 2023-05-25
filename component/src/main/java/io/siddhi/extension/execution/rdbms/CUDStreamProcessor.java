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
import io.siddhi.annotation.SystemParameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

/**
 * This extension can be used to perform SQL CUD (INSERT, UPDATE, DELETE) queries on a datasource.
 */
@Extension(
        name = "cud",
        namespace = "rdbms",
        description = "This function performs SQL CUD (INSERT, UPDATE, DELETE) queries on " +
                "data sources. \nNote: This function to work data sources should be set at the Siddhi Manager level.\n",
        parameters = {
                @Parameter(
                        name = "datasource.name",
                        description = "The name of the datasource for which the query should be performed. If Siddhi " +
                                "is used as a Java/Python library the datasource should be explicitly set in the " +
                                "siddhi manager in order for the function to work.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "query",
                        description = "The update, delete, or insert query(formatted according to " +
                                "the relevant database type) that needs to be performed.",
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
                        parameterNames = {"datasource.name", "query"}
                ),
                @ParameterOverload(
                        parameterNames = {"datasource.name", "query", "parameter"}
                ),
                @ParameterOverload(
                        parameterNames = {"datasource.name", "query", "parameter", "..."}
                )
        },
        systemParameter = {
                @SystemParameter(
                        name = "perform.CUD.operations",
                        description = "If this parameter is set to 'true', the RDBMS CUD function is enabled to " +
                                "perform CUD operations.",
                        defaultValue = "false",
                        possibleParameters = {"true", "false"}
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "numRecords",
                        description = "The number of records manipulated by the query.",
                        type = DataType.INT
                )
        },
        examples = {
                @Example(
                        syntax = "from TriggerStream#rdbms:cud(\"SAMPLE_DB\", \"UPDATE Customers_Table SET " +
                                "customerName='abc' where customerName='xyz'\") \n" +
                                "select numRecords \n" +
                                "insert into  RecordStream;",
                        description = "This query updates the events from the input stream named 'TriggerStream' " +
                                "with an additional attribute named 'numRecords', of which the value indicates the" +
                                " number of records manipulated. The updated events are inserted into an output " +
                                "stream named 'RecordStream'."
                ),
                @Example(
                        syntax = "from TriggerStream#rdbms:cud(\"SAMPLE_DB\", \"UPDATE Customers_Table SET " +
                                "customerName=? where customerName=?\", changedName, previousName) \n" +
                                "select numRecords \n" +
                                "insert into  RecordStream;",
                        description = "This query updates the events from the input stream named 'TriggerStream' " +
                                "with an additional attribute named 'numRecords', of which the value indicates the" +
                                " number of records manipulated. The updated events are inserted into an output " +
                                "stream named 'RecordStream'. Here the values of attributes changedName and " +
                                "previousName in the event will be set to the query."
                )
        }
)
public class CUDStreamProcessor extends StreamProcessor<State> {
    private static final Logger log = LoggerFactory.getLogger(CUDStreamProcessor.class);
    private static final String COMMIT = "commit";
    private static final String ROLLBACK = "rollback";

    /**
     * Stores connections mapped against transactionCorrelationIds.
     * This helps multiple CUD processors to use the same connection, referred by a transactionCorrelationId.
     * Using the same connection can be useful in performing transactions via CUD processors
     * (eg: committing or rolling back queries after a series of insertions).
     */
    private static Map<String, Connection> correlatedConnections = new ConcurrentHashMap<>();

    private SiddhiContext siddhiContext;
    private String dataSourceName;
    private DataSource dataSource;
    private ExpressionExecutor queryExpressionExecutor;
    private boolean isQueryParameterised;
    private List<ExpressionExecutor> expressionExecutors = new ArrayList<>();
    private List<Attribute> attributeList = new ArrayList<>();
    private String transactionCorrelationId;
    private boolean enableCudOperationAutocommit = true;

    @Override
    protected StateFactory<State> init(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                StreamEventClonerHolder streamEventClonerHolder, boolean outputExpectsExpiredEvents,
                                boolean findToBeExecuted, SiddhiQueryContext siddhiQueryContext) {
        boolean performCUDOps = Boolean.parseBoolean(
                configReader.readConfig("perform.CUD.operations", "false"));
        if (!performCUDOps) {
            throw new SiddhiAppValidationException("Performing CUD operations through " +
                    "rdbms cud function is disabled. This is configured through system parameter, " +
                    "'perform.CUD.operations' in '<SIDDHI_HOME>/conf/<profile>/deployment.yaml'");
        }

        this.dataSourceName = RDBMSStreamProcessorUtil.validateDatasourceName(attributeExpressionExecutors[0]);
        this.siddhiContext = siddhiQueryContext.getSiddhiAppContext().getSiddhiContext();
        processAttributeExpressionExecutors(attributeExpressionExecutors);
        attributeList = Collections.singletonList(new Attribute("numRecords", Attribute.Type.INT));
        return null;
    }

    private void processAttributeExpressionExecutors(ExpressionExecutor[] attributeExpressionExecutors) {
        this.queryExpressionExecutor = attributeExpressionExecutors[1];
        if (!(queryExpressionExecutor instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("The parameter 'query' in rdbms query " +
                    "function should be a constant, but found a parameter of instance '" +
                    attributeExpressionExecutors[1].getClass().getName() + "'.");
        }

        String query = ((ConstantExpressionExecutor) queryExpressionExecutor).getValue().toString();
        long parameterizedAttributeCount = query.chars().filter(ch -> ch == '?').count();
        long givenAttributeCount = attributeExpressionExecutors.length - 2;

        if (parameterizedAttributeCount == 0) {
            // Query is not parameterized
            if (givenAttributeCount == 1) {
                // Take the given attribute as transactionCorrelationId
                ExpressionExecutor transactionCorrelationIdExecutor = attributeExpressionExecutors[2];
                if (!(transactionCorrelationIdExecutor instanceof ConstantExpressionExecutor)) {
                    throw new SiddhiAppValidationException("The parameter 'transactionCorrelationId' in rdbms query " +
                            "function should be a constant, but found a parameter of instance '" +
                            transactionCorrelationIdExecutor.getClass().getName() + "'.");
                }
                transactionCorrelationId = ((ConstantExpressionExecutor) transactionCorrelationIdExecutor)
                        .getValue().toString();
            } else if (givenAttributeCount > 1) {
                throw new SiddhiAppValidationException("Siddhi attribute count should be either 0, " +
                        "or 1 which is the transaction correlation ID, but found " + givenAttributeCount);
            }
        } else {
            // Query is parameterized
            this.isQueryParameterised = true;

            int endIndex;
            if (givenAttributeCount == parameterizedAttributeCount) {
                // All the given attributes are parameterized attribute replacements
                endIndex = attributeExpressionExecutors.length;
            } else if (givenAttributeCount == parameterizedAttributeCount + 1) {
                // The last given attribute is the transactionCorrelationId
                ExpressionExecutor transactionCorrelationIdExecutor =
                        attributeExpressionExecutors[attributeExpressionExecutors.length - 1];
                if (!(transactionCorrelationIdExecutor instanceof ConstantExpressionExecutor)) {
                    throw new SiddhiAppValidationException("The parameter 'transactionCorrelationId' in rdbms query " +
                            "function should be a constant, but found a parameter of instance '" +
                            transactionCorrelationIdExecutor.getClass().getName() + "'.");
                }
                transactionCorrelationId =
                        ((ConstantExpressionExecutor) transactionCorrelationIdExecutor).getValue().toString();

                // Former ones are parameterized attribute replacements
                endIndex = attributeExpressionExecutors.length - 1;
            } else {
                throw new SiddhiAppValidationException("The parameter 'query' in rdbms query function contains " +
                        parameterizedAttributeCount + " ordinals. Siddhi attribute count should be either " +
                        parameterizedAttributeCount + ", or " + (parameterizedAttributeCount + 1) +
                        " where the last attribute is the transaction correlation ID, but found " +
                        givenAttributeCount);
            }
            this.expressionExecutors.addAll(Arrays.asList(attributeExpressionExecutors).subList(2, endIndex));
        }

        if (transactionCorrelationId != null) {
            // Providing transactionCorrelationId disables autocommit for this RDBMS query function
            enableCudOperationAutocommit = false;
            log.info("Autocommit has been disabled for RDBMS query function, since transaction correlation ID: '" +
                    transactionCorrelationId + "' has been given. Make sure that, a 'COMMIT' or 'ROLLBACK' query is " +
                    "explicitly executed with transaction correlation ID: '" + transactionCorrelationId +
                    "' in a RDBMS query function (Ignore this message if it has been already done).");
        } else if (query.equalsIgnoreCase(COMMIT) || query.equalsIgnoreCase(ROLLBACK)) {
            log.warn("'" + query + "' operation is present without a transaction correlation ID. Therefore autocommit" +
                    " has NOT been disabled for the RDBMS query function. Please recheck this operation, since" +
                    " it will NOT be effective.");
        }
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           State state) {
        Connection conn = this.getConnection();
        PreparedStatement stmt = null;
        boolean shouldCleanupConnection = enableCudOperationAutocommit;
        try {
            if (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                String query = ((String) queryExpressionExecutor.execute(event));
                stmt = conn.prepareStatement(query);
                if (!streamEventChunk.hasNext() && !isQueryParameterised) {
                    stmt.addBatch();
                }
                if (RDBMSStreamProcessorUtil.queryContainsCheck(query)) {
                    throw new SiddhiAppRuntimeException("Dropping event since the query has " +
                            "unauthorised operations, '" + query + "'. Event: '" + event + "'.");
                }
            }
            streamEventChunk.reset();
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                if (isQueryParameterised) {
                    if (conn.getAutoCommit()) {
                        //commit transaction manually
                        conn.setAutoCommit(false);
                    }
                    for (int i = 0; i < this.expressionExecutors.size(); i++) {
                        ExpressionExecutor attributeExpressionExecutor = this.expressionExecutors.get(i);
                        RDBMSStreamProcessorUtil.populateStatementWithSingleElement(stmt, i + 1,
                                attributeExpressionExecutor.getReturnType(),
                                attributeExpressionExecutor.execute(event));
                    }
                    stmt.addBatch();
                }
            }
            int counter = 0;
            if (stmt != null) {
                int[] numRecords = stmt.executeBatch();
                if (!conn.getAutoCommit() && enableCudOperationAutocommit) {
                    conn.commit();
                    shouldCleanupConnection = true;
                }
                streamEventChunk.reset();
                while (streamEventChunk.hasNext()) {
                    StreamEvent event = streamEventChunk.next();
                    Object[] data = {numRecords[counter]};
                    counter++;
                    complexEventPopulater.populateComplexEvent(event, data);
                }
            }
        } catch (SQLException e) {
            throw new SiddhiAppRuntimeException("Error in manipulating records from " +
                    "datasource '" + this.dataSourceName + "': " + e.getMessage(), e);
        } finally {
            // Always cleanup the prepared statement
            RDBMSStreamProcessorUtil.cleanupConnection(null, stmt, null);

            if (!shouldCleanupConnection) {
                // Connection should be cleaned up if the performed query is "commit" or "rollback".
                String query = ((ConstantExpressionExecutor) queryExpressionExecutor).getValue().toString();
                if (query.equalsIgnoreCase(COMMIT) || query.equalsIgnoreCase(ROLLBACK)) {
                    shouldCleanupConnection = true;
                }
            }

            if (shouldCleanupConnection) {
                RDBMSStreamProcessorUtil.cleanupConnection(null, null, conn);
                if (transactionCorrelationId != null) {
                    // Connection is no more needed since it has done a commit or a rollback.
                    // A CUD expression with the same transactionCorrelationId can create a new connection and use that.
                    correlatedConnections.remove(transactionCorrelationId);
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    private Connection getConnection() {
        if (transactionCorrelationId == null) {
            return createNewConnection();
        } else {
            Connection conn = correlatedConnections.get(transactionCorrelationId);
            try {
                if (conn != null && !conn.isClosed()) {
                    return conn; // Re-use the connection
                }
            } catch (SQLException e) {
                // Absorb and proceed with creating a new connection below
            }

            conn = createNewConnection();
            correlatedConnections.put(transactionCorrelationId, conn);
            return conn;
        }
    }

    private Connection createNewConnection() {
        Connection conn;
        try {
            conn = this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new SiddhiAppRuntimeException("Error initializing datasource connection: " + e.getMessage(), e);
        }
        return conn;
    }

    @Override
    public void start() {
        this.dataSource = this.siddhiContext.getSiddhiDataSource(this.dataSourceName);
        if (this.dataSource == null) {
            this.dataSource = RDBMSStreamProcessorUtil.getDataSourceService(this.dataSourceName);
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return attributeList;
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.SLIDE;
    }

}
