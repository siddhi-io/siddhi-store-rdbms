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
import org.wso2.siddhi.annotation.SystemParameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This extension can be used to perform SQL CUD (INSERT, UPDATE, DELETE) queries on a wso2 datasource.
 */
@Extension(
        name = "cud",
        namespace = "rdbms",
        description = "The function can be used to perform SQL CUD (INSERT, UPDATE, DELETE) queries on a wso2 " +
                "datasource.",
        parameters = {
                @Parameter(
                        name = "datasource.name",
                        description = "The name of the wso2 datasource the query should be performed on",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "query",
                        description = "The select query that needs to be performed",
                        type = DataType.STRING
                )
        },
        systemParameter = {
                @SystemParameter(
                        name = "perform.CUD.operations",
                        description = "Enable/Disable performing CUD operations through rdbms cud function",
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
                                "insert all events into  recordStream;",
                        description = "Events will be modified with additional attribute of name 'numRecords' with " +
                                "the number of records manipulated."
                )
        }
)
public class CUDStreamProcessor extends StreamProcessor {
    private String siddhiAppName;
    private String dataSourceName;
    private HikariDataSource dataSource;
    private ExpressionExecutor queryExpressionExecutor;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {
        this.siddhiAppName = siddhiAppContext.getName();

        boolean performCUDOps = Boolean.parseBoolean(
                configReader.readConfig("perform.CUD.operations", "false"));
        if (!performCUDOps) {
            throw new SiddhiAppValidationException(this.siddhiAppName + " - performing CUD operations through " +
                    "rdbms cud function is disabled. This is configured through system parameter, " +
                    "'perform.CUD.operations' in '<SP_HOME>/conf/<profile>/deployment.yaml'");
        }

        if ((attributeExpressionExecutors.length != 2)) {
            throw new SiddhiAppValidationException(this.siddhiAppName + " - rdbms cud function " +
                    "should have 2 parameters , but found '" + attributeExpressionExecutors.length + "' parameters.");
        }

        this.dataSourceName = RDBMSStreamProcessorUtil
                .validateDatasourceName(attributeExpressionExecutors[0], this.siddhiAppName);

        if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.STRING) {
            queryExpressionExecutor = attributeExpressionExecutors[1];
        } else {
            throw new SiddhiAppValidationException(this.siddhiAppName + " - The parameter 'query' in rdbms cud " +
                    "function should be of type STRING, but found a parameter with type '" +
                    attributeExpressionExecutors[1].getReturnType() + "'.");
        }

        return Collections.singletonList(new Attribute("numRecords", Attribute.Type.INT));
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        Connection conn = this.getConnection();
        PreparedStatement stmt = null;

        try {
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                String query = ((String) queryExpressionExecutor.execute(event));
                if (RDBMSStreamProcessorUtil.queryContainsCheck(false, query)) {
                    throw new SiddhiAppRuntimeException(this.siddhiAppName + " - Dropping event since the query has " +
                            "unauthorised operations, '" + query + "'. Event: '" + event + "'.");
                }
                stmt = conn.prepareStatement(query);
                stmt.addBatch();
            }
            int counter = 0;
            if (stmt != null) {
                int[] numRecords = stmt.executeBatch();
                streamEventChunk.reset();
                while (streamEventChunk.hasNext()) {
                    StreamEvent event = streamEventChunk.next();
                    Object[] data = {numRecords[counter]};
                    counter++;
                    complexEventPopulater.populateComplexEvent(event, data);
                }
            }
        } catch (SQLException e) {
            throw new SiddhiAppRuntimeException(this.siddhiAppName + " - Error in manipulating records from " +
                    "datasource '" + this.dataSourceName + "': " + e.getMessage(), e);
        } finally {
            RDBMSStreamProcessorUtil.cleanupConnection(null, stmt, conn, this.siddhiAppName);
        }
        nextProcessor.process(streamEventChunk);
    }

    private Connection getConnection() {
        Connection conn;
        try {
            conn = this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new SiddhiAppRuntimeException(this.siddhiAppName + " - Error initializing datasource connection: "
                    + e.getMessage(), e);
        }
        return conn;
    }

    @Override
    public void start() {
        this.dataSource = RDBMSStreamProcessorUtil
                .getDataSourceService(this.dataSourceName, this.siddhiAppName);
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
