/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.extension.execution.rdbms.procedure;

import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.extension.execution.rdbms.util.RDBMSStreamProcessorUtil;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleTypes;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * This is the implementation for OracleFunctionStreamProcessor
 */
public class OracleFunctionStreamProcessor implements FunctionStreamProcessor {

    @Override
    public Integer processOutputParameters(ConstantExpressionExecutor constantExpressionExecutor) {
        Integer outputParameter = 0;
        String[] outParameterTypes = constantExpressionExecutor.getValue().toString().split(",");
        for (String outParameterType : outParameterTypes) {
            switch (outParameterType.trim().toLowerCase()) {
                case "varchar":
                    outputParameter = OracleTypes.VARCHAR;
                    break;
                case "cursor":
                    outputParameter = OracleTypes.CURSOR;
                    break;
                case "number":
                    outputParameter = OracleTypes.NUMBER;
                    break;
                case "boolean":
                    outputParameter = OracleTypes.BOOLEAN;
                    break;
                case "double":
                    outputParameter = OracleTypes.DOUBLE;
                    break;
                default:
                    throw new SiddhiAppValidationException("The provided input attribute type " +
                            outParameterType.trim() + "does not supported");
            }
        }
        return outputParameter;
    }

    @Override
    public ComplexEventChunk<StreamEvent> getResults(CallableStatement stmt, int noOfParameterizedPositions,
                                                     Integer outputParameter, StreamEventCloner streamEventCloner,
                                                     List<Attribute> attributeList, StreamEvent event,
                                                     ComplexEventPopulater complexEventPopulater,
                                                     ComplexEventChunk<StreamEvent> streamEventChunk)
            throws SQLException {
        ResultSet resultSet = null;
        StreamEvent clonedEvent;
        Object[] data;
        if (stmt.isWrapperFor(OracleCallableStatement.class)) {
            OracleCallableStatement cstmt = stmt.unwrap(OracleCallableStatement.class);
            switch (outputParameter) {
                case (OracleTypes.CURSOR):
                    resultSet = cstmt.getCursor(noOfParameterizedPositions);
                    while (resultSet.next()) {
                        clonedEvent = streamEventCloner.copyStreamEvent(event);
                        data = RDBMSStreamProcessorUtil.processRecord(attributeList, resultSet);
                        complexEventPopulater.populateComplexEvent(clonedEvent, data);
                        streamEventChunk.insertBeforeCurrent(clonedEvent);
                    }
                    break;
                case (OracleTypes.VARCHAR):
                    String outputString = cstmt.getString(noOfParameterizedPositions);
                    clonedEvent = streamEventCloner.copyStreamEvent(event);
                    data = new Object[]{outputString};
                    complexEventPopulater.populateComplexEvent(clonedEvent, data);
                    streamEventChunk.insertBeforeCurrent(clonedEvent);
                    break;
                case (OracleTypes.NUMBER):
                    int output = cstmt.getInt(noOfParameterizedPositions);
                    clonedEvent = streamEventCloner.copyStreamEvent(event);
                    data = new Object[]{output};
                    complexEventPopulater.populateComplexEvent(clonedEvent, data);
                    streamEventChunk.insertBeforeCurrent(clonedEvent);
                    break;
                case (OracleTypes.BOOLEAN):
                    boolean outputBool = cstmt.getBoolean(noOfParameterizedPositions);
                    clonedEvent = streamEventCloner.copyStreamEvent(event);
                    data = new Object[]{outputBool};
                    complexEventPopulater.populateComplexEvent(clonedEvent, data);
                    streamEventChunk.insertBeforeCurrent(clonedEvent);
                    break;
                case (OracleTypes.DOUBLE):
                    double outputDouble = cstmt.getDouble(noOfParameterizedPositions);
                    clonedEvent = streamEventCloner.copyStreamEvent(event);
                    data = new Object[]{outputDouble};
                    complexEventPopulater.populateComplexEvent(clonedEvent, data);
                    streamEventChunk.insertBeforeCurrent(clonedEvent);
                    break;
                default:
                    // Due to validation in init method, this will not be initiated
                    break;
            }
            RDBMSStreamProcessorUtil.cleanupConnection(resultSet, stmt, null);
            return streamEventChunk;
        } else {
            throw new SiddhiAppRuntimeException("Error Occurred while retrieving results. The callback statement " +
                    "is not a wrapper class of OracleCallableStatement");
        }
    }
}
