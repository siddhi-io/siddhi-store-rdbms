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
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.query.api.definition.Attribute;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * This is the interface for ProcedureStream function
 */
public interface FunctionStreamProcessor {
    Integer processOutputParameters(ConstantExpressionExecutor constantExpressionExecutor);

    ComplexEventChunk<StreamEvent> getResults(CallableStatement stmt, int noOfParameterizedPositions,
                                              Integer outputParameters, StreamEventCloner streamEventCloner,
                                              List<Attribute> attributeList, StreamEvent event,
                                              ComplexEventPopulater complexEventPopulater,
                                              ComplexEventChunk<StreamEvent> streamEventChunk) throws SQLException;
}
