/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.extension.store.rdbms;

import io.siddhi.core.util.collection.operator.CompiledCondition;

import java.util.SortedMap;

/**
 * Implementation class of {@link CompiledCondition} corresponding to the RDBMS Event Table.
 */
public class RDBMSCompiledClause implements CompiledCondition {

    private static final long serialVersionUID = 6106269976155338045L;
  
    /**
     * compiledQuery is a portion of a prepared SQL statement. For example, '?,?' is a compiledQuery
     * where '?' is a placeholder for a expression.
     * These placeholders are replaced using the parameters in the parameters Map.
     */
    private String compiledQuery;

    /**
     * Parameters that will be used to fill in the placeholders in the compiledQuery.
     * The key of the map is the index of the parameter in the compiledQuery. Index starts with 1.
     */
    private SortedMap<Integer, Object> parameters;

    public RDBMSCompiledClause(String compiledQuery, SortedMap<Integer, Object> parameters) {
        this.compiledQuery = compiledQuery;
        this.parameters = parameters;
    }

    public String getCompiledQuery() {
        return compiledQuery;
    }

    public String toString() {
        return getCompiledQuery();
    }

    public SortedMap<Integer, Object> getParameters() {
        return parameters;
    }
}
