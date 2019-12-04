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

import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.table.record.UpdateOrInsertReducer;
import io.siddhi.core.util.collection.operator.CompiledCondition;

import java.util.List;
import java.util.SortedMap;

/**
 * Implementation class of {@link CompiledCondition} corresponding to the RDBMS Event Table.
 * Maintains the condition string returned by the ConditionVisitor as well as a map of parameters to be used at runtime.
 */
public class RDBMSCompiledCondition implements CompiledCondition {

    private String compiledQuery;
    private boolean useSubSelect;
    private String subSelectQuerySelectors;
    private String outerCompiledCondition;
    private UpdateOrInsertReducer updateOrInsertReducer;
    private ExpressionExecutor inMemorySetExpressionExecutor;
    private SortedMap<Integer, Object> parameters;
    private boolean isContainsConditionExist;
    private List<Integer> ordinalOfContainPattern;

    public RDBMSCompiledCondition(String compiledQuery, SortedMap<Integer, Object> parameters,
                                  boolean isContainsConditionExist, List<Integer> ordinalOfContainPattern,
                                  boolean useSubSelect, String subSelectQuerySelectors,
                                  String outerCompiledCondition, UpdateOrInsertReducer updateOrInsertReducer,
                                  ExpressionExecutor inMemorySetExpressionExecutor) {
        this.compiledQuery = compiledQuery;
        this.parameters = parameters;
        this.isContainsConditionExist = isContainsConditionExist;
        this.ordinalOfContainPattern = ordinalOfContainPattern;
        this.useSubSelect = useSubSelect;
        this.subSelectQuerySelectors = subSelectQuerySelectors;
        this.outerCompiledCondition = outerCompiledCondition;
        this.updateOrInsertReducer = updateOrInsertReducer;
        this.inMemorySetExpressionExecutor = inMemorySetExpressionExecutor;
    }

    public void setCompiledQuery(String compiledQuery) {
        this.compiledQuery = compiledQuery;
    }

    public String getCompiledQuery() {
        return compiledQuery;
    }

    public boolean isContainsConditionExist() {
        return isContainsConditionExist;
    }

    public boolean isUseSubSelect() {
        return useSubSelect;
    }

    public String getSubSelectQuerySelectors() {
        return subSelectQuerySelectors;
    }

    public String getOuterCompiledCondition() {
        return outerCompiledCondition;
    }

    public String toString() {
        return getCompiledQuery();
    }

    public SortedMap<Integer, Object> getParameters() {
        return parameters;
    }

    public List<Integer> getOrdinalOfContainPattern() {
        return ordinalOfContainPattern;
    }

    public UpdateOrInsertReducer getUpdateOrInsertReducer() {
        return updateOrInsertReducer;
    }

    public ExpressionExecutor getInMemorySetExpressionExecutor() {
        return inMemorySetExpressionExecutor;
    }
}
