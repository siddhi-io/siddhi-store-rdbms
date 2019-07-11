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

import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;

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
    private SortedMap<Integer, Object> parameters;
    private boolean isContainsConditionExist;
    private int ordinalOfContainPattern;

    public RDBMSCompiledCondition(String compiledQuery, SortedMap<Integer, Object> parameters,
                                  boolean isContainsConditionExist, int ordinalOfContainPattern,
                                  boolean useSubSelect, String subSelectQuerySelectors,
                                  String outerCompiledCondition) {
        this.compiledQuery = compiledQuery;
        this.parameters = parameters;
        this.isContainsConditionExist = isContainsConditionExist;
        this.ordinalOfContainPattern = ordinalOfContainPattern;
        this.useSubSelect = useSubSelect;
        this.subSelectQuerySelectors = subSelectQuerySelectors;
        this.outerCompiledCondition = outerCompiledCondition;
    }

    @Override
    public CompiledCondition cloneCompilation(String key) {
        return new RDBMSCompiledCondition(this.compiledQuery, this.parameters, this.isContainsConditionExist,
                this.ordinalOfContainPattern, this.useSubSelect, this.subSelectQuerySelectors,
                this.outerCompiledCondition);
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

    public void setCompiledQuery(String compiledQuery) {
        this.compiledQuery = compiledQuery;
    }

    public String getCompiledQuery() {
        return compiledQuery;
    }

    public boolean isContainsConditionExist() {
        return isContainsConditionExist;
    }

    public String toString() {
        return getCompiledQuery();
    }

    public SortedMap<Integer, Object> getParameters() {
        return parameters;
    }

    public int getOrdinalOfContainPattern() {
        return ordinalOfContainPattern;
    }
}
