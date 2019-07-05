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

import org.wso2.extension.siddhi.store.rdbms.util.Constant;
import org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants;
import org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableUtils;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.table.record.BaseExpressionVisitor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.condition.Compare;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.Stack;
import java.util.TreeMap;

import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.CLOSE_PARENTHESIS;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.EQUALS;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.OPEN_PARENTHESIS;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.SQL_AS;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.SQL_MAX;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.SUB_SELECT_QUERY_REF;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.WHITESPACE;


/**
 * Class which is used by the Siddhi runtime for instructions on converting the SiddhiQL condition to the condition
 * format understood by the underlying RDBMS data store.
 */
public class RDBMSConditionVisitor extends BaseExpressionVisitor {

    private StringBuilder condition;
    private String finalCompiledCondition;
    private String tableName;
    private boolean isAfterSelectClause;

    private Map<String, Object> placeholders;
    private SortedMap<Integer, Object> parameters;

    private int streamVarCount;
    private int constantCount;

    private boolean isContainsConditionExist;
    private boolean nextProcessContainsPattern;
    private int ordinalOfContainPattern = 1;

    private boolean containsAttributeFunction = false;
    private boolean lastConditionExist = false;
    private Stack<String> lastConditionParams;
    private StringBuilder subSelect;
    private StringBuilder outerCompiledCondition;

    private String[] supportedFunctions = {"sum", "avg", "min", "max"};

    public RDBMSConditionVisitor(String tableName, boolean isAfterSelectClause) {
        this.tableName = tableName;
        this.condition = new StringBuilder();
        this.streamVarCount = 0;
        this.constantCount = 0;
        this.placeholders = new HashMap<>();
        this.parameters = new TreeMap<>();
        this.subSelect = new StringBuilder();
        this.outerCompiledCondition = new StringBuilder();
        this.lastConditionParams = new Stack<>();
        this.isAfterSelectClause = isAfterSelectClause;
    }

    private RDBMSConditionVisitor() {
        //preventing initialization
    }

    public String returnCondition() {
        this.parametrizeCondition();
        return this.finalCompiledCondition.trim();
    }

    public String returnMaxVariableCondition() {
        return this.subSelect.toString();
    }

    public boolean isContainsAttributeFunction() {
        return containsAttributeFunction;
    }

    public String getOuterCompiledCondition() {
        return outerCompiledCondition.toString();
    }

    public boolean isLastConditionExist() {
        return lastConditionExist;
    }

    public int getOrdinalOfContainPattern() {
        return ordinalOfContainPattern;
    }

    public SortedMap<Integer, Object> getParameters() {
        return this.parameters;
    }

    public boolean isContainsConditionExist() {
        return isContainsConditionExist;
    }

    @Override
    public void beginVisitAnd() {
        condition.append(OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitAnd() {
        condition.append(CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitAndLeftOperand() {
        //Not applicable
    }

    @Override
    public void endVisitAndLeftOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitAndRightOperand() {
        condition.append(RDBMSTableConstants.SQL_AND).append(WHITESPACE);
    }

    @Override
    public void endVisitAndRightOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitOr() {
        condition.append(OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitOr() {
        condition.append(CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitOrLeftOperand() {
        //Not applicable
    }

    @Override
    public void endVisitOrLeftOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitOrRightOperand() {
        condition.append(RDBMSTableConstants.SQL_OR).append(WHITESPACE);
    }

    @Override
    public void endVisitOrRightOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitNot() {
        condition.append(RDBMSTableConstants.SQL_NOT).append(WHITESPACE);
    }

    @Override
    public void endVisitNot() {
        //Not applicable
    }

    @Override
    public void beginVisitCompare(Compare.Operator operator) {
        condition.append(OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitCompare(Compare.Operator operator) {
        condition.append(CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitCompareLeftOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void endVisitCompareLeftOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void beginVisitCompareRightOperand(Compare.Operator operator) {
        switch (operator) {
            case EQUAL:
                condition.append(RDBMSTableConstants.SQL_COMPARE_EQUAL);
                break;
            case GREATER_THAN:
                condition.append(RDBMSTableConstants.SQL_COMPARE_GREATER_THAN);
                break;
            case GREATER_THAN_EQUAL:
                condition.append(RDBMSTableConstants.SQL_COMPARE_GREATER_THAN_EQUAL);
                break;
            case LESS_THAN:
                condition.append(RDBMSTableConstants.SQL_COMPARE_LESS_THAN);
                break;
            case LESS_THAN_EQUAL:
                condition.append(RDBMSTableConstants.SQL_COMPARE_LESS_THAN_EQUAL);
                break;
            case NOT_EQUAL:
                condition.append(RDBMSTableConstants.SQL_COMPARE_NOT_EQUAL);
                break;
        }
        condition.append(WHITESPACE);
    }

    @Override
    public void endVisitCompareRightOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void beginVisitIsNull(String streamId) {
    }

    @Override
    public void endVisitIsNull(String streamId) {
        condition.append(RDBMSTableConstants.SQL_IS_NULL).append(WHITESPACE);
    }

    @Override
    public void beginVisitIn(String storeId) {
        condition.append(RDBMSTableConstants.SQL_IN).append(WHITESPACE);
    }

    @Override
    public void endVisitIn(String storeId) {
        //Not applicable
    }

    @Override
    public void beginVisitConstant(Object value, Attribute.Type type) {
        String name;
        if (nextProcessContainsPattern) {
            name = this.generatePatternConstantName();
            nextProcessContainsPattern = false;
        } else {
            name = this.generateConstantName();
        }
        this.placeholders.put(name, new Constant(value, type));
        condition.append("[").append(name).append("]").append(WHITESPACE);
    }

    @Override
    public void endVisitConstant(Object value, Attribute.Type type) {
        //Not applicable
    }

    @Override
    public void beginVisitMath(MathOperator mathOperator) {
        condition.append(OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitMath(MathOperator mathOperator) {
        condition.append(CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitMathLeftOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void endVisitMathLeftOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void beginVisitMathRightOperand(MathOperator mathOperator) {
        switch (mathOperator) {
            case ADD:
                condition.append(RDBMSTableConstants.SQL_MATH_ADD);
                break;
            case DIVIDE:
                condition.append(RDBMSTableConstants.SQL_MATH_DIVIDE);
                break;
            case MOD:
                condition.append(RDBMSTableConstants.SQL_MATH_MOD);
                break;
            case MULTIPLY:
                condition.append(RDBMSTableConstants.SQL_MATH_MULTIPLY);
                break;
            case SUBTRACT:
                condition.append(RDBMSTableConstants.SQL_MATH_SUBTRACT);
                break;
        }
        condition.append(WHITESPACE);
    }

    @Override
    public void endVisitMathRightOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void beginVisitAttributeFunction(String namespace, String functionName) {
        if (RDBMSTableUtils.isEmpty(namespace) &&
                (Arrays.stream(supportedFunctions).anyMatch(functionName::equals))) {
            condition.append(functionName).append(OPEN_PARENTHESIS);
            containsAttributeFunction = true;
        } else if (namespace.trim().equals("str") && functionName.equals("contains")) {
            condition.append("CONTAINS").append(OPEN_PARENTHESIS);
            isContainsConditionExist = true;
            nextProcessContainsPattern = true;
        } else if (namespace.trim().equals("incrementalAggregator") && functionName.equals("last")) {
            lastConditionExist = true;
        } else {
            throw new OperationNotSupportedException("The RDBMS Event table does not support functions other than " +
                    "sum(), avg(), min(), max(), str:contains() and incrementalAggregator:last() but function '" +
                    ((RDBMSTableUtils.isEmpty(namespace)) ? "" + functionName : namespace + ":" + functionName) +
                    "' was specified.");

        }
    }

    @Override
    public void endVisitAttributeFunction(String namespace, String functionName) {
        if ((namespace.trim().equals("str") && functionName.equals("contains")) ||
                (Arrays.stream(supportedFunctions).anyMatch(functionName::equals))) {
            condition.append(CLOSE_PARENTHESIS).append(WHITESPACE);
        } else if ((namespace.trim().equals("incrementalAggregator") && functionName.equals("last"))) {
            String maxVariableName = lastConditionParams.pop();
            subSelect.append(SQL_MAX).append(OPEN_PARENTHESIS)
                    .append(this.tableName).append(".").append(maxVariableName).append(CLOSE_PARENTHESIS)
                    .append(SQL_AS).append("MAX_").append(maxVariableName);
            outerCompiledCondition.append(this.tableName).append(".").append(maxVariableName).append(EQUALS)
                    .append(SUB_SELECT_QUERY_REF).append(".").append("MAX_").append(maxVariableName);

            String attributeName = lastConditionParams.pop();

            condition.append(SQL_MAX).append(OPEN_PARENTHESIS).append(this.tableName).append(".").append(attributeName)
                    .append(CLOSE_PARENTHESIS).append(WHITESPACE);

        } else {
            throw new OperationNotSupportedException("The RDBMS Event table does not support functions other than " +
                    "sum(), avg(), min(), max(), str:contains() and incrementalAggregator:last() but function '" +
                    ((RDBMSTableUtils.isEmpty(namespace)) ? "" + functionName : namespace + ":" + functionName) +
                    "' was specified.");
        }
    }

    @Override
    public void beginVisitParameterAttributeFunction(int index) {
        //Not applicable
    }

    @Override
    public void endVisitParameterAttributeFunction(int index) {
        //Not applicable
    }

    @Override
    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        String name;
        if (nextProcessContainsPattern) {
            name = this.generatePatternStreamVarName();
            nextProcessContainsPattern = false;
        } else {
            name = this.generateStreamVarName();
        }
        this.placeholders.put(name, new Attribute(id, type));
        condition.append("[").append(name).append("]").append(WHITESPACE);
    }

    @Override
    public void endVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        //Not applicable
    }

    @Override
    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        if (!lastConditionExist) {
            if (!isAfterSelectClause) {
                condition.append(this.tableName).append(".").append(attributeName).append(WHITESPACE);
                outerCompiledCondition.append(this.tableName).append(".").append(attributeName).append(EQUALS)
                        .append(SUB_SELECT_QUERY_REF).append(".").append(attributeName);
            } else {
                condition.append(attributeName).append(WHITESPACE);
            }
        } else {
            lastConditionParams.push(attributeName);
        }
    }

    @Override
    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        //Not applicable
    }

    /**
     * Util method for walking through the generated condition string and isolating the parameters which will be filled
     * in later as part of building the SQL statement. This method will:
     * (a) eliminate all temporary placeholders and put "?" in their places.
     * (b) build and maintain a sorted map of ordinals and the coresponding parameters which will fit into the above
     * places in the PreparedStatement.
     */
    private void parametrizeCondition() {
        String query = this.condition.toString();
        String[] tokens = query.split("\\[");
        int ordinal = 1;
        for (String token : tokens) {
            if (token.contains("]")) {
                String candidate = token.substring(0, token.indexOf("]"));
                if (this.placeholders.containsKey(candidate)) {
                    this.parameters.put(ordinal, this.placeholders.get(candidate));
                    ordinal++;
                    if (candidate.equals("pattern-value")) {
                        ordinalOfContainPattern = ordinal;
                    }
                }
            }
        }
        for (String placeholder : this.placeholders.keySet()) {
            query = query.replace("[" + placeholder + "]", "?");
        }
        this.finalCompiledCondition = query;
    }

    /**
     * Method for generating a temporary placeholder for stream variables.
     *
     * @return a placeholder string of known format.
     */
    private String generateStreamVarName() {
        String name = "strVar" + this.streamVarCount;
        this.streamVarCount++;
        return name;
    }

    /**
     * Method for generating a temporary placeholder for constants.
     *
     * @return a placeholder string of known format.
     */
    private String generateConstantName() {
        String name = "const" + this.constantCount;
        this.constantCount++;
        return name;
    }

    /**
     * Method for generating a temporary placeholder for contains pattern as stream variables.
     *
     * @return a placeholder string of known format.
     */
    private String generatePatternStreamVarName() {
        String name = "pattern-value" + this.streamVarCount;
        this.streamVarCount++;
        return name;
    }

    /**
     * Method for generating a temporary placeholder for contains pattern as constants.
     *
     * @return a placeholder string of known format.
     */
    private String generatePatternConstantName() {
        String name = "pattern-value" + this.constantCount;
        this.constantCount++;
        return name;
    }

}
