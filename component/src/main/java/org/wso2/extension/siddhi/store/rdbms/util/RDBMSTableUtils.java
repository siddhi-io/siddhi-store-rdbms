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
package org.wso2.extension.siddhi.store.rdbms.util;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.extension.siddhi.store.rdbms.RDBMSCompiledCondition;
import org.wso2.extension.siddhi.store.rdbms.RDBMSCompiledSelection;
import org.wso2.extension.siddhi.store.rdbms.config.RDBMSQueryConfiguration;
import org.wso2.extension.siddhi.store.rdbms.config.RDBMSQueryConfigurationEntry;
import org.wso2.extension.siddhi.store.rdbms.exception.RDBMSTableException;
import org.wso2.siddhi.core.exception.CannotLoadConfigurationException;
import org.wso2.siddhi.core.util.collection.operator.CompiledExpression;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.annotation.Element;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.CONTAINS_CONDITION_REGEX;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.DATABASE_PRODUCT_NAME;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.MAX_VERSION;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.MIN_VERSION;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_COLUMNS;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_CONDITION;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.PLACEHOLDER_VALUES;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.PROPERTY_SEPARATOR;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.QUESTION_MARK;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.RDBMS_QUERY_CONFIG_FILE;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.SQL_WHERE;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.VERSION;
import static org.wso2.extension.siddhi.store.rdbms.util.RDBMSTableConstants.WHITESPACE;


/**
 * Class which holds the utility methods which are used by various units in the RDBMS Event Table implementation.
 */
public class RDBMSTableUtils {

    private static RDBMSConfigurationMapper mapper;
    private static final Log log = LogFactory.getLog(RDBMSTableUtils.class);
    private static final Pattern CONTAINS_CONDITION_REGEX_PATTERN = Pattern.compile(CONTAINS_CONDITION_REGEX);

    private RDBMSTableUtils() {
        //preventing initialization
    }

    /**
     * Utility method which can be used to check if a given string instance is null or empty.
     *
     * @param field the string instance to be checked.
     * @return true if the field is null or empty.
     */
    public static boolean isEmpty(String field) {
        return (field == null || field.trim().length() == 0);
    }

    /**
     * Method which can be used to clear up and ephemeral SQL connectivity artifacts.
     *
     * @param rs   {@link ResultSet} instance (can be null)
     * @param stmt {@link PreparedStatement} instance (can be null)
     * @param conn {@link Connection} instance (can be null)
     */
    public static void cleanupConnection(ResultSet rs, Statement stmt, Connection conn) {
        if (rs != null) {
            try {
                rs.close();
                if (log.isDebugEnabled()) {
                    log.debug("Closed ResultSet");
                }
            } catch (SQLException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Error closing ResultSet: " + e.getMessage(), e);
                }
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
                if (log.isDebugEnabled()) {
                    log.debug("Closed PreparedStatement");
                }
            } catch (SQLException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Error closing PreparedStatement: " + e.getMessage(), e);
                }
            }
        }
        if (conn != null) {
            try {
                conn.close();
                if (log.isDebugEnabled()) {
                    log.debug("Closed Connection");
                }
            } catch (SQLException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Error closing Connection: " + e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Method which is used to roll back a DB connection (e.g. in case of any errors)
     *
     * @param conn the {@link Connection} instance to be rolled back (can be null).
     */
    public static void rollbackConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException ignore) { /* ignore */ }
        }
    }

    /**
     * Util method used throughout the RDBMS Event Table implementation which accepts a compiled condition (from
     * compile-time) and uses values from the runtime to populate the given {@link PreparedStatement}.
     *
     * @param stmt                  the {@link PreparedStatement} instance which has already been build with '?'
     *                              parameters to be filled.
     * @param compiledCondition     the compiled condition which was built during compile time and now is being provided
     *                              by the Siddhi runtime.
     * @param conditionParameterMap the map which contains the runtime value(s) for the condition.
     * @param seed                  the integer factor by which the ordinal count will be incremented when populating
     *                              the {@link PreparedStatement}.
     * @throws SQLException in the unlikely case where there are errors when setting values to the statement
     *                      (e.g. type mismatches)
     */
    public static int resolveCondition(PreparedStatement stmt, RDBMSCompiledCondition compiledCondition,
                                        Map<String, Object> conditionParameterMap, int seed) throws SQLException {
        int maxOrdinal = 0;
        SortedMap<Integer, Object> parameters = compiledCondition.getParameters();
        for (Map.Entry<Integer, Object> entry : parameters.entrySet()) {
            Object parameter = entry.getValue();
            int ordinal = entry.getKey();
            if (ordinal > maxOrdinal) {
                maxOrdinal = ordinal;
            }
            if (parameter instanceof Constant) {
                Constant constant = (Constant) parameter;
                populateStatementWithSingleElement(stmt, seed + ordinal, constant.getType(),
                        constant.getValue());
            } else {
                Attribute variable = (Attribute) parameter;
                populateStatementWithSingleElement(stmt, seed + ordinal, variable.getType(),
                        conditionParameterMap.get(variable.getName()));
            }
        }
        return maxOrdinal;
    }

    public static void resolveQuery(PreparedStatement stmt, RDBMSCompiledSelection rdbmsCompiledSelection,
                                    RDBMSCompiledCondition rdbmsCompiledCondition,
                                    Map<String, Object> conditionParameterMap, int seed,
                                    boolean isContainsConditionExist) throws SQLException {
        seed = seed + resolveCondition(stmt, rdbmsCompiledSelection.getCompiledSelectClause(),
                conditionParameterMap, seed);

        if (rdbmsCompiledCondition != null) {
            if (isContainsConditionExist) {
                seed = seed + resolveConditionForContainsCheck(stmt, rdbmsCompiledCondition, conditionParameterMap,
                        seed);
            } else {
                seed = seed + resolveCondition(stmt, rdbmsCompiledCondition, conditionParameterMap, seed);
            }
        }

        RDBMSCompiledCondition groupByClause = rdbmsCompiledSelection.getCompiledGroupByClause();
        if (groupByClause != null) {
            seed = seed + resolveCondition(stmt, groupByClause, conditionParameterMap, seed);
        }

        RDBMSCompiledCondition havingClause = rdbmsCompiledSelection.getCompiledHavingClause();
        if (havingClause != null) {
            seed = seed + resolveCondition(stmt, havingClause, conditionParameterMap, seed);
        }

        RDBMSCompiledCondition orderByClause = rdbmsCompiledSelection.getCompiledOrderByClause();
        if (orderByClause != null) {
            resolveCondition(stmt, orderByClause, conditionParameterMap, seed);
        }
    }

    public static int resolveConditionForContainsCheck(PreparedStatement stmt,
                                                        RDBMSCompiledCondition compiledCondition,
                                                        Map<String, Object> conditionParameterMap, int seed)
            throws SQLException {
        int maxOrdinal = 0;
        SortedMap<Integer, Object> parameters = compiledCondition.getParameters();
        for (Map.Entry<Integer, Object> entry : parameters.entrySet()) {
            Object parameter = entry.getValue();
            int ordinal = entry.getKey();
            if (ordinal > maxOrdinal) {
                maxOrdinal = ordinal;
            }
            if (parameter instanceof Constant) {
                Constant constant = (Constant) parameter;
                if (entry.getKey().equals(compiledCondition.getOrdinalOfContainPattern())) {
                    populateStatementWithSingleElement(stmt, seed + entry.getKey(), constant.getType(),
                            "%" + constant.getValue() + "%");
                } else {
                    populateStatementWithSingleElement(stmt, seed + entry.getKey(), constant.getType(),
                            constant.getValue());
                }
            } else {
                Attribute variable = (Attribute) parameter;
                if (entry.getKey().equals(compiledCondition.getOrdinalOfContainPattern())) {
                    populateStatementWithSingleElement(stmt, seed + entry.getKey(), variable.getType(),
                            "%" + conditionParameterMap.get(variable.getName()) + "%");
                } else {
                    populateStatementWithSingleElement(stmt, seed + entry.getKey(), variable.getType(),
                            conditionParameterMap.get(variable.getName()));
                }

            }
        }
        return maxOrdinal;
    }

    public static int enumerateUpdateSetEntries(Map<String, CompiledExpression> updateSetExpressions,
                                                PreparedStatement stmt, Map<String, Object> updateSetMap)
            throws SQLException {
        Object parameter;
        int ordinal = 1;
        for (Map.Entry<String, CompiledExpression> assignmentEntry : updateSetExpressions.entrySet()) {
            for (Map.Entry<Integer, Object> parameterEntry :
                    ((RDBMSCompiledCondition) assignmentEntry.getValue()).getParameters().entrySet()) {
                parameter = parameterEntry.getValue();
                if (parameter instanceof Constant) {
                    Constant constant = (Constant) parameter;
                    RDBMSTableUtils.populateStatementWithSingleElement(stmt, ordinal, constant.getType(),
                            constant.getValue());
                } else {
                    Attribute variable = (Attribute) parameter;
                    RDBMSTableUtils.populateStatementWithSingleElement(stmt, ordinal, variable.getType(),
                            updateSetMap.get(variable.getName()));
                }
                ordinal++;
            }
        }
        return ordinal;
    }

    /**
     * Util method which is used to populate a {@link PreparedStatement} instance with a single element.
     *
     * @param stmt    the statement to which the element should be set.
     * @param ordinal the ordinal of the element in the statement (its place in a potential list of places).
     * @param type    the type of the element to be set, adheres to
     *                {@link org.wso2.siddhi.query.api.definition.Attribute.Type}.
     * @param value   the value of the element.
     * @throws SQLException if there are issues when the element is being set.
     */
    public static void populateStatementWithSingleElement(PreparedStatement stmt, int ordinal, Attribute.Type type,
                                                          Object value) throws SQLException {
        switch (type) {
            case BOOL:
                stmt.setBoolean(ordinal, (Boolean) value);
                break;
            case DOUBLE:
                stmt.setDouble(ordinal, (Double) value);
                break;
            case FLOAT:
                stmt.setFloat(ordinal, (Float) value);
                break;
            case INT:
                stmt.setInt(ordinal, (Integer) value);
                break;
            case LONG:
                stmt.setLong(ordinal, (Long) value);
                break;
            case OBJECT:
                stmt.setObject(ordinal, value);
                break;
            case STRING:
                stmt.setString(ordinal, (String) value);
                break;
        }
    }

    /**
     * Util method which validates the elements from an annotation to verify that they comply to the accepted standards.
     *
     * @param annotation the annotation to be validated.
     */
    public static void validateAnnotation(Annotation annotation) {
        if (annotation == null) {
            return;
        }
        List<Element> elements = annotation.getElements();
        for (Element element : elements) {
            if (isEmpty(element.getValue())) {
                throw new RDBMSTableException("Annotation '" + annotation.getName() + "' contains illegal value(s). " +
                        "Please check your query and try again.");
            }
        }
    }

    /**
     * Util method used to convert a list of elements in an annotation to a comma-separated string.
     *
     * @param elements the list of annotation elements.
     * @return a comma-separated string of all elements in the list.
     */
    public static String flattenAnnotatedElements(List<Element> elements) {
        StringBuilder sb = new StringBuilder();
        elements.forEach(elem -> {
            sb.append(elem.getValue());
            if (elements.indexOf(elem) != elements.size() - 1) {
                sb.append(RDBMSTableConstants.SEPARATOR);
            }
        });
        return sb.toString();
    }

    /**
     * Read and return all string field lengths given in an RDBMS event table definition.
     *
     * @param fieldInfo the field length annotation from the "@Store" definition (can be empty).
     * @return a map of fields and their specified sizes.
     */
    public static Map<String, String> processFieldLengths(String fieldInfo) {
        return processKeyValuePairs(fieldInfo).stream()
                .collect(Collectors.toMap(name -> name[0], value -> value[1]));
    }

    /**
     * Converts a flat string of key/value pairs (e.g. from an annotation) into a list of pairs.
     * Used String[] since Java does not offer tuples.
     *
     * @param annotationString the comma-separated string of key/value pairs.
     * @return a list processed and validated pairs.
     */
    public static List<String[]> processKeyValuePairs(String annotationString) {
        List<String[]> keyValuePairs = new ArrayList<>();
        if (!isEmpty(annotationString)) {
            String[] pairs = annotationString.split(",");
            for (String element : pairs) {
                if (!element.contains(":")) {
                    throw new RDBMSTableException("Property '" + element + "' does not adhere to the expected " +
                            "format: a property must be a key-value pair separated by a colon (:)");
                }
                String[] pair = element.split(":");
                if (pair.length != 2) {
                    throw new RDBMSTableException("Property '" + pair[0] + "' does not adhere to the expected " +
                            "format: a property must be a key-value pair separated by a colon (:)");
                } else {
                    keyValuePairs.add(new String[]{pair[0].trim(), pair[1].trim()});
                }
            }
        }
        return keyValuePairs;
    }

    /**
     * Method for replacing the placeholder for conditions with the SQL Where clause and the actual condition.
     *
     * @param query     the SQL query in string format, with the "{{CONDITION}}" placeholder present.
     * @param condition the actual condition (originating from the ConditionVisitor).
     * @return the formatted string.
     */
    public static String formatQueryWithCondition(String query, String condition) {
        return query.replace(PLACEHOLDER_CONDITION, SQL_WHERE + WHITESPACE + condition);
    }

    /**
     * Utility method used for looking up DB metadata information from a given datasource.
     *
     * @param ds the datasource from which the metadata needs to be looked up.
     * @return a list of DB metadata.
     */
    public static Map<String, Object> lookupDatabaseInfo(DataSource ds) {
        Connection conn = null;
        try {
            conn = ds.getConnection();
            DatabaseMetaData dmd = conn.getMetaData();
            Map<String, Object> result = new HashMap<>();
            result.put(DATABASE_PRODUCT_NAME, dmd.getDatabaseProductName());
            result.put(VERSION, Double.parseDouble(dmd.getDatabaseMajorVersion() + "."
                    + dmd.getDatabaseMinorVersion()));
            return result;
        } catch (SQLException e) {
            throw new RDBMSTableException("Error in looking up database type: " + e.getMessage(), e);
        } finally {
            cleanupConnection(null, null, conn);
        }
    }

    /**
     * Checks and returns an instance of the RDBMS query configuration mapper.
     *
     * @return an instance of {@link RDBMSConfigurationMapper}.
     * @throws CannotLoadConfigurationException if the configuration cannot be loaded.
     */
    private static RDBMSConfigurationMapper loadRDBMSConfigurationMapper() throws CannotLoadConfigurationException {
        if (mapper == null) {
            synchronized (RDBMSTableUtils.class) {
                RDBMSQueryConfiguration config = loadQueryConfiguration();
                mapper = new RDBMSConfigurationMapper(config);
            }
        }
        return mapper;
    }

    /**
     * Isolates a particular RDBMS query configuration entry which matches the retrieved DB metadata.
     *
     * @param ds the datasource against which the entry should be matched.
     * @return the matching RDBMS query configuration entry.
     * @throws CannotLoadConfigurationException if the configuration cannot be loaded.
     */
    public static RDBMSQueryConfigurationEntry lookupCurrentQueryConfigurationEntry(
            DataSource ds, ConfigReader configReader) throws CannotLoadConfigurationException {
        Map<String, Object> dbInfo = lookupDatabaseInfo(ds);
        RDBMSConfigurationMapper mapper = loadRDBMSConfigurationMapper();
        RDBMSQueryConfigurationEntry entry = mapper.lookupEntry((String) dbInfo.get(DATABASE_PRODUCT_NAME),
                (double) dbInfo.get(VERSION), configReader);
        if (entry != null) {
            return entry;
        } else {
            throw new CannotLoadConfigurationException("Cannot find a database section in the RDBMS "
                    + "configuration for the database: " + dbInfo);
        }
    }

    /**
     * Utility method which matches the contain condition regex and replace the contain condition in findCondition
     * by the record contains condition template.
     *
     * @param findCondition                   the find condition string
     * @param recordContainsConditionTemplate the contains condition template based on the database type
     * @return the results of processed find condition by the contains condition template
     */
    public static String processFindConditionWithContainsConditionTemplate(String findCondition,
                                                                           String recordContainsConditionTemplate) {
        Matcher matcher = CONTAINS_CONDITION_REGEX_PATTERN.matcher(findCondition);
        while (matcher.find()) {
            String tableColumnId = matcher.group(2);
            String recordContainsCondition = recordContainsConditionTemplate.replace(PLACEHOLDER_COLUMNS,
                    tableColumnId).
                    replace(PLACEHOLDER_VALUES, QUESTION_MARK);
            return matcher.replaceAll(recordContainsCondition);
        }
        return findCondition;
    }

    /**
     * Utility method which loads the query configuration from file.
     *
     * @return the loaded query configuration.
     * @throws CannotLoadConfigurationException if the configuration cannot be loaded.
     */
    private static RDBMSQueryConfiguration loadQueryConfiguration() throws CannotLoadConfigurationException {
        return new RDBMSTableConfigLoader().loadConfiguration();
    }

    /**
     * Child class with a method for loading the JAXB configuration mappings.
     */
    private static class RDBMSTableConfigLoader {

        /**
         * Method for loading the configuration mappings.
         *
         * @return an instance of {@link RDBMSQueryConfiguration}.
         * @throws CannotLoadConfigurationException if the config cannot be loaded.
         */
        private RDBMSQueryConfiguration loadConfiguration() throws CannotLoadConfigurationException {
            InputStream inputStream = null;
            try {
                JAXBContext ctx = JAXBContext.newInstance(RDBMSQueryConfiguration.class);
                Unmarshaller unmarshaller = ctx.createUnmarshaller();
                ClassLoader classLoader = getClass().getClassLoader();
                inputStream = classLoader.getResourceAsStream(RDBMS_QUERY_CONFIG_FILE);
                if (inputStream == null) {
                    throw new CannotLoadConfigurationException(RDBMS_QUERY_CONFIG_FILE
                            + " is not found in the classpath");
                }
                return (RDBMSQueryConfiguration) unmarshaller.unmarshal(inputStream);
            } catch (JAXBException e) {
                throw new CannotLoadConfigurationException(
                        "Error in processing RDBMS query configuration: " + e.getMessage(), e);
            } finally {
                if (inputStream != null) {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        log.error(String.format("Failed to close the input stream for %s", RDBMS_QUERY_CONFIG_FILE));
                    }
                }
            }
        }
    }

    /**
     * RDBMS configuration mapping class to be used to lookup matching configuration entry with a data source.
     */
    private static class RDBMSConfigurationMapper {

        private List<Map.Entry<Pattern, RDBMSQueryConfigurationEntry>> entries = new ArrayList<>();

        public RDBMSConfigurationMapper(RDBMSQueryConfiguration config) {
            for (RDBMSQueryConfigurationEntry entry : config.getDatabases()) {
                this.entries.add(Maps.immutableEntry(Pattern.compile(
                        entry.getDatabaseName().toLowerCase(Locale.ENGLISH)), entry));
            }
        }

        private boolean checkVersion(RDBMSQueryConfigurationEntry entry, double version, ConfigReader configReader) {
            double minVersion = Double.parseDouble(configReader.readConfig(entry.getDatabaseName() + PROPERTY_SEPARATOR
                    + MIN_VERSION, String.valueOf(entry.getMinVersion())));
            double maxVersion = Double.parseDouble(configReader.readConfig(entry.getDatabaseName() + PROPERTY_SEPARATOR
                    + MAX_VERSION, String.valueOf(entry.getMaxVersion())));
            //Keeping things readable
            if (minVersion != 0 && version < minVersion) {
                return false;
            }
            if (maxVersion != 0 && version > maxVersion) {
                return false;
            }
            return true;
        }

        private List<RDBMSQueryConfigurationEntry> extractMatchingConfigEntries(String dbName) {
            List<RDBMSQueryConfigurationEntry> result = new ArrayList<>();
            this.entries.forEach(entry -> {
                if (entry.getKey().matcher(dbName).find()) {
                    result.add(entry.getValue());
                }
            });
            return result;
        }

        public RDBMSQueryConfigurationEntry lookupEntry(String dbName, double version, ConfigReader configReader) {
            List<RDBMSQueryConfigurationEntry> dbResults = this.extractMatchingConfigEntries(
                    dbName.toLowerCase(Locale.ENGLISH));
            if (dbResults.isEmpty()) {
                return null;
            }
            RDBMSQueryConfigurationEntry versionResults = null;
            for (RDBMSQueryConfigurationEntry entry : dbResults) {
                if (this.checkVersion(entry, version, configReader)) {
                    versionResults = entry;
                }
            }
            return versionResults;
        }
    }
}
