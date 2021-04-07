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

import io.siddhi.core.table.record.RecordIterator;
import io.siddhi.extension.store.rdbms.exception.RDBMSTableException;
import io.siddhi.extension.store.rdbms.util.RDBMSTableUtils;
import io.siddhi.query.api.definition.Attribute;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * A class representing a RecordIterator which is responsible for processing RDBMS Event Table find() operations in a
 * streaming fashion.
 */
public class RDBMSIterator implements RecordIterator<Object[]> {

    private Connection conn;
    private PreparedStatement stmt = null;
    private ResultSet rs = null;

    private boolean preFetched;
    private Object[] nextValue;
    private List<Attribute> attributes;
    private String tableName;
    private boolean allowNullValues;

    public RDBMSIterator(Connection conn, PreparedStatement stmt, ResultSet rs, List<Attribute> attributes,
                         String tableName, boolean allowNullValues) {
        this.conn = conn;
        this.stmt = stmt;
        this.rs = rs;
        this.attributes = attributes;
        this.tableName = tableName;
        this.allowNullValues = allowNullValues;
    }

    @Override
    public boolean hasNext() {
        if (!this.preFetched) {
            this.nextValue = this.next();
            this.preFetched = true;
        }
        return nextValue != null;
    }

    @Override
    public Object[] next() {
        if (this.preFetched) {
            this.preFetched = false;
            Object[] result = this.nextValue;
            this.nextValue = null;
            return result;
        }
        try {
            if (this.rs.next()) {
                return this.extractRecord(this.rs, allowNullValues);
            } else {
                // end of the result set, cleaning up.
                RDBMSTableUtils.cleanupConnection(this.rs, this.stmt, this.conn);
                this.rs = null;
                this.stmt = null;
                this.conn = null;
                return null;
            }
        } catch (Exception e) {
            RDBMSTableUtils.cleanupConnection(this.rs, this.stmt, this.conn);
            throw new RDBMSTableException("Error retrieving records from table '" + this.tableName + "': "
                    + e.getMessage(), e);
        }
    }

    /**
     * Method which is used for extracting record values (in the form of an Object array) from an SQL {@link ResultSet},
     * according to the table's field type order.
     *
     * @param rs the {@link ResultSet} from which the values should be retrieved.
     * @param allowNullValues check if the null has to allow
     * @return an array of extracted values, all cast to {@link Object} type for portability.
     * @throws SQLException if there are errors in extracring the values from the {@link ResultSet} instance according
     *                      to the table definition
     */
    private Object[] extractRecord(ResultSet rs, boolean allowNullValues) throws SQLException {
        List<Object> result = new ArrayList<>();
        for (Attribute attribute : this.attributes) {
            Object value = null;
            switch (attribute.getType()) {
                case BOOL:
                    value = rs.getBoolean(attribute.getName());
                    break;
                case DOUBLE:
                    value = rs.getDouble(attribute.getName());
                    break;
                case FLOAT:
                    value = rs.getFloat(attribute.getName());
                    break;
                case INT:
                    value = rs.getInt(attribute.getName());
                    break;
                case LONG:
                    value = rs.getLong(attribute.getName());
                    break;
                case OBJECT:
                    value = rs.getObject(attribute.getName());
                    break;
                case STRING:
                    value = rs.getString(attribute.getName());
                    break;
            }
            if (allowNullValues && rs.wasNull()) {
                result.add(null);
            } else {
                result.add(value);
            }
        }
        return result.toArray();
    }

    @Override
    public void remove() {
        //Do nothing. This is a read-only iterator.
    }

    @Override
    public void close() throws IOException {
        RDBMSTableUtils.cleanupConnection(this.rs, this.stmt, this.conn);
        this.rs = null;
        this.stmt = null;
        this.conn = null;
    }

    @Override
    protected void finalize() throws Throwable {
        //In the unlikely case this iterator does not go to the end, we have to make sure the connection is cleaned up.
        RDBMSTableUtils.cleanupConnection(this.rs, this.stmt, this.conn);
        super.finalize();
    }

}
