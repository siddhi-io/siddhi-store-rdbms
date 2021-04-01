/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.extension.store.rdbms.config;

/**
 * This class represents the SQL datatype mappings required by Siddhi RDBMS Event Tables per supported DB vendor.
 */
public class RDBMSTypeMapping {

    // -- Type mapping -- //
    private RDBMSDataType binaryType;
    private RDBMSDataType booleanType;
    private RDBMSDataType doubleType;
    private RDBMSDataType floatType;
    private RDBMSDataType integerType;
    private RDBMSDataType longType;
    private RDBMSDataType stringType;
    private RDBMSDataType bigStringType;

    public RDBMSDataType getBinaryType() {
        return binaryType;
    }

    public void setBinaryType(RDBMSDataType binaryType) {
        this.binaryType = binaryType;
    }

    public RDBMSDataType getBooleanType() {
        return booleanType;
    }

    public void setBooleanType(RDBMSDataType booleanType) {
        this.booleanType = booleanType;
    }

    public RDBMSDataType getDoubleType() {
        return doubleType;
    }

    public void setDoubleType(RDBMSDataType doubleType) {
        this.doubleType = doubleType;
    }

    public RDBMSDataType getFloatType() {
        return floatType;
    }

    public void setFloatType(RDBMSDataType floatType) {
        this.floatType = floatType;
    }

    public RDBMSDataType getIntegerType() {
        return integerType;
    }

    public void setIntegerType(RDBMSDataType integerType) {
        this.integerType = integerType;
    }

    public RDBMSDataType getLongType() {
        return longType;
    }

    public void setLongType(RDBMSDataType longType) {
        this.longType = longType;
    }

    public RDBMSDataType getStringType() {
        return stringType;
    }

    public void setStringType(RDBMSDataType stringType) {
        this.stringType = stringType;
    }

    public RDBMSDataType getBigStringType() {
        return bigStringType;
    }

    public void setBigStringType(RDBMSDataType bigStringType) {
        this.bigStringType = bigStringType;
    }
}
