/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.extension.store.rdbms.util;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

/**
 * The custom log4j appender for appending the log event generated while running the tests in TestNG.
 * Configuration for the appender defined in the test/resource/log4j.property file.
 *
 * log4j.appender.testNG=io.siddhi.extension.store.rdbms.util.LoggerAppender
 * log4j.appender.testNG.layout=org.apache.log4j.EnhancedPatternLayout
 * log4j.appender.testNG.layout.ConversionPattern=%d{ABSOLUTE} %5p %c{1}:%L - %m%
 *
 * To enable the appender, set in the root logger,
 * log4j.rootLogger=INFO, stdout, testNG
 */
public class LoggerAppender extends AppenderSkeleton {
    private static LoggerCallBack loggerCallBack;

    public static void setLoggerCallBack(LoggerCallBack loggerCallBack) {
        LoggerAppender.loggerCallBack = loggerCallBack;
    }

    @Override
    protected void append(final LoggingEvent event) {
        if (loggerCallBack != null) {
            loggerCallBack.receiveLoggerEvent((String) event.getMessage());
        }
    }

    @Override
    public void close() {
        loggerCallBack = null;
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }
}
