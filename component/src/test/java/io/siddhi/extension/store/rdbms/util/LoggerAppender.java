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

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.mvel2.util.StringAppender;

/**
 * The custom log4j2 appender for appending the log event generated while running the tests in TestNG.
 * Configuration for the appender defined in the test/resource/log4j.property file.
 *
 * log4j.appender.testNG=io.siddhi.extension.store.rdbms.util.LoggerAppender
 * log4j.appender.testNG.layout=org.apache.log4j.EnhancedPatternLayout
 * log4j.appender.testNG.layout.ConversionPattern=%d{ABSOLUTE} %5p %c{1}:%L - %m%
 *
 * To enable the appender, set in the root logger,
 * log4j.rootLogger=INFO, stdout, testNG
 */

@Plugin(name = "LoggerAppender",
        category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class LoggerAppender extends AbstractAppender {

    private StringAppender messages = new StringAppender();

    public LoggerAppender(String name, Filter filter) {

        super(name, filter, null);
    }

    @PluginFactory
    public static LoggerAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Filter") Filter filter) {

        return new LoggerAppender(name, filter);
    }

    public String getMessages() {

        String results = messages.toString();
        if (results.isEmpty()) {
            return null;
        }
        return results;
    }

    @Override
    public void append(LogEvent event) {

        messages.append(event.getMessage().getFormattedMessage());
    }

}
