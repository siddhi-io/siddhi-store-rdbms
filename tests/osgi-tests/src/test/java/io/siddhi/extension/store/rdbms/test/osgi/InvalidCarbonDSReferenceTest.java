/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.siddhi.extension.store.rdbms.test.osgi;

import io.siddhi.extension.store.rdbms.test.osgi.util.HTTPResponseMessage;
import io.siddhi.extension.store.rdbms.test.osgi.util.TestUtil;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.ExamFactory;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.ops4j.pax.exam.testng.listener.PaxExam;
import org.testng.Assert;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import org.wso2.carbon.container.CarbonContainerFactory;
import org.wso2.carbon.kernel.CarbonServerInfo;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import javax.inject.Inject;

import static org.wso2.carbon.container.options.CarbonDistributionOption.copyFile;

/**
 * SiddhiAsAPI OSGI Tests.
 */

@Listeners(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
@ExamFactory(CarbonContainerFactory.class)
public class InvalidCarbonDSReferenceTest {
    @Inject
    private CarbonServerInfo carbonServerInfo;

    @Configuration
    public Option[] createConfiguration() {
        return new Option[]{
                copyCarbonDSConfigFile(),
                copySiddhiApp()
        };
    }

    private Option copySiddhiApp() {
        Path carbonYmlFilePath;
        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = Paths.get(".").toString();
        }
        carbonYmlFilePath = Paths.get(basedir, "src", "test", "resources", "SiddhiApp2.siddhi");
        return copyFile(carbonYmlFilePath, Paths.get("deployment", "siddhi-files", "SiddhiApp2.siddhi"));
    }

    private Option copyCarbonDSConfigFile() {
        Path carbonYmlFilePath;
        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = Paths.get(".").toString();
        }
        carbonYmlFilePath = Paths.get(basedir, "src", "test", "resources", "deployment2.yaml");
        return copyFile(carbonYmlFilePath, Paths.get("conf", "default", "deployment.yaml"));
    }

    @Test(enabled = false)
    public void testInvalidDataSourceReference() throws SQLException {
        // TODO: 11/21/17 To be enabled after solving https://github.com/wso2-extensions/siddhi-store-rdbms/issues/34
        URI baseURI = URI.create(String.format("http://%s:%d", "localhost", 9090));
        String path = "/simulation/single";
        String contentType = "text/plain";
        String method = "POST";
        String body = "{\n" +
                "  \"siddhiAppName\": \"SiddhiApp2\",\n" +
                "  \"streamName\": \"FooStream\",\n" +
                "  \"timestamp\": null,\n" +
                "  \"data\": [\n" +
                "    \"ID001\", 148.34, 72.00\n" +
                "  ]\n" +
                "}";
        HTTPResponseMessage httpResponseMessage = TestUtil.sendHRequest(body, baseURI, path, contentType, method,
                false, "", "");
        Assert.assertEquals(httpResponseMessage.getResponseCode(), 500);
    }
}
