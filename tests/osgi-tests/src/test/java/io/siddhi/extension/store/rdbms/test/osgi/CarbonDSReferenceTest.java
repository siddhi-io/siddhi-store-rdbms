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
import org.ops4j.pax.exam.ExamFactory;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.ops4j.pax.exam.testng.listener.PaxExam;
import org.testng.Assert;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import org.wso2.carbon.container.CarbonContainerFactory;
import org.wso2.carbon.kernel.CarbonServerInfo;

import java.net.URI;
import javax.inject.Inject;

/**
 * SiddhiAsAPI OSGI Tests.
 */
@Listeners(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
@ExamFactory(CarbonContainerFactory.class)
public class CarbonDSReferenceTest {

    @Inject
    private CarbonServerInfo carbonServerInfo;

    /*
    Siddhi App deployment related test cases
     */
    @Test
    public void testValidDataSourceReference() {
        URI baseURI = URI.create(String.format("http://%s:%d", "localhost", 9090));
        String path = "/stores/query";
        String contentType = "application/json";
        String body = "{" +
                "  \"appName\": \"SiddhiApp1\"," +
                "  \"query\": \"from FooTable select *;\"" +
                "}";
        HTTPResponseMessage httpResponseMessage = TestUtil.sendHRequest(body, baseURI, path, contentType,
                true, "admin", "admin");
        Assert.assertEquals(httpResponseMessage.getResponseCode(), 200);
    }

    @Test
    public void testInvalidDataSourceReference() {
        URI baseURI = URI.create(String.format("http://%s:%d", "localhost", 9090));
        String path = "/stores/query";
        String contentType = "application/json";
        String body = "{" +
                "  \"appName\": \"SiddhiApp2\"," +
                "  \"query\": \"from FooTable select *;\"" +
                "}";
        HTTPResponseMessage httpResponseMessage = TestUtil.sendHRequest(body, baseURI, path, contentType,
                true, "admin", "admin");
        Assert.assertEquals(httpResponseMessage.getResponseCode(), 500);
    }
}
