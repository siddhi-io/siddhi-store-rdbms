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

package io.siddhi.extension.store.rdbms.test.osgi.util;

import io.siddhi.extension.store.rdbms.RDBMSEventTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

/**
 * Util class for test cases.
 */
public class TestUtil {
    private static final Log logger = LogFactory.getLog(RDBMSEventTable.class);

    public static HTTPResponseMessage sendHRequest(String body, URI baseURI, String path, String contentType,
                                                   Boolean auth, String userName, String password) {
        try {
            HttpURLConnection urlConn = null;
            try {
                urlConn = TestUtil.generateRequest(baseURI, path);
            } catch (IOException e) {
                logger.error("IOException occurred while running the HttpsSourceTestCaseForSSL", e);
            }
            assert urlConn != null;
            if (auth) {
                TestUtil.setHeader(urlConn, "Authorization",
                        "Basic " + java.util.Base64.getEncoder().
                                encodeToString((userName + ":" + password).getBytes()));
            }
            if (contentType != null) {
                TestUtil.setHeader(urlConn, "Content-Type", contentType);
            }
            TestUtil.setHeader(urlConn, "HTTP_METHOD", "POST");
            TestUtil.writeContent(urlConn, body);
            HTTPResponseMessage httpResponseMessage = new HTTPResponseMessage(urlConn.getResponseCode(),
                    urlConn.getContentType(), urlConn.getResponseMessage());
            urlConn.disconnect();
            return httpResponseMessage;
        } catch (IOException e) {
            logger.error("IOException occurred while running the HttpsSourceTestCaseForSSL", e);
        }
        return new HTTPResponseMessage();
    }

    private static void writeContent(HttpURLConnection urlConn, String content) throws IOException {
        OutputStreamWriter out = new OutputStreamWriter(
                urlConn.getOutputStream());
        out.write(content);
        out.close();
    }

    private static HttpURLConnection generateRequest(URI baseURI, String path)
            throws IOException {
        URL url = baseURI.resolve(path).toURL();
        HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
        urlConn.setRequestMethod("POST");
        urlConn.setDoOutput(true);
        urlConn.setRequestProperty("Connection", "Keep-Alive");
        return urlConn;
    }

    private static void setHeader(HttpURLConnection urlConnection, String key, String value) {
        urlConnection.setRequestProperty(key, value);
    }

}
