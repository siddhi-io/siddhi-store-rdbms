/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.extension.store.rdbms.metrics;

import com.google.common.base.Stopwatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.carbon.metrics.core.Counter;
import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Class which holds the RDBMS metrics.
 */
public class RDBMSMetrics {
    private static final Map<RDBMSDatabase, Long> RDBMS_LAST_RECEIVED_TIME_MAP = new HashMap<>();
    private static final Map<RDBMSDatabase, RDBMSStatus> RDBMS_STATUS_MAP = new HashMap<>();
    private static final Map<String, Boolean> RDBMS_STATUS_SERVICE_STARTED_MAP = new ConcurrentHashMap<>();
    private static final Logger log = LogManager.getLogger(RDBMSMetrics.class);
    private final String siddhiAppName;
    private final String tableName;
    private final RDBMSDatabase rdbmsDatabase;
    private final Stopwatch processingTime = Stopwatch.createUnstarted();
    private String shortenJdbcUrl;
    private String databaseName;
    private String dbType;
    private long lastInsertTime;
    private long lastUpdateTime;
    private long lastDeleteTime;
    private boolean isInitialised;
    private long lastChangeTime;


    public RDBMSMetrics(String siddhiAppName, String url, String tableName) {
        this.siddhiAppName = siddhiAppName;
        this.tableName = tableName;
        this.rdbmsDatabase = new RDBMSDatabase(siddhiAppName, url + ":" + tableName);
        MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Store.RDBMS", siddhiAppName), Level.INFO).inc();
        RDBMS_STATUS_SERVICE_STARTED_MAP.putIfAbsent(siddhiAppName, false);
    }

    public void updateTableStatus(ExecutorService executorService, String siddhiAppName) {
        if (!RDBMS_STATUS_SERVICE_STARTED_MAP.get(siddhiAppName)) {
            RDBMS_STATUS_SERVICE_STARTED_MAP.replace(siddhiAppName, true);
            executorService.execute(() -> {
                while (RDBMS_STATUS_SERVICE_STARTED_MAP.get(siddhiAppName)) {
                    if (!RDBMS_STATUS_MAP.isEmpty()) {
                        RDBMS_LAST_RECEIVED_TIME_MAP.forEach((rdbmsDatabase, lastReceivedTime) -> {
                            if (rdbmsDatabase.siddhiAppName.equals(siddhiAppName)) {
                                long idleTime = System.currentTimeMillis() - lastReceivedTime;
                                if (idleTime / 1000 > 8) {
                                    RDBMS_STATUS_MAP.replace(rdbmsDatabase, RDBMSStatus.IDLE);
                                }
                            }
                        });
                    }
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        log.error("{}: Error while updating the tables status.", siddhiAppName, e);
                    }
                }
            });
        }
    }

    public Counter getTotalWriteMetrics() { //to count the total writes from siddhi app level.
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Total.Writes.%s", siddhiAppName, "rdbms"),
                        Level.INFO);
    }

    public Counter getWritesCountMetrics() { //to get the total writes count in extension level.
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Store.RDBMS.Write.Count.%s.%s.host.%s.%s.%s",
                        siddhiAppName, dbType, shortenJdbcUrl, databaseName, tableName, getDatabaseURL()), Level.INFO);
    }

    public Counter getInsertCountMetric() {
        if (isInitialised) { //insert time wont get update when initialising metrics
            lastInsertTime = System.currentTimeMillis();
            updateLastReceivedTimeMap(lastInsertTime);
        }
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Store.RDBMS.%s.%s",
                        siddhiAppName, "insert_rows_count", getDatabaseURL()), Level.INFO);
    }

    private void lastInsertTimeMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Store.RDBMS.%s.%s",
                        siddhiAppName, "last_insert_time", getDatabaseURL()),
                        Level.INFO, () -> lastInsertTime);
    }

    public Counter getUpdateCountMetric() {
        if (isInitialised) {
            lastUpdateTime = System.currentTimeMillis();
            updateLastReceivedTimeMap(lastUpdateTime);
        }
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Store.RDBMS.%s.%s",
                        siddhiAppName, "update_rows_count", getDatabaseURL()), Level.INFO);
    }

    private void lastUpdateTimeMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Store.RDBMS.%s.%s",
                        siddhiAppName, "last_update_time", getDatabaseURL()),
                        Level.INFO, () -> lastUpdateTime);
    }

    public Counter getDeleteCountMetric() {
        if (isInitialised) {
            lastDeleteTime = System.currentTimeMillis();
            updateLastReceivedTimeMap(lastDeleteTime);
        }
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Store.RDBMS.%s.%s",
                        siddhiAppName, "delete_rows_count", getDatabaseURL()), Level.INFO);
    }

    private void lastDeleteTimeMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Store.RDBMS.%s.%s",
                        siddhiAppName, "last_delete_time", getDatabaseURL()),
                        Level.INFO, () -> lastDeleteTime);
    }

    private void setLastChangeTime() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Store.RDBMS.%s.%s",
                        siddhiAppName, "last_change_time", getDatabaseURL()),
                        Level.INFO, () -> {
                            synchronized (this) {
                                return lastChangeTime;
                            }
                        });
    }

    private Counter getTotalErrorCountMetric() {
        return MetricsDataHolder.getInstance().getMetricService().counter(
                String.format("io.siddhi.SiddhiApps.%s.Siddhi.Store.RDBMS.%s.%s",
                        siddhiAppName, "total_error_count",  getDatabaseURL()), Level.INFO);
    }

    private void getProcessingTime() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Store.RDBMS.%s.%s",
                        siddhiAppName, "processing_time", getDatabaseURL()),
                        Level.INFO, () -> {
                            if (RDBMS_STATUS_MAP.get(rdbmsDatabase) != RDBMSStatus.IDLE) {
                                return processingTime.elapsed().toMillis();
                            }
                            if (processingTime.isRunning()) {
                                processingTime.stop();
                            }
                            return 0L;
                        });
    }

    private void idleTimeMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Store.RDBMS.%s.%s",
                        siddhiAppName, "idle_time", getDatabaseURL()),
                        Level.INFO, () -> {
                            if (RDBMS_LAST_RECEIVED_TIME_MAP.containsKey(rdbmsDatabase) && RDBMS_STATUS_MAP.get(
                                    rdbmsDatabase) == RDBMSStatus.IDLE) {
                                return System.currentTimeMillis() - RDBMS_LAST_RECEIVED_TIME_MAP.get(rdbmsDatabase);
                            }
                            return 0L;
                        });
    }

    private void setRDBMSDBStatusMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Store.RDBMS.%s.%s",
                        siddhiAppName, "db_status", getDatabaseURL()),
                        Level.INFO, () -> {
                            if (RDBMS_STATUS_MAP.containsKey(rdbmsDatabase)) {
                                return RDBMS_STATUS_MAP.get(rdbmsDatabase).ordinal();
                            }
                            return -1;
                        });
    }

    private synchronized void updateLastReceivedTimeMap(long lastReceivedTime) {
        if (RDBMS_LAST_RECEIVED_TIME_MAP.containsKey(rdbmsDatabase)) {
            if (RDBMS_LAST_RECEIVED_TIME_MAP.get(rdbmsDatabase) < lastReceivedTime) {
                RDBMS_LAST_RECEIVED_TIME_MAP.replace(rdbmsDatabase, lastReceivedTime);
                this.lastChangeTime = lastReceivedTime;
            }
        } else {
            RDBMS_LAST_RECEIVED_TIME_MAP.put(rdbmsDatabase, lastReceivedTime);
            this.lastChangeTime = lastReceivedTime;
        }
    }

    private String getDatabaseURL() {
        return shortenJdbcUrl + "/" + tableName;
    }

    public synchronized void setRDBMSStatus(RDBMSStatus rdbmsStatus) {
        if (!processingTime.isRunning()) { //starts the processing_time stopwatch
            processingTime.reset().start();
        }
        if (rdbmsStatus == RDBMSStatus.ERROR) {
            getTotalErrorCountMetric().inc();
        }
        if (RDBMS_STATUS_MAP.containsKey(rdbmsDatabase)) {
            RDBMS_STATUS_MAP.replace(rdbmsDatabase, rdbmsStatus);
        } else {
            RDBMS_STATUS_MAP.put(rdbmsDatabase, rdbmsStatus);
            lastInsertTimeMetric(); //register metrics after perform an action to table(Insert, Update, Delete).
            lastUpdateTimeMetric();
            lastDeleteTimeMetric();
            setLastChangeTime();
            idleTimeMetric();
            getProcessingTime();
        }
    }

    public void setDatabaseParams(String url, String databaseName, String dbType) {
        String shortenUrl = MetricsUtils.getShortenJDBCURL(url);
        if (!shortenUrl.equals(this.shortenJdbcUrl)) {
            this.shortenJdbcUrl = shortenUrl;
            this.databaseName = databaseName;
            this.dbType = dbType;
            setRDBMSDBStatusMetric();
            getInsertCountMetric(); //register metrics before perform any changes to table.
            getDeleteCountMetric();
            getUpdateCountMetric();
            getWritesCountMetrics();
            getTotalWriteMetrics();
            getTotalErrorCountMetric();
            isInitialised = true;
        }
    }

    /**
     * CDCDatabase holds the SiddhiAppName and the database URL to be use as key.
     */
    private static class RDBMSDatabase {
        protected String cdcURL; //dbURL + ":" + tableName
        protected String siddhiAppName;

        public RDBMSDatabase(String siddhiAppName, String cdcURL) {
            this.cdcURL = cdcURL;
            this.siddhiAppName = siddhiAppName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RDBMSDatabase that = (RDBMSDatabase) o;
            return cdcURL.equals(that.cdcURL) &&
                    siddhiAppName.equals(that.siddhiAppName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cdcURL, siddhiAppName);
        }
    }

}
