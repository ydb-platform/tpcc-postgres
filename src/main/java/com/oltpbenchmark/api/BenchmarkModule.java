/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package com.oltpbenchmark.api;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.catalog.AbstractCatalog;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.ScriptRunner;
import com.oltpbenchmark.util.ThreadUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.*;

/**
 * Base class for all benchmark implementations
 */
public abstract class BenchmarkModule {
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkModule.class);

    // We use virtual threads. There is a limitted number of c3p0 provided connections.
    // When c3p0 runs out of connections, it will block until one is available. Block in a way
    // that carrier threads are blocked. Same time other virtual threads holding connections
    // might be parked waiting for a carrier thread to be available. This will cause a deadlock.
    // To avoid this, we use a semaphore to wait for a connection without blocking the carrier thread.
    private static Semaphore connectionSemaphore = new Semaphore(0);

    private static final Gauge SESSIONS_USED = Gauge.builder("sessions", BenchmarkModule::getUsedConnectionCount)
            .tag("state", "used")
            .register(Metrics.globalRegistry);

    private static final Gauge SESSIONS_QUEUE = Gauge.builder("session_queue_length", connectionSemaphore, Semaphore::getQueueLength)
            .register(Metrics.globalRegistry);

    private static final Timer.Builder GET_SESSION_DURATION = Timer.builder("get_session")
            .serviceLevelObjectives(
                    Duration.ofMillis(1),
                    Duration.ofMillis(2),
                    Duration.ofMillis(4),
                    Duration.ofMillis(8),
                    Duration.ofMillis(16),
                    Duration.ofMillis(32),
                    Duration.ofMillis(64),
                    Duration.ofMillis(128),
                    Duration.ofMillis(256),
                    Duration.ofMillis(512),
                    Duration.ofMillis(1024),
                    Duration.ofMillis(2048),
                    Duration.ofMillis(4096),
                    Duration.ofMillis(8192),
                    Duration.ofMillis(16384),
                    Duration.ofMillis(32768),
                    Duration.ofMillis(65536)
            )
            .publishPercentiles();

    private static HikariDataSource dataSource;

    /**
     * The workload configuration for this benchmark invocation
     */
    protected static WorkloadConfiguration workConf;

    /**
     * These are the variations of the Procedure's Statement SQL
     */
    protected final StatementDialects dialects;

    /**
     * Supplemental Procedures
     */
    private final Set<Class<? extends Procedure>> supplementalProcedures = new HashSet<>();

    /**
     * A single Random object that should be re-used by all a benchmark's components
     */
    private static final ThreadLocal<Random> rng = new ThreadLocal<>();

    private AbstractCatalog catalog = null;

    /**
     * Constructor!
     * @param workConf
     */
    public BenchmarkModule(WorkloadConfiguration workConf) {
        this.workConf = workConf;
        this.dialects = new StatementDialects(workConf);

        if (!workConf.getDisableConnectionPool() && dataSource == null) {
            try {
                dataSource = new HikariDataSource();
                dataSource.setJdbcUrl(workConf.getUrl());
                dataSource.setUsername(workConf.getUsername());
                dataSource.setPassword(workConf.getPassword());

                dataSource.setMaximumPoolSize(workConf.getMaxConnections());

                dataSource.setMetricRegistry(Metrics.globalRegistry);

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    dataSource.close();
                }));
            } catch (Exception e) {
                LOG.error("Unable to initialize DataSource: %s", e.toString());
                throw new RuntimeException("Unable to initialize DataSource", e);
            }
        }
        connectionSemaphore.release(workConf.getMaxConnections());
    }

    // --------------------------------------------------------------------------
    // DATABASE CONNECTION
    // --------------------------------------------------------------------------

    public final Connection makeConnection() throws SQLException {
        long start = System.nanoTime();
        try {
            connectionSemaphore.acquire();
            if (dataSource != null) {
                return dataSource.getConnection();
            }
            if (StringUtils.isEmpty(workConf.getUsername())) {
                return DriverManager.getConnection(workConf.getUrl());
            } else {
                return DriverManager.getConnection(
                        workConf.getUrl(),
                        workConf.getUsername(),
                        workConf.getPassword());
            }
        } catch (SQLException e) {
            connectionSemaphore.release();
            throw e;
        } catch (InterruptedException e) {
            connectionSemaphore.release();
            throw new SQLException(e);
        } finally {
            long end = System.nanoTime();
            GET_SESSION_DURATION.register(Metrics.globalRegistry).record(Duration.ofNanos(end - start));
        }
    }

    public final void returnConnection() {
        connectionSemaphore.release();
    }

    public final static double getUsedConnectionCount() {
        return workConf.getMaxConnections() - connectionSemaphore.availablePermits();
    }

    // --------------------------------------------------------------------------
    // IMPLEMENTING CLASS INTERFACE
    // --------------------------------------------------------------------------

    /**
     * @return
     * @throws IOException
     */
    protected abstract List<Worker<? extends BenchmarkModule>> makeWorkersImpl() throws IOException;

    /**
     * Each BenchmarkModule needs to implement this method to load a sample
     * dataset into the database. The Connection handle will already be
     * configured for you, and the base class will commit+close it once this
     * method returns
     *
     * @return TODO
     */
    protected abstract Loader<? extends BenchmarkModule> makeLoaderImpl();

    protected abstract Package getProcedurePackageImpl();

    // --------------------------------------------------------------------------
    // PUBLIC INTERFACE
    // --------------------------------------------------------------------------

    /**
     * Return the Random generator that should be used by all this benchmark's components.
     * We are using ThreadLocal to make this support multiple threads better.
     * This will set the seed if one is specified in the workload config file
     */
    public Random rng() {
        Random ret = rng.get();
        if (ret == null) {
            if (this.workConf.getRandomSeed() != -1) {
                ret = new Random(this.workConf.getRandomSeed());
            } else {
                ret = new Random();
            }
            rng.set(ret);
        }
        return ret;
    }

    private String convertBenchmarkClassToBenchmarkName() {
        return convertBenchmarkClassToBenchmarkName(this.getClass());
    }

    protected static <T> String convertBenchmarkClassToBenchmarkName(Class<T> clazz) {
        assert(clazz != null);
        String name = clazz.getSimpleName().toLowerCase();
        // Special case for "CHBenCHmark"
        if (!name.equals("chbenchmark") && name.endsWith("benchmark")) {
            name = name.replace("benchmark", "");
        }
        return (name);
    }

    /**
     * Return the URL handle to the DDL used to load the benchmark's database
     * schema.
     *
     * @param db_type
     */
    public String getDatabaseDDLPath(DatabaseType db_type) {

        // The order matters!
        List<String> names = new ArrayList<>();
        if (db_type != null) {
            DatabaseType ddl_db_type = db_type;
            // HACK: Use MySQL if we're given MariaDB
            if (ddl_db_type == DatabaseType.MARIADB) ddl_db_type = DatabaseType.MYSQL;
            names.add("ddl-" + ddl_db_type.name().toLowerCase() + ".sql");
        }
        names.add("ddl-generic.sql");

        for (String fileName : names) {
            final String benchmarkName = getBenchmarkName();
            final String path = "/benchmarks/" + benchmarkName + "/" + fileName;

            try (InputStream stream = this.getClass().getResourceAsStream(path)) {
                if (stream != null) {
                    return path;
                }

            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }


        return null;
    }


    public final List<Worker<? extends BenchmarkModule>> makeWorkers() throws IOException {
        return (this.makeWorkersImpl());
    }

    public final void refreshCatalog() throws SQLException {
        if (this.catalog != null) {
            try {
                this.catalog.close();
            } catch (SQLException throwables) {
                LOG.error(throwables.getMessage(), throwables);
            }
        }
        try (Connection conn = this.makeConnection()) {
            this.catalog = SQLUtil.getCatalog(this, this.getWorkloadConfiguration().getDatabaseType(), conn);
        }
    }

    /**
     * Create the Benchmark Database
     * This is the main method used to create all the database
     * objects (e.g., table, indexes, etc) needed for this benchmark
     */
    public final void createDatabase() throws SQLException, IOException {
        try (Connection conn = this.makeConnection()) {
            this.createDatabase(this.workConf.getDatabaseType(), conn);
        }
    }

    /**
     * Create the Benchmark Database
     * This is the main method used to create all the database
     * objects (e.g., table, indexes, etc) needed for this benchmark
     */
    public final void createDatabase(DatabaseType dbType, Connection conn) throws SQLException, IOException {

            ScriptRunner runner = new ScriptRunner(conn, true, true);

            if (workConf.getDDLPath() != null) {
                String ddlPath = workConf.getDDLPath();
                LOG.warn("Overriding default DDL script path");
                LOG.debug("Executing script [{}] for database type [{}]", ddlPath, dbType);
                runner.runExternalScript(ddlPath);
            } else {
                String ddlPath = this.getDatabaseDDLPath(dbType);
                LOG.debug("Executing script [{}] for database type [{}]", ddlPath, dbType);
                runner.runScript(ddlPath);
            }
    }


    /**
     * Invoke this benchmark's database loader
     */
    public final Loader<? extends BenchmarkModule> loadDatabase() throws SQLException, InterruptedException {
        Loader<? extends BenchmarkModule> loader;

        loader = this.makeLoaderImpl();
        if (loader != null) {


            try {
                List<LoaderThread> loaderThreads = loader.createLoaderThreads();
                int maxConcurrent = workConf.getLoaderThreads();

                ThreadUtil.runLoaderThreads(loaderThreads, maxConcurrent);

                if (!loader.getTableCounts().isEmpty()) {
                    LOG.debug("Table Counts:\n{}", loader.getTableCounts());
                }
            } finally {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Finished loading the %s database", this.getBenchmarkName().toUpperCase()));
                }
            }
        }

        return loader;
    }

    public final void clearDatabase() throws SQLException {

        try (Connection conn = this.makeConnection()) {
            Loader<? extends BenchmarkModule> loader = this.makeLoaderImpl();
            if (loader != null) {
                conn.setAutoCommit(false);
                loader.unload(conn, this.catalog);
                conn.commit();
            }
        }
    }

    // --------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------

    /**
     * Return the unique identifier for this benchmark
     */
    public final String getBenchmarkName() {
        String workConfName = this.workConf.getBenchmarkName();
        return workConfName != null ? workConfName : convertBenchmarkClassToBenchmarkName();
    }

    /**
     * Return the database's catalog
     */
    public final AbstractCatalog getCatalog() {

        if (catalog == null) {
            throw new RuntimeException("getCatalog() has been called before refreshCatalog()");
        }

        return this.catalog;
    }


    /**
     * Return the StatementDialects loaded for this benchmark
     */
    public final StatementDialects getStatementDialects() {
        return (this.dialects);
    }

    @Override
    public final String toString() {
        return getBenchmarkName();
    }


    /**
     * Initialize a TransactionType handle for the get procedure name and id
     * This should only be invoked a start-up time
     *
     * @param procName
     * @param id
     * @return
     */
    @SuppressWarnings("unchecked")
    public final TransactionType initTransactionType(String procName, int id, long preExecutionWait, long postExecutionWait) {
        if (id == TransactionType.INVALID_ID) {
            throw new RuntimeException(String.format("Procedure %s.%s cannot use the reserved id '%d' for %s", getBenchmarkName(), procName, id, TransactionType.INVALID.getClass().getSimpleName()));
        }

        Package pkg = this.getProcedurePackageImpl();

        String fullName = pkg.getName() + "." + procName;
        Class<? extends Procedure> procClass = (Class<? extends Procedure>) ClassUtil.getClass(fullName);

        return new TransactionType(procClass, id, false, preExecutionWait, postExecutionWait);
    }

    public final WorkloadConfiguration getWorkloadConfiguration() {
        return (this.workConf);
    }

    /**
     * Return a mapping from TransactionTypes to Procedure invocations
     *
     * @return
     */
    public Map<TransactionType, Procedure> getProcedures() {
        Map<TransactionType, Procedure> proc_xref = new HashMap<>();
        TransactionTypes txns = this.workConf.getTransTypes();

        if (txns != null) {
            for (Class<? extends Procedure> procClass : this.supplementalProcedures) {
                TransactionType txn = txns.getType(procClass);
                if (txn == null) {
                    txn = new TransactionType(procClass, procClass.hashCode(), true, 0, 0);
                    txns.add(txn);
                }
            }

            for (TransactionType txn : txns) {
                Procedure proc = ClassUtil.newInstance(txn.getProcedureClass(), new Object[0], new Class<?>[0]);
                proc.initialize(this.workConf.getDatabaseType());
                proc_xref.put(txn, proc);
                proc.loadSQLDialect(this.dialects);
            }
        }
        if (proc_xref.isEmpty()) {
            LOG.warn("No procedures defined for {}", this);
        }
        return (proc_xref);
    }

    /**
     * @param procClass
     */
    public final void registerSupplementalProcedure(Class<? extends Procedure> procClass) {
        this.supplementalProcedures.add(procClass);
    }

}
