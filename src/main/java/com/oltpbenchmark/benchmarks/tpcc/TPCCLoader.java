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


package com.oltpbenchmark.benchmarks.tpcc;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.tpcc.pojo.*;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * TPC-C Benchmark Loader
 */
public class TPCCLoader extends Loader<TPCCBenchmark> {

    private static final int FIRST_UNPROCESSED_O_ID = 2101;

    private final long numWarehouses;

    public TPCCLoader(TPCCBenchmark benchmark) {
        super(benchmark);
        numWarehouses = Math.max(Math.round(TPCCConfig.configWhseCount * this.scaleFactor), 1);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();

        final int maxConcurrent = workConf.getLoaderThreads();

        final int firstWarehouse = this.startFromId;
        final int lastWarehouse = this.startFromId + (int)numWarehouses - 1;
        final int lastWarehouseInCompany = lastWarehouse;
        final int warehousesPerThread = Math.max((int)numWarehouses / maxConcurrent, 1);
        final int threadsPerTable = ((int)numWarehouses + warehousesPerThread - 1) / warehousesPerThread;

        final CountDownLatch warehouseItemDistrictLatch = new CountDownLatch(1);
        final CountDownLatch stockCustomerLatch = new CountDownLatch(threadsPerTable);
        final CountDownLatch historyOpenOrdersLatch = new CountDownLatch(threadsPerTable);

        if (firstWarehouse == 1) {
            threads.add(new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) {
                    loadWarehouses(conn, firstWarehouse, lastWarehouseInCompany);
                    loadItems(conn, TPCCConfig.configItemCount);
                    loadDistricts(conn, firstWarehouse, lastWarehouseInCompany, TPCCConfig.configDistPerWhse);
                }

                @Override
                public void afterLoad() {
                    warehouseItemDistrictLatch.countDown();
                }
            });
        }

        for (int w = this.startFromId; w <= lastWarehouse; w += warehousesPerThread) {
            final int loadFrom = w;
            final int loadUntil = Math.min(loadFrom + warehousesPerThread - 1, lastWarehouse);
            LoaderThread t = new LoaderThread(this.benchmark) {
                @Override
                public void beforeLoad() {
                    try {
                        warehouseItemDistrictLatch.await();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }

                @Override
                public void load(Connection conn) {
                    LOG.debug("Starting to load stock for warehouses from {} to {}", loadFrom, loadUntil);
                    loadStock(conn, loadFrom, loadUntil, TPCCConfig.configItemCount);

                    LOG.debug("Starting to load customers for warehouses from {} to {}", loadFrom, loadUntil);
                    loadCustomers(conn, loadFrom, loadUntil, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
                }

                @Override
                public void afterLoad() {
                    stockCustomerLatch.countDown();
                }
            };
            threads.add(t);
        }

        for (int w = this.startFromId; w <= lastWarehouse; w += warehousesPerThread) {
            final int loadFrom = w;
            final int loadUntil = Math.min(loadFrom + warehousesPerThread - 1, lastWarehouse);

            LoaderThread t = new LoaderThread(this.benchmark) {
                @Override
                public void beforeLoad() {
                    try {
                        stockCustomerLatch.await();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }

                @Override
                public void load(Connection conn) {
                    LOG.debug("Starting to load history for warehouses from {} to {}", loadFrom, loadUntil);
                    loadCustomerHistory(conn, loadFrom, loadUntil, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

                    LOG.debug("Starting to load open orders for warehouses from {} to {}", loadFrom, loadUntil);
                    loadOpenOrders(conn, loadFrom, loadUntil, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
                }

                @Override
                public void afterLoad() {
                    historyOpenOrdersLatch.countDown();
                }
            };

            threads.add(t);
        }

        for (int w = this.startFromId; w <= lastWarehouse; w += warehousesPerThread) {
            final int loadFrom = w;
            final int loadUntil = Math.min(loadFrom + warehousesPerThread - 1, lastWarehouse);

            LoaderThread t = new LoaderThread(this.benchmark) {
                @Override
                public void beforeLoad() {
                    try {
                        historyOpenOrdersLatch.await();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }

                @Override
                public void load(Connection conn) {
                    LOG.debug("Starting to load order lines for warehouses from {} to {}", loadFrom, loadUntil);
                    loadOrderLines(conn, loadFrom, loadUntil, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

                    LOG.debug("Starting to load new orders for warehouses from {} to {}", loadFrom, loadUntil);
                    loadNewOrders(conn, firstWarehouse, lastWarehouseInCompany, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
                }
            };
            threads.add(t);
        }

        return (threads);
    }


    private PreparedStatement getInsertStatement(Connection conn, String tableName) throws SQLException {
        Table catalog_tbl = benchmark.getCatalog().getTable(tableName);
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        return conn.prepareStatement(sql);
    }


    protected void loadItems(Connection conn, int itemCount) {

        try (PreparedStatement itemPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_ITEM)) {

            int batchSize = 0;
            for (int i = 1; i <= itemCount; i++) {

                Item item = new Item();
                item.i_id = i;
                item.i_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24, benchmark.rng()));
                item.i_price = TPCCUtil.randomNumber(100, 10000, benchmark.rng()) / 100.0;

                // i_data
                int randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
                int len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
                if (randPct > 10) {
                    // 90% of time i_data isa random string of length [26 .. 50]
                    item.i_data = TPCCUtil.randomStr(len);
                } else {
                    // 10% of time i_data has "ORIGINAL" crammed somewhere in
                    // middle
                    int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), benchmark.rng());
                    item.i_data = TPCCUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + TPCCUtil.randomStr(len - startORIGINAL - 9);
                }

                item.i_im_id = TPCCUtil.randomNumber(1, 10000, benchmark.rng());

                int idx = 1;
                itemPrepStmt.setLong(idx++, item.i_id);
                itemPrepStmt.setString(idx++, item.i_name);
                itemPrepStmt.setDouble(idx++, item.i_price);
                itemPrepStmt.setString(idx++, item.i_data);
                itemPrepStmt.setLong(idx, item.i_im_id);
                itemPrepStmt.addBatch();
                batchSize++;

                if (batchSize == workConf.getBatchSize()) {
                    itemPrepStmt.executeBatch();
                    itemPrepStmt.clearBatch();
                    batchSize = 0;
                }
            }


            if (batchSize > 0) {
                itemPrepStmt.executeBatch();
                itemPrepStmt.clearBatch();
            }

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }

    }


    protected void loadWarehouses(Connection conn, int start_id, int last_id) {

        try (PreparedStatement whsePrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_WAREHOUSE)) {
            int batchSize = 0;
            for (int w_id = start_id; w_id <= last_id; ++w_id) {
                Warehouse warehouse = new Warehouse();

                warehouse.w_id = w_id;
                warehouse.w_ytd = 300000;

                // random within [0.0000 .. 0.2000]
                warehouse.w_tax = (TPCCUtil.randomNumber(0, 2000, benchmark.rng())) / 10000.0;
                warehouse.w_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, benchmark.rng()));
                warehouse.w_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                warehouse.w_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                warehouse.w_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                warehouse.w_state = TPCCUtil.randomStr(3).toUpperCase();
                warehouse.w_zip = "123456789";

                int idx = 1;
                whsePrepStmt.setLong(idx++, warehouse.w_id);
                whsePrepStmt.setDouble(idx++, warehouse.w_ytd);
                whsePrepStmt.setDouble(idx++, warehouse.w_tax);
                whsePrepStmt.setString(idx++, warehouse.w_name);
                whsePrepStmt.setString(idx++, warehouse.w_street_1);
                whsePrepStmt.setString(idx++, warehouse.w_street_2);
                whsePrepStmt.setString(idx++, warehouse.w_city);
                whsePrepStmt.setString(idx++, warehouse.w_state);
                whsePrepStmt.setString(idx, warehouse.w_zip);

                whsePrepStmt.addBatch();
                batchSize++;
                if (batchSize == workConf.getBatchSize()) {
                    whsePrepStmt.executeBatch();
                    whsePrepStmt.clearBatch();
                    batchSize = 0;
                }
            }

            if (batchSize > 0) {
                whsePrepStmt.executeBatch();
                whsePrepStmt.clearBatch();
            }

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }

    }

    protected void loadStock(Connection conn, int start_id, int last_id, int numItems) {

        try (PreparedStatement stockPreparedStatement = getInsertStatement(conn, TPCCConstants.TABLENAME_STOCK)) {
            int batchSize = 0;
            for (int w_id = start_id; w_id <= last_id; ++w_id) {
                for (int i = 1; i <= numItems; i++) {
                    Stock stock = new Stock();
                    stock.s_i_id = i;
                    stock.s_w_id = w_id;
                    stock.s_quantity = TPCCUtil.randomNumber(10, 100, benchmark.rng());
                    stock.s_ytd = 0;
                    stock.s_order_cnt = 0;
                    stock.s_remote_cnt = 0;

                    // s_data
                    int randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
                    int len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
                    if (randPct > 10) {
                        // 90% of time i_data isa random string of length [26 ..
                        // 50]
                        stock.s_data = TPCCUtil.randomStr(len);
                    } else {
                        // 10% of time i_data has "ORIGINAL" crammed somewhere
                        // in middle
                        int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), benchmark.rng());
                        stock.s_data = TPCCUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + TPCCUtil.randomStr(len - startORIGINAL - 9);
                    }

                    int idx = 1;
                    stockPreparedStatement.setLong(idx++, stock.s_w_id);
                    stockPreparedStatement.setLong(idx++, stock.s_i_id);
                    stockPreparedStatement.setLong(idx++, stock.s_quantity);
                    stockPreparedStatement.setDouble(idx++, stock.s_ytd);
                    stockPreparedStatement.setLong(idx++, stock.s_order_cnt);
                    stockPreparedStatement.setLong(idx++, stock.s_remote_cnt);
                    stockPreparedStatement.setString(idx++, stock.s_data);
                    stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                    stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                    stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                    stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                    stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                    stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                    stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                    stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                    stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                    stockPreparedStatement.setString(idx, TPCCUtil.randomStr(24));

                    stockPreparedStatement.addBatch();
                    batchSize++;
                    if (batchSize == workConf.getBatchSize()) {
                        stockPreparedStatement.executeBatch();
                        stockPreparedStatement.clearBatch();
                        batchSize = 0;
                    }
                }
            }

            if (batchSize > 0) {
                stockPreparedStatement.executeBatch();
                stockPreparedStatement.clearBatch();
            }

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }

    }

    protected void loadDistricts(Connection conn, int start_id, int last_id, int districtsPerWarehouse) {

        try (PreparedStatement distPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_DISTRICT)) {
            int batchSize = 0;
            for (int w_id = start_id; w_id <= last_id; w_id++) {
                for (int d = 1; d <= districtsPerWarehouse; d++) {
                    District district = new District();
                    district.d_id = d;
                    district.d_w_id = w_id;
                    district.d_ytd = 30000;

                    // random within [0.0000 .. 0.2000]
                    district.d_tax = (float) ((TPCCUtil.randomNumber(0, 2000, benchmark.rng())) / 10000.0);

                    district.d_next_o_id = TPCCConfig.configCustPerDist + 1;
                    district.d_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, benchmark.rng()));
                    district.d_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                    district.d_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                    district.d_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                    district.d_state = TPCCUtil.randomStr(3).toUpperCase();
                    district.d_zip = "123456789";

                    int idx = 1;
                    distPrepStmt.setLong(idx++, district.d_w_id);
                    distPrepStmt.setLong(idx++, district.d_id);
                    distPrepStmt.setDouble(idx++, district.d_ytd);
                    distPrepStmt.setDouble(idx++, district.d_tax);
                    distPrepStmt.setLong(idx++, district.d_next_o_id);
                    distPrepStmt.setString(idx++, district.d_name);
                    distPrepStmt.setString(idx++, district.d_street_1);
                    distPrepStmt.setString(idx++, district.d_street_2);
                    distPrepStmt.setString(idx++, district.d_city);
                    distPrepStmt.setString(idx++, district.d_state);
                    distPrepStmt.setString(idx, district.d_zip);

                    distPrepStmt.addBatch();
                    batchSize++;
                    if (batchSize == workConf.getBatchSize()) {
                        distPrepStmt.executeBatch();
                        distPrepStmt.clearBatch();
                        batchSize = 0;
                    }
                }
            }

            if (batchSize > 0) {
                distPrepStmt.executeBatch();
                distPrepStmt.clearBatch();
            }
        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }

    }

    protected void loadCustomers(Connection conn, int start_id, int last_id, int districtsPerWarehouse, int customersPerDistrict) {

        try (PreparedStatement custPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_CUSTOMER)) {
            int batchSize = 0;
            for (int w_id = start_id; w_id <= last_id; w_id++) {
                for (int d = 1; d <= districtsPerWarehouse; d++) {
                    for (int c = 1; c <= customersPerDistrict; c++) {
                        Timestamp sysdate = new Timestamp(System.currentTimeMillis());

                        Customer customer = new Customer();
                        customer.c_id = c;
                        customer.c_d_id = d;
                        customer.c_w_id = w_id;

                        // discount is random between [0.0000 ... 0.5000]
                        customer.c_discount = (float) (TPCCUtil.randomNumber(1, 5000, benchmark.rng()) / 10000.0);

                        if (TPCCUtil.randomNumber(1, 100, benchmark.rng()) <= 10) {
                            customer.c_credit = "BC"; // 10% Bad Credit
                        } else {
                            customer.c_credit = "GC"; // 90% Good Credit
                        }
                        if (c <= 1000) {
                            customer.c_last = TPCCUtil.getLastName(c - 1);
                        } else {
                            customer.c_last = TPCCUtil.getNonUniformRandomLastNameForLoad(benchmark.rng());
                        }
                        customer.c_first = TPCCUtil.randomStr(TPCCUtil.randomNumber(8, 16, benchmark.rng()));
                        customer.c_credit_lim = 50000;

                        customer.c_balance = -10;
                        customer.c_ytd_payment = 10;
                        customer.c_payment_cnt = 1;
                        customer.c_delivery_cnt = 0;

                        customer.c_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                        customer.c_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                        customer.c_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                        customer.c_state = TPCCUtil.randomStr(3).toUpperCase();
                        // TPC-C 4.3.2.7: 4 random digits + "11111"
                        customer.c_zip = TPCCUtil.randomNStr(4) + "11111";
                        customer.c_phone = TPCCUtil.randomNStr(16);
                        customer.c_since = sysdate;
                        customer.c_middle = "OE";
                        customer.c_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(300, 500, benchmark.rng()));

                        int idx = 1;
                        custPrepStmt.setLong(idx++, customer.c_w_id);
                        custPrepStmt.setLong(idx++, customer.c_d_id);
                        custPrepStmt.setLong(idx++, customer.c_id);
                        custPrepStmt.setDouble(idx++, customer.c_discount);
                        custPrepStmt.setString(idx++, customer.c_credit);
                        custPrepStmt.setString(idx++, customer.c_last);
                        custPrepStmt.setString(idx++, customer.c_first);
                        custPrepStmt.setDouble(idx++, customer.c_credit_lim);
                        custPrepStmt.setDouble(idx++, customer.c_balance);
                        custPrepStmt.setDouble(idx++, customer.c_ytd_payment);
                        custPrepStmt.setLong(idx++, customer.c_payment_cnt);
                        custPrepStmt.setLong(idx++, customer.c_delivery_cnt);
                        custPrepStmt.setString(idx++, customer.c_street_1);
                        custPrepStmt.setString(idx++, customer.c_street_2);
                        custPrepStmt.setString(idx++, customer.c_city);
                        custPrepStmt.setString(idx++, customer.c_state);
                        custPrepStmt.setString(idx++, customer.c_zip);
                        custPrepStmt.setString(idx++, customer.c_phone);
                        custPrepStmt.setTimestamp(idx++, customer.c_since);
                        custPrepStmt.setString(idx++, customer.c_middle);
                        custPrepStmt.setString(idx, customer.c_data);

                        custPrepStmt.addBatch();
                        batchSize++;
                        if (batchSize == workConf.getBatchSize()) {
                            custPrepStmt.executeBatch();
                            custPrepStmt.clearBatch();
                            batchSize = 0;
                        }
                    }
                }
            }

            if (batchSize > 0) {
                custPrepStmt.executeBatch();
                custPrepStmt.clearBatch();
            }

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }

    }

    protected void loadCustomerHistory(Connection conn, int start_id, int last_id, int districtsPerWarehouse, int customersPerDistrict) {

        try (PreparedStatement histPrepStmt = getInsertStatement(conn, TPCCConstants.TABLENAME_HISTORY)) {
            int batchSize = 0;
            for (int w_id = start_id; w_id <= last_id; ++w_id) {

                for (int d = 1; d <= districtsPerWarehouse; d++) {
                    for (int c = 1; c <= customersPerDistrict; c++) {
                        Timestamp sysdate = new Timestamp(System.currentTimeMillis());

                        History history = new History();
                        history.h_c_id = c;
                        history.h_c_d_id = d;
                        history.h_c_w_id = w_id;
                        history.h_d_id = d;
                        history.h_w_id = w_id;
                        history.h_date = sysdate;
                        history.h_amount = 10;
                        history.h_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 24, benchmark.rng()));


                        int idx = 1;
                        histPrepStmt.setInt(idx++, history.h_c_id);
                        histPrepStmt.setInt(idx++, history.h_c_d_id);
                        histPrepStmt.setInt(idx++, history.h_c_w_id);
                        histPrepStmt.setInt(idx++, history.h_d_id);
                        histPrepStmt.setInt(idx++, history.h_w_id);
                        histPrepStmt.setTimestamp(idx++, history.h_date);
                        histPrepStmt.setDouble(idx++, history.h_amount);
                        histPrepStmt.setString(idx, history.h_data);

                        histPrepStmt.addBatch();
                        batchSize++;
                        if (batchSize == workConf.getBatchSize()) {
                            histPrepStmt.executeBatch();
                            histPrepStmt.clearBatch();
                            batchSize = 0;
                        }
                    }
                }
            }

            if (batchSize > 0) {
                histPrepStmt.executeBatch();
                histPrepStmt.clearBatch();
            }

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }

    }

    protected void loadOpenOrders(Connection conn, int start_id, int last_id, int districtsPerWarehouse, int customersPerDistrict) {

        try (PreparedStatement openOrderStatement = getInsertStatement(conn, TPCCConstants.TABLENAME_OPENORDER)) {
            int batchSize = 0;
            for (int w_id = start_id; w_id <= last_id; ++w_id) {

                for (int d = 1; d <= districtsPerWarehouse; d++) {
                    // TPC-C 4.3.3.1: o_c_id must be a permutation of [1, 3000]
                    int[] c_ids = new int[customersPerDistrict];
                    for (int i = 0; i < customersPerDistrict; ++i) {
                        c_ids[i] = i + 1;
                    }
                    // Collections.shuffle exists, but there is no
                    // Arrays.shuffle
                    for (int i = 0; i < c_ids.length - 1; ++i) {
                        int remaining = c_ids.length - i - 1;
                        int swapIndex = benchmark.rng().nextInt(remaining) + i + 1;

                        int temp = c_ids[swapIndex];
                        c_ids[swapIndex] = c_ids[i];
                        c_ids[i] = temp;
                    }

                    for (int c = 1; c <= customersPerDistrict; c++) {

                        Oorder oorder = new Oorder();
                        oorder.o_id = c;
                        oorder.o_w_id = w_id;
                        oorder.o_d_id = d;
                        oorder.o_c_id = c_ids[c - 1];
                        // o_carrier_id is set *only* for orders with ids < 2101
                        // [4.3.3.1]
                        if (oorder.o_id < FIRST_UNPROCESSED_O_ID) {
                            oorder.o_carrier_id = TPCCUtil.randomNumber(1, 10, benchmark.rng());
                        } else {
                            oorder.o_carrier_id = null;
                        }
                        oorder.o_ol_cnt = getRandomCount(w_id, c, d);
                        oorder.o_all_local = 1;
                        oorder.o_entry_d = new Timestamp(System.currentTimeMillis());


                        int idx = 1;
                        openOrderStatement.setInt(idx++, oorder.o_w_id);
                        openOrderStatement.setInt(idx++, oorder.o_d_id);
                        openOrderStatement.setInt(idx++, oorder.o_id);
                        openOrderStatement.setInt(idx++, oorder.o_c_id);
                        if (oorder.o_carrier_id != null) {
                            openOrderStatement.setInt(idx++, oorder.o_carrier_id);
                        } else {
                            openOrderStatement.setNull(idx++, Types.INTEGER);
                        }
                        openOrderStatement.setInt(idx++, oorder.o_ol_cnt);
                        openOrderStatement.setInt(idx++, oorder.o_all_local);
                        openOrderStatement.setTimestamp(idx, oorder.o_entry_d);

                        openOrderStatement.addBatch();
                        batchSize++;
                        if (batchSize == workConf.getBatchSize()) {
                            openOrderStatement.executeBatch();
                            openOrderStatement.clearBatch();
                            batchSize = 0;
                        }
                    }
                }
            }

            if (batchSize > 0) {
                openOrderStatement.executeBatch();
                openOrderStatement.clearBatch();
            }

        } catch (SQLException se) {
            LOG.error(se.getMessage(), se);
        }

    }

    private int getRandomCount(int w_id, int c, int d) {
        Customer customer = new Customer();
        customer.c_id = c;
        customer.c_d_id = d;
        customer.c_w_id = w_id;

        Random random = new Random(customer.hashCode());

        return TPCCUtil.randomNumber(5, 15, random);
    }

    protected void loadNewOrders(Connection conn, int start_id, int last_id, int districtsPerWarehouse, int customersPerDistrict) {

        try (PreparedStatement newOrderStatement = getInsertStatement(conn, TPCCConstants.TABLENAME_NEWORDER)) {
            int batchSize = 0;
            for (int w_id = start_id; w_id <= last_id; ++w_id) {
                for (int d = 1; d <= districtsPerWarehouse; d++) {

                    for (int c = 1; c <= customersPerDistrict; c++) {

                        // 900 rows in the NEW-ORDER table corresponding to the last
                        // 900 rows in the ORDER table for that district (i.e.,
                        // with NO_O_ID between 2,101 and 3,000)
                        if (c >= FIRST_UNPROCESSED_O_ID) {
                            NewOrder new_order = new NewOrder();
                            new_order.no_w_id = w_id;
                            new_order.no_d_id = d;
                            new_order.no_o_id = c;

                            int idx = 1;
                            newOrderStatement.setInt(idx++, new_order.no_w_id);
                            newOrderStatement.setInt(idx++, new_order.no_d_id);
                            newOrderStatement.setInt(idx, new_order.no_o_id);
                            newOrderStatement.addBatch();
                            batchSize++;
                            if (batchSize == workConf.getBatchSize()) {
                                newOrderStatement.executeBatch();
                                newOrderStatement.clearBatch();
                                batchSize = 0;
                            }
                        }
                    }
                }
            }

            if (batchSize > 0) {
                newOrderStatement.executeBatch();
                newOrderStatement.clearBatch();
            }

        } catch (SQLException se) {
            LOG.error(se.getMessage(), se);
        }

    }

    protected void loadOrderLines(Connection conn, int start_id, int last_id, int districtsPerWarehouse, int customersPerDistrict) {

        try (PreparedStatement orderLineStatement = getInsertStatement(conn, TPCCConstants.TABLENAME_ORDERLINE)) {
            int batchSize = 0;
            for (int w_id = start_id; w_id <= last_id; ++w_id) {
                for (int d = 1; d <= districtsPerWarehouse; d++) {

                    for (int c = 1; c <= customersPerDistrict; c++) {

                        int count = getRandomCount(w_id, c, d);

                        for (int l = 1; l <= count; l++) {
                            OrderLine order_line = new OrderLine();
                            order_line.ol_w_id = w_id;
                            order_line.ol_d_id = d;
                            order_line.ol_o_id = c;
                            order_line.ol_number = l; // ol_number
                            order_line.ol_i_id = TPCCUtil.randomNumber(1, TPCCConfig.configItemCount, benchmark.rng());
                            if (order_line.ol_o_id < FIRST_UNPROCESSED_O_ID) {
                                order_line.ol_delivery_d = new Timestamp(System.currentTimeMillis());
                                order_line.ol_amount = 0;
                            } else {
                                order_line.ol_delivery_d = null;
                                // random within [0.01 .. 9,999.99]
                                order_line.ol_amount = (float) (TPCCUtil.randomNumber(1, 999999, benchmark.rng()) / 100.0);
                            }
                            order_line.ol_supply_w_id = order_line.ol_w_id;
                            order_line.ol_quantity = 5;
                            order_line.ol_dist_info = TPCCUtil.randomStr(24);

                            int idx = 1;
                            orderLineStatement.setInt(idx++, order_line.ol_w_id);
                            orderLineStatement.setInt(idx++, order_line.ol_d_id);
                            orderLineStatement.setInt(idx++, order_line.ol_o_id);
                            orderLineStatement.setInt(idx++, order_line.ol_number);
                            orderLineStatement.setLong(idx++, order_line.ol_i_id);
                            if (order_line.ol_delivery_d != null) {
                                orderLineStatement.setTimestamp(idx++, order_line.ol_delivery_d);
                            } else {
                                orderLineStatement.setNull(idx++, 0);
                            }
                            orderLineStatement.setDouble(idx++, order_line.ol_amount);
                            orderLineStatement.setLong(idx++, order_line.ol_supply_w_id);
                            orderLineStatement.setDouble(idx++, order_line.ol_quantity);
                            orderLineStatement.setString(idx, order_line.ol_dist_info);

                            orderLineStatement.addBatch();
                            batchSize++;
                            if (batchSize == workConf.getBatchSize()) {
                                orderLineStatement.executeBatch();
                                orderLineStatement.clearBatch();
                                batchSize = 0;
                            }

                        }

                    }

                }
            }

            if (batchSize > 0) {
                orderLineStatement.executeBatch();
                orderLineStatement.clearBatch();
            }

        } catch (SQLException se) {
            LOG.error(se.getMessage(), se);
        }

    }

}
