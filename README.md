# Overview

This is a fork of [BenchBase](https://github.com/cmu-db/benchbase) done to enhance TPC-C for PostgresSQL:
1. Fixed some performance issues in the original benchbase to speed up the benchmark.
2. To address issues with running high number of warehouses, we added support for virtual threads (requires Java >= 21).
3. Significantly reduced the memory footprint of the benchmark.
4. Added Hikari as a connection pool for PostgreSQL.

Please, note that this is for PostgreSQL only.

## Hardware requirements

Minumum requirements for running the benchmark against YDB:
* 2 cores and 4 GB RAM (for 100 warehouses)
* 4 cores and 6 GB RAM (for 1000 warehouses)

Above 1000 warehouses, the memory and CPU consumption grow linearly, you need:
* 1 core per 1000 warehouses
* 6 MB RAM per warehouse

E.g. to run 10000 warehouses you need to have at least 10 cores and 64 GB RAM. However, Instead of running 10000 warehouses on a single instance (and machine), we recommend to run at most 5000 warehouses per instance (preferably on separate machines).

To reduce memory consumption, make sure you don't use huge pages or transparent huge pages.

# TPC-C benchmark for PostgreSQL

## How to build

```
./mvnw clean package -P postgres -DskipTests
```

Prebuilt packages:
* [benchbase-postgres.tgz](https://storage.yandexcloud.net/ydb-benchmark-builds/benchbase-postgres.tgz)

## How to run

The simplest way is to use helper scripts from [benchhelpers](https://github.com/ydb-platform/benchhelpers). You can find the full instruction [here](https://github.com/ydb-platform/benchhelpers/blob/main/tpcc/postgres/README.md).
