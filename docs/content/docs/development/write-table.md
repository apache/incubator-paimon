---
title: "Write Table"
weight: 3
type: docs
aliases:
- /development/write-table.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Write Table

```sql
INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name
  [PARTITION part_spec] [column_list] select_statement

part_spec:
  (part_col_name1=val1 [, part_col_name2=val2, ...])

column_list:
  (col_name1 [, column_name2, ...])
```

## Unify Streaming and Batch

Flink Table Store supports read/write under both batch and streaming mode.
Beyond that, it can also write to the same managed table simultaneously by
different streaming and batch jobs.

Suppose you have a warehouse pipeline:

{{< img src="/img/stream_batch_insert.svg" alt="Unify Streaming and Batch" >}}

The DWD layer has a following table:

```sql
-- a managed table ddl
CREATE TABLE MyDwdTable (
  user_id BIGINT,
  item_id BIGINT,
  dt STRING
) PARTITIONED BY (dt);
```

And there is a real-time pipeline to perform the data sync task, followed
by the downstream jobs to perform the rest ETL steps.

```sql
-- Run a streaming job that continuously writes to the table
SET 'execution.runtime-mode' = 'streaming';
INSERT INTO MyDwdTable SELECT user_id, item_id, dt FROM MyCdcTable WHERE some_filter;

-- The downstream aggregation task
INSERT INTO MyDwsTable
SELECT dt, item_id, COUNT(user_id) FROM MyDwdTable GROUP BY dt, item_id;
```

Some backfill tasks are often required to correct historical data, which means
you can start a new batch job overwriting the table's historical partition
without influencing the current streaming pipeline and the downstream tasks.

```sql
-- Run a batch job to revise yesterday's partition
SET 'execution.runtime-mode' = 'batch';
INSERT OVERWRITE MyDwdTable PARTITION ('dt'='20220402')
SELECT user_id, item_id FROM MyCdcTable WHERE dt = '20220402' AND new_filter;
```

This way you revise yesterday's partition without suspending the streaming job.

{{< hint info >}}
__Note:__ Multiple jobs writing to a single partition at the same time is
not supported. The behavior does not result in data errors, but can lead
to job failover.
{{< /hint >}}

## Parallelism

It is recommended that the parallelism of sink should be less than or
equal to the number of buckets, preferably equal. You can control the
parallelism of the sink with the `sink.parallelism` option.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-center" style="width: 5%">Required</th>
      <th class="text-center" style="width: 5%">Default</th>
      <th class="text-center" style="width: 10%">Type</th>
      <th class="text-center" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>sink.parallelism</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Defines the parallelism of the sink operator. By default, the parallelism is determined by the framework using the same parallelism of the upstream chained operator.</td>
    </tr>
    </tbody>
</table>

## Expiring Snapshot

Table Store generates one or two snapshots per commit. To avoid too many snapshots 
that create a lot of small files and redundant storage, Table Store writes defaults
to eliminate expired snapshots:

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-center" style="width: 5%">Required</th>
      <th class="text-center" style="width: 5%">Default</th>
      <th class="text-center" style="width: 10%">Type</th>
      <th class="text-center" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>snapshot.time-retained</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">1 h</td>
      <td>Duration</td>
      <td>The maximum time of completed snapshots to retain.</td>
    </tr>
    <tr>
      <td><h5>snapshot.num-retained</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">Integer.MAX_VALUE</td>
      <td>Integer</td>
      <td>The maximum number of completed snapshots to retain.</td>
    </tr>
    </tbody>
</table>

Please note that too short retain time or too small retain number may result in:
- Batch query cannot find the file. For example, the table is relatively large and
  the batch query takes 10 minutes to read, but the snapshot from 10 minutes ago
  expires, at which point the batch query will read a deleted snapshot.
- Continuous reading jobs on FileStore (Without Log System) fail to restart. At the
  time of the job failover, the snapshot it recorded has expired.

## Performance

Table Store uses LSM data structure, which itself has the ability to support a large
number of updates. Update performance and query performance is a tradeoff, the
following parameters control this tradeoff:

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-center" style="width: 5%">Required</th>
      <th class="text-center" style="width: 5%">Default</th>
      <th class="text-center" style="width: 10%">Type</th>
      <th class="text-center" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>num-sorted-run.max</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">5</td>
      <td>Integer</td>
      <td>The max sorted run number. Includes level0 files (one file one sorted run) and high-level runs (one level one sorted run).</td>
    </tr>
    </tbody>
</table>

- The larger `num-sorted-run.max`, the less merge cost when updating data, which
  can avoid many invalid merges. However, if this value is too large, more memory 
  will be needed when merging files because each FileReader will take up a lot of
  memory.

- The smaller `num-sorted-run.max`, the better performance when querying, fewer
  files will be merged.

## Memory

There are three main places in the Table Store's sink writer that take up memory:
- MemTable's write buffer, which is individually occupied by each partition, each
  bucket, and this memory value can be adjustable by the `write-buffer-size`
  option (default 64 MB).
- The memory consumed by compaction for reading files, it can be adjusted by the
  `num-sorted-run.max` option to change the maximum number of files to be merged.
- The memory consumed by writing file, which is not adjustable.
