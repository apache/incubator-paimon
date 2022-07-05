/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.table.store.utils.OptionsUtils.formatEnumOption;

/** Core options for table store. */
public class CoreOptions implements Serializable {

    public static final ConfigOption<Integer> BUCKET =
            ConfigOptions.key("bucket")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Bucket number for file store.");

    @Internal
    public static final ConfigOption<String> PATH =
            ConfigOptions.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The file path of this table in the filesystem.");

    public static final ConfigOption<String> FILE_FORMAT =
            ConfigOptions.key("file.format")
                    .stringType()
                    .defaultValue("orc")
                    .withDescription("Specify the message format of data files.");

    public static final ConfigOption<String> MANIFEST_FORMAT =
            ConfigOptions.key("manifest.format")
                    .stringType()
                    .defaultValue("avro")
                    .withDescription("Specify the message format of manifest files.");

    public static final ConfigOption<MemorySize> MANIFEST_TARGET_FILE_SIZE =
            ConfigOptions.key("manifest.target-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(8))
                    .withDescription("Suggested file size of a manifest file.");

    public static final ConfigOption<Integer> MANIFEST_MERGE_MIN_COUNT =
            ConfigOptions.key("manifest.merge-min-count")
                    .intType()
                    .defaultValue(30)
                    .withDescription(
                            "To avoid frequent manifest merges, this parameter specifies the minimum number "
                                    + "of ManifestFileMeta to merge.");

    public static final ConfigOption<String> PARTITION_DEFAULT_NAME =
            key("partition.default-name")
                    .stringType()
                    .defaultValue("__DEFAULT_PARTITION__")
                    .withDescription(
                            "The default partition name in case the dynamic partition"
                                    + " column value is null/empty string.");

    public static final ConfigOption<Integer> SNAPSHOT_NUM_RETAINED_MIN =
            ConfigOptions.key("snapshot.num-retained.min")
                    .intType()
                    .defaultValue(10)
                    .withDescription("The minimum number of completed snapshots to retain.");

    public static final ConfigOption<Integer> SNAPSHOT_NUM_RETAINED_MAX =
            ConfigOptions.key("snapshot.num-retained.max")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription("The maximum number of completed snapshots to retain.");

    public static final ConfigOption<Duration> SNAPSHOT_TIME_RETAINED =
            ConfigOptions.key("snapshot.time-retained")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription("The maximum time of completed snapshots to retain.");

    public static final ConfigOption<Duration> CONTINUOUS_DISCOVERY_INTERVAL =
            ConfigOptions.key("continuous.discovery-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("The discovery interval of continuous reading.");

    public static final ConfigOption<MergeEngine> MERGE_ENGINE =
            ConfigOptions.key("merge-engine")
                    .enumType(MergeEngine.class)
                    .defaultValue(MergeEngine.DEDUPLICATE)
                    .withDescription(
                            Description.builder()
                                    .text("Specify the merge engine for table with primary key.")
                                    .linebreak()
                                    .list(
                                            formatEnumOption(MergeEngine.DEDUPLICATE),
                                            formatEnumOption(MergeEngine.PARTIAL_UPDATE))
                                    .build());

    public static final ConfigOption<WriteMode> WRITE_MODE =
            ConfigOptions.key("write-mode")
                    .enumType(WriteMode.class)
                    .defaultValue(WriteMode.CHANGE_LOG)
                    .withDescription(
                            Description.builder()
                                    .text("Specify the write mode for table.")
                                    .linebreak()
                                    .list(formatEnumOption(WriteMode.APPEND_ONLY))
                                    .list(formatEnumOption(WriteMode.CHANGE_LOG))
                                    .build());

    public static final ConfigOption<MemorySize> SOURCE_SPLIT_TARGET_SIZE =
            ConfigOptions.key("source.split.target-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription("Target size of a source split when scanning a bucket.");

    public static final ConfigOption<MemorySize> SOURCE_SPLIT_OPEN_FILE_COST =
            ConfigOptions.key("source.split.open-file-cost")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(4))
                    .withDescription(
                            "Open file cost of a source file. It is used to avoid reading"
                                    + " too many files with a source split, which can be very slow.");

    public static final ConfigOption<MemorySize> WRITE_BUFFER_SIZE =
            ConfigOptions.key("write-buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("256 mb"))
                    .withDescription(
                            "Amount of data to build up in memory before converting to a sorted on-disk file.");

    public static final ConfigOption<MemorySize> PAGE_SIZE =
            ConfigOptions.key("page-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64 kb"))
                    .withDescription("Memory page size.");

    public static final ConfigOption<MemorySize> TARGET_FILE_SIZE =
            ConfigOptions.key("target-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription("Target size of a file.");

    public static final ConfigOption<Integer> NUM_SORTED_RUNS_COMPACTION_TRIGGER =
            ConfigOptions.key("num-sorted-run.compaction-trigger")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The sorted run number to trigger compaction. Includes level0 files (one file one sorted run) and "
                                    + "high-level runs (one level one sorted run).");

    public static final ConfigOption<Integer> NUM_SORTED_RUNS_STOP_TRIGGER =
            ConfigOptions.key("num-sorted-run.stop-trigger")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "The number of sorted-runs that trigger the stopping of writes.");

    public static final ConfigOption<Integer> NUM_LEVELS =
            ConfigOptions.key("num-levels")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Total level number, for example, there are 3 levels, including 0,1,2 levels.");

    public static final ConfigOption<Boolean> COMMIT_FORCE_COMPACT =
            ConfigOptions.key("commit.force-compact")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to force a compaction before commit.");

    public static final ConfigOption<Integer> COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT =
            ConfigOptions.key("compaction.max-size-amplification-percent")
                    .intType()
                    .defaultValue(200)
                    .withDescription(
                            "The size amplification is defined as the amount (in percentage) of additional storage "
                                    + "needed to store a single byte of data in the merge tree.");

    public static final ConfigOption<Integer> COMPACTION_SIZE_RATIO =
            ConfigOptions.key("compaction.size-ratio")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Percentage flexibility while comparing sorted run size. If the candidate sorted run(s) "
                                    + "size is 1% smaller than the next sorted run's size, then include next sorted run "
                                    + "into this candidate set.");

    public static final ConfigOption<Boolean> CHANGELOG_FILE =
            ConfigOptions.key("changelog-file")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to double write to a changelog file when flushing memory table. "
                                    + "This changelog file keeps the order of data input and the details of data changes, "
                                    + "it can be read directly during stream reads.");

    public static final ConfigOption<LogStartupMode> LOG_SCAN =
            ConfigOptions.key("log.scan")
                    .enumType(LogStartupMode.class)
                    .defaultValue(LogStartupMode.FULL)
                    .withDescription(
                            Description.builder()
                                    .text("Specify the startup mode for log consumer.")
                                    .linebreak()
                                    .list(formatEnumOption(LogStartupMode.FULL))
                                    .list(formatEnumOption(LogStartupMode.LATEST))
                                    .list(formatEnumOption(LogStartupMode.FROM_TIMESTAMP))
                                    .build());

    public static final ConfigOption<Long> LOG_SCAN_TIMESTAMP_MILLS =
            ConfigOptions.key("log.scan.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"from-timestamp\" scan mode");

    public static final ConfigOption<Duration> LOG_RETENTION =
            ConfigOptions.key("log.retention")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "It means how long changes log will be kept. The default value is from the log system cluster.");

    public static final ConfigOption<LogConsistency> LOG_CONSISTENCY =
            ConfigOptions.key("log.consistency")
                    .enumType(LogConsistency.class)
                    .defaultValue(LogConsistency.TRANSACTIONAL)
                    .withDescription(
                            Description.builder()
                                    .text("Specify the log consistency mode for table.")
                                    .linebreak()
                                    .list(
                                            formatEnumOption(LogConsistency.TRANSACTIONAL),
                                            formatEnumOption(LogConsistency.EVENTUAL))
                                    .build());

    public static final ConfigOption<LogChangelogMode> LOG_CHANGELOG_MODE =
            ConfigOptions.key("log.changelog-mode")
                    .enumType(LogChangelogMode.class)
                    .defaultValue(LogChangelogMode.AUTO)
                    .withDescription(
                            Description.builder()
                                    .text("Specify the log changelog mode for table.")
                                    .linebreak()
                                    .list(
                                            formatEnumOption(LogChangelogMode.AUTO),
                                            formatEnumOption(LogChangelogMode.ALL),
                                            formatEnumOption(LogChangelogMode.UPSERT))
                                    .build());

    public static final ConfigOption<String> LOG_KEY_FORMAT =
            ConfigOptions.key("log.key.format")
                    .stringType()
                    .defaultValue("json")
                    .withDescription(
                            "Specify the key message format of log system with primary key.");

    public static final ConfigOption<String> LOG_FORMAT =
            ConfigOptions.key("log.format")
                    .stringType()
                    .defaultValue("debezium-json")
                    .withDescription("Specify the message format of log system.");

    private final Configuration options;

    public CoreOptions(Map<String, String> options) {
        this(Configuration.fromMap(options));
    }

    public CoreOptions(Configuration options) {
        this.options = options;
        // TODO validate all keys
        Preconditions.checkArgument(
                snapshotNumRetainMin() > 0,
                SNAPSHOT_NUM_RETAINED_MIN.key() + " should be at least 1");
        Preconditions.checkArgument(
                snapshotNumRetainMin() <= snapshotNumRetainMax(),
                SNAPSHOT_NUM_RETAINED_MIN.key()
                        + " should not be larger than "
                        + SNAPSHOT_NUM_RETAINED_MAX.key());
    }

    public int bucket() {
        return options.get(BUCKET);
    }

    public Path path() {
        return path(options.toMap());
    }

    public static Path path(Map<String, String> options) {
        return new Path(options.get(PATH.key()));
    }

    public static Path path(Configuration options) {
        return new Path(options.get(PATH));
    }

    public FileFormat fileFormat() {
        return FileFormat.fromTableOptions(options, FILE_FORMAT);
    }

    public FileFormat manifestFormat() {
        return FileFormat.fromTableOptions(options, MANIFEST_FORMAT);
    }

    public MemorySize manifestTargetSize() {
        return options.get(MANIFEST_TARGET_FILE_SIZE);
    }

    public String partitionDefaultName() {
        return options.get(PARTITION_DEFAULT_NAME);
    }

    public int snapshotNumRetainMin() {
        return options.get(SNAPSHOT_NUM_RETAINED_MIN);
    }

    public int snapshotNumRetainMax() {
        return options.get(SNAPSHOT_NUM_RETAINED_MAX);
    }

    public Duration snapshotTimeRetain() {
        return options.get(SNAPSHOT_TIME_RETAINED);
    }

    public int manifestMergeMinCount() {
        return options.get(MANIFEST_MERGE_MIN_COUNT);
    }

    public long splitTargetSize() {
        return options.get(SOURCE_SPLIT_TARGET_SIZE).getBytes();
    }

    public long splitOpenFileCost() {
        return options.get(SOURCE_SPLIT_OPEN_FILE_COST).getBytes();
    }

    public long writeBufferSize() {
        return options.get(WRITE_BUFFER_SIZE).getBytes();
    }

    public int pageSize() {
        return (int) options.get(PAGE_SIZE).getBytes();
    }

    public long targetFileSize() {
        return options.get(TARGET_FILE_SIZE).getBytes();
    }

    public int numSortedRunCompactionTrigger() {
        return options.get(NUM_SORTED_RUNS_COMPACTION_TRIGGER);
    }

    public int numSortedRunStopTrigger() {
        return Math.max(numSortedRunCompactionTrigger(), options.get(NUM_SORTED_RUNS_STOP_TRIGGER));
    }

    public int numLevels() {
        // By default, this ensures that the compaction does not fall to level 0, but at least to
        // level 1
        Integer numLevels = options.get(NUM_LEVELS);
        numLevels = numLevels == null ? numSortedRunCompactionTrigger() + 1 : numLevels;
        return numLevels;
    }

    public boolean commitForceCompact() {
        return options.get(COMMIT_FORCE_COMPACT);
    }

    public int maxSizeAmplificationPercent() {
        return options.get(COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT);
    }

    public int sizeRatio() {
        return options.get(COMPACTION_SIZE_RATIO);
    }

    public boolean enableChangelogFile() {
        return options.get(CHANGELOG_FILE);
    }

    /** Specifies the merge engine for table with primary key. */
    public enum MergeEngine implements DescribedEnum {
        DEDUPLICATE("deduplicate", "De-duplicate and keep the last row."),

        PARTIAL_UPDATE("partial-update", "Partial update non-null fields.");

        private final String value;
        private final String description;

        MergeEngine(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** Specifies the startup mode for log consumer. */
    public enum LogStartupMode implements DescribedEnum {
        FULL(
                "full",
                "Perform a snapshot on the table upon first startup,"
                        + " and continue to read the latest changes."),

        LATEST("latest", "Start from the latest."),

        FROM_TIMESTAMP("from-timestamp", "Start from user-supplied timestamp.");

        private final String value;
        private final String description;

        LogStartupMode(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** Specifies the log consistency mode for table. */
    public enum LogConsistency implements DescribedEnum {
        TRANSACTIONAL(
                "transactional",
                "Only the data after the checkpoint can be seen by readers, the latency depends on checkpoint interval."),

        EVENTUAL(
                "eventual",
                "Immediate data visibility, you may see some intermediate states, "
                        + "but eventually the right results will be produced, only works for table with primary key.");

        private final String value;
        private final String description;

        LogConsistency(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** Specifies the log changelog mode for table. */
    public enum LogChangelogMode implements DescribedEnum {
        AUTO("auto", "Upsert for table with primary key, all for table without primary key.."),

        ALL("all", "The log system stores all changes including UPDATE_BEFORE."),

        UPSERT(
                "upsert",
                "The log system does not store the UPDATE_BEFORE changes, the log consumed job"
                        + " will automatically add the normalized node, relying on the state"
                        + " to generate the required update_before.");

        private final String value;
        private final String description;

        LogChangelogMode(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    @Internal
    public static List<ConfigOption<?>> getOptions() {
        final Field[] fields = CoreOptions.class.getFields();
        final List<ConfigOption<?>> list = new ArrayList<>(fields.length);
        for (Field field : fields) {
            if (ConfigOption.class.isAssignableFrom(field.getType())) {
                try {
                    list.add((ConfigOption<?>) field.get(CoreOptions.class));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return list;
    }
}
