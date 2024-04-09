package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StatisticTableTest extends TableTestBase {

    private FileStoreTable table;
    private FileStoreScan scan;
    private FilesTable statisticTable;
    private SnapshotManager snapshotManager;

    @BeforeEach
    public void before() throws Exception {
        String tableName = "MyTable";
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, tableName));
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                        .option(CoreOptions.BUCKET.key(), "2")
                        .option(CoreOptions.SEQUENCE_FIELD.key(), "col1")
                        .build();
        TableSchema tableSchema =
                SchemaUtils.forceCommit(new SchemaManager(fileIO, tablePath), schema);
        table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
        scan = table.store().newScan();

        Identifier statisticTableId =
                identifier(tableName + Catalog.SYSTEM_TABLE_SPLITTER + StatisticTable.STATISTIC);
        statisticTable = (FilesTable) catalog.getTable(statisticTableId);
        snapshotManager = new SnapshotManager(fileIO, tablePath);

        // snapshot 1: append
        write(table, GenericRow.of(1, 1, 1), GenericRow.of(1, 2, 5));

        // snapshot 2: append
        write(table, GenericRow.of(2, 1, 3), GenericRow.of(2, 2, 4));
    }

    @Test
    public void testReadCountFromStatisticTable() throws Exception {
        List<InternalRow> result = read(statisticTable);
        assertThat(result.size() == 1);
    }

    private List<InternalRow> getExceptedResult(long snapshotId) {
        if (!snapshotManager.snapshotExists(snapshotId)) {
            return Collections.emptyList();
        }

        FileStoreScan.Plan plan = scan.withSnapshot(snapshotId).plan();

        List<ManifestEntry> files = plan.files(FileKind.ADD);

        List<InternalRow> expectedRow = new ArrayList<>();
        for (ManifestEntry fileEntry : files) {
            String partition = String.valueOf(fileEntry.partition().getInt(0));
            DataFileMeta file = fileEntry.file();
            String minKey = String.valueOf(file.minKey().getInt(0));
            String maxKey = String.valueOf(file.maxKey().getInt(0));
            String minCol1 = String.valueOf(file.valueStats().minValues().getInt(2));
            String maxCol1 = String.valueOf(file.valueStats().maxValues().getInt(2));
            expectedRow.add(
                    GenericRow.of(
                            BinaryString.fromString(Arrays.toString(new String[] {partition})),
                            fileEntry.bucket(),
                            BinaryString.fromString(file.fileName()),
                            BinaryString.fromString("orc"),
                            file.schemaId(),
                            file.level(),
                            file.rowCount(),
                            file.fileSize(),
                            BinaryString.fromString(Arrays.toString(new String[] {minKey})),
                            BinaryString.fromString(Arrays.toString(new String[] {maxKey})),
                            BinaryString.fromString(
                                    String.format("{col1=%s, pk=%s, pt=%s}", 0, 0, 0)),
                            BinaryString.fromString(
                                    String.format(
                                            "{col1=%s, pk=%s, pt=%s}", minCol1, minKey, partition)),
                            BinaryString.fromString(
                                    String.format(
                                            "{col1=%s, pk=%s, pt=%s}", maxCol1, maxKey, partition)),
                            file.minSequenceNumber(),
                            file.maxSequenceNumber(),
                            file.creationTime()));
        }
        return expectedRow;
    }

}
