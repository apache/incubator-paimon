/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.Utils;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.stream.Stream;

/** Utils for file reading and writing. */
public class FileUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);
    private static final int LIST_MAX_RETRY = 30;

    public static final Configuration DEFAULT_READER_CONFIG = new Configuration();

    static {
        DEFAULT_READER_CONFIG.setInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 1);
    }

    public static final ForkJoinPool COMMON_IO_FORK_JOIN_POOL;

    // if we want to name threads in the fork join pool we need all these
    // see https://stackoverflow.com/questions/34303094/
    static {
        ForkJoinPool.ForkJoinWorkerThreadFactory factory =
                pool -> {
                    ForkJoinWorkerThread worker =
                            ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                    worker.setName("file-store-common-io-" + worker.getPoolIndex());
                    return worker;
                };
        COMMON_IO_FORK_JOIN_POOL =
                new ForkJoinPool(Runtime.getRuntime().availableProcessors(), factory, null, false);
    }

    public static <T> List<T> readListFromFile(
            Path path,
            ObjectSerializer<T> serializer,
            BulkFormat<RowData, FileSourceSplit> readerFactory)
            throws IOException {
        List<T> result = new ArrayList<>();
        long fileSize = FileUtils.getFileSize(path);
        FileSourceSplit split = new FileSourceSplit("ignore", path, 0, fileSize, 0, fileSize);
        BulkFormat.Reader<RowData> reader =
                readerFactory.createReader(DEFAULT_READER_CONFIG, split);
        Utils.forEachRemaining(reader, row -> result.add(serializer.fromRow(row)));
        return result;
    }

    public static long getFileSize(Path path) throws IOException {
        return path.getFileSystem().getFileStatus(path).getLen();
    }

    public static String readFileUtf8(Path file) throws IOException {
        try (FSDataInputStream in = file.getFileSystem().open(file)) {
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
            }
            return builder.toString();
        }
    }

    public static void writeFileUtf8(Path file, String content) throws IOException {
        try (FSDataOutputStream out =
                file.getFileSystem().create(file, FileSystem.WriteMode.NO_OVERWRITE)) {
            writeOutputStreamUtf8(out, content);
        }
    }

    public static void writeOutputStreamUtf8(FSDataOutputStream out, String content)
            throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
        writer.write(content);
        writer.flush();
    }

    public static void deleteOrWarn(Path file) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to delete " + file.toString());
        }

        try {
            FileSystem fs = file.getFileSystem();
            if (!fs.delete(file, false) && fs.exists(file)) {
                LOG.warn("Failed to delete file " + file);
            }
        } catch (IOException e) {
            LOG.warn("Exception occurs when deleting file " + file, e);
        }
    }

    @Nullable
    public static FileStatus[] safelyListFileStatus(Path file) throws IOException {
        int retry = 1;
        FileStatus[] statuses = null;
        while (retry <= LIST_MAX_RETRY) {
            try {
                statuses = file.getFileSystem().listStatus(file);
                break;
            } catch (FileNotFoundException e) {
                // retry again to avoid concurrency issue, until FLINK-25453 is fixed
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Failed to list file status, and {}",
                            retry < LIST_MAX_RETRY - 1 ? "will retry again" : " exceeds max retry");
                }
                retry++;
            }
        }
        return statuses;
    }

    /**
     * List versioned files for the directory.
     *
     * @return version stream
     */
    public static Stream<Long> listVersionedFiles(Path dir, String prefix) throws IOException {
        if (!dir.getFileSystem().exists(dir)) {
            return Stream.of();
        }

        FileStatus[] statuses = FileUtils.safelyListFileStatus(dir);

        if (statuses == null) {
            throw new RuntimeException(
                    String.format(
                            "The return value is null of the listStatus for the '%s' directory.",
                            dir));
        }

        return Arrays.stream(statuses)
                .map(FileStatus::getPath)
                .map(Path::getName)
                .filter(name -> name.startsWith(prefix))
                .map(name -> Long.parseLong(name.substring(prefix.length())));
    }
}
