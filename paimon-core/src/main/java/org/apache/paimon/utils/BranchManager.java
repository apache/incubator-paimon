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

package org.apache.paimon.utils;

import org.apache.paimon.Snapshot;
import org.apache.paimon.branch.TableBranch;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.FileUtils.listVersionedFileStatus;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Manager for {@code Branch}. */
public class BranchManager {

    private static final Logger LOG = LoggerFactory.getLogger(BranchManager.class);

    public static final String BRANCH_PREFIX = "branch-";
    public static final String DEFAULT_MAIN_BRANCH = "main";
    public static final String MAIN_BRANCH_FILE = "MAIN-BRANCH";

    private final FileIO fileIO;
    private final Path tablePath;
    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final SchemaManager schemaManager;

    public BranchManager(
            FileIO fileIO,
            Path path,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            SchemaManager schemaManager) {
        this.fileIO = fileIO;
        this.tablePath = path;
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.schemaManager = schemaManager;
    }

    /** Commit specify branch to main. */
    public void commitMainBranch(String branchName) throws IOException {
        Path mainBranchFile = new Path(tablePath, MAIN_BRANCH_FILE);
        fileIO.overwriteFileUtf8(mainBranchFile, branchName);
    }

    /** Return the root Directory of branch. */
    public Path branchDirectory() {
        return new Path(tablePath + "/branch");
    }

    /** Return the path string of a branch. */
    public static String getBranchPath(FileIO fileIO, Path tablePath, String branchName) {
        if (branchName.equals(DEFAULT_MAIN_BRANCH)) {
            Path path = new Path(tablePath, MAIN_BRANCH_FILE);
            try {
                if (fileIO.exists(path)) {
                    String data = fileIO.readFileUtf8(path);
                    if (StringUtils.isBlank(data)) {
                        return tablePath.toString();
                    } else {
                        return tablePath.toString() + "/branch/" + BRANCH_PREFIX + data;
                    }
                } else {
                    return tablePath.toString();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return tablePath.toString() + "/branch/" + BRANCH_PREFIX + branchName;
    }

    /** Return the path of a branch. */
    public Path branchPath(String branchName) {
        return new Path(getBranchPath(fileIO, tablePath, branchName));
    }

    public void createBranch(String branchName, String tagName) {
        checkArgument(
                !branchName.equals(DEFAULT_MAIN_BRANCH),
                String.format(
                        "Branch name '%s' is the default branch and cannot be used.",
                        DEFAULT_MAIN_BRANCH));
        checkArgument(!StringUtils.isBlank(branchName), "Branch name '%s' is blank.", branchName);
        checkArgument(!branchExists(branchName), "Branch name '%s' already exists.", branchName);
        checkArgument(tagManager.tagExists(tagName), "Tag name '%s' not exists.", tagName);
        checkArgument(
                !branchName.chars().allMatch(Character::isDigit),
                "Branch name cannot be pure numeric string but is '%s'.",
                branchName);

        Snapshot snapshot = tagManager.taggedSnapshot(tagName);

        try {
            // Copy the corresponding tag, snapshot and schema files into the branch directory
            fileIO.copyFileUtf8(
                    tagManager.tagPath(tagName), tagManager.tagPath(branchName, tagName));
            fileIO.copyFileUtf8(
                    snapshotManager.snapshotPath(snapshot.id()),
                    snapshotManager.snapshotPath(branchName, snapshot.id()));
            fileIO.copyFileUtf8(
                    schemaManager.toSchemaPath(snapshot.schemaId()),
                    schemaManager.branchSchemaPath(branchName, snapshot.schemaId()));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when create branch '%s' (directory in %s).",
                            branchName, getBranchPath(fileIO, tablePath, branchName)),
                    e);
        }
    }

    public void deleteBranch(String branchName) {
        checkArgument(branchExists(branchName), "Branch name '%s' doesn't exist.", branchName);
        try {
            // Delete branch directory
            fileIO.delete(branchPath(branchName), true);
        } catch (IOException e) {
            LOG.info(
                    String.format(
                            "Deleting the branch failed due to an exception in deleting the directory %s. Please try again.",
                            getBranchPath(fileIO, tablePath, branchName)),
                    e);
        }
    }

    /** Replace specify branch to main branch. */
    public void replaceBranch(String branchName) {
        checkArgument(!StringUtils.isBlank(branchName), "Branch name '%s' is blank.", branchName);
        checkArgument(branchExists(branchName), "Branch name '%s' not exists.", branchName);
        try {
            // 0. Cache previous tag,snapshot,schema directory.
            Path tagDirectory = tagManager.tagDirectory();
            Path snapshotDirectory = snapshotManager.snapshotDirectory();
            Path schemaDirectory = schemaManager.schemaDirectory();
            // 1. Calculate and copy the snapshots, tags and schemas which should be copied from the
            // main branch to target branch.
            calculateCopyMainToBranch(branchName);
            // 2. Update the Main Branch File to the target branch.
            commitMainBranch(branchName);
            // 3.Drop the previous main branch, including snapshots, tags and schemas.
            dropPreviousMainBranch(tagDirectory, snapshotDirectory, schemaDirectory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Calculate copy main branch to target branch. */
    private void calculateCopyMainToBranch(String branchName) throws IOException {
        TableBranch fromBranch =
                this.branches().stream()
                        .filter(branch -> branch.getBranchName().equals(branchName))
                        .findFirst()
                        .orElse(null);
        if (fromBranch == null) {
            throw new RuntimeException(String.format("No branches found %s", branchName));
        }
        Snapshot fromSnapshot = snapshotManager.snapshot(fromBranch.getCreatedFromSnapshot());
        // Copy tags.
        List<String> tags = tagManager.allTagNames();
        for (String tagName : tags) {
            if (tagManager.tagExists(branchName, tagName)) {
                // If it already exists, skip it directly.
                continue;
            }
            Snapshot snapshot = tagManager.taggedSnapshot(tagName);
            if (snapshot.id() < fromSnapshot.id()) {
                fileIO.copyFileUtf8(
                        tagManager.tagPath(tagName), tagManager.tagPath(branchName, tagName));
            }
        }
        // Copy snapshots.
        Iterator<Snapshot> snapshots = snapshotManager.snapshots();
        while (snapshots.hasNext()) {
            Snapshot snapshot = snapshots.next();
            if (snapshotManager.snapshotExists(branchName, snapshot.id())) {
                // If it already exists, skip it directly.
                continue;
            }
            if (snapshot.id() < fromSnapshot.id()) {
                fileIO.copyFileUtf8(
                        snapshotManager.snapshotPath(snapshot.id()),
                        snapshotManager.snapshotPath(branchName, snapshot.id()));
            }
        }

        // Copy schemas.
        List<Long> schemaIds = schemaManager.listAllIds();
        Set<Long> existsSchemas = new HashSet<>(schemaManager.listAllIdsWithBranch(branchName));
        for (Long schemaId : schemaIds) {
            TableSchema tableSchema = schemaManager.schema(schemaId);
            if (existsSchemas.contains(schemaId)) {
                // If it already exists, skip it directly.
                continue;
            }
            if (tableSchema.id() < fromSnapshot.schemaId()) {
                fileIO.copyFileUtf8(
                        schemaManager.toSchemaPath(schemaId),
                        schemaManager.branchSchemaPath(branchName, schemaId));
            }
        }
    }

    /** Directly delete snapshot, tag , schema directory. */
    private void dropPreviousMainBranch(
            Path tagDirectory, Path snapshotDirectory, Path schemaDirectory) throws IOException {
        // Delete tags.
        fileIO.delete(tagDirectory, true);

        // Delete snapshots.
        fileIO.delete(snapshotDirectory, true);

        // Delete schemas.
        fileIO.delete(schemaDirectory, true);
    }

    /** Check if path exists. */
    public boolean fileExists(Path path) {
        try {
            if (fileIO.exists(path)) {
                return true;
            }
            return false;
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Failed to determine if path '%s' exists.", path), e);
        }
    }

    /** Check if a branch exists. */
    public boolean branchExists(String branchName) {
        Path branchPath = branchPath(branchName);
        return fileExists(branchPath);
    }

    /** Get branch count for the table. */
    public long branchCount() {
        try {
            return listVersionedFileStatus(fileIO, branchDirectory(), BRANCH_PREFIX).count();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Get all branches for the table. */
    public List<TableBranch> branches() {
        try {
            List<Pair<Path, Long>> paths =
                    listVersionedFileStatus(fileIO, branchDirectory(), BRANCH_PREFIX)
                            .map(status -> Pair.of(status.getPath(), status.getModificationTime()))
                            .collect(Collectors.toList());
            List<TableBranch> branches = new ArrayList<>();
            for (Pair<Path, Long> path : paths) {
                String branchName = path.getLeft().getName().substring(BRANCH_PREFIX.length());
                FileStoreTable branchTable =
                        FileStoreTableFactory.create(
                                fileIO, new Path(getBranchPath(fileIO, tablePath, branchName)));
                SortedMap<Snapshot, List<String>> snapshotTags = branchTable.tagManager().tags();
                checkArgument(!snapshotTags.isEmpty());
                Snapshot snapshot = snapshotTags.firstKey();
                List<String> tags = snapshotTags.get(snapshot);
                checkArgument(tags.size() == 1);
                branches.add(
                        new TableBranch(branchName, tags.get(0), snapshot.id(), path.getValue()));
            }

            return branches;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
