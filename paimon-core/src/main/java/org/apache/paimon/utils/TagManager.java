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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.TagDeletion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.paimon.utils.FileUtils.listVersionedFileStatus;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Manager for {@code Tag}. */
public class TagManager {

    private static final String TAG_PREFIX = "tag-";

    private final FileIO fileIO;
    private final Path tablePath;

    public TagManager(FileIO fileIO, Path tablePath) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
    }

    /** Return the root Directory of tags. */
    public Path tagDirectory() {
        return new Path(tablePath + "/tag");
    }

    /** Return the path of a tag. */
    public Path tagPath(String tagName) {
        return new Path(tablePath + "/tag/" + TAG_PREFIX + tagName);
    }

    /** Create a tag from given snapshot and save it in the storage. */
    public void createTag(Snapshot snapshot, String tagName) {
        checkArgument(!StringUtils.isBlank(tagName), "Tag name '%s' is blank.", tagName);
        checkArgument(!tagExists(tagName), "Tag name '%s' already exists.", tagName);
        checkArgument(
                !tagName.chars().allMatch(Character::isDigit),
                "Tag name cannot be pure numeric string but is '%s'.",
                tagName);

        Path newTagPath = tagPath(tagName);
        try {
            fileIO.writeFileUtf8(newTagPath, snapshot.toJson());
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when committing tag '%s' (path %s). "
                                    + "Cannot clean up because we can't determine the success.",
                            tagName, newTagPath),
                    e);
        }
    }

    public void deleteTag(String tagName, TagDeletion tagDeletion, Snapshot earliestSnapshot) {
        checkArgument(!StringUtils.isBlank(tagName), "Tag name '%s' is blank.", tagName);
        checkArgument(tagExists(tagName), "Tag '%s' doesn't exist.", tagName);

        Snapshot taggedSnapshot = taggedSnapshot(tagName);
        List<Snapshot> taggedSnapshots = taggedSnapshots();

        Map<BinaryRow, Map<Integer, Set<String>>> dataFileSkipped = new HashMap<>();
        Set<String> manifestSkipped = new HashSet<>();

        // collect skipping sets from the earliest snapshot
        tagDeletion.collectSkippedDataFiles(dataFileSkipped, earliestSnapshot);
        manifestSkipped.addAll(tagDeletion.collectManifestSkippingSet(earliestSnapshot));

        // collect from the neighbor tags
        int index = findIndex(taggedSnapshot, taggedSnapshots);
        if (index - 1 >= 0) {
            tagDeletion.collectSkippedDataFiles(dataFileSkipped, taggedSnapshots.get(index - 1));
            manifestSkipped.addAll(
                    tagDeletion.collectManifestSkippingSet(taggedSnapshots.get(index - 1)));
        }
        if (index + 1 < taggedSnapshots.size()) {
            tagDeletion.collectSkippedDataFiles(dataFileSkipped, taggedSnapshots.get(index + 1));
            manifestSkipped.addAll(
                    tagDeletion.collectManifestSkippingSet(taggedSnapshots.get(index + 1)));
        }

        // delete data files and empty directories
        Map<BinaryRow, Set<Integer>> deletionBuckets = new HashMap<>();
        tagDeletion.deleteDataFiles(taggedSnapshot, dataFileSkipped, deletionBuckets);
        tagDeletion.tryDeleteDirectories(deletionBuckets);

        // delete manifests
        tagDeletion.deleteManifestFiles(taggedSnapshot, manifestSkipped);

        fileIO.deleteQuietly(tagPath(tagName));
    }

    /** Check if a tag exists. */
    public boolean tagExists(String tagName) {
        Path path = tagPath(tagName);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to determine if tag '%s' exists in path %s.", tagName, path),
                    e);
        }
    }

    /** Get the tagged snapshot by name. */
    public Snapshot taggedSnapshot(String tagName) {
        checkArgument(tagExists(tagName), "Tag '%s' doesn't exist.", tagName);
        return Snapshot.fromPath(fileIO, tagPath(tagName));
    }

    public long tagCount() {
        try {
            return listVersionedFileStatus(fileIO, tagDirectory(), TAG_PREFIX).count();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Get all tagged snapshots sorted by snapshot id. */
    public List<Snapshot> taggedSnapshots() {
        return new ArrayList<>(tags().keySet());
    }

    /** Get all tagged snapshots with names sorted by snapshot id. */
    public SortedMap<Snapshot, String> tags() {

        TreeMap<Snapshot, String> tags = new TreeMap<>(Comparator.comparingLong(Snapshot::id));
        try {
            listVersionedFileStatus(fileIO, tagDirectory(), TAG_PREFIX)
                    .forEach(
                            status -> {
                                Path path = status.getPath();
                                tags.put(
                                        Snapshot.fromPath(fileIO, path),
                                        path.getName().substring(TAG_PREFIX.length()));
                            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return tags;
    }

    /** Rollback to tag. */
    public void rollbackTo(String tagName, Snapshot latestSnapshot, TagDeletion tagDeletion) {
        SortedMap<Snapshot, String> tags = tags();
        List<Snapshot> taggedSnapshots = new ArrayList<>(tags.keySet());
        Snapshot rollback = taggedSnapshot(tagName);
        int index = taggedSnapshots.indexOf(rollback);
        if (index + 1 < taggedSnapshots.size()) {
            rollbackInternal(tags, index + 1, latestSnapshot, tagDeletion);
        }
    }

    /** Delete tags backwards whose id is larger than given tag's. */
    public void rollbackLargerThan(Snapshot latestSnapshot, TagDeletion tagDeletion) {
        SortedMap<Snapshot, String> tags = tags();
        if (tags.isEmpty()) {
            return;
        }

        List<Snapshot> taggedSnapshots = new ArrayList<>(tags.keySet());
        for (int i = taggedSnapshots.size() - 1; i >= 0; i--) {
            if (taggedSnapshots.get(i).id() <= latestSnapshot.id()) {
                if (i + 1 <= taggedSnapshots.size()) {
                    rollbackInternal(tags, i + 1, latestSnapshot, tagDeletion);
                }
                return;
            }
        }
        rollbackInternal(tags, 0, latestSnapshot, tagDeletion);
    }

    private void rollbackInternal(
            SortedMap<Snapshot, String> tags,
            int toIndex,
            Snapshot latestSnapshot,
            TagDeletion tagDeletion) {
        checkArgument(toIndex >= 0 && toIndex < tags.size());
        List<Snapshot> taggedSnapshots = new ArrayList<>(tags.keySet());

        // delete tag files
        for (int i = tags.size() - 1; i >= toIndex; i--) {
            Snapshot snapshot = taggedSnapshots.get(i);
            fileIO.deleteQuietly(tagPath(tags.get(snapshot)));
        }

        // if old snapshots has been expired, rollback-snapshot cannot delete files used by tag
        // so here have to delete them
        Map<BinaryRow, Map<Integer, Set<String>>> dataFileSkipped = new HashMap<>();
        tagDeletion.collectSkippedDataFiles(dataFileSkipped, latestSnapshot);
        Map<BinaryRow, Set<Integer>> deletionBuckets = new HashMap<>();
        for (int i = taggedSnapshots.size() - 1; i >= toIndex; i--) {
            tagDeletion.deleteDataFiles(taggedSnapshots.get(i), dataFileSkipped, deletionBuckets);
        }

        tagDeletion.tryDeleteDirectories(deletionBuckets);

        Set<String> manifestSkipped = tagDeletion.collectManifestSkippingSet(latestSnapshot);
        for (int i = taggedSnapshots.size() - 1; i >= toIndex; i--) {
            tagDeletion.deleteManifestFiles(taggedSnapshots.get(i), manifestSkipped);
        }
    }

    private FileStatus[] listStatus() {
        Path tagDirectory = tagDirectory();
        try {
            if (!fileIO.exists(tagDirectory)) {
                return new FileStatus[0];
            }

            FileStatus[] statuses = fileIO.listStatus(tagDirectory);

            if (statuses == null) {
                throw new RuntimeException(
                        String.format(
                                "The return value is null of the listStatus for the '%s' directory.",
                                tagDirectory));
            }

            return Arrays.stream(statuses)
                    .filter(status -> status.getPath().getName().startsWith(TAG_PREFIX))
                    .toArray(FileStatus[]::new);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Failed to list status in the '%s' directory.", tagDirectory), e);
        }
    }

    private int findIndex(Snapshot taggedSnapshot, List<Snapshot> taggedSnapshots) {
        for (int i = 0; i < taggedSnapshots.size(); i++) {
            if (taggedSnapshot.id() == taggedSnapshots.get(i).id()) {
                return i;
            }
        }
        throw new RuntimeException(
                String.format(
                        "Didn't find tag with snapshot id '%s'.This is unexpected.",
                        taggedSnapshot.id()));
    }
}
