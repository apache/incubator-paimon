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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMetaSerializer;
import org.apache.flink.table.types.logical.RowType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** {@link SimpleVersionedSerializer} for {@link FileCommittable}. */
public class FileCommittableSerializer implements SimpleVersionedSerializer<FileCommittable> {

    private final BinaryRowDataSerializer partSerializer;
    private final SstFileMetaSerializer sstSerializer;

    public FileCommittableSerializer(RowType partitionType, RowType keyType, RowType valueType) {
        this.partSerializer = new BinaryRowDataSerializer(partitionType.getFieldCount());
        this.sstSerializer = new SstFileMetaSerializer(keyType, valueType);
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(FileCommittable obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        partSerializer.serialize(obj.partition(), view);
        view.writeInt(obj.bucket());
        sstSerializer.serializeList(obj.increment().newFiles(), view);
        sstSerializer.serializeList(obj.increment().compactBefore(), view);
        sstSerializer.serializeList(obj.increment().compactAfter(), view);
        return out.toByteArray();
    }

    @Override
    public FileCommittable deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer view = new DataInputDeserializer(serialized);
        return new FileCommittable(
                partSerializer.deserialize(view),
                view.readInt(),
                new Increment(
                        sstSerializer.deserializeList(view),
                        sstSerializer.deserializeList(view),
                        sstSerializer.deserializeList(view)));
    }
}
