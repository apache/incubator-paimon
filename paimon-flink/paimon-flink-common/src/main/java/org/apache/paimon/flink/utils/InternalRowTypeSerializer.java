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

package org.apache.paimon.flink.utils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.types.DataType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/** A TypeSerializer for {@link InternalRow}. */
public class InternalRowTypeSerializer extends TypeSerializer<InternalRow> {

    private static final long serialVersionUID = 1L;

    private final InternalRowSerializer internalRowSerializer;

    public InternalRowTypeSerializer(DataType... types) {
        internalRowSerializer = new InternalRowSerializer(types);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<InternalRow> duplicate() {
        return new InternalRowTypeSerializer(internalRowSerializer.fieldTypes());
    }

    @Override
    public InternalRow createInstance() {
        return new BinaryRow(internalRowSerializer.getArity());
    }

    @Override
    public InternalRow copy(InternalRow from) {
        return internalRowSerializer.copy(from);
    }

    @Override
    public InternalRow copy(InternalRow from, InternalRow reuse) {
        return internalRowSerializer.copyRowData(from, reuse);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(InternalRow record, DataOutputView target) throws IOException {
        BinaryRow row = internalRowSerializer.toBinaryRow(record);
        target.writeInt(row.getSizeInBytes());
        target.write(row.toBytes());
    }

    @Override
    public InternalRow deserialize(DataInputView source) throws IOException {
        return deserialize(createInstance(), source);
    }

    @Override
    public InternalRow deserialize(InternalRow reuse, DataInputView source) throws IOException {
        BinaryRow reuseRow = (BinaryRow) reuse;
        int len = source.readInt();
        byte[] bytes = new byte[len];
        source.readFully(bytes);
        reuseRow.pointTo(MemorySegment.wrap(bytes), 0, len);
        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int length = source.readInt();
        target.writeInt(length);
        target.write(source, length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InternalRowTypeSerializer that = (InternalRowTypeSerializer) o;
        return Objects.equals(internalRowSerializer, that.internalRowSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(internalRowSerializer);
    }

    @Override
    public TypeSerializerSnapshot<InternalRow> snapshotConfiguration() {
        return new InternalRowTypeSerializerSnapshot(internalRowSerializer.fieldTypes());
    }

    /**
     * {@link TypeSerializerSnapshot} for {@link InternalRow}. It checks the compatibility by
     * checking type.
     */
    public static class InternalRowTypeSerializerSnapshot
            implements TypeSerializerSnapshot<InternalRow> {

        private DataType[] dataTypes;
        private KryoSerializer<DataType[]> serializer =
                new KryoSerializer<>(DataType[].class, new ExecutionConfig());

        public InternalRowTypeSerializerSnapshot() {}

        private InternalRowTypeSerializerSnapshot(DataType... dataTypes) {
            this.dataTypes = dataTypes;
        }

        @Override
        public int getCurrentVersion() {
            return 0;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            serializer.serialize(dataTypes, out);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            this.dataTypes = serializer.deserialize(in);
        }

        @Override
        public TypeSerializer<InternalRow> restoreSerializer() {
            return new InternalRowTypeSerializer(dataTypes);
        }

        @Override
        public TypeSerializerSchemaCompatibility<InternalRow> resolveSchemaCompatibility(
                TypeSerializer<InternalRow> newSerializer) {
            if (!(newSerializer instanceof InternalRowTypeSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            InternalRowTypeSerializer other = (InternalRowTypeSerializer) newSerializer;

            int numFields = dataTypes.length;
            if (numFields == other.internalRowSerializer.getArity()) {
                for (int i = 0; i < numFields; i++) {
                    if (!dataTypes[i].equals(other.internalRowSerializer.fieldTypes()[i])) {
                        return TypeSerializerSchemaCompatibility.incompatible();
                    }
                }
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            } else {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
        }
    }
}
