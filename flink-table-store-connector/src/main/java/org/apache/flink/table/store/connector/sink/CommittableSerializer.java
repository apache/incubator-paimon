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

import java.io.IOException;
import java.nio.ByteBuffer;

/** {@link SimpleVersionedSerializer} for {@link Committable}. */
public class CommittableSerializer implements SimpleVersionedSerializer<Committable> {

    private final FileCommittableSerializer fileCommittableSerializer;

    private final SimpleVersionedSerializer<Object> logCommittableSerializer;

    public CommittableSerializer(
            FileCommittableSerializer fileCommittableSerializer,
            SimpleVersionedSerializer<Object> logCommittableSerializer) {
        this.fileCommittableSerializer = fileCommittableSerializer;
        this.logCommittableSerializer = logCommittableSerializer;
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(Committable committable) throws IOException {
        byte[] wrapped;
        int version;
        switch (committable.kind()) {
            case FILE:
                version = fileCommittableSerializer.getVersion();
                wrapped =
                        fileCommittableSerializer.serialize(
                                (FileCommittable) committable.wrappedCommittable());
                break;
            case LOG:
                version = logCommittableSerializer.getVersion();
                wrapped = logCommittableSerializer.serialize(committable.wrappedCommittable());
                break;
            case LOG_OFFSET:
                version = 1;
                wrapped = ((LogOffsetCommittable) committable.wrappedCommittable()).toBytes();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported kind: " + committable.kind());
        }

        return ByteBuffer.allocate(1 + wrapped.length + 4)
                .put(committable.kind().toByteValue())
                .put(wrapped)
                .putInt(version)
                .array();
    }

    @Override
    public Committable deserialize(int i, byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        Committable.Kind kind = Committable.Kind.fromByteValue(buffer.get());
        byte[] wrapped = new byte[bytes.length - 5];
        buffer.get(wrapped);
        int version = buffer.getInt();

        Object wrappedCommittable;
        switch (kind) {
            case FILE:
                wrappedCommittable = fileCommittableSerializer.deserialize(version, wrapped);
                break;
            case LOG:
                wrappedCommittable = logCommittableSerializer.deserialize(version, wrapped);
                break;
            case LOG_OFFSET:
                wrappedCommittable = LogOffsetCommittable.fromBytes(wrapped);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported kind: " + kind);
        }
        return new Committable(kind, wrappedCommittable);
    }
}
