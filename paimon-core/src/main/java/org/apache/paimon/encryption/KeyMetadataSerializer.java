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

package org.apache.paimon.encryption;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.ObjectSerializer;

/** Serializer for {@link KeyMetadata}. */
public class KeyMetadataSerializer extends ObjectSerializer<KeyMetadata> {

    public KeyMetadataSerializer() {
        super(KeyMetadata.schema());
    }

    @Override
    public InternalRow toRow(KeyMetadata record) {
        return GenericRow.of(
                record.kekID(), record.wrappedKEK(), record.wrappedDEK(), record.aadPrefix());
    }

    @Override
    public KeyMetadata fromRow(InternalRow rowData) {
        return new KeyMetadata(
                rowData.getBinary(0),
                rowData.getBinary(1),
                rowData.getBinary(2),
                rowData.getBinary(3));
    }
}
