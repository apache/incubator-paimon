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

import org.apache.paimon.options.Options;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** This is only for test, not for production. */
public class MemoryKMS extends KmsClientBase {

    private static final String IDENTIFIER = "memory";
    private final SecureRandom secureRandom = new SecureRandom();

    private final Map<String, byte[]> map = new ConcurrentHashMap<>();

    @Override
    public void configure(Options options) {}

    @Override
    public CreateKeyResult createKey() {
        String keyId = UUID.randomUUID().toString().replace("-", "");
        byte[] key = new byte[16];
        secureRandom.nextBytes(key);
        map.put(keyId, key);
        return new CreateKeyResult(keyId, key);
    }

    @Override
    public byte[] getKey(String keyId) {
        byte[] encryptedKey = map.get(keyId);
        if (null == encryptedKey) {
            throw new RuntimeException("The key " + keyId + " is not found");
        }

        return encryptedKey;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public void close() throws IOException {
        map.clear();
    }
}
