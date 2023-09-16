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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.UUID;

/** Hadoop KMS client. */
public class HadoopKms extends KmsClientBase {

    public static final String IDENTIFIER = "hadoop";

    private static final String CONFIG_PREFIX = "hadoop.security.";

    private Options options;

    @Override
    public void configure(Options options) {
        this.options = options;
    }

    private Configuration getHadoopConf() {
        Map<String, String> confMap = options.toMap();
        Configuration hadoopConf = new Configuration();
        for (Map.Entry<String, String> conf : confMap.entrySet()) {
            if (conf.getKey().startsWith(CONFIG_PREFIX)) {
                hadoopConf.set(conf.getKey(), conf.getValue());
            }
        }
        return hadoopConf;
    }

    private KeyProvider createProvider(Configuration configuration) {
        Map<String, String> confMap = this.options.toMap();
        if (!confMap.containsKey(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH)) {
            throw new IllegalArgumentException(
                    CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH
                            + " is required for hadoop kms.");
        }

        final KeyProvider ret;
        try {
            URI uri =
                    new URI(
                            confMap.get(
                                    CommonConfigurationKeysPublic
                                            .HADOOP_SECURITY_KEY_PROVIDER_PATH));

            ret = new KMSClientProvider(uri, configuration);
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException("Create hadoop kms client error, ", e);
        }
        return ret;
    }

    @Override
    public CreateKeyResult createKey() {
        // HadoopKms must be serializable, but KeyProvider is not serializable, so KeyProvider
        // cannot be used as a class variable
        KeyProvider keyProvider = null;
        try {
            keyProvider = createProvider(getHadoopConf());
            String keyId = UUID.randomUUID().toString().replace("-", "");
            KeyProvider.KeyVersion keyVersion =
                    keyProvider.createKey(keyId, new KeyProvider.Options(getHadoopConf()));
            return new CreateKeyResult(keyId, keyVersion.getMaterial());
        } catch (NoSuchAlgorithmException | IOException e) {
            throw new RuntimeException("Create key error, ", e);
        } finally {
            try {
                if (keyProvider != null) {
                    keyProvider.close();
                }
            } catch (IOException e) {
                throw new RuntimeException("Close hadoop keyProvider error, ", e);
            }
        }
    }

    @Override
    public byte[] getKey(String keyId) {
        KeyProvider keyProvider = null;
        try {
            keyProvider = createProvider(getHadoopConf());
            return createProvider(getHadoopConf()).getCurrentKey(keyId).getMaterial();
        } catch (IOException e) {
            throw new RuntimeException("Get key error: ", e);
        } finally {
            try {
                if (keyProvider != null) {
                    keyProvider.close();
                }
            } catch (IOException e) {
                throw new RuntimeException("Close hadoop keyProvider error, ", e);
            }
        }
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public void close() throws IOException {}
}
