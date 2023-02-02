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

package org.apache.flink.table.store.oss;

import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.options.CatalogOptions;
import org.apache.flink.table.store.options.Options;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** OSS {@link FileIO}. */
public class OSSFileIO extends HadoopCompliantFileIO {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(OSSFileIO.class);

    /**
     * In order to simplify, we make flink oss configuration keys same with hadoop oss module. So,
     * we add all configuration key with prefix `fs.oss` in flink conf to hadoop conf.
     */
    private static final String[] CONFIG_PREFIXES = {"fs.oss."};

    private static final String OSS_ACCESS_KEY_ID = "fs.oss.accessKeyId";
    private static final String OSS_ACCESS_KEY_SECRET = "fs.oss.accessKeySecret";
    private static final String OSS_SECURITY_TOKEN = "fs.oss.securityToken";

    private static final Map<String, String> CASE_SENSITIVE_KEYS =
            new HashMap<String, String>() {
                {
                    put(OSS_ACCESS_KEY_ID.toLowerCase(), OSS_ACCESS_KEY_ID);
                    put(OSS_ACCESS_KEY_SECRET.toLowerCase(), OSS_ACCESS_KEY_SECRET);
                    put(OSS_SECURITY_TOKEN.toLowerCase(), OSS_SECURITY_TOKEN);
                }
            };

    /**
     * Cache AliyunOSSFileSystem, at present, there is no good mechanism to ensure that the file
     * system will be shut down, so here the fs cache is used to avoid resource leakage.
     */
    private static final Map<CacheKey, AliyunOSSFileSystem> CACHE = new ConcurrentHashMap<>();

    private Options hadoopOptions;

    @Override
    public boolean isObjectStore() {
        return true;
    }

    @Override
    public void configure(CatalogOptions config) {
        hadoopOptions = new Options();
        // read all configuration with prefix 'CONFIG_PREFIXES'
        for (String key : config.keySet()) {
            for (String prefix : CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String value = config.get(key);
                    if (CASE_SENSITIVE_KEYS.containsKey(key.toLowerCase())) {
                        key = CASE_SENSITIVE_KEYS.get(key.toLowerCase());
                    }
                    hadoopOptions.set(key, value);

                    LOG.debug(
                            "Adding config entry for {} as {} to Hadoop config",
                            key,
                            hadoopOptions.get(key));
                }
            }
        }
    }

    @Override
    protected FileSystem createFileSystem(org.apache.hadoop.fs.Path path) {
        final String scheme = path.toUri().getScheme();
        final String authority = path.toUri().getAuthority();
        return CACHE.computeIfAbsent(
                new CacheKey(hadoopOptions, scheme, authority),
                key -> {
                    Configuration hadoopConf = new Configuration();
                    key.options.toMap().forEach(hadoopConf::set);
                    URI fsUri = path.toUri();
                    if (scheme == null && authority == null) {
                        fsUri = FileSystem.getDefaultUri(hadoopConf);
                    } else if (scheme != null && authority == null) {
                        URI defaultUri = FileSystem.getDefaultUri(hadoopConf);
                        if (scheme.equals(defaultUri.getScheme())
                                && defaultUri.getAuthority() != null) {
                            fsUri = defaultUri;
                        }
                    }

                    AliyunOSSFileSystem fs = new AliyunOSSFileSystem();
                    try {
                        fs.initialize(fsUri, hadoopConf);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return fs;
                });
    }

    private static class CacheKey {

        private final Options options;
        private final String scheme;
        private final String authority;

        private CacheKey(Options options, String scheme, String authority) {
            this.options = options;
            this.scheme = scheme;
            this.authority = authority;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(options, cacheKey.options)
                    && Objects.equals(scheme, cacheKey.scheme)
                    && Objects.equals(authority, cacheKey.authority);
        }

        @Override
        public int hashCode() {
            return Objects.hash(options, scheme, authority);
        }
    }
}
