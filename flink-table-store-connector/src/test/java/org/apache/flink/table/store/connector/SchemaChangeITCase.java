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

package org.apache.flink.table.store.connector;

import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for schema changes. */
public class SchemaChangeITCase extends CatalogITCaseBase {

    // TODO cover more cases once Flink supports more ALTER operations.

    @Test
    public void testSetAndRemoveOption() throws Exception {
        sql("CREATE TABLE T (a STRING, b STRING, c STRING)");
        sql("ALTER TABLE T SET ('xyc'='unknown1', 'abc'='unknown2')");

        Map<String, String> options = table("T").getOptions();
        assertThat(options).containsEntry("xyc", "unknown1");
        assertThat(options).containsEntry("abc", "unknown2");

        sql("ALTER TABLE T RESET ('xyc', 'abc')");

        options = table("T").getOptions();
        assertThat(options).doesNotContainKey("xyc");
        assertThat(options).doesNotContainKey("abc");
    }
}
