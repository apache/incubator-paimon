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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.factories.Factory;
import org.apache.paimon.flink.FlinkGenericCatalog;

import org.apache.flink.table.procedures.Procedure;

/** Generic procedure base for FlinkGenericCatalog procedures. */
public abstract class GenericProcedureBase implements Procedure, Factory {

    protected FlinkGenericCatalog flinkGenericCatalog;
    protected String defaultDatabase;

    GenericProcedureBase withFlinkCatalog(FlinkGenericCatalog flinkGenericCatalog) {
        this.flinkGenericCatalog = flinkGenericCatalog;
        return this;
    }

    GenericProcedureBase withDefaultDatabase(String database) {
        this.defaultDatabase = database;
        return this;
    }
}
