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

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.privilege.PrivilegedCatalog;

import org.apache.flink.table.procedure.ProcedureContext;

/**
 * Procedure to initialize privilege system in warehouse. This procedure will automatically create a
 * root user with the provided password. Usage:
 *
 * <pre><code>
 *  CALL sys.init_privilege('rootPassword')
 * </code></pre>
 */
public class InitPrivilegeProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "init_privilege";

    public String[] call(ProcedureContext procedureContext, String rootPassword) {
        ((PrivilegedCatalog) catalog).initializePrivilege(rootPassword);
        return new String[] {
            String.format(
                    "Privilege system is successfully enabled in warehouse %s. "
                            + "Please drop and re-create the catalog.",
                    ((AbstractCatalog) catalog).warehouse())
        };
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
