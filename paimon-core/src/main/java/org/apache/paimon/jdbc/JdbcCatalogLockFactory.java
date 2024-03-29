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

package org.apache.paimon.jdbc;

import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;

import java.util.Map;

import static org.apache.paimon.jdbc.JdbcCatalogLock.acquireTimeout;
import static org.apache.paimon.jdbc.JdbcCatalogLock.checkMaxSleep;

/** Jdbc catalog lock factory. */
public class JdbcCatalogLockFactory implements CatalogLockFactory {

    private static final long serialVersionUID = 1L;

    public static final String IDENTIFIER = "jdbc";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public CatalogLock createLock(CatalogLockContext context) {
        JdbcCatalogLockContext lockContext = (JdbcCatalogLockContext) context;
        Map<String, String> optionsMap = lockContext.options().toMap();
        return new JdbcCatalogLock(
                lockContext.connections(),
                lockContext.catalogKey(),
                checkMaxSleep(optionsMap),
                acquireTimeout(optionsMap));
    }
}
