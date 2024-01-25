/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.privilege;

import org.apache.paimon.catalog.Identifier;

/** Interface for catalog which supports privilege system. */
public interface PrivilegedCatalog {

    void initializePrivilege(String rootPassword);

    void createPrivilegedUser(String user, String password);

    void dropPrivilegedUser(String user);

    void grantPrivilegeOnCatalog(String user, PrivilegeType privilege);

    void grantPrivilegeOnDatabase(String user, String databaseName, PrivilegeType privilege);

    void grantPrivilegeOnTable(String user, Identifier identifier, PrivilegeType privilege);

    int revokePrivilegeOnCatalog(String user, PrivilegeType privilege);

    int revokePrivilegeOnDatabase(String user, String databaseName, PrivilegeType privilege);

    int revokePrivilegeOnTable(String user, Identifier identifier, PrivilegeType privilege);
}
