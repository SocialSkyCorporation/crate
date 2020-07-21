/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.scalar.systeminformation;

import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;


public class PgGetFunctionResultTest extends AbstractScalarFunctionsTest {

    private static final Properties PROPS = new Properties();
    static {
        PROPS.put("user", "crate");
//        PROPS.put("user", "postgres");
        PROPS.put("password", "");
    }

    @Test
    public void test_connection() throws Exception{
        List<FunctionInfo> functions = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5433/", PROPS)) {
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet function = meta.getFunctions(null, null, null);
            while (function.next()) {
                functions.add(new FunctionInfo(function));
            }
        }
        Collections.sort(functions, Comparator.comparing(FunctionInfo::getType));
        for (FunctionInfo info : functions) {
            System.out.println(info);
        }
    }

    public static class FunctionInfo {

        public enum Type {
            SCALAR, TABLE, UNKNOWN_TYPE;

            public static Type of(short type) {
                return switch (type) {
                    case 1 -> SCALAR;
                    case 2 -> TABLE;
                    default -> UNKNOWN_TYPE;
                };
            }
        }

        private Type type;
        private String catalog;
        private String schema;
        private String name;
        private String remarks;
        private String specificName;

        public FunctionInfo(String catalog,
                            String schema,
                            String name,
                            String remarks,
                            short type,
                            String specificName) {
            this.type = Type.of(type);
            this.catalog = catalog;
            this.schema = schema;
            this.name = name;
            this.remarks = remarks;
            this.specificName = specificName;
        }

        public FunctionInfo(ResultSet rs) throws SQLException {
            this(rs.getString(1),
                 rs.getString(2),
                 rs.getString(3),
                 rs.getString(4),
                 rs.getShort(5),
                 rs.getString(6));
        }

        public Type getType() {
            return type;
        }

        public String getCatalog() {
            return catalog;
        }

        public String getSchema() {
            return schema;
        }

        public String getName() {
            return name;
        }

        public String getRemarks() {
            return remarks;
        }

        public String getSpecificName() {
            return specificName;
        }

        @Override
        public String toString() {
            return String.format(
                Locale.ENGLISH,
                "%s %s.%s.%s (%s) '%s'",
                type, catalog, schema, name, specificName, remarks);
        }
    }
}

