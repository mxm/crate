/*
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate.io licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * To enable or use any of the enterprise features, Crate.io must have given
 * you permission to enable and use the Enterprise Edition of CrateDB and you
 * must have a valid Enterprise or Subscription Agreement with Crate.io.  If
 * you enable or use features that are part of the Enterprise Edition, you
 * represent and warrant that you have a valid Enterprise or Subscription
 * Agreement with Crate.io.  Your use of features of the Enterprise Edition
 * is governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.integrationtests;

import com.google.common.collect.ImmutableList;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.Schemas;
import io.crate.operation.udf.UDFLanguage;
import io.crate.operation.udf.UserDefinedFunctionMetaData;
import io.crate.operation.udf.UserDefinedFunctionService;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Test;

import javax.script.ScriptException;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, randomDynamicTemplates = false)
public class UserDefinedFunctionsIntegrationTest extends SQLTransportIntegrationTest {

    private class DummyFunction<InputType> extends Scalar<BytesRef, InputType>  {

        private final FunctionInfo info;
        private final UserDefinedFunctionMetaData metaData;

        private DummyFunction(UserDefinedFunctionMetaData metaData) {
            this.info = new FunctionInfo(new FunctionIdent(metaData.schema(), metaData.name(), metaData.argumentTypes()), DataTypes.STRING);
            this.metaData = metaData;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        public BytesRef evaluate(Input<InputType>... args) {
            // dummy-lang functions simple print the type of the only argument
            return BytesRefs.toBytesRef("DUMMY EATS " + metaData.argumentTypes().get(0).getName());
        }
    }

    private class DummyLang implements UDFLanguage {

        @Override
        public Scalar createFunctionImplementation(UserDefinedFunctionMetaData metaData) throws ScriptException {
            return new DummyFunction<>(metaData);
        }

        @Override
        public String validate(UserDefinedFunctionMetaData metadata) {
            // dummy language does not validate anything
            return null;
        }

        @Override
        public String name() {
            return "dummy_lang";
        }
    }

    private final DummyLang dummyLang = new DummyLang();

    @Before
    public void beforeTest() {
        // clustering by id into two shards must assure that the two inserted
        // records reside on two different nodes configured in the test setup.
        // So then it would be possible to test that a function is created and
        // applied on all of nodes.
        Iterable<UserDefinedFunctionService> udfServices = internalCluster().getInstances(UserDefinedFunctionService.class);
        for (UserDefinedFunctionService udfService : udfServices) {
            udfService.registerLanguage(dummyLang);
        }
    }

    @Test
    public void testCreateOverloadedFunction() throws Exception {
        execute("create table test (id long, str string) clustered by(id) into 2 shards");
        Object[][] rows = new Object[100][];
        for (int i = 0; i < 100; i++) {
            rows[i] = new Object[]{Long.valueOf(i), String.valueOf(i)};
        }
        execute("insert into test (id, str) values (?, ?)", rows);
        refresh();
        try {
            execute("create function foo(long)" +
                " returns string language dummy_lang as 'function foo(x) { return \"1\"; }'");
            assertFunctionIsCreatedOnAll(Schemas.DEFAULT_SCHEMA_NAME, "foo", ImmutableList.of(DataTypes.LONG));

            execute("create function foo(string)" +
                " returns string language dummy_lang as 'function foo(x) { return x; }'");
            assertFunctionIsCreatedOnAll(Schemas.DEFAULT_SCHEMA_NAME, "foo", ImmutableList.of(DataTypes.STRING));

            execute("select foo(str) from test order by id asc");
            assertThat(response.rows()[0][0], is("DUMMY EATS string"));

            execute("select foo(id) from test order by id asc");
            assertThat(response.rows()[0][0], is("DUMMY EATS long"));

        } catch (Exception e){
            dropFunction("foo", ImmutableList.of(DataTypes.LONG));
            dropFunction("foo", ImmutableList.of(DataTypes.STRING));
            throw e;
        }
    }

    @Test
    public void testDropFunction() throws Exception {
        execute("create function custom(string) returns string language dummy_lang as 'DUMMY DUMMY DUMMY'");
        assertFunctionIsCreatedOnAll(Schemas.DEFAULT_SCHEMA_NAME, "custom", ImmutableList.of(DataTypes.STRING));

        dropFunction("custom", ImmutableList.of(DataTypes.STRING));
        assertFunctionIsDeletedOnAll(Schemas.DEFAULT_SCHEMA_NAME, "custom", ImmutableList.of(DataTypes.STRING));
    }

    @Test
    public void testNewSchemaWithFunction() throws Exception {
        execute("create function new_schema.custom() returns integer language dummy_lang as 'function custom() {return 1;}'");
        assertFunctionIsCreatedOnAll("new_schema", "custom", ImmutableList.of());
        execute("select count(*) from information_schema.schemata where schema_name='new_schema'");
        assertThat(response.rows()[0][0], is(1L));

        execute("drop function new_schema.custom()");
        assertFunctionIsDeletedOnAll("new_schema", "custom", ImmutableList.of());
        execute("select count(*) from information_schema.schemata where schema_name='new_schema'");
        assertThat(response.rows()[0][0], is(0L));
    }

    @Test
    public void testSelectFunctionsFromRoutines() throws Exception {
        try {
            execute("create function subtract_test(long, long, long) " +
                    "returns long language dummy_lang " +
                    "as 'function subtract_test(a, b, c) { return a - b - c; }'");
            assertFunctionIsCreatedOnAll(Schemas.DEFAULT_SCHEMA_NAME,
                "subtract_test",
                ImmutableList.of(DataTypes.LONG, DataTypes.LONG, DataTypes.LONG)
            );

            execute("select routine_name, routine_body, data_type, routine_definition, routine_schema, specific_name" +
                    " from information_schema.routines " +
                    " where routine_type = 'FUNCTION' and routine_name = 'subtract_test'");
            assertThat(response.rowCount(), is(1L));
            assertThat(response.rows()[0][0], is("subtract_test"));
            assertThat(response.rows()[0][1], is("dummy_lang"));
            assertThat(response.rows()[0][2], is("long"));
            assertThat(response.rows()[0][3], is("function subtract_test(a, b, c) { return a - b - c; }"));
            assertThat(response.rows()[0][4], is("doc"));
            assertThat(response.rows()[0][5], is("subtract_test(long, long, long)"));
        } catch (Exception e){
            execute("drop function if exists subtract_test(long, long, long)");
            throw e;
        }
    }


    private void dropFunction(String name, List<DataType> types) throws Exception {
        execute(String.format(Locale.ENGLISH, "drop function %s(%s)",
            name, types.stream().map(DataType::getName).collect(Collectors.joining(", "))));
        assertThat(response.rowCount(), is(1L));
        assertFunctionIsDeletedOnAll(Schemas.DEFAULT_SCHEMA_NAME, name, types);
    }
}
