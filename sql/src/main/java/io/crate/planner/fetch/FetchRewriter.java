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

package io.crate.planner.fetch;

import com.google.common.collect.ImmutableSet;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public final class FetchRewriter {

    private final static class FetchDescription {
    }

    public static boolean isFetchFeasible(QuerySpec querySpec) {
        Set<Symbol> querySymbols = extractQuerySymbols(querySpec);
        return FetchFeasibility.isFetchFeasible(querySpec.outputs(), querySymbols);
    }

    private static Set<Symbol> extractQuerySymbols(QuerySpec querySpec) {
        Optional<OrderBy> orderBy = querySpec.orderBy();
        return orderBy.isPresent()
            ? ImmutableSet.copyOf(orderBy.get().orderBySymbols())
            : ImmutableSet.of();
    }

    public static FetchDescription rewrite(QueriedDocTable query) {
        QuerySpec querySpec = query.querySpec();
        Set<Symbol> querySymbols = extractQuerySymbols(querySpec);

        List<Symbol> postFetchOutputs = querySpec.outputs();
        assert FetchFeasibility.isFetchFeasible(postFetchOutputs, querySymbols)
            : "Fetch rewrite shouldn't be done if it's not feasible";


        Reference fetchId = DocSysColumns.forTable(query.tableRelation().tableInfo().ident(), DocSysColumns.FETCHID);
        ArrayList<Symbol> preFetchOutputs = new ArrayList<>(1 + querySymbols.size());
        preFetchOutputs.add(fetchId);
        preFetchOutputs.addAll(querySymbols);
        querySpec.outputs();
        return null;
    }
}
