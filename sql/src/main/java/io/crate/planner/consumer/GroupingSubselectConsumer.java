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

package io.crate.planner.consumer;

import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.planner.Plan;
import io.crate.planner.projection.builder.ProjectionBuilder;


public class GroupingSubselectConsumer implements Consumer {

    private final Visitor visitor;

    GroupingSubselectConsumer(ProjectionBuilder projectionBuilder) {
        visitor = new Visitor(projectionBuilder);
    }

    @Override
    public Plan consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        private final ProjectionBuilder projectionBuilder;

        public Visitor(ProjectionBuilder projectionBuilder) {
            this.projectionBuilder = projectionBuilder;
        }

        @Override
        public Plan visitQueriedSelectRelation(QueriedSelectRelation relation, ConsumerContext context) {
            QuerySpec querySpec = relation.querySpec();

            // Check if group by is present - if not skip and continue with next consumer in chain.
            if (!querySpec.groupBy().isPresent()) {
                return null;
            }

            return null;
        }
    }
}

