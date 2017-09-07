/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.executor.transport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.crate.Constants;
import io.crate.analyze.ConstraintsValidator;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.ArrayRow;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.jobs.JobContextService;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.operation.InputFactory;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.reference.ReferenceResolver;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.DocumentSourceMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TTLFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.crate.exceptions.Exceptions.userFriendlyMessage;

/**
 * Realizes Upserts of tables which either results in an Insert or an Update.
 */
@Singleton
public class TransportShardUpsertAction extends TransportShardAction<ShardUpsertRequest, ShardUpsertRequest.Item> {

    private static final String ACTION_NAME = "indices:crate/data/write/upsert";

    private final SchemaUpdateClient schemaUpdateClient;
    private final Schemas schemas;
    private final InputFactory inputFactory;

    @Inject
    public TransportShardUpsertAction(Settings settings,
                                      ThreadPool threadPool,
                                      ClusterService clusterService,
                                      TransportService transportService,
                                      SchemaUpdateClient schemaUpdateClient,
                                      ActionFilters actionFilters,
                                      JobContextService jobContextService,
                                      IndicesService indicesService,
                                      ShardStateAction shardStateAction,
                                      Functions functions,
                                      Schemas schemas,
                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, indexNameExpressionResolver, clusterService,
            indicesService, threadPool, shardStateAction, actionFilters, ShardUpsertRequest::new);
        this.schemaUpdateClient = schemaUpdateClient;
        this.schemas = schemas;
        this.inputFactory = new InputFactory(functions);
        jobContextService.addListener(this);
    }

    @Override
    protected WritePrimaryResult<ShardUpsertRequest, ShardResponse> processRequestItems(IndexShard indexShard,
                                                                                        ShardUpsertRequest request,
                                                                                        AtomicBoolean killed) throws InterruptedException {
        ShardResponse shardResponse = new ShardResponse();
        DocTableInfo tableInfo = schemas.getTableInfo(TableIdent.fromIndexName(request.index()), Operation.INSERT);

        Collection<ColumnIdent> notUsedNonGeneratedColumns = ImmutableList.of();
        if (request.validateConstraints()) {
            notUsedNonGeneratedColumns = getNotUsedNonGeneratedColumns(request.insertColumns(), tableInfo);
        }

        Translog.Location translogLocation = null;
        for (ShardUpsertRequest.Item item : request.items) {
            int location = item.location();
            if (killed.get()) {
                // set failure on response and skip all next items.
                // this way replica operation will be executed, but only items with a valid source (= was processed on primary)
                // will be processed on the replica
                shardResponse.failure(new InterruptedException());
                break;
            }
            try {
                translogLocation = indexItem(
                    tableInfo,
                    request,
                    item,
                    indexShard,
                    item.insertValues() != null, // try insert first
                    notUsedNonGeneratedColumns,
                    0);
                shardResponse.add(location);
            } catch (Exception e) {
                if (retryPrimaryException(e)) {
                    Throwables.propagate(e);
                }
                logger.debug("{} failed to execute upsert for [{}]/[{}]",
                    e, request.shardId(), request.type(), item.id());

                // *mark* the item as failed by setting the source to null
                // to prevent the replica operation from processing this concrete item
                item.source(null);

                if (!request.continueOnError()) {
                    shardResponse.failure(e);
                    break;
                }
                shardResponse.add(location,
                    new ShardResponse.Failure(
                        item.id(),
                        userFriendlyMessage(e),
                        (e instanceof VersionConflictEngineException)));
            }
        }
        return new WritePrimaryResult<>(request, shardResponse, translogLocation, null, indexShard, logger);
    }

    @Override
    protected WriteReplicaResult<ShardUpsertRequest> processRequestItemsOnReplica(IndexShard indexShard, ShardUpsertRequest request) throws IOException {
        Translog.Location location = null;
        for (ShardUpsertRequest.Item item : request.items()) {
            if (item.source() == null) {
                if (logger.isTraceEnabled()) {
                    logger.trace("[{} (R)] Document with id {}, has no source, primary operation must have failed",
                        indexShard.shardId(), item.id());
                }
                continue;
            }
            Engine.IndexResult indexResult = shardIndexOperationOnReplica(request, item, indexShard);
            location = indexResult.getTranslogLocation();
        }
        return new WriteReplicaResult<>(request, location, null, indexShard, logger);
    }

    protected Translog.Location indexItem(DocTableInfo tableInfo,
                                          ShardUpsertRequest request,
                                          ShardUpsertRequest.Item item,
                                          IndexShard indexShard,
                                          boolean tryInsertFirst,
                                          Collection<ColumnIdent> notUsedNonGeneratedColumns,
                                          int retryCount) throws Exception {
        long version;
        // try insert first without fetching the document
        if (tryInsertFirst) {
            // set version so it will fail if already exists (will be overwritten for updates, see below)
            version = Versions.MATCH_DELETED;
            try {
                item.source(prepareInsert(tableInfo, notUsedNonGeneratedColumns, request, item));
            } catch (IOException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
            if (!request.overwriteDuplicates()) {
                item.opType(IndexRequest.OpType.CREATE);
            } else {
                version = Versions.MATCH_ANY;
                item.opType(IndexRequest.OpType.INDEX);
            }
        } else {
            item.opType(IndexRequest.OpType.INDEX);
            SourceAndVersion sourceAndVersion = prepareUpdate(tableInfo, request, item, indexShard);
            item.source(sourceAndVersion.source);
            version = sourceAndVersion.version;
        }

        Engine.Index operation = prepareIndexOnPrimary(indexShard, version, request, item);
        operation = updateMappingIfRequired(request, item, version, indexShard, operation);
        Engine.IndexResult indexResult = indexShard.index(operation);

        Exception failure = indexResult.getFailure();
        if (failure != null) {
            if (failure instanceof VersionConflictEngineException) {
                if (item.updateAssignments() != null) {
                    if (tryInsertFirst) {
                        // insert failed, document already exists, try update
                        return indexItem(tableInfo, request, item, indexShard, false, notUsedNonGeneratedColumns, 0);
                    } else if (item.retryOnConflict()) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("[{}] VersionConflict, retrying operation for document id {}, retry count: {}",
                                indexShard.shardId(), item.id(), retryCount);
                        }
                        return indexItem(tableInfo, request, item, indexShard, false, notUsedNonGeneratedColumns,
                            retryCount + 1);
                    }
                }
            }
            throw failure;
        }

        // update the version on request for the replicas
        item.versionType(item.versionType().versionTypeForReplicationAndRecovery());
        item.version(indexResult.getVersion());

        assert item.versionType().validateVersionForWrites(item.version()) : "item.version() must be valid";

        return indexResult.getTranslogLocation();
    }

    private GetResult getDocument(IndexShard indexShard, ShardUpsertRequest request, ShardUpsertRequest.Item item) {
        GetResult getResult = indexShard.getService().get(
            request.type(),
            item.id(),
            new String[]{RoutingFieldMapper.NAME, ParentFieldMapper.NAME, TTLFieldMapper.NAME},
            true,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            FetchSourceContext.FETCH_SOURCE
        );

        if (!getResult.isExists()) {
            throw new DocumentMissingException(request.shardId(), request.type(), item.id());
        }

        if (getResult.internalSourceRef() == null) {
            // no source, we can't do nothing, through a failure...
            throw new DocumentSourceMissingException(request.shardId(), request.type(), item.id());
        }

        if (item.version() != Versions.MATCH_ANY && item.version() != getResult.getVersion()) {
            throw new VersionConflictEngineException(
                indexShard.shardId(), Constants.DEFAULT_MAPPING_TYPE, item.id(), "TODO: add explanation");
        }

        return getResult;
    }

    private List<Input<?>> resolveSymbols(ReferenceResolver<CollectExpression<GetResult, ?>> referenceResolver,
                                          GetResult getResult,
                                          Iterable<? extends Symbol> updateAssignments,
                                          @Nullable Object[] insertValues) {

        InputFactory.ContextInputAware<CollectExpression<GetResult, ?>> universalContext =
            inputFactory.ctxForRefsWithInputCols(referenceResolver);

        List<Input<?>> updateInputs = new ArrayList<>();
        for (Symbol symbol : updateAssignments) {
            if (symbol instanceof GeneratedReference) {
                symbol = ((GeneratedReference) symbol).generatedExpression();
            }
            updateInputs.add(universalContext.add(symbol));
        }

        List<CollectExpression<GetResult, ?>> expressions = universalContext.expressions();
        for (CollectExpression<GetResult, ?> expression : expressions) {
            expression.setNextRow(getResult);
        }

        List<CollectExpression<Row, ?>> inputColExpressions = universalContext.inputColExpressions();
        ArrayRow arrayRow = new ArrayRow();
        arrayRow.cells(insertValues);
        for (CollectExpression<Row, ?> rowCollectExpression : inputColExpressions) {
            rowCollectExpression.setNextRow(arrayRow);
        }

        return updateInputs;
    }

    /**
     * Prepares an update request by converting it into an index request.
     * <p/>
     * TODO: detect a NOOP and return an update response if true
     */
    private SourceAndVersion prepareUpdate(DocTableInfo tableInfo,
                                           ShardUpsertRequest request,
                                           ShardUpsertRequest.Item item,
                                           IndexShard indexShard) throws ElasticsearchException {

        GetResult getResult = getDocument(indexShard, request, item);

        List<Input<?>> updateInputs = resolveSymbols(GetResultRefResolver.INSTANCE,
            getResult, Arrays.asList(item.updateAssignments()), item.insertValues());

        Map<String, Object> pathsToUpdate = new LinkedHashMap<>();
        Map<String, Object> updatedGeneratedColumns = new LinkedHashMap<>();
        for (int i = 0; i < request.updateColumns().length; i++) {
            /*
             * NOTE: mapping isn't applied. So if an Insert was done using the ES Rest Endpoint
             * the data might be returned in the wrong format (date as string instead of long)
             */
            String columnPath = request.updateColumns()[i];
            Object value = updateInputs.get(i).value();

            Reference reference = tableInfo.getReference(ColumnIdent.fromPath(columnPath));

            if (reference != null) {
                /*
                 * it is possible to insert NULL into column that does not exist yet.
                 * if there is no column reference, we must not validate!
                 */
                ConstraintsValidator.validate(value, reference, tableInfo.notNullColumns());
            }

            if (reference instanceof GeneratedReference) {
                updatedGeneratedColumns.put(columnPath, value);
            } else {
                pathsToUpdate.put(columnPath, value);
            }
        }

        // For updates we always have to enforce the validation of constraints on shards.
        // Currently the validation is done only for generated columns.
        processGeneratedColumns(tableInfo, pathsToUpdate, updatedGeneratedColumns, true, getResult);

        Tuple<XContentType, Map<String, Object>> sourceAndContent =
            XContentHelper.convertToMap(getResult.internalSourceRef(), true);
        final XContentType updateSourceContentType = sourceAndContent.v1();
        final Map<String, Object> updatedSourceAsMap = sourceAndContent.v2();

        updateSourceByPaths(updatedSourceAsMap, pathsToUpdate);

        try {
            XContentBuilder builder = XContentFactory.contentBuilder(updateSourceContentType);
            builder.map(updatedSourceAsMap);
            return new SourceAndVersion(builder.bytes(), getResult.getVersion());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + updatedSourceAsMap + "]", e);
        }
    }

    private BytesReference prepareInsert(DocTableInfo tableInfo,
                                         Collection<ColumnIdent> notUsedNonGeneratedColumns,
                                         ShardUpsertRequest request,
                                         ShardUpsertRequest.Item item) throws IOException {

        List<GeneratedReference> generatedReferencesWithValue = new ArrayList<>();
        BytesReference source;
        if (request.isRawSourceInsert()) {
            assert item.insertValues().length > 0 : "empty insert values array";
            source = new BytesArray((BytesRef) item.insertValues()[0]);
        } else {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

            // For direct inserts it is enough to have constraints validation on a handler.
            // validateConstraints() of ShardUpsertRequest should result in false in this case.
            if (request.validateConstraints()) {
                ConstraintsValidator.validateConstraintsForNotUsedColumns(notUsedNonGeneratedColumns, tableInfo);
            }

            for (int i = 0; i < item.insertValues().length; i++) {
                Object value = item.insertValues()[i];
                Reference ref = request.insertColumns()[i];

                ConstraintsValidator.validate(value, ref, tableInfo.notNullColumns());

                if (ref.granularity() == RowGranularity.DOC) {
                    // don't include values for partitions in the _source
                    // ideally columns with partition granularity shouldn't be part of the request
                    builder.field(ref.ident().columnIdent().fqn(), value);
                    if (ref instanceof GeneratedReference) {
                        generatedReferencesWithValue.add((GeneratedReference) ref);
                    }
                }
            }
            builder.endObject();
            source = builder.bytes();
        }

        int generatedColumnSize = 0;
        for (GeneratedReference reference : tableInfo.generatedColumns()) {
            if (!tableInfo.partitionedByColumns().contains(reference)) {
                generatedColumnSize++;
            }
        }

        int numMissingGeneratedColumns = generatedColumnSize - generatedReferencesWithValue.size();
        if (numMissingGeneratedColumns > 0 ||
            (generatedReferencesWithValue.size() > 0 && request.validateConstraints())) {
            // we need to evaluate some generated column expressions
            Map<String, Object> sourceMap = processGeneratedColumnsOnInsert(tableInfo, request.insertColumns(),
                item.insertValues(), request.isRawSourceInsert(), request.validateConstraints());
            source = XContentFactory.jsonBuilder().map(sourceMap).bytes();
        }

        return source;
    }

    private Engine.Index prepareIndexOnPrimary(IndexShard indexShard,
                                               long version,
                                               ShardUpsertRequest request,
                                               ShardUpsertRequest.Item item) {
        SourceToParse sourceToParse = SourceToParse.source(
            SourceToParse.Origin.PRIMARY,
            indexShard.shardId().getIndexName(),
            request.type(),
            item.id(),
            item.source(),
            XContentType.JSON
        );

        if (logger.isTraceEnabled()) {
            logger.trace("[{}] shard operation with opType={} id={} version={}  source={}",
                indexShard.shardId(), item.opType(), item.id(), version, item.source().utf8ToString());
        }
        return indexShard.prepareIndexOnPrimary(
            sourceToParse, version, item.versionType(), -1, request.isRetry());
    }

    private Engine.Index updateMappingIfRequired(ShardUpsertRequest request,
                                                 ShardUpsertRequest.Item item,
                                                 long version,
                                                 IndexShard indexShard,
                                                 Engine.Index operation) throws Exception {
        Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
        if (update != null) {
            validateMapping(update.root().iterator(), false);

            schemaUpdateClient.blockingUpdateOnMaster(request.shardId().getIndex(), update);

            operation = prepareIndexOnPrimary(indexShard, version, request, item);
            if (operation.parsedDoc().dynamicMappingsUpdate() != null) {
                throw new ReplicationOperation.RetryOnPrimaryException(request.shardId(),
                    "Dynamics mappings are not available on the node that holds the primary yet");
            }
        }
        return operation;
    }

    @VisibleForTesting
    static void validateMapping(Iterator<Mapper> mappers, boolean nested) {
        while (mappers.hasNext()) {
            Mapper mapper = mappers.next();
            if (nested) {
                ColumnIdent.validateObjectKey(mapper.simpleName());
            } else {
                ColumnIdent.validateColumnName(mapper.simpleName());
            }
            validateMapping(mapper.iterator(), true);
        }
    }

    private Engine.IndexResult shardIndexOperationOnReplica(ShardUpsertRequest request,
                                                            ShardUpsertRequest.Item item,
                                                            IndexShard indexShard) throws IOException {
        SourceToParse sourceToParse = SourceToParse.source(
                SourceToParse.Origin.REPLICA,
                request.index(), request.type(),
                item.id(), item.source(),
                XContentType.JSON)
            .routing(request.routing());

        if (logger.isTraceEnabled()) {
            logger.trace("[{} (R)] Index document id={} source={} opType={} version={} versionType={}",
                indexShard.shardId(),
                item.id(),
                item.source().utf8ToString(),
                item.opType(),
                item.version(),
                item.versionType());
        }
        Engine.Index index = indexShard.prepareIndexOnReplica(
            sourceToParse, item.version(), item.versionType(), -1, request.isRetry());

        Mapping update = index.parsedDoc().dynamicMappingsUpdate();
        if (update != null) {
            throw new RetryOnReplicaException(
                indexShard.shardId(),
                "Mappings are not available on the replica yet, triggered update: " + update);
        }
        return indexShard.index(index);
    }

    private Map<String, Object> processGeneratedColumnsOnInsert(DocTableInfo tableInfo,
                                                                Reference[] insertColumns,
                                                                Object[] insertValues,
                                                                boolean isRawSourceInsert,
                                                                boolean validateExpressionValue) {
        Map<String, Object> sourceAsMap = buildMapFromSource(insertColumns, insertValues, isRawSourceInsert);
        processGeneratedColumns(tableInfo, sourceAsMap, sourceAsMap, validateExpressionValue, null);
        return sourceAsMap;
    }

    @VisibleForTesting
    Map<String, Object> buildMapFromSource(Reference[] insertColumns,
                                           Object[] insertValues,
                                           boolean isRawSourceInsert) {
        Map<String, Object> sourceAsMap;
        if (isRawSourceInsert) {
            BytesRef source = (BytesRef) insertValues[0];
            sourceAsMap = XContentHelper.convertToMap(new BytesArray(source), true).v2();
        } else {
            sourceAsMap = new LinkedHashMap<>(insertColumns.length);
            for (int i = 0; i < insertColumns.length; i++) {
                sourceAsMap.put(insertColumns[i].ident().columnIdent().fqn(), insertValues[i]);
            }
        }
        return sourceAsMap;
    }

    @VisibleForTesting
    void processGeneratedColumns(DocTableInfo tableInfo,
                                 Map<String, Object> updatedColumns,
                                 Map<String, Object> updatedGeneratedColumns,
                                 boolean validateConstraints,
                                 @Nullable GetResult getResult) {

        List<GeneratedReference> generatedReferences = tableInfo.generatedColumns();

        List<Input<?>> generatedSymbols = resolveSymbols(
            new GetResultOrGeneratedColumnsResolver(updatedColumns),
            getResult,
            generatedReferences,
            null);
        for (int i = 0; i < generatedReferences.size(); i++) {
            final GeneratedReference reference = generatedReferences.get(i);
            // partitionedBy columns cannot be updated
            if (!tableInfo.partitionedByColumns().contains(reference)) {
                Object userSuppliedValue = updatedGeneratedColumns.get(reference.ident().columnIdent().fqn());
                if (validateConstraints) {
                    ConstraintsValidator.validate(userSuppliedValue, reference, tableInfo.notNullColumns());
                }

                if ((userSuppliedValue != null && validateConstraints)
                    ||
                    generatedExpressionEvaluationNeeded(reference.referencedReferences(), updatedColumns.keySet())) {
                    // at least one referenced column was updated, need to evaluate expression and update column
                    Object generatedValue = generatedSymbols.get(i).value();

                    if (userSuppliedValue == null) {
                        // add column & value
                        updatedColumns.put(reference.ident().columnIdent().fqn(), generatedValue);
                    } else if (validateConstraints &&
                               reference.valueType().compareValueTo(generatedValue, userSuppliedValue) != 0) {
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                            "Given value %s for generated column does not match defined generated expression value %s",
                            userSuppliedValue, generatedValue));
                    }
                }
            }
        }
    }

    /**
     * Evaluation is needed either if expression contains no reference at all
     * or if a referenced column value has changed.
     */
    private boolean generatedExpressionEvaluationNeeded(List<Reference> referencedReferences,
                                                        Collection<String> updatedColumns) {
        boolean evalNeeded = referencedReferences.isEmpty();
        for (Reference reference : referencedReferences) {
            for (String columnName : updatedColumns) {
                if (reference.ident().columnIdent().fqn().equals(columnName)
                    || reference.ident().columnIdent().isChildOf(ColumnIdent.fromPath(columnName))) {
                    evalNeeded = true;
                    break;
                }
            }
        }
        return evalNeeded;
    }

    /**
     * Overwrite given values on the source. If the value is a map,
     * it will not be merged but overwritten. The keys of the changes map representing a path of
     * the source map tree.
     * If the path doesn't exists, a new tree will be inserted.
     * <p/>
     * TODO: detect NOOP
     */
    @SuppressWarnings("unchecked")
    static void updateSourceByPaths(@Nonnull Map<String, Object> source, @Nonnull Map<String, Object> changes) {
        for (Map.Entry<String, Object> changesEntry : changes.entrySet()) {
            String key = changesEntry.getKey();
            int dotIndex = key.indexOf(".");
            if (dotIndex > -1) {
                // sub-path detected, dive recursive to the wanted tree element
                String currentKey = key.substring(0, dotIndex);
                if (!source.containsKey(currentKey)) {
                    // insert parent tree element
                    source.put(currentKey, new HashMap<String, Object>());
                }
                Map<String, Object> subChanges = new HashMap<>();
                subChanges.put(key.substring(dotIndex + 1, key.length()), changesEntry.getValue());

                Map<String, Object> innerSource = (Map<String, Object>) source.get(currentKey);
                if (innerSource == null) {
                    throw new NullPointerException(String.format(Locale.ENGLISH,
                        "Object %s is null, cannot write %s onto it", currentKey, subChanges));
                }
                updateSourceByPaths(innerSource, subChanges);
            } else {
                // overwrite or insert the field
                source.put(key, changesEntry.getValue());
            }
        }
    }

    public static Collection<ColumnIdent> getNotUsedNonGeneratedColumns(Reference[] targetColumns,
                                                                        DocTableInfo tableInfo) {
        Set<String> targetColumnsSet = new HashSet<>();
        Collection<ColumnIdent> columnsNotUsed = new ArrayList<>();

        if (targetColumns != null) {
            for (Reference targetColumn : targetColumns) {
                targetColumnsSet.add(targetColumn.ident().columnIdent().fqn());
            }
        }

        for (Reference reference : tableInfo.columns()) {
            if (!(reference instanceof GeneratedReference) && !reference.isNullable()) {
                if (!targetColumnsSet.contains(reference.ident().columnIdent().fqn())) {
                    columnsNotUsed.add(reference.ident().columnIdent());
                }
            }
        }
        return columnsNotUsed;
    }

    private static class GetResultRefResolver implements ReferenceResolver<CollectExpression<GetResult, ?>> {

        private static final GetResultRefResolver INSTANCE = new GetResultRefResolver();

        @Override
        public CollectExpression<GetResult, ?> getImplementation(Reference ref) {
            ColumnIdent columnIdent = ref.ident().columnIdent();
            String fqn = columnIdent.fqn();
            switch (fqn) {
                case DocSysColumns.Names.VERSION:
                    return RowContextCollectorExpression.forFunction(GetResult::getVersion);

                case DocSysColumns.Names.ID:
                    return RowContextCollectorExpression.objToBytesRef(GetResult::getId);

                case DocSysColumns.Names.RAW:
                    return RowContextCollectorExpression.forFunction(r -> r.sourceRef().toBytesRef());

                case DocSysColumns.Names.DOC:
                    return RowContextCollectorExpression.forFunction(GetResult::getSource);

                default:
                    return RowContextCollectorExpression.forFunction(response -> {
                        if (response == null) {
                            return null;
                        }
                        Map<String, Object> sourceAsMap = response.sourceAsMap();
                        return ref.valueType().value(XContentMapValues.extractValue(fqn, sourceAsMap));
                    });
            }
        }
    }

    private static class GetResultOrGeneratedColumnsResolver extends GetResultRefResolver {

        private final Map<String, Object> updatedColumns;

        GetResultOrGeneratedColumnsResolver(Map<String, Object> updatedColumns) {
            super();
            this.updatedColumns = updatedColumns;
        }

        @Override
        public CollectExpression<GetResult, ?> getImplementation(Reference ref) {
            Object suppliedValue = updatedColumns.get(ref.ident().columnIdent().fqn());
            final Object value;
            if (suppliedValue == null && !ref.ident().isColumn()) {
                value = XContentMapValues.extractValue(ref.ident().columnIdent().fqn(), updatedColumns);
            } else {
                value = suppliedValue;
            }
            if (value != null) {
                return RowContextCollectorExpression.forFunction(ignored -> ref.valueType().value(value));
            }
            return super.getImplementation(ref);
        }
    }

    private static class SourceAndVersion {

        final BytesReference source;
        final long version;

        SourceAndVersion(BytesReference source, long version) {
            this.source = source;
            this.version = version;
        }
    }
}
