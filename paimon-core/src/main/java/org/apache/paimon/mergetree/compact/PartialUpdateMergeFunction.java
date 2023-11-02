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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.mergetree.compact.aggregate.FieldLastNonNullValueAgg;
import org.apache.paimon.mergetree.compact.aggregate.FieldLastValueAgg;
import org.apache.paimon.mergetree.compact.aggregate.RetractStrategy;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.sink.SequenceGenerator;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.paimon.CoreOptions.FIELDS_PREFIX;
import static org.apache.paimon.mergetree.compact.aggregate.AggregateMergeFunction.AGG_FUNCTION;
import static org.apache.paimon.mergetree.compact.aggregate.AggregateMergeFunction.IGNORE_RETRACT;
import static org.apache.paimon.mergetree.compact.aggregate.AggregateMergeFunction.RETRACT_STRATEGY;
import static org.apache.paimon.options.ConfigOptions.key;
import static org.apache.paimon.utils.InternalRowUtils.createFieldGetters;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record, update
 * non-null fields on merge.
 */
public class PartialUpdateMergeFunction implements MergeFunction<KeyValue> {

    public static final String SEQUENCE_GROUP = "sequence-group";

    private final InternalRow.FieldGetter[] getters;
    private final boolean ignoreDelete;
    private final Map<Integer, SequenceGenerator> fieldSequences;

    private final Map<Integer, FieldAggregator> fieldAggregators;

    private InternalRow currentKey;
    private long latestSequenceNumber;
    private boolean isEmpty;
    private GenericRow row;
    private KeyValue reused;

    protected PartialUpdateMergeFunction(
            InternalRow.FieldGetter[] getters,
            boolean ignoreDelete,
            Map<Integer, SequenceGenerator> fieldSequences,
            Map<Integer, FieldAggregator> fieldAggregators) {
        this.getters = getters;
        this.ignoreDelete = ignoreDelete;
        this.fieldSequences = fieldSequences;
        this.fieldAggregators = fieldAggregators;
    }

    @Override
    public void reset() {
        this.currentKey = null;
        this.row = new GenericRow(getters.length);
        this.isEmpty = true;
    }

    @Override
    public void add(KeyValue kv) {
        // refresh key object to avoid reference overwritten
        currentKey = kv.key();

        // ignore delete?
        if (kv.valueKind().isRetract()) {
            if (ignoreDelete) {
                return;
            }

            if (fieldSequences.size() > 1) {
                retractWithSequenceGroup(kv);
                return;
            }

            String msg =
                    String.join(
                            "By default, Partial update can not accept delete records,"
                                    + " you can choose one of the following solutions:",
                            "1. Configure 'partial-update.ignore-delete' to ignore delete records.",
                            "2. Configure 'sequence-group's to retract partial columns.");

            throw new IllegalArgumentException(msg);
        }

        latestSequenceNumber = kv.sequenceNumber();
        isEmpty = false;
        update(kv);
    }

    private void update(KeyValue kv) {
        for (int i = 0; i < getters.length; i++) {
            Object field = getters[i].getFieldOrNull(kv.value());
            SequenceGenerator sequenceGen = fieldSequences.get(i);
            FieldAggregator aggregator = fieldAggregators.get(i);
            Object accumulator = getters[i].getFieldOrNull(row);
            if (sequenceGen == null) {
                row.setField(i, aggregator.agg(accumulator, field));
            } else {
                Long currentSeq = sequenceGen.generateNullable(kv.value());
                if (currentSeq != null) {
                    Long previousSeq = sequenceGen.generateNullable(row);
                    if (previousSeq == null || currentSeq >= previousSeq) {
                        row.setField(i, aggregator.agg(accumulator, field));
                    } else {
                        row.setField(i, aggregator.aggForOldSequence(accumulator, field));
                    }
                }
            }
        }
    }

    private void retractWithSequenceGroup(KeyValue kv) {
        for (int i = 0; i < getters.length; i++) {
            SequenceGenerator sequenceGen = fieldSequences.get(i);
            if (sequenceGen != null) {
                Long currentSeq = sequenceGen.generateNullable(kv.value());
                if (currentSeq != null) {
                    Long previousSeq = sequenceGen.generateNullable(row);
                    if (previousSeq == null || currentSeq >= previousSeq) {
                        if (sequenceGen.index() == i) {
                            // update sequence field
                            row.setField(i, getters[i].getFieldOrNull(kv.value()));
                        } else {
                            // retract normal field
                            FieldAggregator aggregator = fieldAggregators.get(i);
                            Object accumulator = getters[i].getFieldOrNull(row);
                            row.setField(
                                    i,
                                    aggregator.retract(
                                            accumulator, getters[i].getFieldOrNull(kv.value())));
                        }
                    }
                }
            }
        }
    }

    @Override
    @Nullable
    public KeyValue getResult() {
        if (isEmpty) {
            return null;
        }

        if (reused == null) {
            reused = new KeyValue();
        }
        return reused.replace(currentKey, latestSequenceNumber, RowKind.INSERT, row);
    }

    public static MergeFunctionFactory<KeyValue> factory(
            Options options, RowType rowType, List<String> primaryKeys) {
        return new Factory(options, rowType, primaryKeys);
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final boolean ignoreDelete;
        private final List<DataType> tableTypes;
        private final Map<Integer, SequenceGenerator> fieldSequences;

        private final Map<Integer, FieldAggregator> fieldAggregators;

        private Factory(Options options, RowType rowType, List<String> primaryKeys) {
            this.ignoreDelete = options.get(CoreOptions.PARTIAL_UPDATE_IGNORE_DELETE);
            this.tableTypes = rowType.getFieldTypes();

            List<String> fieldNames = rowType.getFieldNames();
            this.fieldSequences = new HashMap<>();
            for (Map.Entry<String, String> entry : options.toMap().entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                if (k.startsWith(FIELDS_PREFIX) && k.endsWith(SEQUENCE_GROUP)) {
                    String sequenceFieldName =
                            k.substring(
                                    FIELDS_PREFIX.length() + 1,
                                    k.length() - SEQUENCE_GROUP.length() - 1);
                    SequenceGenerator sequenceGen =
                            new SequenceGenerator(sequenceFieldName, rowType);
                    Arrays.stream(v.split(","))
                            .map(
                                    fieldName -> {
                                        int field = fieldNames.indexOf(fieldName);
                                        if (field == -1) {
                                            throw new IllegalArgumentException(
                                                    String.format(
                                                            "Field %s can not be found in table schema",
                                                            fieldName));
                                        }
                                        return field;
                                    })
                            .forEach(
                                    field -> {
                                        if (fieldSequences.containsKey(field)) {
                                            throw new IllegalArgumentException(
                                                    String.format(
                                                            "Field %s is defined repeatedly by multiple groups: %s",
                                                            fieldNames.get(field), k));
                                        }
                                        fieldSequences.put(field, sequenceGen);
                                    });

                    // add self
                    fieldSequences.put(sequenceGen.index(), sequenceGen);
                }
            }
            this.fieldAggregators = createFieldAggregators(rowType, primaryKeys, options);
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            if (projection != null) {
                Map<Integer, SequenceGenerator> projectedSequences = new HashMap<>();
                Map<Integer, FieldAggregator> projectedAggregator = new HashMap<>();
                int[] projects = Projection.of(projection).toTopLevelIndexes();
                Map<Integer, Integer> indexMap = new HashMap<>();
                for (int i = 0; i < projects.length; i++) {
                    indexMap.put(projects[i], i);
                }
                fieldSequences.forEach(
                        (field, sequence) -> {
                            int newField = indexMap.getOrDefault(field, -1);
                            if (newField != -1) {
                                int newSequenceId = indexMap.getOrDefault(sequence.index(), -1);
                                if (newSequenceId == -1) {
                                    throw new RuntimeException(
                                            String.format(
                                                    "Can not find new sequence field for new field. new field index is %s",
                                                    newField));
                                } else {
                                    projectedSequences.put(
                                            newField,
                                            new SequenceGenerator(
                                                    newSequenceId, sequence.fieldType()));
                                }
                            }
                        });
                for (int i = 0; i < projects.length; i++) {
                    projectedAggregator.put(i, fieldAggregators.get(projects[i]));
                }

                return new PartialUpdateMergeFunction(
                        createFieldGetters(Projection.of(projection).project(tableTypes)),
                        ignoreDelete,
                        projectedSequences,
                        projectedAggregator);
            } else {
                return new PartialUpdateMergeFunction(
                        createFieldGetters(tableTypes),
                        ignoreDelete,
                        fieldSequences,
                        fieldAggregators);
            }
        }

        @Override
        public AdjustedProjection adjustProjection(@Nullable int[][] projection) {
            if (fieldSequences.isEmpty()) {
                return new AdjustedProjection(projection, null);
            }

            if (projection == null) {
                return new AdjustedProjection(null, null);
            }
            LinkedHashSet<Integer> extraFields = new LinkedHashSet<>();
            int[] topProjects = Projection.of(projection).toTopLevelIndexes();
            Set<Integer> indexSet = Arrays.stream(topProjects).boxed().collect(Collectors.toSet());
            for (int index : topProjects) {
                SequenceGenerator generator = fieldSequences.get(index);
                if (generator != null && !indexSet.contains(generator.index())) {
                    extraFields.add(generator.index());
                }
            }

            int[] allProjects =
                    Stream.concat(Arrays.stream(topProjects).boxed(), extraFields.stream())
                            .mapToInt(Integer::intValue)
                            .toArray();

            int[][] pushdown = Projection.of(allProjects).toNestedIndexes();
            int[][] outer =
                    Projection.of(IntStream.range(0, topProjects.length).toArray())
                            .toNestedIndexes();
            return new AdjustedProjection(pushdown, outer);
        }

        /**
         * Creating aggregation function for the columns.
         *
         * @return The aggregators for each column.
         */
        private Map<Integer, FieldAggregator> createFieldAggregators(
                RowType rowType, List<String> primaryKeys, Options options) {
            List<String> fieldNames = rowType.getFieldNames();
            List<DataType> fieldTypes = rowType.getFieldTypes();
            Map<Integer, FieldAggregator> fieldAggregators = new HashMap<>();
            for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i);
                DataType fieldType = fieldTypes.get(i);
                // aggregate by primary keys, so they do not aggregate
                boolean isPrimaryKey = primaryKeys.contains(fieldName);
                String strAggFunc =
                        options.get(
                                key(FIELDS_PREFIX + "." + fieldName + "." + AGG_FUNCTION)
                                        .stringType()
                                        .noDefaultValue());
                RetractStrategy retractStrategy =
                        options.get(
                                key(FIELDS_PREFIX + "." + fieldName + "." + RETRACT_STRATEGY)
                                        .enumType(RetractStrategy.class)
                                        .noDefaultValue());
                boolean ignoreRetract =
                        options.get(
                                key(FIELDS_PREFIX + "." + fieldName + "." + IGNORE_RETRACT)
                                        .booleanType()
                                        .defaultValue(false));

                if (retractStrategy == null) {
                    // for not agg field, we use set null for compatibility
                    if (strAggFunc == null) {
                        retractStrategy = RetractStrategy.SET_NULL;
                    } else {
                        retractStrategy =
                                ignoreRetract ? RetractStrategy.IGNORE : RetractStrategy.DEFAULT;
                    }
                }

                if (strAggFunc != null) {
                    fieldAggregators.put(
                            i,
                            FieldAggregator.createFieldAggregator(
                                    fieldType,
                                    strAggFunc,
                                    retractStrategy,
                                    isPrimaryKey,
                                    () -> {
                                        throw new RuntimeException("Unexpected usage!");
                                    }));
                } else {
                    if (fieldSequences.containsKey(i)) {
                        // for the column in sequence group, the default aggregation strategy is
                        // last_value
                        fieldAggregators.put(
                                i,
                                FieldAggregator.createFieldAggregator(
                                        fieldType,
                                        null,
                                        retractStrategy,
                                        isPrimaryKey,
                                        () -> new FieldLastValueAgg(fieldType)));
                    } else {
                        fieldAggregators.put(
                                i,
                                FieldAggregator.createFieldAggregator(
                                        fieldType,
                                        null,
                                        retractStrategy,
                                        isPrimaryKey,
                                        () -> new FieldLastNonNullValueAgg(fieldType)));
                    }
                }
            }
            return fieldAggregators;
        }
    }
}
