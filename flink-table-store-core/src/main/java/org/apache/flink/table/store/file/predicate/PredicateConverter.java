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

package org.apache.flink.table.store.file.predicate;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.store.utils.TypeUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static org.apache.flink.table.data.conversion.DataStructureConverters.getConverter;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsImplicitCast;

/** Convert {@link Expression} to {@link Predicate}. */
public class PredicateConverter implements ExpressionVisitor<Predicate> {

    public static final PredicateConverter CONVERTER = new PredicateConverter();

    @Nullable
    public Predicate fromMap(Map<String, String> map, RowType rowType) {
        List<String> fieldNames = rowType.getFieldNames();
        Predicate predicate = null;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            int idx = fieldNames.indexOf(entry.getKey());
            LogicalType type = rowType.getTypeAt(idx);
            Literal literal = new Literal(type, TypeUtils.castFromString(entry.getValue(), type));
            if (predicate == null) {
                predicate = new Equal(idx, literal);
            } else {
                predicate = new And(predicate, new Equal(idx, literal));
            }
        }
        return predicate;
    }

    @Override
    public Predicate visit(CallExpression call) {
        FunctionDefinition func = call.getFunctionDefinition();
        List<Expression> children = call.getChildren();

        if (func == BuiltInFunctionDefinitions.AND) {
            return new And(children.get(0).accept(this), children.get(1).accept(this));
        } else if (func == BuiltInFunctionDefinitions.OR) {
            return new Or(children.get(0).accept(this), children.get(1).accept(this));
        } else if (func == BuiltInFunctionDefinitions.EQUALS) {
            return visitBiFunction(children, Equal::new, Equal::new);
        } else if (func == BuiltInFunctionDefinitions.NOT_EQUALS) {
            return visitBiFunction(children, NotEqual::new, NotEqual::new);
        } else if (func == BuiltInFunctionDefinitions.GREATER_THAN) {
            return visitBiFunction(children, GreaterThan::new, LessThan::new);
        } else if (func == BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL) {
            return visitBiFunction(children, GreaterOrEqual::new, LessOrEqual::new);
        } else if (func == BuiltInFunctionDefinitions.LESS_THAN) {
            return visitBiFunction(children, LessThan::new, GreaterThan::new);
        } else if (func == BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL) {
            return visitBiFunction(children, LessOrEqual::new, GreaterOrEqual::new);
        } else if (func == BuiltInFunctionDefinitions.IS_NULL) {
            return extractFieldReference(children.get(0))
                    .map(FieldReferenceExpression::getFieldIndex)
                    .map(IsNull::new)
                    .orElseThrow(UnsupportedExpression::new);
        } else if (func == BuiltInFunctionDefinitions.IS_NOT_NULL) {
            return extractFieldReference(children.get(0))
                    .map(FieldReferenceExpression::getFieldIndex)
                    .map(IsNotNull::new)
                    .orElseThrow(UnsupportedExpression::new);
        }

        // TODO is_xxx, between_xxx, like, similar, in, not_in, not?

        throw new UnsupportedExpression();
    }

    private Predicate visitBiFunction(
            List<Expression> children,
            BiFunction<Integer, Literal, Predicate> visit1,
            BiFunction<Integer, Literal, Predicate> visit2) {
        Optional<FieldReferenceExpression> fieldRefExpr = extractFieldReference(children.get(0));
        Optional<Literal> literal;
        if (fieldRefExpr.isPresent()) {
            literal = extractLiteral(fieldRefExpr.get().getOutputDataType(), children.get(1));
            if (literal.isPresent()) {
                return visit1.apply(fieldRefExpr.get().getFieldIndex(), literal.get());
            }
        } else {
            fieldRefExpr = extractFieldReference(children.get(1));
            if (fieldRefExpr.isPresent()) {
                literal = extractLiteral(fieldRefExpr.get().getOutputDataType(), children.get(0));
                if (literal.isPresent()) {
                    return visit2.apply(fieldRefExpr.get().getFieldIndex(), literal.get());
                }
            }
        }

        throw new UnsupportedExpression();
    }

    private Optional<FieldReferenceExpression> extractFieldReference(Expression expression) {
        if (expression instanceof FieldReferenceExpression) {
            return Optional.of((FieldReferenceExpression) expression);
        }
        return Optional.empty();
    }

    private Optional<Literal> extractLiteral(DataType expectedType, Expression expression) {
        LogicalType expectedLogicalType = expectedType.getLogicalType();
        if (!supportsPredicate(expectedLogicalType)) {
            return Optional.empty();
        }
        Literal literal = null;
        if (expression instanceof ValueLiteralExpression) {
            ValueLiteralExpression valueExpression = (ValueLiteralExpression) expression;
            DataType actualType = valueExpression.getOutputDataType();
            LogicalType actualLogicalType = actualType.getLogicalType();
            Optional<?> valueOpt = valueExpression.getValueAs(actualType.getConversionClass());
            if (!valueOpt.isPresent()) {
                return Optional.empty();
            }
            Object value = valueOpt.get();
            if (actualLogicalType.getTypeRoot().equals(expectedLogicalType.getTypeRoot())) {
                literal =
                        new Literal(
                                expectedLogicalType,
                                getConverter(expectedType).toInternalOrNull(value));
            } else if (supportsImplicitCast(actualLogicalType, expectedLogicalType)) {
                try {
                    value = TypeUtils.castFromString(value.toString(), expectedLogicalType);
                    literal = new Literal(expectedLogicalType, value);
                } catch (Exception ignored) {
                    // ignore here, let #visit throw UnsupportedExpression
                }
            }
        }
        return literal == null ? Optional.empty() : Optional.of(literal);
    }

    private boolean supportsPredicate(LogicalType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return true;
            default:
                return false;
        }
    }

    @Override
    public Predicate visit(ValueLiteralExpression valueLiteralExpression) {
        throw new RuntimeException("Literal should be resolved in call expression.");
    }

    @Override
    public Predicate visit(FieldReferenceExpression fieldReferenceExpression) {
        throw new RuntimeException("Field reference should be resolved in call expression.");
    }

    @Override
    public Predicate visit(TypeLiteralExpression typeLiteralExpression) {
        throw new RuntimeException(
                "Type literal is unsupported: " + typeLiteralExpression.asSummaryString());
    }

    @Override
    public Predicate visit(Expression expression) {
        throw new RuntimeException("Unsupported expression: " + expression.asSummaryString());
    }

    @Nullable
    public static Predicate convert(List<ResolvedExpression> filters) {
        return filters != null
                ? filters.stream()
                        .map(filter -> filter.accept(PredicateConverter.CONVERTER))
                        .reduce(And::new)
                        .orElse(null)
                : null;
    }

    /** Encounter an unsupported expression, the caller can choose to ignore this filter branch. */
    public static class UnsupportedExpression extends RuntimeException {}
}
