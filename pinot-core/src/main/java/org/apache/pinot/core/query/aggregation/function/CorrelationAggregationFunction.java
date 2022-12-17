/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.core.query.aggregation.function;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.utils.StatisticalAggregationFunctionUtils;
import org.apache.pinot.segment.local.customobject.CorrelationTuple;
import org.apache.pinot.segment.spi.AggregationFunctionType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CorrelationAggregationFunction implements AggregationFunction<CorrelationTuple, Double> {
    private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;
    protected final ExpressionContext _expression1;
    protected final ExpressionContext _expression2;
    protected final boolean _isSample;

    public CorrelationAggregationFunction(List<ExpressionContext> arguments, boolean isSample) {
        _expression1 = arguments.get(0);
        _expression2 = arguments.get(1);
        _isSample = isSample;
    }

    @Override
    public AggregationFunctionType getType() {
        return AggregationFunctionType.CORR;
    }

    @Override
    public String getColumnName() {
        return getType().getName() + "_" + _expression1 + "_" + _expression2;
    }

    @Override
    public String getResultColumnName() {
        return getType().getName().toLowerCase() + "(" + _expression1 + "," + _expression2 + ")";
    }

    @Override
    public List<ExpressionContext> getInputExpressions() {
        ArrayList<ExpressionContext> inputExpressions = new ArrayList<>();
        inputExpressions.add(_expression1);
        inputExpressions.add(_expression2);
        return inputExpressions;
    }

    @Override
    public AggregationResultHolder createAggregationResultHolder() {
        return new ObjectAggregationResultHolder();
    }

    @Override
    public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
        return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
    }

    @Override
    public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
                          Map<ExpressionContext, BlockValSet> blockValSetMap) {
        double[] values1 = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression1);
        double[] values2 = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression2);

        double sumX = 0.0;
        double sumY = 0.0;
        double sumXY = 0.0;

        double squareSumX = 0.0;
        double squareSumY = 0.0;

        for (int i = 0; i < length; i++) {
            sumX += values1[i];
            sumY += values2[i];
            sumXY += values1[i] * values2[i];
            squareSumX = squareSumX + values1[i] * values1[i];
            squareSumY = squareSumY + values2[i] * values2[i];
        }
        setAggregationResult(aggregationResultHolder, sumX, sumY, sumXY, squareSumX, squareSumY, length);
    }

    protected void setAggregationResult(AggregationResultHolder aggregationResultHolder, double sumX, double sumY,
                                        double sumXY, double squareSumX, double squareSumY, long count) {
        CorrelationTuple correlationTuple = aggregationResultHolder.getResult();
        if (correlationTuple == null) {
            aggregationResultHolder.setValue(new CorrelationTuple(sumX, sumY, sumXY, squareSumX, squareSumY, count));
        } else {
            correlationTuple.apply(sumX, sumY, sumXY, squareSumX, squareSumY, count);
        }
    }

    protected void setGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, double sumX, double sumY,
                                    double sumXY, double squareSumX, double squareSumY, long count) {
        CorrelationTuple correlationTuple = groupByResultHolder.getResult(groupKey);
        if (correlationTuple == null) {
            groupByResultHolder.setValueForKey(groupKey, new CorrelationTuple(sumX, sumY, sumXY,
                    squareSumX, squareSumY, count));
        } else {
            correlationTuple.apply(sumX, sumY, sumXY, squareSumX, squareSumY, count);
        }
    }

    @Override
    public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
                                   Map<ExpressionContext, BlockValSet> blockValSetMap) {
        double[] values1 = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression1);
        double[] values2 = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression2);
        for (int i = 0; i < length; i++) {
            setGroupByResult(groupKeyArray[i], groupByResultHolder, values1[i], values2[i],
                    values1[i] * values2[i], values1[i] * values1[i], values2[i] * values2[i], 1L);
        }
    }

    @Override
    public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
                                   Map<ExpressionContext, BlockValSet> blockValSetMap) {
        double[] values1 = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression1);
        double[] values2 = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression2);
        for (int i = 0; i < length; i++) {
            for (int groupKey : groupKeysArray[i]) {
                setGroupByResult(groupKey, groupByResultHolder, values1[i], values2[i],
                        values1[i] * values2[i], values1[i] * values1[i], values2[i] * values2[i], 1L);
            }
        }
    }

    @Override
    public CorrelationTuple extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
        CorrelationTuple correlationTuple = aggregationResultHolder.getResult();
        if (correlationTuple == null) {
            return new CorrelationTuple(0.0, 0.0, 0.0, 0.0, 0.0, 0L);
        } else {
            return correlationTuple;
        }
    }

    @Override
    public CorrelationTuple extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
        return groupByResultHolder.getResult(groupKey);
    }

    @Override
    public CorrelationTuple merge(CorrelationTuple intermediateResult1, CorrelationTuple intermediateResult2) {
        intermediateResult1.apply(intermediateResult2);
        return intermediateResult1;
    }

    @Override
    public DataSchema.ColumnDataType getIntermediateResultColumnType() {
        return DataSchema.ColumnDataType.OBJECT;
    }

    @Override
    public DataSchema.ColumnDataType getFinalResultColumnType() {
        return DataSchema.ColumnDataType.DOUBLE;
    }

    @Override
    public Double extractFinalResult(CorrelationTuple correlationTuple) {
        long count = correlationTuple.getCount();
        if (count == 0L) {
            return DEFAULT_FINAL_RESULT;
        } else {
            double sumX = correlationTuple.getSumX();
            double sumY = correlationTuple.getSumY();
            double sumXY = correlationTuple.getSumXY();
            double squareSumX = correlationTuple.getSquareSumX();
            double squareSumY = correlationTuple.getSquareSumY();

            return (sumXY / count) - (sumX * sumY) / (count * count);
        }
    }

    @Override
    public String toExplainString() {
        StringBuilder stringBuilder = new StringBuilder(getType().getName()).append('(');
        int numArguments = getInputExpressions().size();
        if (numArguments > 0) {
            stringBuilder.append(getInputExpressions().get(0).toString());
            for (int i = 1; i < numArguments; i++) {
                stringBuilder.append(", ").append(getInputExpressions().get(i).toString());
            }
        }
        return stringBuilder.append(')').toString();
    }
}
