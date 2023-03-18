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

package org.apache.pinot.segment.local.customobject;

import java.nio.ByteBuffer;
import javax.annotation.Nonnull;

public class CorrelationTuple implements Comparable<CorrelationTuple> {

    private double _sumX;
    private double _sumY;
    private double _sumXY;
    private double _squareSumX;
    private double _squareSumY;
    private long _count;

    public CorrelationTuple(double sumX, double sumY, double sumXY, double squareSumX, double squareSumY, long count) {
        _sumX = sumX;
        _sumY = sumY;
        _sumXY = sumXY;
        _squareSumX = squareSumX;
        _squareSumY = squareSumY;
        _count = count;
    }

    public void apply(double sumX, double sumY, double sumXY, double squareSumX, double squareSumY, long count) {
        _sumX += sumX;
        _sumY += sumY;
        _sumXY += sumXY;
        _squareSumX += squareSumX;
        _squareSumY += squareSumY;
        _count += count;
    }

    public void apply(@Nonnull CorrelationTuple correlationTuple) {
        _sumX += correlationTuple._sumX;
        _sumY += correlationTuple._sumY;
        _sumXY += correlationTuple._sumXY;
        _squareSumX = correlationTuple._squareSumX;
        _squareSumY = correlationTuple._squareSumY;
        _count += correlationTuple._count;
    }

    public double getSumX() {
        return _sumX;
    }

    public double getSumY() {
        return _sumY;
    }

    public double getSumXY() {
        return _sumXY;
    }

    public double getSquareSumX() {
        return _squareSumX;
    }

    public double getSquareSumY() {
        return _squareSumY;
    }

    public long getCount() {
        return _count;
    }

    @Nonnull
    public byte[] toBytes() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES + Double.BYTES + Double.BYTES
                + Double.BYTES + Double.BYTES + Long.BYTES);
        byteBuffer.putDouble(_sumX);
        byteBuffer.putDouble(_sumY);
        byteBuffer.putDouble(_sumXY);
        byteBuffer.putDouble(_squareSumX);
        byteBuffer.putDouble(_squareSumY);
        byteBuffer.putLong(_count);
        return byteBuffer.array();
    }

    @Nonnull
    public static CorrelationTuple fromBytes(byte[] bytes) {
        return fromByteBuffer(ByteBuffer.wrap(bytes));
    }

    @Nonnull
    public static CorrelationTuple fromByteBuffer(ByteBuffer byteBuffer) {
        return new CorrelationTuple(byteBuffer.getDouble(), byteBuffer.getDouble(), byteBuffer.getDouble(),
                byteBuffer.getDouble(), byteBuffer.getDouble(), byteBuffer.getLong());
    }

    @Override
    public int compareTo(@Nonnull CorrelationTuple correlationTuple) {
        if (_count == 0) {
            if (correlationTuple._count == 0) {
                return 0;
            } else {
                return -1;
            }
        } else {
            if (correlationTuple._count == 0) {
                return 1;
            } else {
                double cov1 = _sumXY / _count - (_sumX / _count) * (_sumY / _count);
                double cov2 =
                        correlationTuple._sumXY / correlationTuple._count
                                - (correlationTuple._sumX / correlationTuple._count) * (
                                correlationTuple._sumY / correlationTuple._count);

                double stdX1 = Math.sqrt(_squareSumX / _count - _sumX * _sumX / _count / _count);
                double stdY1 = Math.sqrt(_squareSumY / _count - _sumY * _sumY / _count / _count);

                double stdX2 = Math.sqrt(correlationTuple._squareSumX / correlationTuple._count
                        - correlationTuple._sumX * correlationTuple._sumX / correlationTuple._count
                        / correlationTuple._count);
                double stdY2 = Math.sqrt(correlationTuple._squareSumY / correlationTuple._count
                        - correlationTuple._sumY * correlationTuple._sumY / correlationTuple._count
                        / correlationTuple._count);

                double corr1 = cov1 / (stdX1 * stdY1);
                double corr2 = cov2 / (stdX2 * stdY2);

                return Double.compare(corr1, corr2);

            }
        }
    }
}
