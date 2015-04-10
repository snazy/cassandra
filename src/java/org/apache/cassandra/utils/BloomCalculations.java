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
package org.apache.cassandra.utils;

/**
 * The following calculations are taken from:
 * http://www.cs.wisc.edu/~cao/papers/summary-cache/node8.html
 * "Bloom Filters - the math"
 *
 * This class's static methods are meant to facilitate the use of the Bloom
 * Filter class by helping to choose correct values of 'bits per element' and
 * 'number of hash functions, k'.
 */
final class BloomCalculations {

    private static final int minK = 1;
    private static final int maxBuckets = 20;
    private static final int maxK = 15;

    // this is ln(2)
    private static final double ln2 = 0.6931471805599453d;
    // this is ln(2)^2
    private static final double ln2square = 0.4804530139182014d;

    /**
     * A wrapper class that holds two key parameters for a Bloom Filter: the
     * number of hash functions used, and the number of buckets per element used.
     */
    public static class BloomSpecification
    {
        final int K; // number of hash functions.
        final double bucketsPerElement;

        public BloomSpecification(int k, double bucketsPerElement)
        {
            K = k;
            this.bucketsPerElement = bucketsPerElement;
        }

        public String toString()
        {
            return String.format("BloomSpecification(K=%d, bucketsPerElement=%f)", K, bucketsPerElement);
        }
    }

    public static BloomSpecification computeBloomSpec(long numElements, int m)
    {
        // exact computation: (source: https://en.wikipedia.org/wiki/Bloom_filter)
        //
        // n = number of elements
        // p = false-positive-chance
        // m = number of bits
        // k = number of hashes
        //
        // m = - ( (n ln(p) ) / ( ln(2)^2 ) )
        //
        // k = ( m / n ) * ln(2)
        //
        // note: ln(2) = 0.69314718056

        double n = numElements;

        double k = m;
        k /= n;
        k *= ln2;

        double bucketsPerElement = ((double)m) / numElements;
        int hashCount = Math.max(1, (int) Math.round(k));

        return new BloomSpecification(hashCount, bucketsPerElement);
    }

    public static BloomSpecification computeBloomSpec(long numElements, double maxFalsePosProb)
    {
        // exact computation:
        //
        // n = number of elements
        // p = false-positive-chance
        // m = number of bits
        // k = number of hashes
        //
        // m = - ( (n ln(p) ) / ( ln(2)^2 ) )
        //
        // k = ( m / n ) * ln(2)
        //
        // note: ln(2) = 0.69314718056

        double n = numElements;
        double p = maxFalsePosProb;

        double m = -n;
        m *= Math.log(p);
        m /= ln2square;

        double k = m;
        k /= n;
        k *= ln2;

        double bucketsPerElement = m / numElements;
        bucketsPerElement = Math.min(maxBuckets, bucketsPerElement);

        int hashCount = Math.max(minK, (int) Math.round(k));
        hashCount = Math.min(hashCount, maxK);

        return new BloomSpecification(hashCount, bucketsPerElement);
    }
}
