package org.apache.cassandra.utils;

import java.util.Random;

public class PrimitiveIntSort {
    static int MIN_QSORT_SIZE = 7;

    public static void isort(int[] x, CompareInt compare) {
        for (int i = 0; i < x.length; ++i) {
            int t = x[i];
            int j = i;
            for ( ; j > 0 && compare.lessThan(t,x[j-1]); --j)
                x[j] = x[j-1];
            x[j] = t;
        }
    }

    public static void qsort(int[] x, CompareInt compare) {
        Random random = new Random();
        qsortPartial(x,0,x.length-1,compare,random);
        isort(x,compare);
    }

    static void qsortPartial(int[] x, int lower, int upper,
                             CompareInt compare,
                             Random random) {
        if (upper - lower < MIN_QSORT_SIZE)
            return;
        swap(x, lower, lower + random.nextInt(upper-lower+1));
        int t = x[lower];
        int i = lower;
        int j = upper + 1;
        while (true) {
            do {
                ++i;
            } while (i <= upper && compare.lessThan(x[i],t));
            do {
                --j;
            } while (compare.lessThan(t,x[j]));
            if (i > j)
                break;
            swap(x,i,j);
        }
    }

    public static void swap(int[] xs, int i, int j) {
        int temp = xs[i];
        xs[i] = xs[j];
        xs[j] = temp;
    }

    public interface CompareInt {
        public boolean lessThan(int a, int b);
    }
}

