package org.apache.cassandra.db.index.search.plan;

import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.index.SuffixArraySecondaryIndex;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator.OperationType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.thrift.IndexExpression;

public class QueryPlan
{
    /**
     * A sanity ceiling on the number of max rows we'll ever return for a query.
     */
    private static final int MAX_ROWS = 10000;

    private final SuffixArraySecondaryIndex backend;
    private final ExtendedFilter filter;

    private final Operation operationTree;

    public QueryPlan(SuffixArraySecondaryIndex backend, ExtendedFilter filter)
    {
        this.backend = backend;
        this.filter = filter;
        this.operationTree = analyze();
    }

    /**
     * Converts list of IndexExpressions' in polish notation into
     * operation tree preserving priority.
     *
     * Operation tree allows us to do a couple of important optimizations
     * namely, group flattening for AND operations (query rewrite), expression bounds checks,
     * satisfies by checks for resulting rows with an early exit.
     *
     * @return root of the operations tree.
     */
    private Operation analyze()
    {
        AbstractType<?> comparator = backend.getBaseCfs().getComparator();

        Stack<Operation> operations = new Stack<>();
        Stack<IndexExpression> group = new Stack<>();

        boolean isOldFormat = true;
        for (IndexExpression e : filter.getClause())
        {
            if (e.isSetLogicalOp())
            {
                OperationType op = OperationType.valueOf(e.logicalOp.name());

                Operation sideL, sideR;

                switch (group.size())
                {
                    case 0: // both arguments come from the operations stack
                        sideL = operations.pop();
                        sideR = operations.pop();

                        Operation operation = new Operation(op, comparator);

                        /**
                         * If both underlying operations are the same we can improve performance
                         * and decrease work set size by combining operations under one AND/OR
                         * instead of making a tree, example:
                         *
                         *         AND                                 AND
                         *       /     \                            /   |   \
                         *     AND     AND              =>      name  age   height
                         *    /   \       \
                         * name:p  age:20  height:180
                         */
                        if (sideL.op == sideR.op)
                        {
                            operation.add(sideL.expressions);
                            operation.add(sideR.expressions);
                        }
                        else
                        {
                            operation.setLeft(sideL).setRight(sideR);
                        }

                        operations.push(operation);
                        break;

                    case 1: // one of the arguments is from group other is from already constructed operations stack
                        sideR = operations.pop();

                        /**
                         * As all of the AND operators are of the same priority
                         * we can do an optimization that gives us a lot of benefits on the execution side.
                         *
                         * We try to detect situation where two or more AND operations are linked together
                         * in the tree and pull all of their expressions up to the first seen AND statement,
                         * that helps expression analyzer to figure out proper bounds for the same field,
                         * as well as reduces number of SSTableIndex files we have to use to satisfy the
                         * query by means of primary expression analysis.
                         *
                         *         AND            AND              AND
                         *        /   \         /  |  \          /     \
                         *      AND   a<7  => f:x a>5 a<7  =>  f:x  5 < a < 7
                         *     /   \
                         *   f:x    a>5
                         */
                        if (op == OperationType.AND && op == sideR.op)
                        {
                            sideR.add(group.pop());
                            operations.push(sideR);
                        }
                        else
                        {
                            operation = new Operation(op, comparator, group.pop());
                            operation.setRight(sideR);

                            operations.push(operation);
                        }

                        break;

                    default: // all arguments come from group expressions
                        operations.push(new Operation(op, comparator, group.pop(), group.pop()));
                        break;
                }

                isOldFormat = false;
            }
            else
            {
                group.add(e);
            }
        }

        // we need to support old IndexExpression format without
        // logical expressions, so we assume that all of the columns
        // are linked with AND.
        if (isOldFormat)
        {
            Operation operation = new Operation(OperationType.AND, comparator);

            while (!group.empty())
                operation.add(group.pop());

            operations.push(operation);
        }

        Operation root = operations.pop();
        root.complete(backend);

        return root;
    }

    public List<Row> execute()
    {
        AbstractBounds<RowPosition> range = filter.dataRange.keyRange();

        final int maxRows = Math.min(filter.maxColumns(), Math.min(MAX_ROWS, filter.maxRows()));
        final List<Row> rows = new ArrayList<>(maxRows);

        operationTree.skipTo(((LongToken) range.left.getToken()).token);

        intersection:
        while (operationTree.hasNext())
        {
            for (DecoratedKey key : operationTree.next())
            {
                if (!range.contains(key) || rows.size() >= maxRows)
                    break intersection;

                Row row = getRow(key, filter);
                if (row != null && operationTree.satisfiedBy(row))
                    rows.add(row);
            }
        }

        return rows;
    }

    private Row getRow(DecoratedKey key, ExtendedFilter filter)
    {
        ColumnFamilyStore store = backend.getBaseCfs();
        ReadCommand cmd = ReadCommand.create(store.metadata.ksName,
                                             key.key,
                                             store.metadata.cfName,
                                             System.currentTimeMillis(),
                                             filter.columnFilter(key.key));

        return cmd.getRow(store.keyspace);
    }
}
