package org.apache.cassandra.db.index.search.plan;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.index.SuffixArraySecondaryIndex;
import org.apache.cassandra.db.index.SuffixArraySecondaryIndex.IndexMode;
import org.apache.cassandra.db.index.SuffixArraySecondaryIndex.SAView;
import org.apache.cassandra.db.index.search.SSTableIndex;
import org.apache.cassandra.db.index.search.SuffixIterator;
import org.apache.cassandra.db.index.search.analyzer.NoOpAnalyzer;
import org.apache.cassandra.db.index.search.container.TokenTree.Token;
import org.apache.cassandra.db.index.search.memory.IndexMemtable;
import org.apache.cassandra.db.index.search.analyzer.AbstractAnalyzer;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator;
import org.apache.cassandra.db.index.utils.LazyMergeSortIterator.OperationType;
import org.apache.cassandra.db.index.utils.SkippableIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.utils.Pair;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Operation
{
    private static final Logger logger = LoggerFactory.getLogger(Operation.class);

    protected final OperationType op;
    protected final AbstractType<?> comparator;
    protected final List<IndexExpression> expressions;

    protected List<Expression> analyzed;

    protected Operation left, right;

    public Operation(OperationType operation, AbstractType<?> comparator, IndexExpression... columns)
    {
        this.op = operation;
        this.comparator = comparator;
        this.expressions = new ArrayList<>();
        Collections.addAll(expressions, columns);
    }

    public Operation setRight(Operation operation)
    {
        this.right = operation;
        return this;
    }

    public Operation setLeft(Operation operation)
    {
        this.left = operation;
        return this;
    }

    public void add(IndexExpression e)
    {
        expressions.add(e);
    }

    /**
     * Recursive "satisfies" checks based on operation
     * and data from the lower level members using depth-first search
     * and bubbling the results back to the top level caller.
     *
     * Most of the work here is done by {@link #localSatisfiedBy(Row)}
     * see it's comment for details, if there are no local expressions
     * assigned to Operation it will call satisfiedBy(Row) on it's children.
     *
     * Query: first_name = X AND (last_name = Y OR address = XYZ AND street = IL AND city = C) OR (state = 'CA' AND country = 'US')
     * Row: key1: (first_name: X, last_name: Z, address: XYZ, street: IL, city: C, state: NY, country:US)
     *
     * #1                       OR
     *                        /    \
     * #2       (first_name) AND   AND (state, country)
     *                          \
     * #3            (last_name) OR
     *                             \
     * #4                          AND (address, street, city)
     *
     *
     * Evaluation of the key1 is top-down depth-first search:
     *
     * --- going down ---
     * Level #1 is evaluated, OR expression has to pull results from it's children which are at level #2 and OR them together,
     * Level #2 AND (state, country) could be be evaluated right away, AND (first_name) refers to it's "right" child from level #3
     * Level #3 OR (last_name) requests results from level #4
     * Level #4 AND (address, street, city) does logical AND between it's 3 fields, returns result back to level #3.
     * --- bubbling up ---
     * Level #3 computes OR between AND (address, street, city) result and it's "last_name" expression
     * Level #2 computes AND between "first_name" and result of level #3, AND (state, country) which is already computed
     * Level #1 does OR between results of AND (first_name) and AND (state, country) and returns final result.
     *
     * @param row The row to check.
     * @return true if give Row satisfied all of the expressions in the tree,
     *         false otherwise.
     */
    public boolean satisfiedBy(Row row)
    {
        boolean sideL, sideR;

        if (analyzed == null)
        {
            sideL = left != null && left.satisfiedBy(row);
            sideR = right != null && right.satisfiedBy(row);
        }
        else
        {
            sideL = localSatisfiedBy(row);

            // if there is no right it means that this expression
            // is last in the sequence, we can just return result from local expressions
            if (right == null)
                return sideL;

            sideR = right.satisfiedBy(row);
        }

        return op.apply(sideL, sideR);
    }

    /**
     * Check every expression in the analyzed list to figure out if the
     * columns in the give row match all of the based on the operation
     * set to the current operation node.
     *
     * The algorithm is as follows: for every given expression from analyzed
     * list get corresponding column from the Row:
     *   - apply {@link Expression#contains(ByteBuffer)}
     *     method to figure out if it's satisfied;
     *   - apply logical operation between boolean accumulator and current boolean result;
     *   - if result == false and node's operation is AND return right away;
     *
     * After all of the expressions have been evaluated return resulting accumulator variable.
     *
     * Example:
     *
     * Operation = (op: AND, columns: [first_name = p, 5 < age < 7, last_name: y])
     * Row = (first_name: pavel, last_name: y, age: 6, timestamp: 15)
     *
     * #1 get "first_name" = p (expressions)
     *      - row-get "first_name"                      => "pavel"
     *      - compare "pavel" against "p"               => true (current)
     *      - set accumulator current                   => true (because this is expression #1)
     *
     * #2 get "last_name" = y (expressions)
     *      - row-get "last_name"                       => "y"
     *      - compare "y" against "y"                   => true (current)
     *      - set accumulator to accumulator & current  => true
     *
     * #3 get 5 < "age" < 7 (expressions)
     *      - row-get "age"                             => "6"
     *      - compare 5 < 6 < 7                         => true (current)
     *      - set accumulator to accumulator & current  => true
     *
     * #4 return accumulator => true (row satisfied all of the conditions)
     *
     * @param row The row to check.
     * @return true if give Row satisfied all of the analyzed expressions,
     *         false otherwise.
     */
    private boolean localSatisfiedBy(Row row)
    {
        if (row == null || row.cf == null || row.cf.isMarkedForDelete())
            return false;

        final long now = System.currentTimeMillis();

        boolean result = false;

        for (int i = 0; i < analyzed.size(); i++)
        {
            Expression expression = analyzed.get(i);

            Column column = row.cf.getColumn(expression.name);
            if (column == null)
                throw new IllegalStateException("All indexed columns should be included into the column slice, missing: "
                                              + comparator.getString(expression.name));

            if (!column.isLive(now))
                return false;

            boolean current = expression.contains(column.value());

            if (i == 0)
            {
                result = current;
                continue;
            }

            result = op.apply(result, current);

            // exit early because we already got a single false
            if (op == OperationType.AND && !result)
                return false;
        }

        return result;
    }

    public SkippableIterator<Long, Token> complete(SuffixArraySecondaryIndex backend)
    {
        List<SkippableIterator<Long, Token>> iterators = new ArrayList<>(2);

        if (!expressions.isEmpty())
        {
            analyzed = analyzeGroup(backend, comparator, op, expressions);

            iterators.add(getMergeIterator(backend, op, analyzed));
            if (right != null)
                iterators.add(right.complete(backend));
        }
        else
        {
            if (left != null)
                iterators.add(left.complete(backend));

            if (right != null)
                iterators.add(right.complete(backend));
        }

        return new LazyMergeSortIterator<>(op, iterators);
    }

    @VisibleForTesting
    protected static List<Expression> analyzeGroup(SuffixArraySecondaryIndex backend, AbstractType<?> comparator, OperationType op, List<IndexExpression> expressions)
    {
        Multimap<ByteBuffer, Expression> analyzed = HashMultimap.create();

        for (final IndexExpression e : expressions)
        {
            if (e.isSetLogicalOp())
                continue;

            ByteBuffer name = ByteBuffer.wrap(e.getColumn_name());
            Pair<ColumnDefinition, IndexMode> column = backend.getIndexDefinition(name);

            Collection<Expression> perColumn = analyzed.get(name);

            if (column == null)
            {
                ColumnDefinition nonIndexedColumn = backend.getBaseCfs().metadata.getColumnDefinition(name);

                if (nonIndexedColumn == null)
                {
                    logger.error("Requested column: " + comparator.getString(name) + ", wasn't found in the schema.");
                    continue;
                }

                column = Pair.create(nonIndexedColumn, new IndexMode(null, false, null, 0));
            }

            AbstractType<?> validator = column.left.getValidator();

            AbstractAnalyzer analyzer = getAnalyzer(column);
            analyzer.reset(ByteBuffer.wrap(e.getValue()));

            switch (e.getOp())
            {
                // '=' can have multiple expressions e.g. text = "Hello World",
                // becomes text = "Hello" AND text = "WORLD"
                // because "space" is always interpreted as a split point.
                case EQ:
                    while (analyzer.hasNext())
                    {
                        final ByteBuffer token = analyzer.next();
                        perColumn.add(new Expression(name, comparator, validator, analyzer, column.right.mode != null)
                        {{
                            add(e.op, token);
                        }});
                    }
                    break;

                // default means "range" or not-equals operator, combines both bounds together into the single expression,
                // iff operation of the group is AND, otherwise we are forced to create separate expressions,
                // not-equals is combined with the range iff operator is AND
                default:
                    Expression range;
                    if (perColumn.size() == 0 || op != OperationType.AND)
                        perColumn.add((range = new Expression(name, comparator, validator, analyzer, column.right.mode != null)));
                    else
                        range = Iterators.getLast(perColumn.iterator());

                    while (analyzer.hasNext())
                        range.add(e.op, analyzer.next());

                    break;
            }
        }

        List<Expression> result = new ArrayList<>();
        for (Map.Entry<ByteBuffer, Expression> e : analyzed.entries())
            result.add(e.getValue());

        return result;
    }

    private Pair<Expression, Set<SSTableIndex>> getPrimaryExpression(SuffixArraySecondaryIndex backend, List<Expression> expressions)
    {
        Expression expression = null;
        Set<SSTableIndex> primaryIndexes = null;

        for (Expression e : expressions)
        {
            if (!e.isIndexed)
                continue;

            SAView view = backend.getView(e.name);

            if (view == null)
                continue;

            Set<SSTableIndex> indexes = view.match(e);
            if (primaryIndexes == null || primaryIndexes.size() > indexes.size())
            {
                primaryIndexes = indexes;
                expression = e;
            }
        }

        return Pair.create(expression, primaryIndexes);
    }

    /**
     * Get merge iterator for given set of the pre-analyzed expressions.
     *
     * @param op The operation to join expressions on.
     * @param expressions The expression group to join.
     *
     * @return The joined iterator over data collected from given expressions.
     */
    private SkippableIterator<Long, Token> getMergeIterator(SuffixArraySecondaryIndex backend, OperationType op, List<Expression> expressions)
    {
        Expression primaryExpression;
        Set<SSTableIndex> primaryIndexes;

        final IndexMemtable currentMemtable = backend.getMemtable();

        // try to compute primary only for AND operation
        if (op == OperationType.OR)
        {
            primaryExpression = null;
            primaryIndexes = Collections.emptySet();
        }
        else
        {
            Pair<Expression, Set<SSTableIndex>> primary = getPrimaryExpression(backend, expressions);
            primaryExpression = primary.left;
            primaryIndexes = primary.right != null ? primary.right : Collections.<SSTableIndex>emptySet();
        }

        List<SkippableIterator<Long, Token>> unions = new ArrayList<>(expressions.size());
        for (Expression e : expressions)
        {
            // NO_EQ and non-index column query should only act as FILTER BY for satisfiedBy(Row) method
            // because otherwise it likely to go through the whole index.
            if (!e.isIndexed || e.getOp() == Expression.Op.NOT_EQ)
                continue;

            if (primaryExpression != null && primaryExpression.equals(e))
            {
                unions.add(new SuffixIterator(primaryExpression,
                                              currentMemtable.search(primaryExpression),
                                              primaryIndexes));
                continue;
            }

            Set<SSTableIndex> readers = new HashSet<>();
            SAView view = backend.getView(e.name);

            if (view != null && primaryIndexes.size() > 0)
            {
                for (SSTableIndex index : primaryIndexes)
                    readers.addAll(view.match(index.minKey(), index.maxKey()));
            }
            else if (view != null)
            {
                readers.addAll(view.match(e));
            }

            unions.add(new SuffixIterator(e, currentMemtable.search(e), readers));
        }

        return new LazyMergeSortIterator<>(op, unions);
    }

    private static AbstractAnalyzer getAnalyzer(Pair<ColumnDefinition, IndexMode> column)
    {
        if (column == null || column.right.mode == null)
            return new NoOpAnalyzer();

        AbstractAnalyzer analyzer = SuffixArraySecondaryIndex.getAnalyzer(column);
        analyzer.init(column.left.getIndexOptions(), column.left.getValidator());

        return analyzer;
    }
}
