package org.apache.cassandra.db.index.search;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.index.utils.LazyMergeSortIterator.OperationType;
import org.apache.cassandra.db.index.utils.TypeUtil;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Expression
{
    private static final Logger logger = LoggerFactory.getLogger(Expression.class);

    static enum Type
    {
        LOGICAL, COLUMN
    }

    protected final AbstractType<?> comparator;
    protected final Type type;

    private Expression(AbstractType<?> comparator, Type type)
    {
        this.comparator = comparator;
        this.type = type;
    }

    public Type getType()
    {
        return type;
    }

    public boolean isLogical()
    {
        return type == Type.LOGICAL;
    }

    public Column toColumnExpression()
    {
        assert type == Type.COLUMN;
        return (Column) this;
    }

    public Logical toLogicalExpression()
    {
        assert type == Type.LOGICAL;
        return (Logical) this;
    }

    public static class Logical extends Expression
    {
        public final OperationType op;

        public Logical(OperationType op)
        {
            super(null, Type.LOGICAL);

            this.op = op;
        }

        @Override
        public String toString()
        {
            return String.format("Expression.Logical{op: %s}", op);
        }
    }

    public static class Column extends Expression
    {
        public final ByteBuffer name;
        public final AbstractType<?> validator;
        public final boolean isSuffix;

        public Bound lower, upper;
        public boolean isEquality;

        public Column(ByteBuffer name, AbstractType<?> comparator, AbstractType<?> validator)
        {
            super(comparator, Type.COLUMN);

            this.name = name;
            this.validator = validator;
            this.isSuffix = validator instanceof AsciiType || validator instanceof UTF8Type;
        }

        public void add(IndexOperator op, ByteBuffer value)
        {
            switch (op)
            {
                case EQ:
                    lower = new Bound(value, true);
                    upper = lower;
                    isEquality = true;
                    break;

                case LT:
                    upper = new Bound(value, false);
                    break;

                case LTE:
                    upper = new Bound(value, true);
                    break;

                case GT:
                    lower = new Bound(value, false);
                    break;

                case GTE:
                    lower = new Bound(value, true);
                    break;

            }
        }

        public boolean contains(ByteBuffer value)
        {
            if (!TypeUtil.isValid(value, validator))
            {
                int size = value.remaining();
                if ((value = TypeUtil.tryUpcast(value, validator)) == null)
                {
                    logger.error("Can't cast value for {} to size accepted by {}, value size is {} bytes.",
                                 comparator.getString(name),
                                 validator,
                                 size);
                    return false;
                }
            }

            if (lower != null)
            {
                // suffix check
                if (isSuffix)
                {
                    if (!ByteBufferUtil.contains(value, lower.value))
                        return false;
                }
                else
                {
                    // range or equals - (mainly) for numeric values
                    int cmp = validator.compare(lower.value, value);

                    // in case of EQ lower == upper
                    if (isEquality)
                        return cmp == 0;

                    if (cmp > 0 || (cmp == 0 && !lower.inclusive))
                        return false;
                }
            }

            if (upper != null && lower != upper)
            {
                // suffix check
                if (isSuffix)
                {
                    if (!ByteBufferUtil.contains(value, upper.value))
                        return false;
                }
                else
                {
                    // range - mainly for numeric values
                    int cmp = validator.compare(upper.value, value);
                    if (cmp < 0 || (cmp == 0 && !upper.inclusive))
                        return false;
                }
            }
            return true;
        }

        @Override
        public String toString()
        {
            return String.format("Expression.Column{name: %s, lower: %s, upper: %s}",
                                 UTF8Type.instance.getString(name),
                                 lower == null ? "null" : validator.getString(lower.value),
                                 upper == null ? "null" : validator.getString(upper.value));
        }
    }

    public static class Bound
    {
        public final ByteBuffer value;
        public final boolean inclusive;

        public Bound(ByteBuffer value, boolean inclusive)
        {
            this.value = value;
            this.inclusive = inclusive;
        }
    }
}
