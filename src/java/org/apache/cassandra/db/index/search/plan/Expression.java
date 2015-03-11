package org.apache.cassandra.db.index.search.plan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.db.index.utils.TypeUtil;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Expression
{
    private static final Logger logger = LoggerFactory.getLogger(Expression.class);

    public enum Op
    {
        EQ, NOT_EQ, RANGE
    }

    public final ByteBuffer name;
    public final AbstractType<?> comparator, validator;
    public final boolean isSuffix;
    public final boolean isIndexed;

    @VisibleForTesting
    protected Op operation;

    public Bound lower, upper;
    public List<ByteBuffer> exclusions = new ArrayList<>();

    public Expression(ByteBuffer name, AbstractType<?> comparator, AbstractType<?> validator)
    {
        this(name, comparator, validator, true);
    }

    public Expression(ByteBuffer name, AbstractType<?> comparator, AbstractType<?> validator, boolean isIndexed)
    {
        this.name = name;
        this.comparator = comparator;
        this.validator = validator;
        this.isSuffix = validator instanceof AsciiType || validator instanceof UTF8Type;
        this.isIndexed = isIndexed;
    }

    public Expression(Expression other)
    {
        this(other.name, other.comparator, other.validator);
        operation = other.operation;
    }

    public Expression setLower(Bound newLower)
    {
        lower = newLower == null ? null : new Bound(newLower.value, newLower.inclusive);
        return this;
    }

    public Expression setUpper(Bound newUpper)
    {
        upper = newUpper == null ? null : new Bound(newUpper.value, newUpper.inclusive);
        return this;
    }

    public Expression setOp(Op op)
    {
        this.operation = op;
        return this;
    }

    public Expression add(IndexOperator op, ByteBuffer value)
    {
        boolean lowerInclusive = false, upperInclusive = false;
        switch (op)
        {
            case EQ:
                lower = new Bound(value, true);
                upper = lower;
                operation = Op.EQ;
                break;

            case NOT_EQ:
                // equals can't have exclusions, so
                // if we see that operation is already EQ and tries to add
                // an exclusion it is an analyzer bug
                assert operation != Op.EQ;

                // optimization for ranges with holes e.g.
                // a > 5 and a < 10 and a != 7
                exclusions.add(value);
                if (operation == null)
                    operation = Op.NOT_EQ;
                break;

            case LTE:
                upperInclusive = true;
            case LT:
                operation = Op.RANGE;
                upper = new Bound(value, upperInclusive);
                break;

            case GTE:
                lowerInclusive = true;
            case GT:
                operation = Op.RANGE;
                lower = new Bound(value, lowerInclusive);
                break;
        }

        return this;
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
                // range or (not-)equals - (mainly) for numeric values
                int cmp = validator.compare(lower.value, value);

                // in case of EQ lower == upper
                if (operation == Op.EQ)
                    return cmp == 0;

                if (operation == Op.NOT_EQ)
                    return cmp != 0;

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

        // as a last step let's check exclusion for the given field,
        // this covers both cases - a. NOT_EQ operation and b. RANGE with exclusions
        for (ByteBuffer term : exclusions)
        {
            if (isSuffix && ByteBufferUtil.contains(value, term))
                return false;
            else if (validator.compare(term, value) == 0)
                return false;
        }

        return true;
    }

    public Op getOp()
    {
        return operation;
    }

    @Override
    public String toString()
    {
        return String.format("Expression.Column{name: %s, op: %s, lower: (%s, %s), upper: (%s, %s)}",
                             name == null ? "null" : UTF8Type.instance.getString(name),
                             operation,
                             lower == null ? "null" : validator.getString(lower.value),
                             lower != null && lower.inclusive,
                             upper == null ? "null" : validator.getString(upper.value),
                             upper != null && upper.inclusive);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder().append(name)
                                    .append(operation)
                                    .append(validator)
                                    .append(lower).append(upper)
                                    .append(exclusions).build();
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof Expression))
            return false;

        if (this == other)
            return true;

        Expression o = (Expression) other;

        return Objects.equals(name, o.name)
                && validator.equals(o.validator)
                && operation == o.operation
                && Objects.equals(lower, o.lower)
                && Objects.equals(upper, o.upper)
                && exclusions.equals(o.exclusions);
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

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof Bound))
                return false;

            Bound o = (Bound) other;
            return value.equals(o.value) && inclusive == o.inclusive;
        }
    }
}
