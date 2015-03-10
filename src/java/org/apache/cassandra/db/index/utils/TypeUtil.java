package org.apache.cassandra.db.index.utils;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.serializers.MarshalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TypeUtil
{
    private static final Logger logger = LoggerFactory.getLogger(TypeUtil.class);

    public static boolean isValid(ByteBuffer term, AbstractType<?> validator)
    {
        try
        {
            validator.validate(term);
            return true;
        }
        catch (MarshalException e)
        {
            return false;
        }
    }

    public static ByteBuffer tryUpcast(ByteBuffer term, AbstractType<?> validator)
    {
        if (term.remaining() == 0)
            return null;

        try
        {
            if (validator instanceof LongType)
            {
                long upcastToken;

                switch (term.remaining())
                {
                    case 2:
                        upcastToken = (long) term.getShort(term.position());
                        break;

                    case 4:
                        upcastToken = (long) Int32Type.instance.compose(term);
                        break;

                    default:
                        return null;
                }

                return LongType.instance.decompose(upcastToken);
            }
            else if (validator instanceof DoubleType)
            {
                if (term.remaining() != 4)
                    return null;

                return DoubleType.instance.decompose((double) FloatType.instance.compose(term));
            }
        }
        catch (Exception e)
        {
            logger.error("Unexpected error:", e);
            return null;
        }

        return null;
    }
}
