package org.apache.cassandra.cql3;

public class LogicalRelation extends Relation
{
    public LogicalRelation()
    {}

    public LogicalRelation(LogicalOperator op)
    {
        setOp(op);
    }

    public void setOp(LogicalOperator op)
    {
        super.logicalOperator = op;
    }

    @Override
    public boolean isLogical()
    {
        return true;
    }

    @Override
    public boolean isMultiColumn()
    {
        return false;
    }
}
