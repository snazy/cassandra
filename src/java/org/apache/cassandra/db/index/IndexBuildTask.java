package org.apache.cassandra.db.index;

import org.apache.cassandra.db.compaction.CompactionInfo;

public abstract class IndexBuildTask extends CompactionInfo.Holder
{
    public abstract void build();
}
