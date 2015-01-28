package org.apache.cassandra.db.index.search;

/**
 * Object descriptor for suffix array index files. Similar to, and based upon, the sstable descriptor.
 *
 * TODO move the file name/manipulations into this class (from SASI/OnDiskSA...)
 */
public class Descriptor
{
    public static final String current_version = "aa";

    public static class Version
    {
        public final String version;

        public Version(String version)
        {
            this.version = version;
        }
    }

    public final Version version;

    public Descriptor(String v)
    {
        this.version = new Version(v);
    }
}
