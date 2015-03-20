package org.apache.cassandra.db.index.search;

/**
 * Object descriptor for suffix array index files. Similar to, and based upon, the sstable descriptor.
 *
 * TODO move the file name/manipulations into this class (from SASI/OnDiskSA...)
 */
public class Descriptor
{
    public static final String VERSION_AA = "aa";
    public static final String VERSION_AB = "ab";
    public static final String CURRENT_VERSION = VERSION_AB;
    public static final Descriptor CURRENT = new Descriptor(CURRENT_VERSION);

    public static class Version
    {
        public final String version;

        public Version(String version)
        {
            this.version = version;
        }

        public String toString()
        {
            return version;
        }
    }

    public final Version version;

    public Descriptor(String v)
    {
        this.version = new Version(v);
    }
}
