package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.config.DatabaseDescriptor;

public class EncryptedSegmentWriterTest
{
    private static File tmpDir;
    private static final AtomicInteger generation = new AtomicInteger();

    @BeforeClass
    public static void before() throws IOException
    {
        System.setProperty("cassandra.config", "cassandra-enc.yaml");
        tmpDir = Files.createTempDirectory("commitlogs-").toFile();
    }

    @Test
    public void bufferSizes() throws IOException
    {
        File logFile = new File(tmpDir, new CommitLogDescriptor(generation.getAndIncrement(), true).fileName());
        EncryptedSegmentWriter esw = new EncryptedSegmentWriter(logFile, DatabaseDescriptor.getTransparentDataEncryptionOptions());
        esw.init();
        ByteBuffer bb = esw.getBuffer();
        Assert.assertFalse(esw.hasCapacityFor(logFile.length() + 1));
        Assert.assertTrue(esw.hasCapacityFor(0));
        int previousSize = bb.capacity();

        // make sure buffer is reallocated to a larger size
        int minNewSize = previousSize * 2;
        Assert.assertTrue(esw.hasCapacityFor(minNewSize));
        bb = esw.getBuffer();
        Assert.assertTrue(bb.capacity() > minNewSize);

        byte val = 42;
        bb.put(0, val);

        // make sure the buffer was not flushed
        esw.hasCapacityFor(bb.capacity() - 1);
        Assert.assertEquals(val, bb.get(0));

        // make sure buffer *was* flushed (and reset)
        esw.hasCapacityFor(bb.capacity());
        byte val2 = (byte)(val + 1);
        bb.put(val2);
        Assert.assertFalse(val == bb.get(1));
    }
}
