package org.apache.cassandra.io;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.Checksum;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.TransparentDataEncryptionOptions;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.EncryptedSegmentWriter;
import org.apache.cassandra.db.commitlog.SegmentWriter;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.PureJavaCrc32;

public class EncryptedRandomAccessReaderTest extends SchemaLoader
{
    private static final Logger logger = LoggerFactory.getLogger(EncryptedRandomAccessReaderTest.class);
    private static final String KS_NAME = "KeyspaceForEncryption";
    private static final String CF_NAME = "enc_cf";

    private static File tmpDir;
    private static final AtomicInteger generation = new AtomicInteger();
    private final Checksum checksum = new PureJavaCrc32();
    private byte[] buffer = new byte[1028];

    @BeforeClass
    public static void loadSchema() throws IOException, ConfigurationException
    {
        System.setProperty("cassandra.config", "cassandra-enc.yaml");
        loadSchema(false);
        tmpDir = Files.createTempDirectory("commitlogs-").toFile();
    }

    @After
    public void cleanUp()
    {
        Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME).truncateBlocking();
    }

    @Test
    public void readHeader() throws IOException
    {
        File tmpFile = new File(tmpDir, new CommitLogDescriptor(generation.getAndIncrement(), true).fileName());
        EncryptedSegmentWriter esw = new EncryptedSegmentWriter(tmpFile, DatabaseDescriptor.getTransparentDataEncryptionOptions());
        esw.init();
        esw.close();
        new EncryptedRandomAccessReader(tmpFile);
    }

    @Test
    public void replayCommitLogFile() throws IOException, InterruptedException
    {
        File tmpFile = new File(tmpDir, new CommitLogDescriptor(generation.getAndIncrement(), true).fileName());
        TransparentDataEncryptionOptions tde = DatabaseDescriptor.getTransparentDataEncryptionOptions();
        int maxBufferSize = tde.chunk_length_kb * 10;

        EncryptedSegmentWriter esw = new EncryptedSegmentWriter(tmpFile, tde);
        esw.init();

        int writeCount = writeMutations(esw, Integer.MAX_VALUE, maxBufferSize);
        esw.close();
        int readCount = confirmCommitLog(tmpFile);
        Assert.assertEquals(writeCount, readCount);

        // now do the same thing, but recycle the file, and write significantly less data to the file
        esw = new EncryptedSegmentWriter(tmpFile, tde);
        esw.init();
        writeCount = writeMutations(esw, writeCount / 2, maxBufferSize);
        esw.close();
        readCount = confirmCommitLog(tmpFile);
        Assert.assertEquals(writeCount, readCount);
    }

    private static RowMutation newMutation(String primaryKey, String data, long timestamp)
    {
        RowMutation rm = new RowMutation(KS_NAME, AsciiType.instance.decompose(primaryKey));
        rm.add(CF_NAME, ByteBufferUtil.bytes("key"), AsciiType.instance.decompose("jason"), timestamp);
        return rm;
    }

    // write some data into the log, stopping either once we've hit the max dize for the file or the passed in limit
    private int writeMutations(EncryptedSegmentWriter esw, int maxWriteCount, int maxBufferSize) throws IOException
    {
        int writeCount = 0;
        Random rand = new Random();
        byte[] buf = new byte[rand.nextInt(maxBufferSize)];
        while (writeCount < maxWriteCount)
        {
            rand.nextBytes(buf);
            RowMutation rm = newMutation(String.valueOf(writeCount), Arrays.toString(buf), System.currentTimeMillis());
            long mutationSize = RowMutation.serializer.serializedSize(rm, MessagingService.current_version);
            if (mutationSize + SegmentWriter.ENTRY_OVERHEAD_SIZE > DatabaseDescriptor.getCommitLogSegmentSize())
                continue;

            if (esw.hasCapacityFor(mutationSize))
            {
                esw.write(rm, (int)mutationSize);
                writeCount++;
            }
            else
            {
                break;
            }
        }
        return writeCount;
    }

    // modified from CommitLogReplay.recover()
    int confirmCommitLog(File file) throws IOException
    {
        int mutationCount = 0;
        CommitLogDescriptor desc = CommitLogDescriptor.fromFileName(file.getName());
        Assert.assertTrue(desc.isEncrypted());

        EncryptedRandomAccessReader reader = new EncryptedRandomAccessReader(new File(file.getAbsolutePath()));

        assert reader.length() <= Integer.MAX_VALUE;
        int replayPosition = reader.getInitialOffset();
        reader.seek(replayPosition);

        /* read the logs populate RowMutation and apply */
        while (!reader.isEOF())
        {
            long claimedCRC32;
            int serializedSize;
            try
            {
                // any of the reads may hit EOF
                serializedSize = reader.readInt();
                if (serializedSize == CommitLog.END_OF_SEGMENT_MARKER)
                    break;

                // RowMutation must be at LEAST 10 bytes:
                // 3 each for a non-empty Keyspace and Key (including the
                // 2-byte length from writeUTF/writeWithShortLength) and 4 bytes for column count.
                // This prevents CRC by being fooled by special-case garbage in the file; see CASSANDRA-2128
                if (serializedSize < 10)
                {
                    logger.info("******* serialized size is less < 10 bytes :(");
                    break;
                }

                long claimedSizeChecksum = reader.readLong();
                checksum.reset();
                if (desc.getVersion() < CommitLogDescriptor.VERSION_20)
                    checksum.update(serializedSize);
                else
                    FBUtilities.updateChecksumInt(checksum, serializedSize);

                if (checksum.getValue() != claimedSizeChecksum)
                {
                    logger.info("******* CRC checksum for length size not matching");
                    break; // entry wasn't synced correctly/fully. that's
                    // ok.
                }

                if (serializedSize > buffer.length)
                    buffer = new byte[(int) (1.2 * serializedSize)];
                reader.readFully(buffer, 0, serializedSize);
                claimedCRC32 = reader.readLong();
            }
            catch (EOFException eof)
            {
                logger.info("******* a seemingly EOF", eof);
                break; // last CL entry didn't get completely written. that's ok.
            }

            checksum.update(buffer, 0, serializedSize);
            if (claimedCRC32 != checksum.getValue())
            {
                // this entry must not have been fsynced. probably the rest is bad too,
                // but just in case there is no harm in trying them (since we still read on an entry boundary)
                logger.info("******* CRC checksum for data not matching");
                continue;
            }

            /* deserialize the commit log entry */
            FastByteArrayInputStream bufIn = new FastByteArrayInputStream(buffer, 0, serializedSize);
            final RowMutation rm = RowMutation.serializer.deserialize(new DataInputStream(bufIn),
                                                                      desc.getMessagingVersion(),
                                                                      ColumnSerializer.Flag.LOCAL);
            mutationCount++;
        }

        return mutationCount;
    }
}
