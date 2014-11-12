package org.apache.cassandra.db.index.search;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;

import junit.framework.Assert;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;

import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

public class OnDiskSATest
{
    @Test
    public void testStringSAConstruction() throws Exception
    {
        Map<ByteBuffer, RoaringBitmap> data = new HashMap<ByteBuffer, RoaringBitmap>()
        {{
                put(UTF8Type.instance.decompose("scat"), bitMapOf(1));
                put(UTF8Type.instance.decompose("mat"), bitMapOf(2));
                put(UTF8Type.instance.decompose("fat"), bitMapOf(3));
                put(UTF8Type.instance.decompose("cat"), bitMapOf(1, 4));
                put(UTF8Type.instance.decompose("till"), bitMapOf(2, 6));
                put(UTF8Type.instance.decompose("bill"), bitMapOf(5));
                put(UTF8Type.instance.decompose("foo"), bitMapOf(7));
                put(UTF8Type.instance.decompose("bar"), bitMapOf(9, 10));
                put(UTF8Type.instance.decompose("michael"), bitMapOf(11, 12, 1));
        }};

        OnDiskSABuilder builder = new OnDiskSABuilder(UTF8Type.instance, OnDiskSABuilder.Mode.SUFFIX);
        for (Map.Entry<ByteBuffer, RoaringBitmap> e : data.entrySet())
            builder.add(e.getKey(), e.getValue());

        File index = File.createTempFile("on-disk-sa-string", "db");
        index.deleteOnExit();

        builder.finish(index);

        OnDiskSA onDisk = new OnDiskSA(index, UTF8Type.instance);

        // first check if we can find exact matches
        for (Map.Entry<ByteBuffer, RoaringBitmap> e : data.entrySet())
        {
            if (UTF8Type.instance.getString(e.getKey()).equals("cat"))
                continue; // cat is embedded into scat, we'll test it in next section

            Assert.assertEquals(e.getValue(), onDisk.search(e.getKey()));
        }

        // check that cat returns positions for scat & cat
        Assert.assertEquals(RoaringBitmap.bitmapOf(1, 4), onDisk.search(UTF8Type.instance.fromString("cat")));

        // random suffix queries
        Assert.assertEquals(RoaringBitmap.bitmapOf(9, 10), onDisk.search(UTF8Type.instance.fromString("ar")));
        Assert.assertEquals(RoaringBitmap.bitmapOf(1, 2, 3, 4), onDisk.search(UTF8Type.instance.fromString("at")));
        Assert.assertEquals(RoaringBitmap.bitmapOf(1, 11, 12), onDisk.search(UTF8Type.instance.fromString("mic")));
        Assert.assertEquals(RoaringBitmap.bitmapOf(1, 11, 12), onDisk.search(UTF8Type.instance.fromString("ae")));
        Assert.assertEquals(RoaringBitmap.bitmapOf(2, 5, 6), onDisk.search(UTF8Type.instance.fromString("ll")));
        Assert.assertEquals(RoaringBitmap.bitmapOf(1, 2, 5, 6, 11, 12), onDisk.search(UTF8Type.instance.fromString("l")));
        Assert.assertEquals(RoaringBitmap.bitmapOf(7), onDisk.search(UTF8Type.instance.fromString("oo")));
        Assert.assertEquals(RoaringBitmap.bitmapOf(7), onDisk.search(UTF8Type.instance.fromString("o")));
        Assert.assertEquals(RoaringBitmap.bitmapOf(1, 2, 3, 4), onDisk.search(UTF8Type.instance.fromString("t")));

        onDisk.close();
    }

    @Test
    public void testIntegerSAConstruction() throws Exception
    {
        final Map<ByteBuffer, RoaringBitmap> data = new HashMap<ByteBuffer, RoaringBitmap>()
        {{
                put(Int32Type.instance.decompose(5), bitMapOf(1));
                put(Int32Type.instance.decompose(7), bitMapOf(2));
                put(Int32Type.instance.decompose(1), bitMapOf(3));
                put(Int32Type.instance.decompose(3), bitMapOf(1, 4));
                put(Int32Type.instance.decompose(8), bitMapOf(2, 6));
                put(Int32Type.instance.decompose(10), bitMapOf(5));
                put(Int32Type.instance.decompose(6), bitMapOf(7));
                put(Int32Type.instance.decompose(4), bitMapOf(9, 10));
                put(Int32Type.instance.decompose(0), bitMapOf(11, 12, 1));
        }};

        OnDiskSABuilder builder = new OnDiskSABuilder(Int32Type.instance, OnDiskSABuilder.Mode.ORIGINAL);
        for (Map.Entry<ByteBuffer, RoaringBitmap> e : data.entrySet())
            builder.add(e.getKey(), e.getValue());

        File index = File.createTempFile("on-disk-sa-int", "db");
        index.deleteOnExit();

        builder.finish(index);

        OnDiskSA onDisk = new OnDiskSA(index, Int32Type.instance);

        for (Map.Entry<ByteBuffer, RoaringBitmap> e : data.entrySet())
            Assert.assertEquals(e.getValue(), onDisk.search(e.getKey()));

        List<ByteBuffer> sortedNumbers = new ArrayList<ByteBuffer>()
        {{
                for (ByteBuffer num : data.keySet())
                    add(num);
        }};

        Collections.sort(sortedNumbers, new Comparator<ByteBuffer>()
        {
            @Override
            public int compare(ByteBuffer a, ByteBuffer b)
            {
                return Int32Type.instance.compare(a, b);
            }
        });


        // test full iteration
        int idx = 0;
        for (OnDiskSA.DataSuffix suffix : onDisk)
        {
            ByteBuffer number = sortedNumbers.get(idx++);
            Assert.assertEquals(number, suffix.getSuffix());
            Assert.assertEquals(data.get(number), suffix.getKeys());
        }

        // test partial iteration
        idx = 3; // start from the 3rd element
        Iterator<OnDiskSA.DataSuffix> partialIter = onDisk.iteratorAt(sortedNumbers.get(idx));
        while (partialIter.hasNext())
        {
            OnDiskSA.DataSuffix suffix = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx++);

            Assert.assertEquals(number, suffix.getSuffix());
            Assert.assertEquals(data.get(number), suffix.getKeys());
        }

        onDisk.close();
    }

    private static RoaringBitmap bitMapOf(int... values)
    {
        RoaringBitmap map = new RoaringBitmap();
        for (int v : values)
            map.add(v);
        return map;
    }

    @SuppressWarnings("unused")
    private static void printSA(OnDiskSA sa) throws IOException
    {
        int level = 0;
        for (OnDiskSA.PointerLevel l : sa.levels)
        {
            System.out.println(" !!!! level " + (level++) + " !!!! (blocks: " + Arrays.toString(l.blockOffsets) + ")");
            for (int i = 0; i < l.blockOffsets.length; i++)
            {
                System.out.println(" --- block " + i + " ---- ");
                OnDiskSA.PointerBlock block = l.getBlock(i);
                for (int j = 0; j < block.getElementsSize(); j++)
                {
                    OnDiskSA.PointerSuffix p = block.getElement(j);
                    System.out.printf("PointerSuffix(chars: %s, blockIdx: %d)%n", sa.comparator.compose(p.getSuffix()), p.getBlock());
                }
            }
        }

        System.out.println(" !!!!! data blocks !!!!! ");
        for (int i = 0; i < sa.dataLevel.blockOffsets.length; i++)
        {
            System.out.println(" --- block " + i + " ---- ");
            OnDiskSA.DataBlock block = sa.dataLevel.getBlock(i);
            for (int j = 0; j < block.getElementsSize(); j++)
            {
                OnDiskSA.DataSuffix p = block.getElement(j);
                System.out.printf("DataSuffix(chars: %s, keys: %s)%n", sa.comparator.compose(p.getSuffix()), p.getKeys());
            }
        }

        System.out.println(" ***** end of level printout ***** ");
    }
}
