package org.apache.cassandra.db.index.search.container;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.SuffixArraySecondaryIndex;
import org.apache.cassandra.db.index.search.OnDiskSABuilder;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.AbstractIterator;

public class TokenTreeBuilder {

    public static final int BLOCK_BYTES = 4096;
    public static final int BLOCK_HEADER_BYTES = 64;
    public static final int TOKENS_PER_BLOCK = 248; // TODO (jwest): calculate using other constants
    //public static final int TOKENS_PER_BLOCK = 2;

    // TODO (jwest): merge iterator would be better here but bc duplicate keys just use SortedMap.putAll? how can we coalesce duplicates?
    // TODO (jwest): use a comparator for DK
    private final SortedMap<DecoratedKey, long[]> tokens = new TreeMap<>(SuffixArraySecondaryIndex.TOKEN_COMPARATOR);
    private int numBlocks;


    private Node root;
    private InteriorNode rightmostParent;
    private Leaf leftmostLeaf;
    private Leaf rightmostLeaf;
    private long tokenCount = 0;
    private TokenRange treeTokenRange = new TokenRange();

    public TokenTreeBuilder(SortedMap<DecoratedKey, long[]> data)
    {
        add(data);
    }

    public void add(SortedMap<DecoratedKey, long[]> data)
    {
        // TODO (jwest): deal with collisions
        tokens.putAll(data);
    }

    public int serializedSize()
    {
        maybeBulkLoad();

        return numBlocks * BLOCK_BYTES;
    }

    public void write(DataOutput out) throws IOException
    {

        maybeBulkLoad();

        ByteBuffer blockBuffer = ByteBuffer.allocate(BLOCK_BYTES);
        Iterator<Node> levelIterator = root.levelIterator();
        long childBlockIndex = 1;

        while (levelIterator != null)
        {

            Node firstChild = null;
            while (levelIterator.hasNext())
            {
                Node block = levelIterator.next();

                if (firstChild == null && !block.isLeaf())
                    firstChild = ((InteriorNode) block).children.get(0);

                block.serialize(childBlockIndex, blockBuffer);
                flushBuffer(blockBuffer, out);

                childBlockIndex += block.childCount();
            }

            levelIterator = (firstChild == null) ? null : firstChild.levelIterator();
        }
    }

    public Iterator<Pair<DecoratedKey, long[]>> iterator()
    {
        maybeBulkLoad();
        return new TokenIterator(leftmostLeaf.levelIterator());
    }

    private void maybeBulkLoad()
    {
        if (root == null)
            bulkLoad();
    }

    private void flushBuffer(ByteBuffer buffer, DataOutput o) throws IOException
    {
        // seek to end of last block before flushing
        alignBuffer(buffer, BLOCK_BYTES);
        buffer.flip();
        ByteBufferUtil.write(buffer, o);
        buffer.clear();
    }

    private static void alignBuffer(ByteBuffer buffer, int blockSize)
    {
        long curPos = buffer.position();
        if ((curPos & (blockSize - 1)) != 0) // align on the block boundary if needed
            buffer.position((int) OnDiskSABuilder.align(curPos, blockSize));
    }

    private void bulkLoad()
    {
        tokenCount = tokens.size();
        treeTokenRange.setRange(stripDK(tokens.firstKey()), stripDK(tokens.lastKey()));
        numBlocks = 1;

        // special case the tree that only has a single block in it (so we don't create a useless root)
        if (tokenCount <= TOKENS_PER_BLOCK)
        {
            leftmostLeaf = new Leaf(tokens);
            rightmostLeaf = leftmostLeaf;
            root = leftmostLeaf;
        }
        else
        {
            root = new InteriorNode();
            rightmostParent = (InteriorNode) root;

            int i = 0;
            Leaf lastLeaf = null;
            DecoratedKey firstToken = tokens.firstKey();
            DecoratedKey finalToken = tokens.lastKey();
            DecoratedKey lastToken = null;
            for (DecoratedKey token : tokens.keySet())
            {
                if (i == 0 || (i % TOKENS_PER_BLOCK != 0 && i != (tokenCount - 1)))
                {
                    i++;
                    continue;
                }

                lastToken = token;
                Leaf leaf = (i != (tokenCount - 1) || token.equals(finalToken)) ?
                        new Leaf(tokens.subMap(firstToken, lastToken)) : new Leaf(tokens.tailMap(firstToken));

                if (i == TOKENS_PER_BLOCK)
                    leftmostLeaf = leaf;
                else
                    lastLeaf.next = leaf;

                rightmostParent.add(leaf);
                lastLeaf = leaf;
                rightmostLeaf = leaf;
                firstToken = lastToken;
                i++;
                numBlocks++;

                if (token.equals(finalToken))
                {
                    Leaf finalLeaf = new Leaf(tokens.tailMap(token));
                    lastLeaf.next = finalLeaf;
                    rightmostParent.add(finalLeaf);
                    rightmostLeaf = finalLeaf;
                    numBlocks++;
                }
            }

        }
    }

    // TODO (jwest): rename me
    private Long stripDK(DecoratedKey k)
    {
        return MurmurHash.hash2_64(k.key, k.key.position(), k.key.remaining(), 0);
    }

    private abstract class Node
    {
        protected InteriorNode parent;
        protected Node next;
        protected TokenRange tokenRange = new TokenRange();

        public abstract void serialize(long childBlockIndex, ByteBuffer buf);
        public abstract int childCount();
        public abstract int tokenCount();
        public abstract Long smallestToken();

        public Iterator<Node> levelIterator()
        {
            return new LevelIterator(this);
        }

        public boolean isLeaf()
        {
            return (this instanceof Leaf);
        }

        protected boolean isLastLeaf()
        {
            return this == rightmostLeaf;
        }

        protected boolean isRoot()
        {
            return this == root;
        }

        protected void serializeHeader(ByteBuffer buf)
        {
            Header header;
            if (isRoot())
                header = new RootHeader();
            else if (!isLeaf())
                header = new InteriorNodeHeader();
            else
                header = new LeafHeader();

            header.serialize(buf);
            alignBuffer(buf, BLOCK_HEADER_BYTES);
        }

        private abstract class Header
        {
            public void serialize(ByteBuffer buf)
            {
                buf.put(infoByte())                        // info byte
                        .putShort((short) (tokenCount())) // block token count
                        .putLong(tokenRange.minToken)      // min token in root block
                        .putLong(tokenRange.maxToken);     // max token in root block

            }

            protected abstract byte infoByte();
        }

        private class RootHeader extends Header
        {
            @Override
            public void serialize(ByteBuffer buf)
            {
                super.serialize(buf);
                buf.putLong(tokenCount)                    // total number of tokens in tree
                        .putLong(treeTokenRange.minToken)  // min token in tree
                        .putLong(treeTokenRange.maxToken);  // max token in tree
            }

            protected byte infoByte()
            {
                // if leaf, set leaf indicator and last leaf indicator (bits 0 & 1)
                // if not leaf, clear both bits
                return (byte) ((isLeaf()) ? 3 : 0);
            }
        }

        private class InteriorNodeHeader extends Header
        {
            // bit 0 (leaf indicator) & bit 1 (last leaf indicator) cleared
            protected byte infoByte()
            {
                return 0;
            }
        }

        // TODO (jwest): add collision information
        private class LeafHeader extends Header
        {
            // bit 0 set as leaf indicator
            // bit 1 set if this is last leaf of data
            protected byte infoByte()
            {
                byte infoByte = 1;
                infoByte |= (isLastLeaf()) ? (1 << 1) : 0; // TODO (jwest): dont hardcode indicator pos

                return infoByte;
            }
        }

    }

    private class Leaf extends Node
    {
        private SortedMap<DecoratedKey, long[]> tokens;

        // TODO (jwest): enforce toks length <= TOKENS_PER_BLOCK;
        Leaf(SortedMap<DecoratedKey, long[]> data)
        {
            tokenRange.setRange(stripDK(data.firstKey()), stripDK(data.lastKey()));
            tokens = data;
        }

        public Long largestToken()
        {
            return tokenRange.maxToken;
        }

        // TODO (jwest): deal with collisions
        public void serialize(long childBlockIndex, ByteBuffer buf)
        {
            serializeHeader(buf);
            serializeData(buf);
        }

        public int childCount()
        {
            return 0;
        }

        public int tokenCount()
        {
            return tokens.size();
        }

        public Long smallestToken()
        {
            return tokenRange.minToken;
        }

        public Iterator<Map.Entry<DecoratedKey, long[]>> tokenIterator()
        {
            return tokens.entrySet().iterator();
        }

        // TODO (jwest): deal w/ collisions
        // TODO (jwest): deal w/ offsets greater than Integer.MAX_VALUE
        private void serializeData(ByteBuffer buf)
        {
//            for (Long token : tokens.keySet())
//                buf.putLong(token);
            for (Map.Entry<DecoratedKey, long[]> entry : tokens.entrySet())
                buf.putInt(0)                                // entry info byte & unused collision pack bytes
                        .putLong(stripDK(entry.getKey()))
                        .putInt((int) entry.getValue()[0]);
        }

    }

    private class InteriorNode extends Node
    {
        private List<Long> tokens = new ArrayList<>(TOKENS_PER_BLOCK);
        private List<Node> children = new ArrayList<>(TOKENS_PER_BLOCK + 1);
        private int position = 0; // TODO (jwest): can get rid of this and use array size


        public void serialize(long childBlockIndex, ByteBuffer buf)
        {
            serializeHeader(buf);
            serializeTokens(buf);
            serializeChildOffsets(childBlockIndex, buf);
        }

        public int childCount()
        {
            return children.size();
        }

        public int tokenCount()
        {
            return tokens.size();
        }

        public Long smallestToken()
        {
            return tokens.get(0);
        }

        protected void add(Long token, InteriorNode leftChild, InteriorNode rightChild)
        {
            int pos = tokens.size();
            if (pos == TOKENS_PER_BLOCK)
            {
                InteriorNode sibling = split();
                sibling.add(token, leftChild, rightChild);

            }
            else {
                if (leftChild != null)
                    children.add(pos, leftChild);

                if (rightChild != null)
                {
                    children.add(pos + 1, rightChild);
                    rightChild.parent = this;
                }

                tokenRange.updateRange(token);
                tokens.add(pos, token);
            }
        }

        protected void add(Leaf node)
        {

            if (position == (TOKENS_PER_BLOCK + 1))
            {
                rightmostParent = split();
                rightmostParent.add(node);
            }
            else
            {

                node.parent = this;
                children.add(position, node);
                position++;

                // the first child is referenced only during bulk load. we don't take a value
                // to store into the tree, one is subtracted since position has already been incremented
                // for the next node to be added
                if (position - 1 == 0)
                    return;


                // tokens are inserted one behind the current position, but 2 is subtracted because
                // position has already been incremented for the next add
                Long smallestToken = node.smallestToken();
                tokenRange.updateRange(smallestToken);
                tokens.add(position - 2, smallestToken);
            }

        }

        protected InteriorNode split()
        {
            Pair<Long, InteriorNode> splitResult = splitBlock();
            Long middleValue = splitResult.left;
            InteriorNode sibling = splitResult.right;
            InteriorNode leftChild = null;

            // create a new root if necessary
            if (parent == null)
            {
                parent = new InteriorNode();
                root = parent;
                sibling.parent = parent;
                leftChild = this;
                numBlocks++;
            }

            parent.add(middleValue, leftChild, sibling);

            return sibling;
        }

        protected Pair<Long, InteriorNode> splitBlock()
        {

            // TODO (jwest): this only works when TOKENS_PER_BLOCK is even
            final int middlePosition = TOKENS_PER_BLOCK / 2;
            InteriorNode sibling = new InteriorNode();
            sibling.parent = parent;
            next = sibling;

            Long middleValue = tokens.get(middlePosition);

            for (int i = middlePosition; i < TOKENS_PER_BLOCK; i++)
            {
                if (i != TOKENS_PER_BLOCK && i != middlePosition)
                {
                    long token = tokens.get(i);
                    sibling.tokenRange.updateRange(token);
                    sibling.tokens.add(token);
                }

                Node child = children.get(i + 1);
                child.parent = sibling;
                sibling.children.add(child);
                sibling.position++;
            }

            for (int i = TOKENS_PER_BLOCK; i >= middlePosition; i--)
            {
                if (i != TOKENS_PER_BLOCK)
                    tokens.remove(i);

                if (i != middlePosition)
                    children.remove(i);
            }

            // TODO (jwest): avoid token iteration here
            tokenRange.setRange(smallestToken(), Collections.max(tokens));
            numBlocks++;

            return Pair.create(middleValue, sibling);
        }

        protected boolean isFull()
        {
            return (position >= TOKENS_PER_BLOCK + 1);
        }

        private void serializeTokens(ByteBuffer buf)
        {
            for (Long token : tokens)
                buf.putLong(token);
        }


        private void serializeChildOffsets(long childBlockIndex, ByteBuffer buf)
        {
            for (int i = 0; i < children.size(); i++)
                buf.putLong((childBlockIndex + i) * BLOCK_BYTES);
        }
    }

    public static class LevelIterator extends AbstractIterator<Node>
    {
        private Node currentNode;

        LevelIterator(Node first)
        {
            currentNode = first;
        }

        public Node computeNext()
        {
            if (currentNode == null)
                return endOfData();

            Node returnNode = currentNode;
            currentNode = returnNode.next;

            return returnNode;
        }


    }

    public static class TokenIterator extends AbstractIterator<Pair<DecoratedKey, long[]>>
    {
        private Iterator<Node> levelIterator;
        private Iterator<Map.Entry<DecoratedKey, long[]>> currentIterator;

        TokenIterator(Iterator<Node> level)
        {
            levelIterator = level;
            if (levelIterator.hasNext())
                currentIterator = ((Leaf) levelIterator.next()).tokenIterator();
        }

        @Override
        public Pair<DecoratedKey, long[]> computeNext()
        {
            if (currentIterator != null && currentIterator.hasNext())
            {
                Map.Entry<DecoratedKey, long[]> next = currentIterator.next();
                return Pair.create(next.getKey(), next.getValue());
            }
            else
            {
                if (!levelIterator.hasNext())
                    return endOfData();
                else
                {
                    currentIterator = ((Leaf) levelIterator.next()).tokenIterator();
                    return computeNext();
                }
            }

        }
    }
}