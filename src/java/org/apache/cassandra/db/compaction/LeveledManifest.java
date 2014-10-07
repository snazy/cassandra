/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.Pair;

public class LeveledManifest implements CompactionManifest
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledManifest.class);

    /**
     * limit the number of L0 sstables we do at once, because compaction bloom filter creation
     * uses a pessimistic estimate of how many keys overlap (none), so we risk wasting memory
     * or even OOMing when compacting highly overlapping sstables
     */
    private static final int MAX_COMPACTING_L0 = 32;
    /**
     * If we go this many rounds without compacting
     * in the highest level, we start bringing in sstables from
     * that level into lower level compactions
     */
    private static final int NO_COMPACTION_LIMIT = 25;

    private final ColumnFamilyStore cfs;
    @VisibleForTesting
    protected final List<SSTableReader>[] generations;
    @VisibleForTesting
    protected final List<SSTableReader> unrepairedL0;
    private final RowPosition[] lastCompactedKeys;
    private final int maxSSTableSizeInBytes;
    public final int maxOverlappingLevel;
    private final SizeTieredCompactionStrategyOptions options;
    private boolean hasRepairedData = false;
    private final int [] compactionCounter;

    private LeveledManifest(ColumnFamilyStore cfs, int maxSSTableSizeInMB, int maxOverlappingLevel, SizeTieredCompactionStrategyOptions options)
    {
        this.cfs = cfs;
        this.hasRepairedData = cfs.getRepairedSSTables().size() > 0;
        this.maxSSTableSizeInBytes = maxSSTableSizeInMB * 1024 * 1024;
        this.maxOverlappingLevel = maxOverlappingLevel;
        this.options = options;

        // allocate enough generations for a PB of data, with a 1-MB sstable size.  (Note that if maxSSTableSize is
        // updated, we will still have sstables of the older, potentially smaller size.  So don't make this
        // dependent on maxSSTableSize.)
        int n = (int) Math.log10(1000 * 1000 * 1000);
        generations = new List[n];
        lastCompactedKeys = new RowPosition[n];
        for (int i = 0; i < generations.length; i++)
        {
            generations[i] = new ArrayList<>();
            lastCompactedKeys[i] = cfs.partitioner.getMinimumToken().minKeyBound();
        }
        unrepairedL0 = new ArrayList<>();
        compactionCounter = new int[n];
    }

    public static LeveledManifest create(ColumnFamilyStore cfs, int maxSSTableSize, int maxOverlappingLevel, List<SSTableReader> sstables)
    {
        return create(cfs, maxSSTableSize, maxOverlappingLevel, sstables, new SizeTieredCompactionStrategyOptions());
    }

    public static LeveledManifest create(ColumnFamilyStore cfs, int maxSSTableSize, int maxOverlappingLevel, Iterable<SSTableReader> sstables, SizeTieredCompactionStrategyOptions options)
    {
        LeveledManifest manifest = new LeveledManifest(cfs, maxSSTableSize, maxOverlappingLevel, options);

        // ensure all SSTables are in the manifest
        for (SSTableReader ssTableReader : sstables)
        {
            manifest.add(ssTableReader);
        }
        // Start at the first non-overlapping level
        for (int i = maxOverlappingLevel + 1; i < manifest.getAllLevelSize().length; i++)
        {
            manifest.repairOverlappingSSTables(i);
        }
        return manifest;
    }

    public synchronized void add(SSTableReader reader)
    {
        if (!hasRepairedData && reader.isRepaired())
        {
            // this is the first repaired sstable we get - we need to
            // rebuild the entire manifest, unrepaired data should be
            // in unrepairedL0. Note that we keep the sstable level in
            // the sstable metadata since we are likely to be able to
            // re-add it at a good level later (during anticompaction
            // for example).
            hasRepairedData = true;
            rebuildManifestAfterFirstRepair();
        }

        int level = reader.getSSTableLevel();
        if (hasRepairedData && !reader.isRepaired())
        {
            logger.debug("Adding unrepaired {} to unrepaired L0", reader);
            unrepairedL0.add(reader);
        }
        else
        {
            assert level < generations.length : "Invalid level " + level + " out of " + (generations.length - 1);
            logDistribution();
            if (canAddSSTable(reader))
            {
                // adding the sstable does not cause overlap in the level
                logger.debug("Adding {} to L{}", reader, level);
                generations[level].add(reader);
            }
            else
            {
                // this can happen if:
                // * a compaction has promoted an overlapping sstable to the given level, or
                // * we promote a non-repaired sstable to repaired at level > 0, but an ongoing compaction
                //   was also supposed to add an sstable at the given level.
                //
                // The add(..):ed sstable will be sent to level 0
                try
                {
                    reader.descriptor.getMetadataSerializer().mutateLevel(reader.descriptor, 0);
                    reader.reloadSSTableMetadata();
                }
                catch (IOException e)
                {
                    logger.error("Could not change sstable level - adding it at level 0 anyway, we will find it at restart.", e);
                }
                generations[0].add(reader);
            }
        }

    }


    /**
     * Since we run standard LCS when we have no repaired data
     * we need to move all sstables from the leveling
     * to unrepairedL0.
     */
    private void rebuildManifestAfterFirstRepair()
    {
        for (int i = 0; i < getAllLevelSize().length; i++)
        {
            List<SSTableReader> oldLevel = generations[i];
            generations[i] = new ArrayList<>();
            for (SSTableReader sstable : oldLevel)
                add(sstable);
        }
    }

    public synchronized void replace(Collection<SSTableReader> removed, Collection<SSTableReader> added)
    {
        assert !removed.isEmpty(); // use add() instead of promote when adding new sstables
        logDistribution();
        if (logger.isDebugEnabled())
            logger.debug("Replacing [{}]", toString(removed));

        // the level for the added sstables is the max of the removed ones,
        // plus one if the removed were all on the same level
        int minLevel = Integer.MAX_VALUE;

        for (SSTableReader sstable : removed)
        {
            int thisLevel = remove(sstable);
            minLevel = Math.min(minLevel, thisLevel);
        }

        // it's valid to do a remove w/o an add (e.g. on truncate)
        if (added.isEmpty())
            return;

        if (logger.isDebugEnabled())
            logger.debug("Adding [{}]", toString(added));

        for (SSTableReader ssTableReader : added)
            add(ssTableReader);
        lastCompactedKeys[minLevel] = SSTableReader.sstableOrdering.max(added).last;
    }

    public synchronized void repairOverlappingSSTables(int level)
    {
        SSTableReader previous = null;
        Collections.sort(generations[level], SSTableReader.sstableComparator);
        List<SSTableReader> outOfOrderSSTables = new ArrayList<SSTableReader>();
        for (SSTableReader current : generations[level])
        {
            if (previous != null && current.first.compareTo(previous.last) <= 0)
            {
                logger.warn(String.format("At level %d, %s [%s, %s] overlaps %s [%s, %s].  This could be caused by a bug in Cassandra 1.1.0 .. 1.1.3 or due to the fact that you have dropped sstables from another node into the data directory. " +
                                          "Sending back to L0.  If you didn't drop in sstables, and have not yet run scrub, you should do so since you may also have rows out-of-order within an sstable",
                                          level, previous, previous.first, previous.last, current, current.first, current.last));
                outOfOrderSSTables.add(current);
            }
            else
            {
                previous = current;
            }
        }

        if (!outOfOrderSSTables.isEmpty())
        {
            for (SSTableReader sstable : outOfOrderSSTables)
                sendBackToL0(sstable);
        }
    }

    /**
     * Checks if adding the sstable creates an overlap in the level
     * @param sstable the sstable to add
     * @return true if it is safe to add the sstable in the level.
     */
    private boolean canAddSSTable(SSTableReader sstable)
    {
        int level = sstable.getSSTableLevel();
        if (level <= maxOverlappingLevel)
            return true;

        List<SSTableReader> copyLevel = new ArrayList<>(generations[level]);
        copyLevel.add(sstable);
        Collections.sort(copyLevel, SSTableReader.sstableComparator);

        SSTableReader previous = null;
        for (SSTableReader current : copyLevel)
        {
            if (previous != null && current.first.compareTo(previous.last) <= 0)
                return false;
            previous = current;
        }
        return true;
    }

    private synchronized void sendBackToL0(SSTableReader sstable)
    {
        remove(sstable);
        try
        {
            sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 0);
            sstable.reloadSSTableMetadata();
            add(sstable);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not reload sstable meta data", e);
        }
    }

    public synchronized void repairStatusChanged(Collection<SSTableReader> sstables)
    {
        for(SSTableReader sstable : sstables)
        {
            remove(sstable);
            add(sstable);
        }
    }

    private String toString(Collection<SSTableReader> sstables)
    {
        StringBuilder builder = new StringBuilder();
        for (SSTableReader sstable : sstables)
        {
            builder.append(sstable.descriptor.cfname)
                   .append('-')
                   .append(sstable.descriptor.generation)
                   .append("(L")
                   .append(sstable.getSSTableLevel())
                   .append("), ");
        }
        return builder.toString();
    }

    @VisibleForTesting
    long maxBytesForLevel(int level)
    {
        if (level == 0)
            return 4L * maxSSTableSizeInBytes;
        double bytes = Math.pow(10, level) * maxSSTableSizeInBytes;
        if (bytes > Long.MAX_VALUE)
            throw new RuntimeException("At most " + Long.MAX_VALUE + " bytes may be in a compaction level; your maxSSTableSize must be absurdly high to compute " + bytes);
        return (long) bytes;
    }

    /**
     * @return highest-priority sstables to compact, and level to compact them to
     * If no compactions are necessary, will return null
     */
    public synchronized CompactionCandidate getCompactionCandidates()
    {
        // if we don't have any repaired data, continue as usual
        if (hasRepairedData)
        {
            Collection<SSTableReader> unrepairedMostInterresting = getSSTablesForSTCS(unrepairedL0);
            if (!unrepairedMostInterresting.isEmpty())
            {
                logger.info("Unrepaired data is most interresting, compacting {} sstables with STCS", unrepairedMostInterresting.size());
                for (SSTableReader reader : unrepairedMostInterresting)
                    assert !reader.isRepaired();
                return new CompactionCandidate(unrepairedMostInterresting, 0, Long.MAX_VALUE);
            }
        }

        // If overlapping is not enabled, need to start search at the highest generation instead of the lowest.
        if (maxOverlappingLevel == 0)
        {
            for (int i = generations.length - 1; i > 0; i--)
            {
                List<SSTableReader> sstables = getLevel(i);
                if (sstables.isEmpty())
                    continue; // mostly this just avoids polluting the debug log with zero scores
                // we want to calculate score excluding compacting ones
                Set<SSTableReader> sstablesInLevel = Sets.newHashSet(sstables);
                Set<SSTableReader> remaining = Sets.difference(sstablesInLevel, cfs.getDataTracker().getCompacting());
                double score = (double) SSTableReader.getTotalBytes(remaining) / (double) maxBytesForLevel(i);
                logger.debug("Compaction score for level {} is {}", i, score);

                if (score > 1.001) {
                    // before proceeding with a higher level, let's see if L0 is far enough behind to warrant STCS
                    if (!DatabaseDescriptor.getDisableSTCSInL0()
                            && getLevel(0).size() > MAX_COMPACTING_L0)
                    {
                        List<SSTableReader> mostInteresting = getSSTablesForSTCS(getLevel(0));
                        if (!mostInteresting.isEmpty())
                        {
                            logger.debug("L0 is too far behind, performing size-tiering there first");
                            return new CompactionCandidate(mostInteresting, 0, Long.MAX_VALUE);
                        }
                    }

                    Collection<SSTableReader> candidates = getCandidatesForUplevelCompaction(i);
                    if (!candidates.isEmpty())
                    {
                        int nextLevel = getNextLevel(candidates);
                        candidates = getOverlappingStarvedSSTables(nextLevel, candidates);
                        if (logger.isDebugEnabled())
                            logger.debug("Compaction candidates for L{} are {}", i, toString(candidates));
                        return new CompactionCandidate(candidates, nextLevel, cfs.getCompactionStrategy().getMaxSSTableBytes());
                    }
                    else
                    {
                        logger.debug("No compaction candidates for L{}", i);
                    }
                }
            }
        }
        else
        {
            for (int i = 0; i < generations.length; i++)
            {
                List<SSTableReader> sstables = getLevel(i);
                if (sstables.isEmpty())
                    continue; // mostly this just avoids polluting the debug log with zero scores
                // we want to calculate score excluding compacting ones
                Set<SSTableReader> sstablesInLevel = Sets.newHashSet(sstables);
                Set<SSTableReader> remaining = Sets.difference(sstablesInLevel, cfs.getDataTracker().getCompacting());
                double score = (double) SSTableReader.getTotalBytes(remaining) / (double)maxBytesForLevel(i);
                logger.debug("Compaction score for level {} is {}", i, score);

                if (score > 1.001)
                {
                    Collection<SSTableReader> candidates = getCandidatesForUplevelCompaction(i);
                    if (!candidates.isEmpty())
                    {
                        int nextLevel = getNextLevel(candidates);
                        candidates = getOverlappingStarvedSSTables(nextLevel, candidates);
                        if (logger.isDebugEnabled())
                            logger.debug("Compaction candidates for L{} are {}", i, toString(candidates));
                        return new CompactionCandidate(candidates, nextLevel, cfs.getCompactionStrategy().getMaxSSTableBytes());
                    }
                    else
                    {
                        logger.debug("No compaction candidates for L{}", i);
                    }
                }
            }

            // All levels are less than the maximum, so let's see if we should do a consolidating compaction in any
            // overlapping level
            int overlappingLevel= -1;
            double overlappingScore = 0.0;
            int sstableCountLevel = -1;
            double sstableCountScore = 0.0;
            for (int i = 0; i <= maxOverlappingLevel; i++)
            {
                double sscScore, olScore;
                {
                    // For the level, let's figure out what the number of files should (optimally) be
                    long optimalSSTableNumber = maxBytesForLevel(i) / maxSSTableSizeInBytes;
                    sscScore = ((double) getLevelSize(i)) / optimalSSTableNumber;
                    if (sscScore > sstableCountScore)
                    {
                        sstableCountScore = sscScore;
                        sstableCountLevel = i;
                    }
                }

                {
                    olScore = calculateOverlappingScore(i);
                    if (olScore > overlappingScore)
                    {
                        overlappingScore = olScore;
                        overlappingLevel = i;
                    }
                }
                logger.debug("Overlapping scores for L{}: sscScore {}, olScore {}", i, sscScore, olScore);
            }

            if (overlappingScore > 0)
            {
                Collection<SSTableReader> candidates = getCandidatesForSameLevelCompaction(overlappingLevel, MAX_COMPACTING_L0 * maxSSTableSizeInBytes);
                if (candidates.isEmpty())
                {
                    return null;
                }

                // We have to at least compact to L1
                int compactToLevel = 1;
                for (SSTableReader sstable: candidates)
                {
                    if (sstable.getSSTableLevel() > compactToLevel)
                        compactToLevel = sstable.getSSTableLevel();
                }
                return new CompactionCandidate(candidates, compactToLevel, cfs.getCompactionStrategy().getMaxSSTableBytes());
            }
            else if (sstableCountScore > 1.001)
            {
                Collection<SSTableReader> candidates = getCandidatesForUplevelCompaction(sstableCountLevel);
                if (candidates.isEmpty())
                {
                    return null;
                }

                return new CompactionCandidate(candidates, getNextLevel(candidates), cfs.getCompactionStrategy().getMaxSSTableBytes());
            }
        }
        return null;
    }

    private double calculateOverlappingScore(int level)
    {
        SortedSet<SSTableReader> readers = getLevelSorted(level, SSTableReader.sstableComparator);
        final Set<SSTableReader> compacting = cfs.getDataTracker().getCompacting();
        SSTableReader last = null;
        int overlaps = 0, possibleOverlaps = 0;
        for (SSTableReader reader: readers)
        {
            // We only want to count a possible overlap when the SSTable's in question are not being compacted.
            if (!overlapping(reader, compacting).isEmpty())
                continue;

            if (last != null)
            {
                if (last.last.compareTo(reader.first) < 0) {
                    overlaps++;
                }
            }
            last = reader;
            possibleOverlaps++;
        }
        double overlapScore = ((double)overlaps)/possibleOverlaps;
        overlapScore = Double.isNaN(overlapScore) ? 0.0 : overlapScore;
        if (logger.isDebugEnabled())
            logger.debug("L{} overlap score: {}", level, overlapScore);
        return overlapScore;
    }

    private List<SSTableReader> getSSTablesForSTCS(Collection<SSTableReader> sstables)
    {
        Iterable<SSTableReader> candidates = cfs.getDataTracker().getUncompactingSSTables(sstables);
        List<Pair<SSTableReader,Long>> pairs = SizeTieredCompactionStrategy.createSSTableAndLengthPairs(AbstractCompactionStrategy.filterSuspectSSTables(candidates));
        List<List<SSTableReader>> buckets = SizeTieredCompactionStrategy.getBuckets(pairs,
                                                                                    options.bucketHigh,
                                                                                    options.bucketLow,
                                                                                    options.minSSTableSize);
        return SizeTieredCompactionStrategy.mostInterestingBucket(buckets, 4, 32);
    }

    /**
     * If we do something that makes many levels contain too little data (cleanup, change sstable size) we will "never"
     * compact the high levels.
     *
     * This method finds if we have gone many compaction rounds without doing any high-level compaction, if so
     * we start bringing in one sstable from the highest level until that level is either empty or is doing compaction.
     *
     * @param targetLevel the level the candidates will be compacted into
     * @param candidates the original sstables to compact
     * @return
     */
    private Collection<SSTableReader> getOverlappingStarvedSSTables(int targetLevel, Collection<SSTableReader> candidates)
    {
        Set<SSTableReader> withStarvedCandidate = new HashSet<>(candidates);

        for (int i = generations.length - 1; i > 0; i--)
            compactionCounter[i]++;
        compactionCounter[targetLevel] = 0;
        if (logger.isDebugEnabled())
        {
            for (int j = 0; j < compactionCounter.length; j++)
                logger.debug("CompactionCounter: {}: {}", j, compactionCounter[j]);
        }

        for (int i = generations.length - 1; i > 0; i--)
        {
            if (getLevelSize(i) > 0)
            {
                if (compactionCounter[i] > NO_COMPACTION_LIMIT)
                {
                    // we try to find an sstable that is fully contained within  the boundaries we are compacting;
                    // say we are compacting 3 sstables: 0->30 in L1 and 0->12, 12->33 in L2
                    // this means that we will not create overlap in L2 if we add an sstable
                    // contained within 0 -> 33 to the compaction
                    RowPosition max = null;
                    RowPosition min = null;
                    for (SSTableReader candidate : candidates)
                    {
                        if (min == null || candidate.first.compareTo(min) < 0)
                            min = candidate.first;
                        if (max == null || candidate.last.compareTo(max) > 0)
                            max = candidate.last;
                    }
                    Set<SSTableReader> compacting = cfs.getDataTracker().getCompacting();
                    Range<RowPosition> boundaries = new Range<>(min, max);
                    for (SSTableReader sstable : getLevel(i))
                    {
                        Range<RowPosition> r = new Range<RowPosition>(sstable.first, sstable.last);
                        if (boundaries.contains(r) && !compacting.contains(sstable))
                        {
                            logger.info("Adding high-level (L{}) {} to candidates", sstable.getSSTableLevel(), sstable);
                            withStarvedCandidate.add(sstable);
                            return withStarvedCandidate;
                        }
                    }
                }
                return candidates;
            }
        }

        return candidates;
    }

    public synchronized int getLevelSize(int i)
    {
        if (i >= generations.length)
            throw new ArrayIndexOutOfBoundsException("Maximum valid generation is " + (generations.length - 1));
        return getLevel(i).size();
    }

    public synchronized int[] getAllLevelSize()
    {
        int[] counts = new int[generations.length];
        for (int i = 0; i < counts.length; i++)
            counts[i] = getLevel(i).size();
        return counts;
    }

    private void logDistribution()
    {
        if (logger.isDebugEnabled())
        {
            for (int i = 0; i < generations.length; i++)
            {
                if (!getLevel(i).isEmpty())
                {
                    logger.debug("L{} contains {} SSTables ({} bytes) in {}",
                                 i, getLevel(i).size(), SSTableReader.getTotalBytes(getLevel(i)), this);
                }
            }
        }
    }

    @VisibleForTesting
    public int remove(SSTableReader reader)
    {
        int level = reader.getSSTableLevel();
        assert level >= 0 : reader + " not present in manifest: "+level;
        generations[level].remove(reader);
        unrepairedL0.remove(reader);
        return level;
    }

    private static Set<SSTableReader> overlapping(Collection<SSTableReader> candidates, Iterable<SSTableReader> others)
    {
        assert !candidates.isEmpty();
        /*
         * Picking each sstable from others that overlap one of the sstable of candidates is not enough
         * because you could have the following situation:
         *   candidates = [ s1(a, c), s2(m, z) ]
         *   others = [ s3(e, g) ]
         * In that case, s2 overlaps none of s1 or s2, but if we compact s1 with s2, the resulting sstable will
         * overlap s3, so we must return s3.
         *
         * Thus, the correct approach is to pick sstables overlapping anything between the first key in all
         * the candidate sstables, and the last.
         */
        Iterator<SSTableReader> iter = candidates.iterator();
        SSTableReader sstable = iter.next();
        Token first = sstable.first.getToken();
        Token last = sstable.last.getToken();
        while (iter.hasNext())
        {
            sstable = iter.next();
            first = first.compareTo(sstable.first.getToken()) <= 0 ? first : sstable.first.getToken();
            last = last.compareTo(sstable.last.getToken()) >= 0 ? last : sstable.last.getToken();
        }
        return overlapping(first, last, others);
    }

    @VisibleForTesting
    static Set<SSTableReader> overlapping(SSTableReader sstable, Iterable<SSTableReader> others)
    {
        return overlapping(sstable.first.getToken(), sstable.last.getToken(), others);
    }

    /**
     * @return sstables from @param sstables that contain keys between @param start and @param end, inclusive.
     */
    private static Set<SSTableReader> overlapping(Token start, Token end, Iterable<SSTableReader> sstables)
    {
        assert start.compareTo(end) <= 0;
        Set<SSTableReader> overlapped = new HashSet<SSTableReader>();
        Bounds<Token> promotedBounds = new Bounds<Token>(start, end);
        for (SSTableReader candidate : sstables)
        {
            Bounds<Token> candidateBounds = new Bounds<Token>(candidate.first.getToken(), candidate.last.getToken());
            if (candidateBounds.intersects(promotedBounds))
                overlapped.add(candidate);
        }
        return overlapped;
    }

    private static final Predicate<SSTableReader> suspectP = new Predicate<SSTableReader>()
    {
        public boolean apply(SSTableReader candidate)
        {
            return candidate.isMarkedSuspect();
        }
    };

    /**
     * Select some of the candidate SSTables which can be used which are maximally overlapping
     *
     * Try to select up to maxCompactingBytes of SSTables to be compacted.
     * @param level              Level to look at when selecting the SSTables to compact
     * @param maxCompactingBytes Maximal number of bytes to include in this compaction
     * @return
     */
    private Collection<SSTableReader> getCandidatesForSameLevelCompaction(int level, int maxCompactingBytes)
    {
        assert level <= maxOverlappingLevel : "Cannot run a same level compaction for a level which is not overlapping.";
        logger.debug("Choosing candidates for same level at L{}", level);
        final Set<SSTableReader> compacting = cfs.getDataTracker().getCompacting();
        Set<SSTableReader> candidates = new HashSet<>();

        Collection<SSTableReader> readers = getLevelSorted(level, SSTableReader.sstableComparator);

        SSTableReader last = null;
        for (final SSTableReader newCandidate: readers)
        {
            if (last != null)
            {
                if (last.last.compareTo(newCandidate.first) < 0)
                {
                    if (!candidates.contains(last))
                    {
                        candidates.add(last);
                        maxCompactingBytes -= last.onDiskLength();
                    }
                    candidates.add(newCandidate);
                    maxCompactingBytes -= newCandidate.onDiskLength();
                }
            }
            if (maxCompactingBytes < 0)
                break;
            last = newCandidate;
        }

        if (level == maxOverlappingLevel)
        {
            if (candidates.isEmpty())
            {
                return Collections.emptyList();
            }

            Collection<SSTableReader> nonOverlappingLevel = getLevel(level + 1);

            candidates = Sets.union(candidates, overlapping(candidates, nonOverlappingLevel));
 
            if (!overlapping(candidates, compacting).isEmpty())
            {
                candidates = Collections.emptySet();
            }
        }
        return candidates;
    }

    /**
     * @return highest-priority sstables to compact for the given level.
     * If no compactions are possible (because of concurrent compactions or because some sstables are blacklisted
     * for prior failure), will return an empty list.  Never returns null.
     */
    private Collection<SSTableReader> getCandidatesForUplevelCompaction(int level)
    {
        assert !getLevel(level).isEmpty();
        logger.debug("Choosing candidates for up level compaction at L{}", level);

        final Set<SSTableReader> compacting = cfs.getDataTracker().getCompacting();

        // In the case of an overlapping level, we want to get the oldest SSTables to move up
        if (level <= maxOverlappingLevel)
        {
            Set<SSTableReader> compactingL0 = ImmutableSet.copyOf(Iterables.filter(getLevel(level), Predicates.in(compacting)));

            // When we have an overlapping level, we want to determine the smallest set of tables which should be
            // compacted together.
            //
            // L0 is the dumping ground for new sstables which thus may overlap each other.
            //
            // We treat L0 compactions specially:
            // 1a. add sstables to the candidate set until we have at least maxSSTableSizeInMB
            // 1b. prefer choosing older sstables as candidates, to newer ones
            // 1c. any L0 sstables that overlap a candidate, will also become candidates
            // 2. At most MAX_COMPACTING_L0 sstables from L0 will be compacted at once
            // 3. If total candidate size is less than maxSSTableSizeInMB, we won't bother compacting with L1,
            //    and the result of the compaction will stay in L0 instead of being promoted (see promote())
            //
            // Note that we ignore suspect-ness of L1 sstables here, since if an L1 sstable is suspect we're
            // basically screwed, since we expect all or most L0 sstables to overlap with each L1 sstable.
            // So if an L1 sstable is suspect we can't do much besides try anyway and hope for the best.
            Set<SSTableReader> candidates = new HashSet<SSTableReader>();
            Set<SSTableReader> remaining = new HashSet<SSTableReader>();
            Iterables.addAll(remaining, Iterables.filter(getLevel(level), Predicates.not(suspectP)));
            for (SSTableReader sstable : ageSortedSSTables(remaining))
            {
                if (candidates.contains(sstable))
                    continue;

                Sets.SetView<SSTableReader> overlappedL0 = Sets.union(Collections.singleton(sstable), overlapping(sstable, remaining));
                if (!Sets.intersection(overlappedL0, compactingL0).isEmpty())
                    continue;

                for (SSTableReader newCandidate : overlappedL0)
                {
                    // overlappedL0 could contain sstables that are not in compactingL0, but do overlap
                    // other sstables that are
                    if (overlapping(newCandidate, compactingL0).isEmpty())
                        candidates.add(newCandidate);
                    remaining.remove(newCandidate);
                }

                if (candidates.size() > MAX_COMPACTING_L0)
                {
                    // limit to only the MAX_COMPACTING_L0 oldest candidates
                    candidates = new HashSet<>(ageSortedSSTables(candidates).subList(0, MAX_COMPACTING_L0));
                    break;
                }
            }

            // leave everything in L0 if we didn't end up with a full sstable's worth of data
            if (SSTableReader.getTotalBytes(candidates) > maxSSTableSizeInBytes)
            {
                // add sstables from L1 that overlap candidates
                // if the overlapping ones are already busy in a compaction, leave it out.
                // TODO try to find a set of L0 sstables that only overlaps with non-busy L1 sstables
                if (level == maxOverlappingLevel)
                {
                    Set<SSTableReader> levelOverlapping = overlapping(candidates, getLevel(level + 1));
                    if (Sets.intersection(levelOverlapping, compacting).size() > 0)
                        return Collections.emptyList();
                    candidates = Sets.union(candidates, levelOverlapping);
                }
            }
            if (candidates.size() < 2)
                return Collections.emptyList();
            else
                return candidates;
        }

        // for non-L0 compactions, pick up where we left off last time
        Collections.sort(getLevel(level), SSTableReader.sstableComparator);
        int start = 0; // handles case where the prior compaction touched the very last range
        for (int i = 0; i < getLevel(level).size(); i++)
        {
            SSTableReader sstable = getLevel(level).get(i);
            if (sstable.first.compareTo(lastCompactedKeys[level]) > 0)
            {
                start = i;
                break;
            }
        }

        // look for a non-suspect keyspace to compact with, starting with where we left off last time,
        // and wrapping back to the beginning of the generation if necessary
        for (int i = 0; i < getLevel(level).size(); i++)
        {
            SSTableReader sstable = getLevel(level).get((start + i) % getLevel(level).size());
            Set<SSTableReader> candidates = Sets.union(Collections.singleton(sstable), overlapping(sstable, getLevel(level + 1)));
            if (Iterables.any(candidates, suspectP))
                continue;
            if (Sets.intersection(candidates, compacting).isEmpty())
                return candidates;
        }

        // all the sstables were suspect or overlapped with something suspect
        return Collections.emptyList();
    }

    private List<SSTableReader> ageSortedSSTables(Collection<SSTableReader> candidates)
    {
        List<SSTableReader> ageSortedCandidates = new ArrayList<SSTableReader>(candidates);
        Collections.sort(ageSortedCandidates, SSTableReader.maxTimestampComparator);
        return ageSortedCandidates;
    }

    @Override
    public String toString()
    {
        return "Manifest@" + hashCode();
    }

    public int getLevelCount()
    {
        for (int i = generations.length - 1; i >= 0; i--)
        {
            if (getLevel(i).size() > 0)
                return i;
        }
        return 0;
    }

    public synchronized SortedSet<SSTableReader> getLevelSorted(int level, Comparator<SSTableReader> comparator)
    {
        return ImmutableSortedSet.copyOf(comparator, getLevel(level));
    }

    public List<SSTableReader> getLevel(int i)
    {
        return generations[i];
    }

    public List<String> getSSTables(int level)
    {
        List<SSTableReader> generation = getLevel(level);
        List<String> sstables = new ArrayList<>(generation.size());
        for (int i = 0; i < generation.size(); i++)
        {
            String filename = generation.get(i).getFilename();
            sstables.add(i, filename.substring(filename.lastIndexOf('/') + 1));
        }
        return sstables;
    }

    public synchronized int getEstimatedTasks()
    {
        long tasks = 0;
        long[] estimated = new long[generations.length];

        for (int i = generations.length - 1; i >= 0; i--)
        {
            List<SSTableReader> sstables = getLevel(i);
            estimated[i] = Math.max(0L, SSTableReader.getTotalBytes(sstables) - maxBytesForLevel(i)) / maxSSTableSizeInBytes;
            tasks += estimated[i];
        }

        logger.debug("Estimating {} compactions to do for {}.{}",
                     Arrays.toString(estimated), cfs.keyspace.getName(), cfs.name);
        return Ints.checkedCast(tasks);
    }

    public int getNextLevel(Collection<SSTableReader> sstables)
    {
        int maximumLevel = Integer.MIN_VALUE;
        int minimumLevel = Integer.MAX_VALUE;
        for (SSTableReader sstable : sstables)
        {
            maximumLevel = Math.max(sstable.getSSTableLevel(), maximumLevel);
            minimumLevel = Math.min(sstable.getSSTableLevel(), minimumLevel);
        }

        int newLevel;
        if (minimumLevel == 0 && minimumLevel == maximumLevel && SSTableReader.getTotalBytes(sstables) < maxSSTableSizeInBytes)
        {
            newLevel = 0;
        }
        else
        {
            newLevel = minimumLevel == maximumLevel ? maximumLevel + 1 : maximumLevel;
            assert newLevel > 0;
        }
        return newLevel;

    }

    public boolean hasRepairedData()
    {
        return hasRepairedData;
    }

    public static class CompactionCandidate
    {
        public final Collection<SSTableReader> sstables;
        public final int level;
        public final long maxSSTableBytes;

        public CompactionCandidate(Collection<SSTableReader> sstables, int level, long maxSSTableBytes)
        {
            this.sstables = sstables;
            this.level = level;
            this.maxSSTableBytes = maxSSTableBytes;
        }
    }
}
