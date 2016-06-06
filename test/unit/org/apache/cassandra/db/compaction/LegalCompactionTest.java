package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.service.StorageService;
import org.junit.Test;

public class LegalCompactionTest
{
    @Test(expected=IllegalStateException.class)
    public void aTest() throws IOException, ExecutionException, InterruptedException
    {
        StorageService.instance.forceKeyspaceCompaction(true, "bob", "der");
    }
    
    @Test(expected=IllegalStateException.class)
    public void anotherTest() throws IOException, ExecutionException, InterruptedException
    {
        StorageService.instance.forceKeyspaceCleanup("fake", "cool");  
    }
}
