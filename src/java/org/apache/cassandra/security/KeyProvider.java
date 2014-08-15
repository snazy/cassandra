package org.apache.cassandra.security;

import java.io.IOException;
import java.security.Key;

/**
 * Customizable key retrieval mechanism. Implementations should expect that retrieved keys will be cached.
 * Further, each key will be requested non-concurrently (that is, no stampeding herds for the same key), although
 * unique keys may be requested concurrently (unless you mark {@code getSecretKey} synchronized).
 */
public interface KeyProvider
{
    Key getSecretKey(String alias) throws IOException;
}
