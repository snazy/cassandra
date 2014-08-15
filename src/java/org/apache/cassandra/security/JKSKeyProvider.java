package org.apache.cassandra.security;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.Key;
import java.security.KeyStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.TransparentDataEncryptionOptions;
import org.apache.cassandra.io.util.FileUtils;

/**
 * A {@code KeyProvider} that retrieves keys from a java keystore.
 */
public class JKSKeyProvider implements KeyProvider
{
    private final Logger logger = LoggerFactory.getLogger(JKSKeyProvider.class);

    private final KeyStore store;
    private final boolean isJceks;
    private final TransparentDataEncryptionOptions options;

    public JKSKeyProvider()
    {
        options = DatabaseDescriptor.getTransparentDataEncryptionOptions();
        logger.info("initializing keystore from file {}", options.getKeystore());
        FileInputStream inputStream = null;
        try
        {
            inputStream = new FileInputStream(options.getKeystore());
            store = KeyStore.getInstance(options.getKeystoreType());
            store.load(inputStream, options.getKeystorePassword().toCharArray());
            isJceks = store.getType().equalsIgnoreCase("jceks");
        }
        catch (Exception e)
        {
            throw new RuntimeException("couldn't load keystore", e);
        }
        finally
        {
            FileUtils.closeQuietly(inputStream);
        }
    }

    public Key getSecretKey(String keyAlias) throws IOException
    {
        // there's a lovely behavior with jceks files that all aliases are lower-cased
        if (isJceks)
            keyAlias = keyAlias.toLowerCase();

        Key key;
        try
        {
            key = store.getKey(keyAlias, options.getKeystorePassword().toCharArray());
        }
        catch (Exception e)
        {
            throw new IOException("unable to load key from keystore");
        }
        if (key == null)
            throw new IOException(String.format("key %s was not found in keystore", keyAlias));
        return key;
    }
}
