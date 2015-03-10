package org.apache.cassandra.config;

import java.util.LinkedHashMap;

public class TransparentDataEncryptionOptions
{
    public boolean enabled = false;
    public int chunk_length_kb = 64;
    public String cipher = "AES/CBC/PKCS5Padding";
    public String key_alias;
    public int iv_length = 16;
    public String key_provider;
    public LinkedHashMap<String, ?> key_provider_parameters = new LinkedHashMap<>(0);

    public String getKeystore()
    {
        return get("keystore");
    }

    public String get(String key)
    {
        Object o = key_provider_parameters.get(key);
        if (o == null)
            throw new IllegalArgumentException(key + " not found in transparent_data_encryption options");
        return o.toString();
    }

    public String getKeystorePassword()
    {
        return get("keystore_password");
    }

    public String getKeystoreType()
    {
        return get("store_type");
    }
}
