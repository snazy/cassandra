package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.Hex;

/**
 * Custom {@code ICompressor} implementation that performs encryption in addition to compression.
 *
 * Compress then encrypt, or decrypt then decompress - that way you encrypt a smaller payload, instead of
 * compressing the encrypted array.
 */
public class EncryptingCompressor implements ICompressor
{
    private static final Logger logger = LoggerFactory.getLogger(EncryptingCompressor.class);

    // supported options to set on CF
    public static final String ALG = "cipher_algorithm";
    public static final String IV = "initialization_vector";
    public static final String KEY_VERSION = "key_alias";

    private final Cipher cipher;
    private boolean encrypting; //for paranoia checking
    private final ICompressor compressor;
    private WrappedArray wrappedArray;

    public EncryptingCompressor(Map<String, String> compressionOptions)
    {
        compressor = LZ4Compressor.create(compressionOptions);
        String alg = compressionOptions.get(ALG);
        assert alg != null : "no cipher algorithm declared";

        String keyAlias = compressionOptions.get(KEY_VERSION);
        assert keyAlias != null : "no key alias declared";

        // if the IV is present, it was written out during a previous sstable write;
        // thus we know we're decrypting. if the IV is null, therefore, we must encrypting
        String iv = compressionOptions.get(IV);
        try
        {
            if (iv != null)
            {
                cipher = DatabaseDescriptor.getCipherFactory().getDecryptor(alg, keyAlias, Hex.hexToBytes(iv));
                encrypting = false;
            }
            else
            {
                cipher = DatabaseDescriptor.getCipherFactory().getEncryptor(alg, keyAlias);
                encrypting = true;
            }
        }
        catch (IOException ioe)
        {
            throw new RuntimeException("unable to load cipher", ioe);
        }
    }

    public static EncryptingCompressor create(Map<String, String> compressionOptions)
    {
        return new EncryptingCompressor(compressionOptions);
    }

    public int initialCompressedBufferLength(int chunkLength)
    {
        int compressedInitialSize = compressor.initialCompressedBufferLength(chunkLength);
        return cipher.getOutputSize(compressedInitialSize);
    }

    public int compress(byte[] input, int inputOffset, int inputLength, WrappedArray output, int outputOffset) throws IOException
    {
        // a paranoia check to make sure "we haven't crossed the streams"
        assert encrypting : String.format("trying to perform action that is opposite to the init'ed cipher: request = true, cipher's state = %b", encrypting);

        //compress the data into the ctx's WrapperArray
        ensureWrapperArray(output.buffer.length - outputOffset);
        int compressedSize = compressor.compress(input, inputOffset, inputLength, wrappedArray, 0);

        // size the encryption output buffer properly
        int expectedCipherTextlen = cipher.getOutputSize(compressedSize);
        if (output.buffer.length - outputOffset < expectedCipherTextlen)
        {
            output.buffer = new byte[expectedCipherTextlen];
            outputOffset = 0;
        }

        // now encrypt into the argument WrapperArray
        int outputLen = 0;
        try
        {
            outputLen += cipher.doFinal(wrappedArray.buffer, 0, compressedSize, output.buffer, outputOffset);
        }
        catch (ShortBufferException sbe)
        {
            //should not happen as we size correctly based on the call to Cipher.getOutputSize()
            assert false : "failed to size buffer correctly, even though we asked the cipher for the correct size";
        }
        catch (IllegalBlockSizeException | BadPaddingException e)
        {
            throw new IOException("problem while encrypting a block", e);
        }
        return outputLen;
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        // a paranoia check to make sure "we haven't crossed the streams"
        assert !encrypting : String.format("trying to perform action that is opposite to the init'ed cipher: request = false, cipher's state = %b", encrypting);

        // size the encryption output buffer properly (member WrapperArray)
        ensureWrapperArray(cipher.getOutputSize(inputLength));

        //decrypt the data into the member WrapperArray
        int decryptSize = 0;
        try
        {
            decryptSize = cipher.doFinal(input, inputOffset, inputLength, wrappedArray.buffer, 0);
        }
        catch (ShortBufferException sbe)
        {
            //should not happen as we size correctly based on the call to Cipher.getOutputSize()
            assert false : "failed to size buffer correctly, even though we asked the cipher for the correct size";
        }
        catch (IllegalBlockSizeException | BadPaddingException e)
        {
            throw new IOException("problem while decrypting a block", e);
        }

        // hope like hell output buffer is large enough 'cuz we can't resize it (like with a WrapperArray)
        assert output.length - outputOffset >= wrappedArray.buffer.length :
            String.format("buffer to uncompress into is not large enough; buf size = {}, buf offset = {}, target size = {}",
                          output.length, outputOffset, wrappedArray.buffer.length);
        return compressor.uncompress(wrappedArray.buffer, 0, decryptSize, output, outputOffset);
    }

    private void ensureWrapperArray(int targetSize)
    {
        if (wrappedArray == null)
        {
            byte[] buf = new byte[targetSize];
            wrappedArray = new WrappedArray(buf);
        }
        else if (wrappedArray.buffer.length < targetSize)
        {
            wrappedArray.buffer = new byte[targetSize];
        }
    }

    public Set<String> supportedOptions()
    {
        //not sure if we should expose the compressor's supportedOptions()
        Set<String> opts = new HashSet<>();
        opts.add(ALG);
        opts.add(KEY_VERSION);
        opts.add(IV);
        return opts;
    }

    public Map<String, String> compressionParameters()
    {
        Map<String, String> map = new HashMap<>(1);
        map.put(IV, Hex.bytesToHex(cipher.getIV()));
        return map;
    }
}
