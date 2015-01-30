package org.apache.cassandra.db.index.search.tokenization;

import java.io.IOException;
import java.io.Reader;

/**
 * Internal interface for supporting versioned grammars.
 */
public interface StandardTokenizerInterface
{

    public String getText();

    public char[] getArray();

    public byte[] getBytes();

    /**
     * Returns the current position.
     */
    public int yychar();

    /**
     * Returns the length of the matched text region.
     */
    public int yylength();

    /**
     * Resumes scanning until the next regular expression is matched,
     * the end of input is encountered or an I/O-Error occurs.
     *
     * @return      the next token, {@link #YYEOF} on end of stream
     * @exception   java.io.IOException  if any I/O-Error occurs
     */
    public int getNextToken() throws IOException;

    /**
     * Resets the scanner to read from a new input stream.
     * Does not close the old reader.
     *
     * All internal variables are reset, the old input stream
     * <b>cannot</b> be reused (internal buffer is discarded and lost).
     * Lexical state is set to <tt>ZZ_INITIAL</tt>.
     *
     * @param reader   the new input stream
     */
    public void yyreset(Reader reader);
}
