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
package org.apache.cassandra.udftest;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.apache.cassandra.cql3.CqlLexer;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.ErrorCollector;
import org.apache.cassandra.cql3.ErrorListener;
import org.apache.cassandra.cql3.statements.CreateFunctionStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;

public class UDFs
{
    public static UDF create(String cql) throws SyntaxException, InvalidRequestException
    {
        CharStream stream = new ANTLRStringStream(cql);
        CqlLexer lexer = new CqlLexer(stream);

        TokenStream tokenStream = new CommonTokenStream(lexer);
        CqlParser parser = new CqlParser(tokenStream);
        ErrorListener errorListener = new ErrorCollector(cql);
        parser.addErrorListener(errorListener);

        ParsedStatement statement;
        try
        {
            statement = parser.query();
        }
        catch (RuntimeException re)
        {
            throw new SyntaxException(String.format("Failed parsing statement: [%s] reason: %s %s",
                                                    cql,
                                                    re.getClass().getSimpleName(),
                                                    re.getMessage()));
        }
        catch (RecognitionException e)
        {
            throw new SyntaxException("Invalid or malformed CQL query string: " + e.getMessage());
        }
        if (!(statement instanceof CreateFunctionStatement))
            throw new IllegalArgumentException("Must pass a CREATE FUNCTION statement (was "+statement.getClass().getSimpleName()+")");

        return new UDF((CreateFunctionStatement) statement);
    }
}
