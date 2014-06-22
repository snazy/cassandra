/*
 *      Copyright (C) 2014 Robert Stupp, Koeln, Germany, robert-stupp.de
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class AlterSystemStatement extends ParsedStatement implements CQLStatement {

    public AlterSystemStatement(String cfgName, String cfgValue, String dc, String rack, String node) {

    }

    @Override public int getBoundTerms() {
        return 0;
    }

    @Override public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException {

    }

    @Override public void validate(ClientState state) throws RequestValidationException {

    }

    @Override public ResultMessage execute(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException {
        return new ResultMessage.Void();
    }

    @Override public ResultMessage executeInternal(QueryState state, QueryOptions options)
        throws RequestValidationException, RequestExecutionException {
        return new ResultMessage.Void();
    }

    @Override public Prepared prepare() throws RequestValidationException {
        return new Prepared(this);
    }
}
