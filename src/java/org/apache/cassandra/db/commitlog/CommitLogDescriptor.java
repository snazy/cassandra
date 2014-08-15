/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.commitlog;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;

import org.apache.cassandra.net.MessagingService;

public class CommitLogDescriptor
{
    private static final String SEPARATOR = "-";
    private static final String FILENAME_PREFIX = "CommitLog" + SEPARATOR;
    static final String ENCRYPTED_TAG = "enc";
    private static final String FILENAME_EXTENSION = ".log";
    // match both legacy and new version of commitlogs Ex: CommitLog-12345.log and CommitLog-4-12345.log.
    private static final Pattern COMMIT_LOG_FILE_PATTERN = Pattern.compile(FILENAME_PREFIX + "((\\d+)(" + SEPARATOR + "\\d+)?(" + SEPARATOR + ENCRYPTED_TAG + ")?)" + FILENAME_EXTENSION);

    public static final int VERSION_12 = 2;
    public static final int VERSION_20 = 3;
    /**
     * Increment this number if there is a changes in the commit log disc layout or MessagingVersion changes.
     * Note: make sure to handle {@link #getMessagingVersion()}
     */
    public static final int current_version = VERSION_20;

    private final int version;
    public final long id;
    private final boolean encrypted;

    public CommitLogDescriptor(int version, long id, boolean encrypted)
    {
        this.version = version;
        this.id = id;
        this.encrypted = encrypted;
    }

    public CommitLogDescriptor(long id, boolean encrypted)
    {
        this(current_version, id, encrypted);
    }

    public static CommitLogDescriptor fromFileName(String name)
    {
        Matcher matcher;
        if (!(matcher = COMMIT_LOG_FILE_PATTERN.matcher(name)).matches())
            throw new RuntimeException("Cannot parse the version of the file: " + name);

        if (matcher.group(3) == null)
            throw new UnsupportedOperationException("Commitlog segment is too old to open; upgrade to 1.2.5+ first");

        long id = Long.parseLong(matcher.group(3).split(SEPARATOR)[1]);
        boolean encrypted = name.contains(ENCRYPTED_TAG);
        return new CommitLogDescriptor(Integer.parseInt(matcher.group(2)), id, encrypted);
    }

    public int getMessagingVersion()
    {
        switch (version)
        {
            case VERSION_12:
                return MessagingService.VERSION_12;
            case VERSION_20:
                return MessagingService.VERSION_20;
            default:
                throw new IllegalStateException("Unknown commitlog version " + version);
        }
    }

    public int getVersion()
    {
        return version;
    }

    public boolean isEncrypted()
    {
        return encrypted;
    }

    public String fileName()
    {
        String encTag = encrypted ? ENCRYPTED_TAG : null;
        String baseName = Joiner.on(SEPARATOR).skipNulls().join(FILENAME_PREFIX + version, id, encTag);
        return baseName + FILENAME_EXTENSION;
    }

    /**
     * @param   filename  the filename to check
     * @return true if filename could be a commit log based on it's filename
     */
    public static boolean isValid(String filename)
    {
        return COMMIT_LOG_FILE_PATTERN.matcher(filename).matches();
    }
}
