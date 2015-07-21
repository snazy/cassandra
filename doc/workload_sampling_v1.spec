
                             Workload Recording v1

2. Overview

  Workload recordings contain frames received and submitted by a Cassandra daemon serialized to a frame.
  A recorded file may or may not contain all frames as the recording operation itself can be constainted.

  Each recorded file starts with a file header, followed by a dump of the schema and the prepared statements.
  After that, 0 or more native protocol frames prefixed with some meta information follow.

  The binary content of the frames in the workload recording is in the form as defined by the appropriate
  native protocol version, which is contained in the frame meta data. This spec makes no assumptions about
  the content of the native protocol frames, which is defined in the native procol spec for the native
  protocol version.

3. Notations

  To describe the binary layout of the workload recording, we define the following:

    [short]        A 2 bytes unsigned integer
    [int]          A 4 bytes integer
    [long]         A 8 bytes integer
    [double]       A 8 bytes float
    [boolean]      A 1 byte boolean
    [bytes]        A [int] n, followed by n bytes if n >= 0. If n < 0,
                   no byte should follow and the value represented is `null`.
    [string]       A [short] n, followed by n bytes representing an UTF-8
                   string.


        public static short MAGIC = (short) 0xCA55;
        public static short CURRENT_VERSION = (short) 0x0010;

        headerBuffer.writeShort(MAGIC); // some magic
        headerBuffer.writeShort(CURRENT_VERSION); // some version
        headerBuffer.writeLong(timestamp); // timestamp (millis)
        writeByteArray(headerBuffer, version.getBytes());
        writeByteArray(headerBuffer, broadcastAddress.getAddress());
        writeByteArray(headerBuffer, localAddress.getAddress());
        headerBuffer.writeInt(seconds);
        headerBuffer.writeInt(maxMBytes);
        headerBuffer.writeDouble(recordProbability);
        headerBuffer.writeBoolean(includeResponses);

        // write schema
        // for each keyspace
        //      int     OPCODE_KEYSPACE_DEFINITION
        //      for-each { table in orderedSchemaTables }
        //          int     number of rows of SELECT over keyspace
        //          for-each { row in rows }
        //              int     number of columns
        //              bytes   column name
        //              bytes   value (serialized using the protocol version for the current C* version)

        // write prepared statements
        // for each prepared statement:
        //      int     OPCODE_PREPARED_STATEMENT
        //      bytes   MD5 digest
        //      string  query string (CQL statement)


            // write workload frame header:
            // short    length of timestamp + remote port + remote address bytes
            // long     offset in nanoseconds since start of recording
            // short    remote port
            // bytes    remote address
            //
            // write native protocol header
            //
            // write native protocol body

5. References

  See native_protocol_v1,2,3,4.spec files for the specification of native protocol frames.

6. Changes

  None, initial version.
