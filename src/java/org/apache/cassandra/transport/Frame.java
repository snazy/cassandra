
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
package org.apache.cassandra.transport;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.messages.ErrorMessage;

public class Frame
{
    public static final byte PROTOCOL_VERSION_MASK = 0x7f;

    public final Header header;
    public final ByteBuf body;

    /**
     * An on-wire frame consists of a header and a body.
     *
     * The header is defined the following way in native protocol version 3 and later:
     *
     *   0         8        16        24        32         40
     *   +---------+---------+---------+---------+---------+
     *   | version |  flags  |      stream       | opcode  |
     *   +---------+---------+---------+---------+---------+
     *   |                length                 |
     *   +---------+---------+---------+---------+
     */
    private Frame(Header header, ByteBuf body)
    {
        this.header = header;
        this.body = body;
    }

    public void retain()
    {
        body.retain();
    }

    public boolean release()
    {
        return body.release();
    }

    public static Frame create(Message.Type type, int streamId, int version, EnumSet<Header.Flag> flags, ByteBuf body)
    {
        Header header = new Header(version, flags, streamId, type);
        return new Frame(header, body);
    }

    public static final class Header
    {
        // 9 bytes in protocol version 3 and later
        public static final int LENGTH = 9;

        private static final int OFFSET_DIR_AND_VERSION = 0;
        private static final int OFFSET_FLAGS = 1;
        private static final int OFFSET_STREAM_ID = 2;
        private static final int OFFSET_TYPE = 4;
        private static final int OFFSET_BODY_LENGTH = 5;

        private final byte[] data = new byte[LENGTH];

        private Header(int version, int flags, int streamId, Message.Type type)
        {
            int off = 0;
            data[off++] = (byte) type.direction.addToVersion(version);
            data[off++] = (byte) flags;

            if (version >= Server.VERSION_3)
                data[off++] = (byte) (streamId >> 8);
            data[off++] = (byte) streamId;

            data[off++] = (byte) type.opcode;
            data[off++] = 0;
            data[off++] = 0;
            data[off++] = 0;
            data[off] = 0;
        }

        private Header(int version, EnumSet<Flag> flags, int streamId, Message.Type type)
        {
            this(version, Flag.serialize(flags), streamId, type);
        }

        public Header(ByteBuf buffer, int idx)
        {
            buffer.getBytes(idx, data);
        }

        public boolean containsFlag(Flag flag)
        {
            return (data[OFFSET_FLAGS] & flag.ordinal()) != 0;
        }

        public void addFlag(Flag flag)
        {
            data[OFFSET_FLAGS] |= flag.ordinal();
        }

        public Message.Type type()
        {
            return Message.Type.fromOpcode(data[OFFSET_TYPE], direction());
        }

        public Message.Direction direction()
        {
            return Message.Direction.extractFromVersion(data[OFFSET_DIR_AND_VERSION]);
        }

        public int version()
        {
            return data[OFFSET_DIR_AND_VERSION] & PROTOCOL_VERSION_MASK;
        }

        public int streamId()
        {
            return (data[OFFSET_STREAM_ID] & 0xff) << 8 | (data[OFFSET_STREAM_ID + 1] & 0xff);
        }

        public int bodyLength()
        {
            return ((data[OFFSET_BODY_LENGTH] << 24) & 0xff000000) |
                   ((data[OFFSET_BODY_LENGTH + 1] << 16) & 0xff0000) |
                   ((data[OFFSET_BODY_LENGTH + 2] << 8) & 0xff00) |
                   ((data[OFFSET_BODY_LENGTH + 3]) & 0xff);
        }

        public void bodyLength(int bodyLength)
        {
            data[OFFSET_BODY_LENGTH] = (byte) (bodyLength >> 24);
            data[OFFSET_BODY_LENGTH + 1] = (byte) (bodyLength >> 16);
            data[OFFSET_BODY_LENGTH + 2] = (byte) (bodyLength >> 8);
            data[OFFSET_BODY_LENGTH + 3] = (byte) bodyLength;
        }

        public String toString()
        {
            return "Header(direction=" + direction() +
                   ", type=" + type() +
                   ", flags=" + Flag.deserialize(data[OFFSET_FLAGS]) +
                   ", streamId=" + streamId() +
                   ", bodyLength=" + bodyLength();
        }

        public static enum Flag
        {
            // The order of that enum matters!!
            COMPRESSED,
            TRACING,
            CUSTOM_PAYLOAD,
            WARNING;

            private static final Flag[] ALL_VALUES = values();

            public static EnumSet<Flag> deserialize(int flags)
            {
                EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
                for (int n = 0; n < ALL_VALUES.length; n++)
                {
                    if ((flags & (1 << n)) != 0)
                        set.add(ALL_VALUES[n]);
                }
                return set;
            }

            public static int serialize(EnumSet<Flag> flags)
            {
                int i = 0;
                for (Flag flag : flags)
                    i |= 1 << flag.ordinal();
                return i;
            }
        }
    }

    public Frame with(ByteBuf newBody)
    {
        return new Frame(header, newBody);
    }

    public static class Decoder extends ByteToMessageDecoder
    {
        private static final int MAX_FRAME_LENGTH = DatabaseDescriptor.getNativeTransportMaxFrameSize();

        private boolean discardingTooLongFrame;
        private long tooLongFrameLength;
        private long bytesToDiscard;
        private int tooLongStreamId;

        private final Connection.Factory factory;

        public Decoder(Connection.Factory factory)
        {
            this.factory = factory;
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> results)
        throws Exception
        {
            if (discardingTooLongFrame)
            {
                bytesToDiscard = discard(buffer, bytesToDiscard);
                // If we have discarded everything, throw the exception
                if (bytesToDiscard <= 0)
                    fail();
                return;
            }

            int readableBytes = buffer.readableBytes();
            if (readableBytes == 0)
                return;

            int idx = buffer.readerIndex();

            // Check the first byte for the protocol version before we wait for a complete header.  Protocol versions
            // 1 and 2 use a shorter header, so we may never have a complete header's worth of bytes.
            int firstByte = buffer.getByte(idx);
            int version = firstByte & PROTOCOL_VERSION_MASK;
            if (version < Server.MIN_SUPPORTED_VERSION || version > Server.CURRENT_VERSION)
                throw new ProtocolException(String.format("Invalid or unsupported protocol version (%d); the lowest supported version is %d and the greatest is %d",
                                                          version, Server.MIN_SUPPORTED_VERSION, Server.CURRENT_VERSION));

            // Wait until we have the complete header
            if (readableBytes < Header.LENGTH)
                return;

            Header header = new Header(buffer, idx);
            idx += Header.LENGTH;

            // This throws a protocol exceptions if the opcode is unknown
            try
            {
                header.type();
            }
            catch (ProtocolException e)
            {
                throw ErrorMessage.wrap(e, header.streamId());
            }

            int bodyLength = header.bodyLength();
            int streamId = header.streamId();

            long frameLength = bodyLength + Header.LENGTH;
            if (frameLength > MAX_FRAME_LENGTH)
            {
                // Enter the discard mode and discard everything received so far.
                discardingTooLongFrame = true;
                tooLongStreamId = streamId;
                tooLongFrameLength = frameLength;
                bytesToDiscard = discard(buffer, frameLength);
                if (bytesToDiscard <= 0)
                    fail();
                return;
            }

            if (buffer.readableBytes() < frameLength)
                return;

            // extract body
            ByteBuf body;
            if (bodyLength > 0)
            {
                body = buffer.slice(idx, bodyLength);
                body.retain();
            }
            else
            {
                // empty buffer for empty request body
                body = Unpooled.EMPTY_BUFFER;
            }
            
            idx += bodyLength;
            buffer.readerIndex(idx);

            Connection connection = ctx.channel().attr(Connection.attributeKey).get();
            if (connection == null)
            {
                // First message seen on this channel, attach the connection object
                connection = factory.newConnection(ctx.channel(), version);
                ctx.channel().attr(Connection.attributeKey).set(connection);
            }
            else if (connection.getVersion() != version)
            {
                throw ErrorMessage.wrap(
                        new ProtocolException(String.format(
                                "Invalid message version. Got %d but previous messages on this connection had version %d",
                                version, connection.getVersion())),
                        streamId);
            }

            results.add(new Frame(header, body));
        }

        private void fail()
        {
            // Reset to the initial state and throw the exception
            long tooLongFrameLength = this.tooLongFrameLength;
            this.tooLongFrameLength = 0;
            discardingTooLongFrame = false;
            String msg = String.format("Request is too big: length %d exceeds maximum allowed length %d.", tooLongFrameLength,  MAX_FRAME_LENGTH);
            throw ErrorMessage.wrap(new InvalidRequestException(msg), tooLongStreamId);
        }
    }

    // How much remains to be discarded
    private static long discard(ByteBuf buffer, long remainingToDiscard)
    {
        int availableToDiscard = (int) Math.min(remainingToDiscard, buffer.readableBytes());
        buffer.skipBytes(availableToDiscard);
        return remainingToDiscard - availableToDiscard;
    }

    @ChannelHandler.Sharable
    public static class Encoder extends MessageToMessageEncoder<Frame>
    {
        public void encode(ChannelHandlerContext ctx, Frame frame, List<Object> results)
        throws IOException
        {
            frame.header.bodyLength(frame.body.readableBytes());
            results.add(Unpooled.wrappedBuffer(frame.header.data));
            results.add(frame.body);
        }
    }

    @ChannelHandler.Sharable
    public static class Decompressor extends MessageToMessageDecoder<Frame>
    {
        public void decode(ChannelHandlerContext ctx, Frame frame, List<Object> results)
        throws IOException
        {
            Connection connection = ctx.channel().attr(Connection.attributeKey).get();

            if (!frame.header.containsFlag(Header.Flag.COMPRESSED) || connection == null)
            {
                results.add(frame);
                return;
            }

            FrameCompressor compressor = connection.getCompressor();
            if (compressor == null)
            {
                results.add(frame);
                return;
            }

            results.add(compressor.decompress(frame));
        }
    }

    @ChannelHandler.Sharable
    public static class Compressor extends MessageToMessageEncoder<Frame>
    {
        public void encode(ChannelHandlerContext ctx, Frame frame, List<Object> results)
        throws IOException
        {
            Connection connection = ctx.channel().attr(Connection.attributeKey).get();

            // Never compress STARTUP messages
            if (frame.header.type() == Message.Type.STARTUP || connection == null)
            {
                results.add(frame);
                return;
            }

            FrameCompressor compressor = connection.getCompressor();
            if (compressor == null)
            {
                results.add(frame);
                return;
            }

            frame.header.addFlag(Header.Flag.COMPRESSED);
            results.add(compressor.compress(frame));
        }
    }
}
