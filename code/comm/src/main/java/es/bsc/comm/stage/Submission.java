/*
 *  Copyright 2008-2016 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package es.bsc.comm.stage;

import es.bsc.comm.CommException;
import es.bsc.comm.Connection;
import es.bsc.comm.EventManager;
import es.bsc.comm.InternalConnection;
import es.bsc.comm.nio.NIOException;
import es.bsc.comm.nio.NIOProperties;
import es.bsc.comm.util.Serializer;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import java.util.LinkedList;
import java.util.List;

public class Submission extends Transfer {

    private static int tokens = NIOProperties.MAX_SENDS;
    private static LinkedList<InternalConnection> pausedConnections = new LinkedList<InternalConnection>();

    protected InputStream streamIn;
    private boolean hasToken = false;
    protected Token token;

    public Submission(Type type, byte[] array) {
        super(true);
        this.type = type;
        this.destination = Destination.ARRAY;
        this.array = array;
        this.remainingSize = 0;
    }

    // New object to send
    public Submission(Type type, Object o) {
        super(true);
        this.type = type;
        this.destination = Destination.OBJECT;
        this.object = o;
        this.remainingSize = 0;
    }

    // New file to send
    public Submission(String name) {
        super(true);
        this.type = Type.DATA;
        this.destination = Destination.FILE;
        this.fileName = name;
        this.remainingSize = 0;
    }

    // Set size for SEND
    private void setSize() {
        long length = 0;
        if (destination == Destination.OBJECT || destination == Destination.ARRAY) {
            length = (long) array.length;
        } else if (destination == Destination.FILE) {
            File f = new File(fileName);
            length = f.length();
        }
        setSize(length);
    }

    // Open the send stream
    private void openStream() {
        try {
            if (destination == Destination.FILE) {
                // Open a stream from the source file
                streamIn = (InputStream) new FileInputStream(fileName);
            } else {
                if (destination == Destination.OBJECT) {
                    array = Serializer.serialize(object);
                }
                streamIn = (InputStream) new ByteArrayInputStream(array);
            }

        } catch (IOException e) {
            LOGGER.error("Exception openning stream for " + this, e);
        }
    }

    // Read the total size of the buffer first
    public void loadFirstToken() throws IOException {
        int available = streamIn.available();
        int tokenSize = (int) Math.min(
                Math.min(
                        (long) Token.MAX_PAYLOAD,
                        (long) available + (long) HEADER_SIZE),
                remainingSize + HEADER_SIZE);
        byte[] content = new byte[tokenSize];

        ByteBuffer bb = ByteBuffer.wrap(content);
        bb.putLong(totalSize);
        bb.putInt(type.ordinal());
        bb.putInt(destination.ordinal());
        streamIn.read(content, HEADER_SIZE, tokenSize - HEADER_SIZE);
        remainingSize -= tokenSize - HEADER_SIZE;
        if (remainingSize == 0) {
            closeStream();
        }
        token = new Token(content);
    }

    public void loadNextToken() throws IOException {
        int available = streamIn.available();
        int tokenSize = (int) Math.min((long) Math.min(Token.MAX_PAYLOAD, available), remainingSize);
        byte[] content = new byte[tokenSize];
        streamIn.read(content, 0, tokenSize);
        remainingSize -= tokenSize;
        if (remainingSize == 0) {
            closeStream();
        }
        token = new Token(content);
    }

    private void closeStream() {
        try {
            // Close the  stream
            streamIn.close();
        } catch (IOException e) {
            LOGGER.error("Error closing input stream on connection " + this, e);
        }
    }

    @Override
    public Direction getDirection() {
        return Direction.SEND;
    }

    @Override
    public void start(InternalConnection connection, List<ByteBuffer> received, List<ByteBuffer> transmit) throws IOException {
        tokens--;
        hasToken = true;
        // Open the stream
        openStream();
        setSize();
        loadFirstToken();
        sendTokenPacket(token, transmit);
    }

    @Override
    public void progress(InternalConnection connection, List<ByteBuffer> received, List<ByteBuffer> transmit) throws IOException {
        this.sendToken(token, transmit);
        while (token.isCompletelyRead() && remainingSize > 0) {
            loadNextToken();
            this.sendToken(token, transmit);

        }
    }

    @Override
    public boolean checkViability(boolean closedCommunication, List<ByteBuffer> received, List<ByteBuffer> transmit) throws NIOException {
        if (!closedCommunication) {
            return tokens > 0;
        } else {
            throw new NIOException(NIOException.ErrorType.CLOSED_CONNECTION, new Exception("Channel was already closed for connection " + this));
        }
    }

    @Override
    public void pause(InternalConnection ic) {
        pausedConnections.add(ic);
    }

    @Override
    public boolean isComplete(List<ByteBuffer> received, List<ByteBuffer> transmit) {
        return sizeInit && remainingSize == 0 && transmit.isEmpty() && token.isCompletelyRead();
    }

    @Override
    public void notifyCompletion(Connection c, EventManager<?> em) {
        em.writeFinished(c, this);
        tokens++;
        hasToken = false;
        if (!pausedConnections.isEmpty()) {
            InternalConnection ic = pausedConnections.removeFirst();
            ic.resume();
        }
    }

    @Override
    public void notifyError(Connection c, EventManager<?> em, CommException exc) {
        em.notifyError(c, this, exc);
        if (hasToken) {
            tokens++;
            hasToken = false;
            if (!pausedConnections.isEmpty()) {
                InternalConnection ic = pausedConnections.removeFirst();
                ic.resume();
            }
        }

    }

}
