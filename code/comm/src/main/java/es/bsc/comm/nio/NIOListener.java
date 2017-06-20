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
package es.bsc.comm.nio;

import es.bsc.comm.Node;
import es.bsc.comm.TransferManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import es.bsc.comm.nio.event.ErrorEvent;
import es.bsc.comm.nio.event.LowBufferEvent;
import es.bsc.comm.nio.event.PacketEntry;
import es.bsc.comm.nio.event.ClosedChannelEvent;
import es.bsc.comm.nio.event.ConnectionEstablished;
import es.bsc.comm.nio.event.EmptyBufferEvent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NIOListener extends Thread {

    public static class ChangeRequest {

        private final Channel channel;
        private final NIOConnection connection;
        private final int type;
        private final boolean close;

        public ChangeRequest(Channel socket, NIOConnection nc, int type, boolean close) {
            this.channel = socket;
            this.connection = nc;
            this.type = type;
            this.close = close;
        }
    }

    private static final Logger LOGGER = LogManager.getLogger(TransferManager.LOGGER_NAME);

    private static final LinkedList<ChangeRequest> PENDING_CHANGES = new LinkedList<ChangeRequest>();
    private static final HashMap<SocketChannel, ChangeRequest> PENDING_INTEREST = new HashMap<SocketChannel, ChangeRequest>();
    private static final HashMap<Integer, ServerSocketChannel> SERVERS = new HashMap<Integer, ServerSocketChannel>();

    private static final String DEFAULT_ADDRESS_IP = "127.0.0.1";

    // Stops the thread if true
    private static boolean stop = false;

    private static final long SELECTOR_SLEEP = 1_000L;
    private static Selector selector;

    private static NIOEventManager eventManager;

    private static NIOConnection closingConnection;

    public NIOListener() {
        super();

        // Name current thread
        Thread.currentThread().setName("NIO Listener");
    }

    public static void init(NIOEventManager nem) throws NIOException {
        try {
            selector = SelectorProvider.provider().openSelector();
        } catch (IOException e) {
            throw new NIOException(NIOException.ErrorType.LOADING_LISTENER, e);
        }

        eventManager = nem;
    }

    // Start a new non-blocking server
    public static void startServer(Node n) throws NIOException {
        NIONode nn = (NIONode) n;
        String ip = nn.getIp();
        int port = nn.getPort();
        try {
            ServerSocketChannel ssc = ServerSocketChannel.open();
            SERVERS.put(port, ssc);
            ssc.configureBlocking(false);
            ssc.setOption(StandardSocketOptions.SO_REUSEADDR, true);

            InetSocketAddress inet = null;
            if (ip == null) {
                inet = new InetSocketAddress(port);
            } else {
                inet = new InetSocketAddress(ip, port);
            }
            ssc.socket().setReuseAddress(true);
            ssc.socket().bind(inet, 7000);

            ChangeRequest cr = new ChangeRequest(ssc, null, SelectionKey.OP_ACCEPT, false);
            synchronized (PENDING_CHANGES) {
                PENDING_CHANGES.add(cr);
                selector.wakeup();
            }
        } catch (IOException e) {
            LOGGER.error("Exception starting server", e);
            throw new NIOException(NIOException.ErrorType.STARTING_SERVER, e);
        }
    }

    // All sockets except the server are already closed
    public void closeServers() {
        try {
            for (ServerSocketChannel ssc : SERVERS.values()) {
                ssc.close();
            }
        } catch (IOException e) {
            LOGGER.error("Error closing server connection", e);
        }
    }

    public void closeConnections() {
        NIOConnection.abortPendingConnections();

        for (SocketChannel sc : NIOConnection.getAllSockets()) {
            if (closingConnection != null && closingConnection.getSocket() == sc) {
                continue;
            }

            if (sc.isConnected()) {
                LOGGER.debug("Close channel " + sc.hashCode() + " from closeConnections");
            } else {
                LOGGER.warn("Channel " + sc.hashCode() + " is not connected");
            }
            SelectionKey key = sc.keyFor(selector);
            closeChannel(key, sc);
        }
    }

    // Change interest
    public static void closeSocket(NIOConnection nc, SocketChannel sc) {
        ChangeRequest cr = new ChangeRequest(sc, nc, 0, true);
        synchronized (PENDING_CHANGES) {
            PENDING_CHANGES.add(cr);
            selector.wakeup();
        }
    }

    // Change interest
    public static void changeInterest(NIOConnection nc, SocketChannel sc, int interest) {
        ChangeRequest cr = new ChangeRequest(sc, nc, interest, false);
        synchronized (PENDING_CHANGES) {
            PENDING_CHANGES.add(cr);
            selector.wakeup();
        }
    }

    public void run() {
        LOGGER.info("NIO Listener started");
        while (!stop) {
            try {
                // Do any pending changes
                // NOTE: All modifications to selector should be done in the same thread
                applyInterestChanges();
                // Timeout necessary in case selector.wakeup() is done after
                // synchronized (pendingChanges) and before select()
                // Loop through the ready channels
                selector.select(SELECTOR_SLEEP);
                processKeys(selector.selectedKeys().iterator());
            } catch (IOException e) {
                LOGGER.error("Error listening on connection changes", e);
            }
        }

        LOGGER.info("Closing all connections " + (closingConnection != null ? "but " + closingConnection : ""));
        closeServers();
        closeConnections();

        while (NIOConnection.areAliveConnections()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Waiting for closing connection changes");
                StringBuilder sb = new StringBuilder("Alive channels: ");
                for (SocketChannel s : NIOConnection.getAllSockets()) {
                    sb.append(s.hashCode()).append(" ");
                }
                LOGGER.debug(sb.toString());
            }
            try {
                applyInterestChanges();
                selector.select(SELECTOR_SLEEP);
                processKeys(selector.selectedKeys().iterator());
            } catch (IOException e) {
                LOGGER.debug("Error listening on closing connection changes", e);
            }
        }

        LOGGER.debug("Notifying closure to Event Manager");
        eventManager.listennerStopped();

        LOGGER.info("NIO Listener stopped");
    }

    private void applyInterestChanges() throws IOException {
        synchronized (PENDING_CHANGES) {
            Iterator<ChangeRequest> changes = PENDING_CHANGES.iterator();
            while (changes.hasNext()) {
                try {
                    ChangeRequest change = (ChangeRequest) changes.next();
                    if (change.close) {
                        SocketChannel sc = (SocketChannel) change.channel;
                        SelectionKey key = sc.keyFor(selector);
                        //Close Connection
                        LOGGER.debug("Closing channel " + sc.hashCode() + " no more data to write");
                        closeChannel(key, sc);
                    } else // Change is NOT a closure request
                     if (change.type != SelectionKey.OP_ACCEPT) {
                            SocketChannel sc = (SocketChannel) change.channel;
                            if (!sc.isConnected() && change.type != SelectionKey.OP_CONNECT) {
                                PENDING_INTEREST.put(sc, change);
                            } else {
                                SelectionKey key = ((SelectableChannel) change.channel).register(selector, change.type);
                                if (change.type == SelectionKey.OP_WRITE) {
                                    sc.setOption(StandardSocketOptions.SO_SNDBUF, NIOProperties.NETWORK_BUFFER_SIZE);
                                }
                                key.attach(change.connection);
                            }
                        } else {
                            SelectionKey key = ((SelectableChannel) change.channel).register(selector, change.type);
                            key.attach(change.connection);
                        }
                } catch (ClosedChannelException e) {
                    LOGGER.error("Exception closing channel ", e);
                }
            }

            PENDING_CHANGES.clear();
        }
    }

    private void processKeys(Iterator<SelectionKey> selectedKeys) {
        while (selectedKeys.hasNext()) {
            SelectionKey key = (SelectionKey) selectedKeys.next();
            selectedKeys.remove();
            if (!key.isValid()) {
                continue;
            }
            if (key.isAcceptable()) {
                accept(key);
            } else if (key.isConnectable()) {
                connect(key);
            } else if (key.isReadable()) {
                read(key);
            } else if (key.isWritable()) {
                write(key);
            }
        }
    }

    public static void shutdown(NIOConnection connection) {
        closingConnection = connection;
        stop = true;
        selector.wakeup();
    }

    // Start a connection
    // Client -> Server
    // Step 1 of the handshake
    public static NIOConnection startConnection(Node targetNode) {
        NIONode n = (NIONode) targetNode;
        SocketChannel sc = null;
        NIOConnection nc = null;
        try {
            sc = SocketChannel.open();
            sc.configureBlocking(false);
            sc.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            if (n.getIp() == null) {
                sc.connect(new InetSocketAddress(DEFAULT_ADDRESS_IP, n.getPort()));
            } else {
                sc.connect(new InetSocketAddress(n.getIp(), n.getPort()));
            }
            sc.socket().setKeepAlive(true);
            sc.socket().setReuseAddress(true);

            nc = new NIOConnection(eventManager, sc, n);
            ChangeRequest cr = new ChangeRequest(sc, nc, SelectionKey.OP_CONNECT, false);
            synchronized (PENDING_CHANGES) {
                PENDING_CHANGES.add(cr);
                selector.wakeup();
            }

        } catch (Exception e) {
            LOGGER.error("Error starting connection", e);
            NIOException ne = new NIOException(NIOException.ErrorType.STARTING_CONNECTION, e);
            nc = new NIOConnection(eventManager, null, n);
            eventManager.addEvent(new ErrorEvent(nc, ne));
        }

        return nc;
    }

    public static void restartConnection(NIOConnection nc, NIONode targetNode) {
        try {
            SocketChannel sc = SocketChannel.open();
            sc.configureBlocking(false);
            sc.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            if (targetNode.getIp() == null) {
                sc.connect(new InetSocketAddress(DEFAULT_ADDRESS_IP, targetNode.getPort()));
            } else {
                sc.connect(new InetSocketAddress(targetNode.getIp(), targetNode.getPort()));
            }
            sc.socket().setKeepAlive(true);
            sc.socket().setReuseAddress(true);

            nc.replaceChannel(sc);

            ChangeRequest cr = new ChangeRequest(sc, nc, SelectionKey.OP_CONNECT, false);
            synchronized (PENDING_CHANGES) {
                PENDING_CHANGES.add(cr);
                selector.wakeup();
            }
        } catch (Exception e) {
            NIOException ne = new NIOException(NIOException.ErrorType.RESTARTING_CONNECTION, e);
            eventManager.addEvent(new ErrorEvent(nc, ne));
        }
    }

    // Accept an incoming connection
    // Server -> Client
    // Step 2 of the handshake
    private void accept(SelectionKey key) {
        try {
            ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
            SocketChannel sc = ssc.accept();
            sc.configureBlocking(false);

            String address = sc.socket().getInetAddress().getHostAddress();
            int port = sc.socket().getPort();

            NIONode n = new NIONode(address, port);
            NIOConnection nc = new NIOServerConnection(eventManager, sc, n);
            changeInterest(nc, sc, SelectionKey.OP_READ);

            eventManager.addEvent(new ConnectionEstablished(nc));
        } catch (IOException e) {
            NIOException ne = new NIOException(NIOException.ErrorType.ACCEPTING_CONNECTION, e);
            eventManager.addEvent(new ErrorEvent(ne));
        }
    }

    // Confirm that the connection has been established
    // Client -> Server
    // Step 3 of the handshake
    private void connect(SelectionKey key) {
        SocketChannel sc = (SocketChannel) key.channel();
        NIOConnection nc = (NIOConnection) key.attachment();
        try {
            if (sc.finishConnect()) {
                acceptedConnection(nc, sc);
            } else {
                refusedConnection(nc, sc, null);
                key.cancel();
            }
        } catch (Exception e) {
            refusedConnection(nc, sc, e);
            key.cancel();
        }
    }

    private void acceptedConnection(NIOConnection nc, SocketChannel sc) {
        eventManager.addEvent(new ConnectionEstablished(nc));
        ChangeRequest cr;
        synchronized (PENDING_CHANGES) {
            if ((cr = PENDING_INTEREST.get(sc)) != null) {
                PENDING_CHANGES.add(cr);
            } else if (PENDING_CHANGES.isEmpty()) {
                cr = new ChangeRequest(sc, nc, SelectionKey.OP_READ, false);
                PENDING_CHANGES.add(cr);
            }
        }
    }

    private void refusedConnection(NIOConnection nc, SocketChannel sc, Exception e) {
        synchronized (PENDING_CHANGES) {
            PENDING_INTEREST.remove(sc);
        }
        NIOException ne = new NIOException(NIOException.ErrorType.FINISHING_CONNECTION, e);
        eventManager.addEvent(new ErrorEvent(nc, ne));
        try {
            sc.close();
        } catch (IOException ee) {
            NIOException ne2 = new NIOException(NIOException.ErrorType.FINISHING_CONNECTION, ee);
            eventManager.addEvent(new ErrorEvent(nc, ne2));
        }
    }

    // Read from a channel and send the data to the TransferManager
    private void read(SelectionKey key) {
        SocketChannel sc = (SocketChannel) key.channel();
        NIOConnection nc = (NIOConnection) key.attachment();
        try {
            // Read data from the socket
            ByteBuffer readBuffer = ByteBuffer.allocate(NIOProperties.PACKET_SIZE);
            try {
                int size = sc.read(readBuffer);
                if (size == -1) {
                    closeChannel(key, sc);
                    return;
                }
            } catch (IOException e) {
                NIOException ne = new NIOException(NIOException.ErrorType.READ, e);
                eventManager.addEvent(new ErrorEvent(nc, ne));
                key.cancel();
                sc.close();
            }
            readBuffer.flip();

            // Ask TransferManager to process the data
            PacketEntry p = new PacketEntry(nc, readBuffer);
            eventManager.addEvent(p);
        } catch (Exception e) {
            LOGGER.error("Exception reading key in channel " + sc.hashCode(), e);
            NIOException ne = new NIOException(NIOException.ErrorType.READ, e);
            eventManager.addEvent(new ErrorEvent(nc, ne));
        }
    }

    // Write data to a channel
    private void write(SelectionKey key) {
        SocketChannel sc = (SocketChannel) key.channel();
        NIOConnection nc = (NIOConnection) key.attachment();
        LinkedList<ByteBuffer> sendBuffer = nc.getSendBuffer();

        ByteBuffer writeBuffer = null;
        synchronized (sendBuffer) {
            // If there is no data to write
            if (sendBuffer.isEmpty()) {
                if (nc.getCurrentTransfer() != null) {
                    changeInterest(nc, sc, SelectionKey.OP_READ);
                    eventManager.addEvent(new EmptyBufferEvent(nc));
                }
                return;
            } else {
                writeBuffer = sendBuffer.getFirst();
            }
        }
        // Write data to the Socket
        if (writeBuffer != null) {
            try {
                if (writeBuffer.remaining() > 0) {
                    int written = sc.write(writeBuffer);
                    LOGGER.debug(written + " bythes written to socket " + sc.hashCode());
                }
            } catch (IOException e) {
                LOGGER.error("Error writting key in channel " + this.hashCode() + ". Closing Channel " + sc.hashCode(), e);
                NIOException ne = new NIOException(NIOException.ErrorType.WRITE, e);
                eventManager.addEvent(new ErrorEvent(nc, ne));
                closeChannel(key, sc);
                return;
            }
            // All the buffer has been sent
            if (writeBuffer.remaining() == 0) {
                Integer count;
                synchronized (sendBuffer) {
                    sendBuffer.removeFirst();
                    count = sendBuffer.size();

                    if (count == 0) {
                        changeInterest(nc, sc, SelectionKey.OP_READ);
                        eventManager.addEvent(new EmptyBufferEvent(nc));
                    } else if (count < NIOProperties.MIN_BUFFERED_PACKETS) {
                        // Ask TransferManager to enqueue more buffers
                        eventManager.addEvent(new LowBufferEvent(nc));
                    }
                }
            }
            // If there is still data to write in the buffer
            // it will do it the next iteration
        }
    }

    private void closeChannel(SelectionKey key, SocketChannel sc) {
        // Cancel the key
        if (key != null) {
            key.cancel();
        }

        // If the socket is open, request and notify the connection closure and close it
        if (sc.isOpen()) {
            LOGGER.debug("Channel " + sc.hashCode() + "is open. Requesting closure");
            NIOConnection nc = (NIOConnection) key.attachment();
            eventManager.addEvent(new ClosedChannelEvent(nc));
            try {
                sc.close();
            } catch (IOException e) {
                LOGGER.error("Could not close channel " + sc, e);
            }
        }
    }

}
