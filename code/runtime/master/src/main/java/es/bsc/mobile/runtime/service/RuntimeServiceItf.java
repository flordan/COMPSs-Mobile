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
package es.bsc.mobile.runtime.service;

import es.bsc.mobile.runtime.types.CEI;
import es.bsc.mobile.runtime.types.Task;
import es.bsc.mobile.types.comm.Node;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 *
 * @author flordan
 */
public interface RuntimeServiceItf {

    public abstract static class Stub extends Thread implements RuntimeServiceItf {

        private static final Logger LOGGER = LogManager.getLogger("Runtime.Service");
        private static final int PORT = 20000;

        private static final String NULL = "<<<null>>>";
        private static final String START = "start";
        private static final String DATA_CREATION = "notifyDataCreation";
        private static final String DATA_EXISTENCE = "requestDataExistence";
        private static final String DATA_LOCATIONS = "requestDataLocations";
        private static final String EXECUTE_TASK = "executeTask";
        private static final String GET_CORES = "getCores";

        private BufferedReader in;
        private PrintWriter out;

        public static RuntimeServiceItf asInterface() {
            return new Proxy();
        }

        @Override
        public void run() {
            try {
                LOGGER.debug("Runtime service listens on port " + PORT);
                ServerSocket ss = new ServerSocket(PORT);
                Socket cs = ss.accept();
                in = new BufferedReader(new InputStreamReader(cs.getInputStream()));
                out = new PrintWriter(cs.getOutputStream(), true);
            } catch (Exception e) {
                LOGGER.fatal("Cannot open Server.", e);
                return;
            }

            while (true) {
                try {
                    String line = in.readLine();
                    if (line == null) {
                        continue;
                    }
                    switch (line) {
                        case START:
                            handleStart(in);
                            break;
                        case DATA_CREATION:
                            handleDataCreation(in);
                            break;
                        case DATA_EXISTENCE:
                            handleDataExistence(in);
                            break;
                        case DATA_LOCATIONS:
                            handleDataLocations(in);
                            break;
                        case EXECUTE_TASK:
                            handleExecuteTask(in);
                            break;
                        case GET_CORES:
                            handleGetCores(in);
                            break;
                        default:
                    }
                } catch (IOException | NumberFormatException e) {
                    LOGGER.error(e);
                }
            }
        }

        private void handleStart(BufferedReader in) {
            CEI cei = readNextObject(in);
            start(cei);
        }

        private void handleDataCreation(BufferedReader in) {
            try {
                String dataId = readNextValue(in);
                String ip = readNextValue(in);
                String readPort = readNextValue(in);
                notifyDataCreation(dataId, new Node(ip, Integer.parseInt(readPort)));
            } catch (IOException ex) {
                LOGGER.error(ex);
            }
        }

        private void handleDataExistence(BufferedReader in) {
            try {
                String dataId = readNextValue(in);
                String ip = readNextValue(in);
                String readPort = readNextValue(in);
                requestDataExistence(dataId, new Node(ip, Integer.parseInt(readPort)));
            } catch (IOException ex) {
                LOGGER.error(ex);
            }
        }

        private void handleDataLocations(BufferedReader in) {
            try {
                String dataId = readNextValue(in);
                String ip = readNextValue(in);
                String readPort = readNextValue(in);
                requestDataLocations(dataId, new Node(ip, Integer.parseInt(readPort)));
            } catch (IOException ex) {
                LOGGER.error(ex);
            }
        }

        private void handleExecuteTask(BufferedReader in) {
            Task t = readNextObject(in);
            executeTask(t);
        }

        private void handleGetCores(BufferedReader in) {
            String[] signatures = readNextObject(in);
            int[] result = getCoreIds(signatures);
            try {
                byte[] b = serializeBinary(result);
                out.println(b.length);
                char[] s = new char[b.length];
                for (int i = 0; i < b.length; i++) {
                    s[i] = (char) b[i];
                }
                out.println(s);
            } catch (IOException e) {
                LOGGER.error("Error serializing Core Ids.", e);
            }

        }

        private static class Proxy implements RuntimeServiceItf {

            private PrintWriter out;
            private BufferedReader in;

            public Proxy() {
                try {
                    Socket sc = new Socket("127.0.0.1", PORT);
                    out = new PrintWriter(sc.getOutputStream(), true);
                    in = new BufferedReader(new InputStreamReader(sc.getInputStream()));
                } catch (IOException ex) {
                    LOGGER.error("Error encenent el socket", ex);
                }

            }

            @Override
            public void start(CEI cei) {
                out.println(START);
                try {
                    byte[] b = serializeBinary(cei);
                    out.println(b.length);
                    char[] s = new char[b.length];
                    for (int i = 0; i < b.length; i++) {
                        s[i] = (char) b[i];
                    }
                    out.println(s);
                } catch (IOException e) {
                    LOGGER.error("Error serializing CEI.", e);
                }
            }

            @Override
            public void notifyDataCreation(String dataId, Node hostNode) {
                out.println(DATA_CREATION);
                out.println(dataId);
                if (hostNode.getIp() == null) {
                    out.println(NULL);
                } else {
                    out.println(hostNode.getIp());
                }
                out.println(hostNode.getPort());
            }

            @Override
            public void requestDataExistence(String dataId, Node querier) {
                out.println(DATA_EXISTENCE);
                out.println(dataId);
                if (querier.getIp() == null) {
                    out.println(NULL);
                } else {
                    out.println(querier.getIp());
                }
                out.println(querier.getPort());
            }

            @Override
            public void requestDataLocations(String dataId, Node querier) {
                out.println(DATA_LOCATIONS);
                out.println(dataId);
                if (querier.getIp() == null) {
                    out.println(NULL);
                } else {
                    out.println(querier.getIp());
                }
                out.println(querier.getPort());
            }

            @Override
            public void executeTask(Task t) {
                out.println(EXECUTE_TASK);
                try {
                    byte[] b = serializeBinary(t);
                    out.println(b.length);
                    char[] s = new char[b.length];
                    for (int i = 0; i < b.length; i++) {
                        s[i] = (char) b[i];
                    }
                    out.println(s);
                } catch (IOException e) {
                    LOGGER.error("Error serializing task.", e);
                }
            }

            @Override
            public int[] getCoreIds(String[] signatures) {
                out.println(GET_CORES);
                try {
                    byte[] b = serializeBinary(signatures);
                    out.println(b.length);
                    char[] s = new char[b.length];
                    for (int i = 0; i < b.length; i++) {
                        s[i] = (char) b[i];
                    }
                    out.println(s);
                } catch (IOException e) {
                    LOGGER.error("Error serializing signatures.", e);
                }
                return (int[]) readNextObject(in);
            }
        }

        private static String readNextValue(BufferedReader in) throws IOException {
            String val = null;
            while (val == null) {
                val = in.readLine();
            }
            if (val.equals(NULL)) {
                return null;
            } else {
                return val;
            }
        }

        private static <T> T readNextObject(BufferedReader in) {
            T t = null;
            try {
                int size = Integer.parseInt(in.readLine());
                byte[] cei = new byte[size];
                int readCount = 0;

                while (readCount < size) {
                    char[] b = new char[size - readCount];
                    int read = in.read(b);
                    for (int i = 0; i < read; i++) {
                        cei[readCount++] = (byte) b[i];
                    }
                }

                t = (T) deserializeBinary(cei);
            } catch (Exception e) {
                LOGGER.error("Error reading from socket.", e);
            }
            return t;
        }

        private static byte[] serializeBinary(Object o) throws IOException {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = null;
            try {
                out = new ObjectOutputStream(bos);
                out.writeObject(o);
                return bos.toByteArray();
            } finally {
                try {
                    if (out != null) {
                        out.close();
                    }
                    bos.close();
                } catch (IOException ex) {
                    LOGGER.error("Error closing object Stream to serialize.", ex);
                }
            }
        }

        private static Object deserializeBinary(byte[] data) throws IOException, ClassNotFoundException {
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            ObjectInput in = null;

            try {
                in = new ObjectInputStream(bis);
                return in.readObject();
            } finally {
                try {
                    bis.close();
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException ex) {
                    LOGGER.error("Error closing object Stream to deserialize.", ex);
                }
            }
        }
    }

    void start(CEI cei);

    int[] getCoreIds(String[] signatures);

    void notifyDataCreation(String dataId, Node hostNode);

    void requestDataExistence(String dataId, Node querier);

    void requestDataLocations(String dataId, Node querier);

    void executeTask(Task t);
}
