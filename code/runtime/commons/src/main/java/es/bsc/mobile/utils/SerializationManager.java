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
package es.bsc.mobile.utils;

import es.bsc.mobile.data.DataRegister;
import es.bsc.mobile.data.operations.DataOp;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;

/**
 *
 * @author flordan
 */
public class SerializationManager {

    private final LinkedList<Request> priorityRequests = new LinkedList<>();
    private final LinkedList<Request> requests = new LinkedList<>();
    private int priorityFree = 0;
    private int waiting = 0;

    public SerializationManager(int threads) {
        for (int i = 0; i < threads; i++) {
            Thread t = new Thread(new RequestProcessor());
            t.setName("Object Serializer " + i);
            t.start();
        }
    }

    public void requestDeserialization(boolean priority, DataRegister dr, DataOp op) {
        dr.addOperationForDeserialization(op);
        addRequest(priority, new DeserializeRequest(dr));
    }

    public void requestSerialization(boolean priority, DataRegister dr, DataOp op) {
        dr.addOperationForSerialization(op);
        addRequest(priority, new SerializeRequest(dr));
    }

    public void replicateFile(boolean priority, DataRegister reg, String targetLocation, DataOp op) {
        addRequest(priority, new CopyFileRequest(reg, targetLocation));
    }

    public void writeToFile(DataRegister reg, String folder) {
        addRequest(false, new ToFileRequest(reg, folder));
    }

    private static abstract class Request {

        protected final DataRegister dr;

        private Request(DataRegister dr) {
            this.dr = dr;
        }

        public abstract void perform() throws Exception;

    }

    private static class DeserializeRequest extends Request {

        public DeserializeRequest(DataRegister dr) {
            super(dr);
        }

        @Override
        public void perform() throws Exception {
            byte[] serialized;
            Object o = dr.getValue();
            if (o == null) {
                if (dr.isSerialized() && (serialized = dr.getArrayValue()) != null) {
                    o = Serializer.deserialize(serialized);
                } else if (dr.isOnFile()) {
                    o = Serializer.deserialize(dr.getLocation());
                }
            }
            dr.deserialized(o);
        }

        public String toString() {
            return "Deserialize value on register " + dr.getName();
        }

    }

    private static class SerializeRequest extends Request {

        public SerializeRequest(DataRegister reg) {
            super(reg);
        }

        @Override
        public void perform() throws Exception {
            if (dr.isOnFile()) {
                dr.serialized(dr.getLocation());
            } else if (dr.isSerialized()) {
                dr.serialized(dr.getArrayValue());
            } else {
                byte[] serialized = Serializer.serialize(dr.getValue());
                dr.serialized(serialized);
            }
        }
    }

    private static class CopyFileRequest extends Request {

        private final String targetLocation;

        public CopyFileRequest(DataRegister reg, String targetLocation) {
            super(reg);
            this.targetLocation = targetLocation;
        }

        @Override
        public void perform() throws Exception {
            String location = dr.getLocation();
            Path source = Paths.get(location);
            Path destination = Paths.get(targetLocation);
            Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static class ToFileRequest extends Request {

        private final String path;

        public ToFileRequest(DataRegister reg, String path) {
            super(reg);
            this.path = path;
        }

        @Override
        public void perform() throws Exception {
            String location = (path.endsWith(File.separator) ? path : path + File.separator) + dr.getName();
            if (!dr.isOnFile()) {
                byte[] serialized = dr.getArrayValue();
                if (serialized != null) {
                    try {
                        FileOutputStream fos = new FileOutputStream(location);
                        fos.write(serialized);
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    }
                    dr.removeArrayValue();
                } else {
                    Serializer.serialize(dr.getValue(), location);
                }
                dr.serialized(location);
            }
        }
    }

    private static class ReadRequest extends Request {

        public ReadRequest(DataRegister dr) {
            super(dr);
        }

        @Override
        public void perform() throws Exception {
            String location = dr.getLocation();
            byte[] serialized = Files.readAllBytes(Paths.get(location));
        }

    }

    private final class RequestProcessor implements Runnable {

        String name;

        @Override
        public void run() {
            name = Thread.currentThread().getName();
            synchronized (SerializationManager.this) {
                priorityFree++;
            }

            boolean processingNonPriority = false;
            while (true) {
                Request r;
                synchronized (SerializationManager.this) {
                    if (processingNonPriority) {
                        processingNonPriority = false;
                        priorityFree++;
                    }

                    if (!priorityRequests.isEmpty()) {
                        r = priorityRequests.pollFirst();
                    } else if (priorityFree > 1 && !requests.isEmpty()) {
                        r = requests.pollFirst();
                        priorityFree--;
                        processingNonPriority = true;
                    } else {
                        waiting++;
                        try {
                            SerializationManager.this.wait();
                        } catch (InterruptedException ex) {
                            //Do nothing
                        }
                        waiting--;
                        continue;
                    }
                }
                try {
                    r.perform();
                } catch (Exception e) {
                    continue;
                }
            }
        }

    }

    private void addRequest(boolean hasPriority, Request req) {
        synchronized (this) {
            if (hasPriority) {
                priorityRequests.add(req);
                if (waiting > 0) {
                    this.notify();
                }
            } else {
                boolean empty = requests.isEmpty();
                requests.add(req);
                if (empty && waiting > 0) {
                    this.notify();
                }
            }

        }
    }

}
