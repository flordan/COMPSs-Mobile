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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The serializer class is an utility to Serialize and deserialize objects
 * passed as a parameter of a remote task
 */
public class Serializer {

    private static final Logger LOGGER = LogManager.getLogger("Runtime.Serializer");
    private static final boolean INFO_ENABLED = LOGGER.isInfoEnabled();

    private static final String ERR_SERIALIZING = "Error serializing object ";
    private static final String ERR_DESERIALIZING = "Error deserializing object ";
    private static final String FROM_FILE = " from file ";
    private static final String TO_FILE = " to file ";
    private static final String FROM_ARRAY = " from byte array. ";
    private static final String TO_ARRAY = " to byte array.";

    private Serializer() {
    }

    /**
     * Serializes an objects and leaves it in a file
     *
     * @param o object to be serialized
     * @param file file where the serialized object will be stored
     * @throws java.io.IOException
     */
    public static void serialize(Object o, String file) throws IOException {
        if (INFO_ENABLED) {
            LOGGER.info("Serializing " + o + TO_FILE + file);
        }
        serializeBinary(o, file);
    }

    /**
     * Serializes an objects
     *
     * @param o object to be serialized
     * @return
     * @throws java.io.IOException
     */
    public static byte[] serialize(Object o) throws IOException {
        if (INFO_ENABLED) {
            LOGGER.info("Serializing " + o + TO_ARRAY + ".");
        }
        return serializeBinary(o);
    }

    /**
     * Reads an object from a file
     *
     * @param file containing the serialized object
     * @return the object read from the file
     * @throws java.io.IOException
     * @throws java.lang.ClassNotFoundException
     *
     */
    public static Object deserialize(String file) throws IOException, ClassNotFoundException {
        if (INFO_ENABLED) {
            LOGGER.info("Deserializing object " + FROM_FILE + file + ".");
        }
        return deserializeBinary(file);
    }

    /**
     * Reads an object from a byte array
     *
     * @param data byte array containing the object to deserialize
     * @return the object
     * @throws java.io.IOException
     * @throws java.lang.ClassNotFoundException
     */
    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        if (INFO_ENABLED) {
            LOGGER.info("Deserializing object " + FROM_ARRAY + ".");
        }
        return deserializeBinary(data);
    }

    /**
     * Serializes an objects using the default java serializer
     *
     * @param o object to be serialized
     * @throws IOException Error writing the byte stream
     */
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
            } catch (IOException ex) {
                LOGGER.error(ERR_SERIALIZING + o + TO_ARRAY + ".", ex);
            }
            try {
                bos.close();
            } catch (IOException ex) {
                LOGGER.error(ERR_SERIALIZING + o + TO_ARRAY + ".", ex);
            }
        }
    }

    /**
     * Serializes an objects using the default java serializer and leaves it in
     * a file
     *
     * @param o object to be serialized
     * @param file file where to store the serialized object
     *
     */
    private static void serializeBinary(Object o, String file) throws IOException {
        FileOutputStream fout = new FileOutputStream(file);
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(fout);
            oos.writeObject(o);
        } finally {
            try {
                if (oos != null) {
                    oos.close();
                }
            } catch (IOException ex) {
                LOGGER.error(ERR_SERIALIZING + o + TO_FILE + ".", ex);
            }
            try {
                fout.close();
            } catch (IOException ex) {
                LOGGER.error(ERR_SERIALIZING + o + TO_FILE + ".", ex);
            }
        }
    }

    /**
     * Reads a binary-serialized object from a byte array
     *
     * @param data containing the serialized object
     * @return the object read from the data
     *
     */
    private static Object deserializeBinary(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInput in = null;

        try {
            in = new ObjectInputStream(bis);
            return in.readObject();
        } finally {
            try {
                bis.close();
            } catch (IOException ex) {
                LOGGER.error(ERR_DESERIALIZING + TO_ARRAY + ".", ex);
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                LOGGER.error(ERR_DESERIALIZING + TO_ARRAY + ".", ex);
            }
        }
    }

    /**
     * Reads a binary-serialized object from a file
     *
     * @param file containing the serialized object
     * @return the object read from the file
     *
     */
    private static Object deserializeBinary(String file) throws IOException, ClassNotFoundException {
        Object o;

        FileInputStream fis = new FileInputStream(file);
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(fis);
            o = ois.readObject();
        } finally {
            try {
                if (ois != null) {
                    ois.close();
                }
            } catch (IOException e) {
                LOGGER.error(ERR_DESERIALIZING + TO_FILE + ".", e);
                //Error closing the output stream. Don't do anything
            }
            try {
                fis.close();
            } catch (IOException e) {
                LOGGER.error(ERR_DESERIALIZING + TO_FILE + ".", e);
                //Error closing the output stream. Don't do anything
            }
        }
        return o;
    }

}
