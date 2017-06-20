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
package es.bsc.mobile.runtime;

public class ArrayAccessWatcher {

    public static void arrayWriteObject(Object array, int index, Object value) throws Throwable {
        ((Object[]) array)[index] = value;
    }

    public static void arrayWriteInt(Object array, int index, int value) throws Throwable {
        ((int[]) array)[index] = value;
    }

    public static void arrayWriteByteOrBoolean(Object array, int index, byte value) throws Throwable {
        if (array instanceof boolean[]) {
            boolean boolValue = value != 0;
            ((boolean[]) array)[index] = boolValue;
        } else {
            ((byte[]) array)[index] = value;
        }
    }

    public static void arrayWriteChar(Object array, int index, char value) throws Throwable {
        ((char[]) array)[index] = value;
    }

    public static void arrayWriteDouble(Object array, int index, double value) throws Throwable {
        ((double[]) array)[index] = value;
    }

    public static void arrayWriteShort(Object array, int index, short value) throws Throwable {
        ((short[]) array)[index] = value;
    }

    public static void arrayWriteFloat(Object array, int index, float value) throws Throwable {
        ((float[]) array)[index] = value;
    }

    public static void arrayWriteLong(Object array, int index, long value) throws Throwable {
        ((long[]) array)[index] = value;
    }

    public static Object arrayReadObject(Object array, int index) throws Throwable {
        return ((Object[]) array)[index];
    }

    public static int arrayReadInt(Object array, int index) throws Throwable {
        return ((int[]) array)[index];
    }

    public static byte arrayReadByteOrBoolean(Object array, int index) throws Throwable {
        if (array instanceof boolean[]) {
            boolean b = ((boolean[]) array)[index];
            if (b) {
                return 1;
            } else {
                return 0;
            }
        } else {
            return ((byte[]) array)[index];
        }
    }

    public static char arrayReadChar(Object array, int index) throws Throwable {
        return ((char[]) array)[index];
    }

    public static double arrayReadDouble(Object array, int index) throws Throwable {
        return ((double[]) array)[index];
    }

    public static float arrayReadFloat(Object array, int index) throws Throwable {
        return ((float[]) array)[index];
    }

    public static long arrayReadLong(Object array, int index) throws Throwable {
        return ((long[]) array)[index];
    }

    public static short arrayReadShort(Object array, int index) throws Throwable {
        return ((short[]) array)[index];
    }

}
