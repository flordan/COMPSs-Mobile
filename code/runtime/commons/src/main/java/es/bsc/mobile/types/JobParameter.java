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
package es.bsc.mobile.types;

import es.bsc.mobile.data.access.DaAccess;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import es.bsc.mobile.annotations.Parameter.Direction;
import es.bsc.mobile.annotations.Parameter.Type;

public abstract class JobParameter implements Externalizable {

    private static final long serialVersionUID = 1L;

    private static final String WITH_VALUE = " with value ";
    private Direction dir;
    private Type type;

    public JobParameter() {
    }

    JobParameter(Direction dir, Type type) {
        this.dir = dir;
        this.type = type;
    }

    public Direction getDirection() {
        return dir;
    }

    public Type getType() {
        return type;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(type).append(" parameter ").append(dir);
        return sb.toString();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(dir.ordinal());
        out.writeInt(type.ordinal());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        dir = Direction.values()[in.readInt()];
        type = Type.values()[in.readInt()];
    }

    public static class BooleanJobParameter extends JobParameter {

        private boolean value;

        public BooleanJobParameter() {
        }

        public BooleanJobParameter(Direction dir, boolean value) {
            super(dir, Type.BOOLEAN);
            this.value = value;
        }

        public boolean getValue() {
            return value;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeBoolean(value);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            value = in.readBoolean();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(super.toString());
            sb.append(WITH_VALUE).append(value);
            return sb.toString();
        }
    }

    public static class CharJobParameter extends JobParameter {

        private char value;

        public CharJobParameter() {
        }

        public CharJobParameter(Direction dir, char value) {
            super(dir, Type.CHAR);
            this.value = value;
        }

        public char getValue() {
            return value;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeChar(value);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            value = in.readChar();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(super.toString());
            sb.append(WITH_VALUE).append(value);
            return sb.toString();
        }
    }

    public static class StringJobParameter extends JobParameter {

        private String value;

        public StringJobParameter() {
        }

        public StringJobParameter(Direction dir, String value) {
            super(dir, Type.STRING);
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeUTF(value);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            value = in.readUTF();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(super.toString());
            sb.append(WITH_VALUE).append(value);
            return sb.toString();
        }
    }

    public static class ByteJobParameter extends JobParameter {

        private byte value;

        public ByteJobParameter() {
        }

        public ByteJobParameter(Direction dir, byte value) {
            super(dir, Type.BYTE);
            this.value = value;
        }

        public byte getValue() {
            return value;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeByte(value);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            value = in.readByte();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(super.toString());
            sb.append(WITH_VALUE).append(value);
            return sb.toString();
        }
    }

    public static class ShortJobParameter extends JobParameter {

        private short value;

        public ShortJobParameter() {
        }

        public ShortJobParameter(Direction dir, short value) {
            super(dir, Type.SHORT);
            this.value = value;
        }

        public short getValue() {
            return value;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeShort(value);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            value = in.readShort();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(super.toString());
            sb.append(WITH_VALUE).append(value);
            return sb.toString();
        }
    }

    public static class IntegerJobParameter extends JobParameter {

        private int value;

        public IntegerJobParameter() {
        }

        public IntegerJobParameter(Direction dir, int value) {
            super(dir, Type.INT);
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeInt(value);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            value = in.readInt();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(super.toString());
            sb.append(WITH_VALUE).append(value);
            return sb.toString();
        }
    }

    public static class LongJobParameter extends JobParameter {

        private long value;

        public LongJobParameter() {
        }

        public LongJobParameter(Direction dir, long value) {
            super(dir, Type.LONG);
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeLong(value);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            value = in.readLong();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(super.toString());
            sb.append(WITH_VALUE).append(value);
            return sb.toString();
        }
    }

    public static class FloatJobParameter extends JobParameter {

        private float value;

        public FloatJobParameter() {
        }

        public FloatJobParameter(Direction dir, float value) {
            super(dir, Type.FLOAT);
            this.value = value;
        }

        public float getValue() {
            return value;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeFloat(value);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            value = in.readFloat();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(super.toString());
            sb.append(WITH_VALUE).append(value);
            return sb.toString();
        }
    }

    public static class DoubleJobParameter extends JobParameter {

        private double value;

        public DoubleJobParameter() {
        }

        public DoubleJobParameter(Direction dir, double value) {
            super(dir, Type.DOUBLE);
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeDouble(value);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            value = in.readDouble();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(super.toString());
            sb.append(WITH_VALUE).append(value);
            return sb.toString();
        }
    }

    private abstract static class DataJobParameter extends JobParameter {

        protected DaAccess dataAccess;

        public DataJobParameter() {
        }

        public DataJobParameter(Direction dir, Type t, DaAccess dataAccess) {
            super(dir, t);
            this.dataAccess = dataAccess;
        }

        public DaAccess getDataAccess() {
            return dataAccess;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeObject(dataAccess);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            dataAccess = (DaAccess) in.readObject();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(super.toString());
            sb.append(" with value ").append(dataAccess);
            return sb.toString();
        }
    }

    public static class ObjectJobParameter extends DataJobParameter {

        public ObjectJobParameter() {
        }

        public ObjectJobParameter(Direction dir, DaAccess dataAccess) {
            super(dir, Type.OBJECT, dataAccess);
        }

    }

    public static class FileJobParameter extends DataJobParameter {

        public FileJobParameter() {
        }

        public FileJobParameter(Direction dir, DaAccess dataAccess) {
            super(dir, Type.FILE, dataAccess);
        }

    }

}
