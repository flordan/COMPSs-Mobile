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
package es.bsc.mobile.apps.heatsweeper;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class HeatSource implements Externalizable {

    private float posX;
    private float posY;
    private float size;
    private float temp;

    public HeatSource() {
    }

    public HeatSource(float posX, float posY, float size, float temp) {
        this.posX = posX;
        this.posY = posY;
        this.size = size;
        this.temp = temp;
    }

    public HeatSource(String line) {
        String[] values = line.split(" ");
        posX = Float.parseFloat(values[0]);
        posY = Float.parseFloat(values[2]);
        size = Float.parseFloat(values[4]);
        temp = Float.parseFloat(values[6]);
    }

    public float getPosX() {
        return posX;
    }

    public float getPosY() {
        return posY;
    }

    public float getSize() {
        return size;
    }

    public float getTemp() {
        return temp;
    }

    public String toString() {
        return "Heat Source [" + posX + ", " + posY + "] " + size + " " + temp;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeFloat(posX);
        out.writeFloat(posY);
        out.writeFloat(size);
        out.writeFloat(temp);

    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        posX = in.readFloat();
        posY = in.readFloat();
        size = in.readFloat();
        temp = in.readFloat();
    }
}
