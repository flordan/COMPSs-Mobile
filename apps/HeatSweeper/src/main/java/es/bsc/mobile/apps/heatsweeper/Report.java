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

public class Report implements Externalizable {

    public static int nextId = 0;
    public int id;
    HeatSource[] sources;
    int iterations;
    boolean achieved;

    public Report() {
        this.id = nextId++;

    }

    public Report(HeatSource[] sources, int iterations, boolean achieved) {
        this.id = nextId++;
        this.sources = sources;
        this.iterations = iterations;
        this.achieved = achieved;
    }

    public Report(Report a) {
        this.id = nextId++;
        this.sources = a.sources;
        this.iterations = a.iterations;
        this.achieved = a.achieved;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sources = (HeatSource[]) in.readObject();
        iterations = in.readInt();
        achieved = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(sources);
        out.writeInt(iterations);
        out.writeBoolean(achieved);
    }

    public static Report getBest(Report a, Report b) {
        if (a.achieved) {
            if (b.achieved) {
                if (a.iterations > b.iterations) {
                    return new Report(b);
                } else {
                    return new Report(a);
                }
            } else {
                return new Report(a);
            }
        } else if (b.achieved) {
            return new Report(b);
        } else if (a.iterations > b.iterations) {
            return new Report(b);
        } else {
            return new Report(a);
        }
    }

    public String toString() {
        if (!achieved) {
            return "Goal not Achieved";
        }
        return "Best solution needs " + iterations + " and sources are " + sources;
    }
}
