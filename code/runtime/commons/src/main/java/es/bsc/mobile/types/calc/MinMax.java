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
package es.bsc.mobile.types.calc;

public class MinMax {

    private long count = 0;
    private long average = 0;
    private long min = Long.MAX_VALUE;
    private long max = 0;

    public MinMax() {
        this.count = 0;
        this.average = 0;
        this.min = Long.MAX_VALUE;
        this.max = 0;
    }

    public MinMax(MinMax base) {
        this.count = base.count;
        this.min = base.min;
        this.average = base.average;
        this.max = base.max;
    }

    public MinMax(String value) {
        int index = value.indexOf(']');
        count = Long.parseLong(value.substring(1, index));
        value = value.substring(index + 1);
        index = value.indexOf(']');
        average = Long.parseLong(value.substring(1, index));
        value = value.substring(index + 1);
        index = value.indexOf(']');
        min = Long.parseLong(value.substring(1, index));
        value = value.substring(index + 1);
        index = value.indexOf(']');
        max = Long.parseLong(value.substring(1, index));

    }

    public String toStore() {
        return "[" + count + "]" + "[" + average + "]" + "[" + min + "]" + "[" + max + "]";
    }

    public void newValue(long value) {
        count++;
        average += (value - average) / count;
        if (min > value) {
            min = value;
        }
        if (max < value) {
            max = value;
        }
    }

    public String dump(String prefix) {
        return prefix + " min: " + min + " avg:" + average + " max:" + max;
    }

    @Override
    public String toString() {
        return " min: " + min + " avg:" + average + " max:" + max;
    }

    public long minim() {
        if (count > 0) {
            return min;
        } else {
            return 0;
        }
    }

    public long average() {
        return average;
    }

    public long max() {
        return max;
    }

    public void aggregate(MinMax mm) {
        this.average = this.average + mm.average;
        this.count = this.count + mm.count;
        this.max = this.max + mm.max;
        this.min = this.min + mm.min;
    }

    public void aggregate(int times, MinMax mm) {
        this.average = this.average + mm.average * times;
        this.count = this.count + mm.count * times;
        this.max = this.max + mm.max * times;
        this.min = this.min + mm.min * times;
    }

    public void multiply(long times) {
        this.average = this.average * times;
        this.max = this.max * times;
        this.min = this.min * times;
    }

}
