/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.window;

import java.io.Serializable;

/**
 * A {@link TimeWindow} that represents a time interval from {@code start} (inclusive) to
 * {@code start + size} (exclusive).
 */
public class TimeWindow implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long start;
    private final long end;

    public TimeWindow() {
        this.start = 0;
        this.end = 0;
    }

    public TimeWindow(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public long maxTimestamp() {
        return end - 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimeWindow window = (TimeWindow) o;

        return end == window.end && start == window.start;
    }

    @Override
    public int hashCode() {
        int result = (int) (start ^ (start >>> 32));
        result = 31 * result + (int) (end ^ (end >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "TimeWindow[" + start + ", " + end + "]";
    }

    /**
     * Returns {@code true} if this window intersects the given window.
     */
    public boolean intersects(TimeWindow other) {
        return this.start <= other.end && this.end >= other.start;
    }

    /**
     * Returns the minimal window covers both this window and the given window.
     */
    public TimeWindow cover(TimeWindow other) {
        return new TimeWindow(Math.min(start, other.start), Math.max(end, other.end));
    }

}
