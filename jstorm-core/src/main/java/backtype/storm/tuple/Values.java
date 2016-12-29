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
package backtype.storm.tuple;

import java.util.ArrayList;

/**
 * A convenience class for making tuple values using new Values("field1", 2, 3) syntax.
 */
public class Values extends ArrayList<Object> {
    public static final int OBJECT = 0;
    public static final int STRING = 1;
    public static final int INTEGER = 2;

    public int type;

    public Values() {

    }

    public Values(Object... vals) {
        super(vals.length);
        for (Object o : vals) {
            add(o);
        }
        type = OBJECT;
    }

    public Values(String... strs) {
    	super(strs.length);
        for (String s : strs) {
            add(s);
        }
        type = STRING;
    }

    public Values(Integer... ints) {
    	super(ints.length);
        for (Integer i : ints) {
            add(i);
        }
        type = INTEGER;
    }

    @Override
    public boolean add(Object obj) {
        Object val = obj;
        if (type == STRING && !(obj instanceof String)) {
            type = OBJECT;
        } else if (type == INTEGER && !(obj instanceof Integer)) {
            type = OBJECT;
        }
        return super.add(val);
    }
}
