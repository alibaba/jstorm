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

import java.util.List;

public interface ITuple {

    /**
     * Returns the number of fields in this tuple.
     */
    int size();

    /**
     * Returns true if this tuple contains the specified name of the field.
     */
    boolean contains(String field);

    /**
     * Gets the names of the fields in this tuple.
     */
    Fields getFields();

    /**
     * Returns the position of the specified field in this tuple.
     */
    int fieldIndex(String field);

    /**
     * Returns a subset of the tuple based on the fields selector.
     */
    List<Object> select(Fields selector);

    /**
     * Gets the field at position i in the tuple. Returns object since tuples are dynamically typed.
     */
    Object getValue(int i);

    /**
     * Returns the String at position i in the tuple. If that field is not a String, you will get a runtime error.
     */
    String getString(int i);

    /**
     * Returns the Integer at position i in the tuple. If that field is not an Integer, you will get a runtime error.
     */
    Integer getInteger(int i);

    /**
     * Returns the Long at position i in the tuple. If that field is not a Long, you will get a runtime error.
     */
    Long getLong(int i);

    /**
     * Returns the Boolean at position i in the tuple. If that field is not a Boolean, you will get a runtime error.
     */
    Boolean getBoolean(int i);

    /**
     * Returns the Short at position i in the tuple. If that field is not a Short, you will get a runtime error.
     */
    Short getShort(int i);

    /**
     * Returns the Byte at position i in the tuple. If that field is not a Byte, you will get a runtime error.
     */
    Byte getByte(int i);

    /**
     * Returns the Double at position i in the tuple. If that field is not a Double, you will get a runtime error.
     */
    Double getDouble(int i);

    /**
     * Returns the Float at position i in the tuple. If that field is not a Float, you will get a runtime error.
     */
    Float getFloat(int i);

    /**
     * Returns the byte array at position i in the tuple. If that field is not a byte array, you will get a runtime error.
     */
    byte[] getBinary(int i);

    Object getValueByField(String field);

    String getStringByField(String field);

    Integer getIntegerByField(String field);

    Long getLongByField(String field);

    Boolean getBooleanByField(String field);

    Short getShortByField(String field);

    Byte getByteByField(String field);

    Double getDoubleByField(String field);

    Float getFloatByField(String field);

    byte[] getBinaryByField(String field);

    /**
     * Gets all the values in this tuple.
     */
    List<Object> getValues();

}
