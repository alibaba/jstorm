/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package backtype.storm.utils;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class LocalStateTest {

    LocalState ls;
    VersionedStore vs;
    String dir;

    @Before
    public void setUp() throws Exception {
        dir = System.getProperty("java.io.tmpdir") + File.separator + UUID.randomUUID().toString();
        ls = new LocalState(dir);
        vs = new VersionedStore(dir);

    }

    @Test
    public void testOneEmptyState() throws Exception {
        //touch empty files
        touchVersion(dir + File.separator + "12345");

        assertEquals(null, ls.get("a"));
        ls.put("c", 1);
        assertEquals(1, ls.get("c"));
    }

    @Test
    public void testAllEmptyState() throws Exception {
        //touch empty files
        touchVersion(vs.createVersion());
        touchVersion(vs.createVersion());
        touchVersion(vs.createVersion());
        touchVersion(vs.createVersion());

        assertEquals(null, ls.get("c"));
        ls.put("c", 1);
        assertEquals(1, ls.get("c"));
    }

    @Test
    public void testFourFileWithOneEmptyState() throws Exception {
        ls.put("c", 1);
        assertEquals(1, ls.get("c"));
        ls.put("c", 2);
        assertEquals(2, ls.get("c"));
        ls.put("c", 3);
        assertEquals(3, ls.get("c"));

        //touch empty file
        String emptyPath = vs.createVersion();
        touchVersion(emptyPath);
        assertEquals(3, ls.get("c"));

        //another empty file
        emptyPath = vs.createVersion();
        touchVersion(emptyPath);
        assertEquals(3, ls.get("c"));

        ls.put("c", 4);
        assertEquals(4, ls.get("c"));
    }

    private void touchVersion(String versionPath) throws IOException {
        FileUtils.openOutputStream(new File(versionPath));
        FileUtils.openOutputStream(new File(versionPath + ".version"));
    }
}