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
package com.alibaba.jstorm.hdfs.blobstore;

import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import com.alibaba.jstorm.blobstore.AtomicOutputStream;
import com.alibaba.jstorm.blobstore.ClientBlobStore;
import com.alibaba.jstorm.blobstore.InputStreamWithMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 *  Client to access the HDFS blobStore. At this point, this is meant to only be used by the
 *  supervisor.  Don't trust who the client says they are so pass null for all Subjects.
 *
 *  The HdfsBlobStore implementation takes care of the null Subjects. It assigns Subjects
 *  based on what hadoop says who the users are. These users must be configured accordingly
 *  in the SUPERVISOR_ADMINS for ACL validation and for the supervisors to download the blobs.
 *  This API is only used by the supervisor in order to talk directly to HDFS.
 */
public class HdfsClientBlobStore extends ClientBlobStore {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsClientBlobStore.class);
    private HdfsBlobStore _blobStore;
    private Map _conf;

    @Override
    public void prepare(Map conf) {
        this._conf = conf;
        _blobStore = new HdfsBlobStore();
        _blobStore.prepare(conf, null, null);
    }

    @Override
    public AtomicOutputStream createBlobToExtend(String key, SettableBlobMeta meta)
            throws KeyAlreadyExistsException {
        return _blobStore.createBlob(key, meta);
    }

    @Override
    public AtomicOutputStream updateBlob(String key)
            throws KeyNotFoundException {
        return _blobStore.updateBlob(key);
    }

    @Override
    public ReadableBlobMeta getBlobMeta(String key)
            throws KeyNotFoundException {
        return _blobStore.getBlobMeta(key);
    }

    @Override
    public void setBlobMetaToExtend(String key, SettableBlobMeta meta)
            throws KeyNotFoundException {
        _blobStore.setBlobMeta(key, meta);
    }

    @Override
    public void deleteBlob(String key) throws KeyNotFoundException {
        _blobStore.deleteBlob(key);
    }

    @Override
    public InputStreamWithMeta getBlob(String key)
            throws KeyNotFoundException {
        return _blobStore.getBlob(key);
    }

    @Override
    public Iterator<String> listKeys() {
        return _blobStore.listKeys();
    }

    @Override
    public int getBlobReplication(String key) throws KeyNotFoundException {
        return _blobStore.getBlobReplication(key);
    }

    @Override
    public int updateBlobReplication(String key, int replication) throws KeyNotFoundException {
        return _blobStore.updateBlobReplication(key, replication);
    }

    @Override
    public boolean setClient(Map conf, NimbusClient client) {
        return true;
    }

    @Override
    public void createStateInZookeeper(String key) {
        // Do nothing
    }

    @Override
    public void shutdown() {
        // do nothing
    }
}
