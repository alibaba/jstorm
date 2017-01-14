package storm.trident.topology.state;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import backtype.storm.utils.ZookeeperAuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.framework.api.ProtectACLCreateModePathAndBytesable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.json.simple.JSONValue;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class TransactionalStateZkStorage implements ITransactionalStateStorage {

    CuratorFramework _curator;
    List<ACL> _zkAcls = null;

    public TransactionalStateZkStorage(Map conf, String id, String subroot) {
        try {
            conf = new HashMap(conf);
            String transactionalRoot = (String) conf.get(Config.TRANSACTIONAL_ZOOKEEPER_ROOT);
            String rootDir = transactionalRoot + "/" + id + "/" + subroot;
            List<String> servers = (List<String>) getWithBackup(conf, Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, Config.STORM_ZOOKEEPER_SERVERS);
            Object port = getWithBackup(conf, Config.TRANSACTIONAL_ZOOKEEPER_PORT, Config.STORM_ZOOKEEPER_PORT);
            ZookeeperAuthInfo auth = new ZookeeperAuthInfo(conf);
            CuratorFramework initter = Utils.newCuratorStarted(conf, servers, port, auth);
            _zkAcls = Utils.getWorkerACL(conf);
            try {
                createNode(initter, transactionalRoot, null, null, null);
            } catch (KeeperException.NodeExistsException e) {
            }
            try {
                createNode(initter, rootDir, null, _zkAcls, null);
            } catch (KeeperException.NodeExistsException e) {
            }
            initter.close();

            _curator = Utils.newCuratorStarted(conf, servers, port, rootDir, auth);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected String forPath(PathAndBytesable<String> builder,
                             String path, byte[] data) throws Exception {
        return (data == null)
                ? builder.forPath(path)
                : builder.forPath(path, data);
    }

    protected void createNode(CuratorFramework curator, String path,
                              byte[] data, List<ACL> acls, CreateMode mode) throws Exception {
        ProtectACLCreateModePathAndBytesable<String> builder =
                curator.create().creatingParentsIfNeeded();

        if (acls == null) {
            if (mode == null) {
                forPath(builder, path, data);
            } else {
                forPath(builder.withMode(mode), path, data);
            }
            return;
        }

        forPath(builder.withACL(acls), path, data);
    }

    public void setData(String path, Object obj) {
        path = "/" + path;
        byte[] ser;
        try {
            ser = JSONValue.toJSONString(obj).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        try {
            if (_curator.checkExists().forPath(path) != null) {
                _curator.setData().forPath(path, ser);
            } else {
                createNode(_curator, path, ser, _zkAcls,
                        CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(String path) {
        path = "/" + path;
        try {
            _curator.delete().forPath(path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> list(String path) {
        path = "/" + path;
        try {
            if (_curator.checkExists().forPath(path) == null) {
                return new ArrayList<String>();
            } else {
                return _curator.getChildren().forPath(path);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void mkdir(String path) {
        setData(path, 7);
    }

    public Object getData(String path) {
        path = "/" + path;
        try {
            if (_curator.checkExists().forPath(path) != null) {
                return JSONValue.parse(new String(_curator.getData().forPath(path), "UTF-8"));
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        _curator.close();
    }

    private Object getWithBackup(Map amap, Object primary, Object backup) {
        Object ret = amap.get(primary);
        if (ret == null) return amap.get(backup);
        return ret;
    }
}
