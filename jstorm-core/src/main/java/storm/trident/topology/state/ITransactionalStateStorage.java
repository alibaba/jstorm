package storm.trident.topology.state;

import java.util.List;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public interface ITransactionalStateStorage {

    public void setData(String path, Object obj);
    public void delete(String path);
    public List<String> list(String path);
    public void mkdir(String path);
    public Object getData(String path);
    public void close();
}
