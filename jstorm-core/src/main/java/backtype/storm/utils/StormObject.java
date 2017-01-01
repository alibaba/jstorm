package backtype.storm.utils;

import java.util.Map;

public interface StormObject {

	void init(Map conf) throws Exception;
	void cleanup();
}
