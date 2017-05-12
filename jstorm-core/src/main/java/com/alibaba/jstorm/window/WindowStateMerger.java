package com.alibaba.jstorm.window;

import java.io.Serializable;

/**
 * Merge window states which are assigned separately but belong to the same session window later.
 *
 * @author wange
 * @since 16/12/19
 */
public interface WindowStateMerger extends Serializable {
    Object reduceState(Object state1, Object state2);
}
