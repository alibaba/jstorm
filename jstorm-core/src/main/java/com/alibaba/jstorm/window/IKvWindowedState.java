package com.alibaba.jstorm.window;

import java.util.Collection;
import java.util.Map;

import com.alibaba.jstorm.window.TimeWindow;

public interface IKvWindowedState<K, V> {
    public V get(TimeWindow window, K key);

    public void put(TimeWindow window, K key, V value);

    public void putBatch(TimeWindow window, Map<K, V> batch);

    public Map<K, V> getBatch(TimeWindow window, Collection<K> keys);

    public Collection<K> getAllKeys(TimeWindow window);

    public Map<K, V> getBatch(TimeWindow window);

    public void remove(TimeWindow window, K key);

    public Collection<TimeWindow> listWindows();

    public void removeWindow(TimeWindow window);

    public void clear();
}