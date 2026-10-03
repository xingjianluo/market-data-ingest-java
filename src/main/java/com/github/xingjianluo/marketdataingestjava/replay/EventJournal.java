package com.github.xingjianluo.marketdataingestjava.replay;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 原始事件日志：按接收顺序留存每一条进入管道的原始 JSON，以及首次处理结论。
 *
 * <p>用途：整体重放核对时，从干净状态按日志逐条重放，与首次结论比对。
 * 追加与快照都在管道单一临界区内完成；遍历/拷贝另加自身锁，保证读者看到一致列表。
 */
public class EventJournal {

    private final List<JournaledEvent> entries = new ArrayList<>();
    private final AtomicInteger sequence = new AtomicInteger();

    public void append(JournaledEvent entry) {
        synchronized (entries) {
            entries.add(entry);
        }
    }

    public int size() {
        synchronized (entries) {
            return entries.size();
        }
    }

    /** 返回按接收顺序的拷贝，供从干净状态整体重放使用。 */
    public List<JournaledEvent> copyAll() {
        synchronized (entries) {
            return new ArrayList<>(entries);
        }
    }

    public void clear() {
        synchronized (entries) {
            entries.clear();
            sequence.set(0);
        }
    }

    public int nextOrdinal() {
        return sequence.getAndIncrement();
    }
}
