package com.github.xingjianluo.marketdataingestjava.state;

/**
 * 纯内存快照仓库：不落盘、不共享。供整体重放核对等
 * “必须从一个干净空白状态起步”的场景使用——每次新建实例即为空白起点。
 */
public class InMemorySnapshotRepository implements SnapshotRepository {

    private PipelineState state;

    @Override
    public synchronized void save(PipelineState state) {
        this.state = state;
    }

    @Override
    public synchronized PipelineState load() {
        return state;
    }
}
