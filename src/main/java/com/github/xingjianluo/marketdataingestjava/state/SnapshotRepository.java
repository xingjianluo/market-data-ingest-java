package com.github.xingjianluo.marketdataingestjava.state;

/**
 * 快照持久化接口。默认提供本地文件实现，不依赖任何外部服务。
 */
public interface SnapshotRepository {

    void save(PipelineState state);

    /** 无快照时返回 null。 */
    PipelineState load();
}
