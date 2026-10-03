package com.github.xingjianluo.marketdataingestjava.replay;

import com.github.xingjianluo.marketdataingestjava.state.PipelineState;
import com.github.xingjianluo.marketdataingestjava.state.SnapshotRepository;

/**
 * 重放专用快照仓库：干净重放不得触碰线上快照文件，
 * 任何 save/load 都直接失败/返回空，确保重放只从内存空白状态开始。
 */
public class NoopSnapshotRepository implements SnapshotRepository {

    @Override
    public void save(PipelineState state) {
        throw new UnsupportedOperationException("scratch replay must not persist snapshots");
    }

    @Override
    public PipelineState load() {
        return null;
    }
}
