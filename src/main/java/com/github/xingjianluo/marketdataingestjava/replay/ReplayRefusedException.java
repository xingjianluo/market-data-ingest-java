package com.github.xingjianluo.marketdataingestjava.replay;

/**
 * 整体重放被明确拒绝：调用方给出的续处理起始位点不允许在干净起点上使用
 * （干净状态每源已处理位点为 -1，任何非 -1 的位置——更靠前或更靠后——都拒绝，
 * 不允许从中间悄悄接上去）。
 */
public class ReplayRefusedException extends RuntimeException {

    public ReplayRefusedException(String message) {
        super(message);
    }
}
