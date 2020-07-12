package com.stream.watermark;

import com.happy.common.model.TransInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author happy
 * @create 2020-07-11 16:49
 */
@Slf4j
public class PunctuatedWatermark implements AssignerWithPunctuatedWatermarks<TransInfo> {
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(TransInfo transInfo, long l) {
        log.info("checkAndGetNextWatermark transInfo:"+transInfo.toString()+" ,l:"+l);
        return l % 3 ==0? new Watermark(l):null;
    }

    @Override
    public long extractTimestamp(TransInfo transInfo, long l) {
        log.info("extractTimestamp transInfo:"+transInfo.toString()+" ,l:"+l);
        return transInfo.getTimeStamp();
    }
}
