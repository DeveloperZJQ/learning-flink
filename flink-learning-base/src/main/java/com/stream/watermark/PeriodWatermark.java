package com.stream.watermark;

import com.happy.common.model.TransInfo;
import com.happy.common.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

import java.util.Objects;

import static com.happy.common.utils.DateUtil.YYYY_MM_DD_HH_MM_SS;

/**
 * @author happy
 * @since 2020-07-11 16:30
 */
@Slf4j
public class PeriodWatermark implements AssignerWithPeriodicWatermarks<TransInfo> {
    private Long currentTimestamp = Long.MIN_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long maxTimeLag = 5000;       //最大延时5s
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
    }

    @Override
    public long extractTimestamp(TransInfo transInfo, long l) {
        log.info("extractTimestamp transInfo:" + transInfo.toString() + " ,l:" + l);

        long timestamp = transInfo.getTimeStamp();
        currentTimestamp = Math.max(timestamp, currentTimestamp);
        log.info("event timestamp = {}, {}, CurrentWatermark = {}, {}", transInfo.getTimeStamp(),
                DateUtil.format(transInfo.getTimeStamp(), YYYY_MM_DD_HH_MM_SS),
                Objects.requireNonNull(getCurrentWatermark()).getTimestamp(),
                DateUtil.format(getCurrentWatermark().getTimestamp(), YYYY_MM_DD_HH_MM_SS));
        return transInfo.getTimeStamp();
    }
}
