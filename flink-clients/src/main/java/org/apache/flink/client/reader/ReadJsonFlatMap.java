package org.apache.flink.client.reader;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class ReadJsonFlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textFile = env.readTextFile("location.json");

        Location location = new Location();
        SingleOutputStreamOperator<Location> countryOperator = textFile.flatMap(
                (FlatMapFunction<String, Location>) (value, out) -> {
                    JSONArray jsonArray = JSON.parseArray(value);
                    for (Object o : jsonArray) {
                        Country country = JSON.parseObject(o.toString(), Country.class);
                        List<Province> states = country.getStates();
                        if (states.isEmpty()) {
                            System.out.println("states isEmpty.");
                            continue;
                        }
                        for (Province state : states) {
                            List<City> cities = state.getCities();
                            if (cities.isEmpty()) {
                                System.out.println("cities isEmpty.");
                                continue;
                            }
                            for (City city : cities) {
                                location.setId(Long.parseLong(country.getCouId() + city.getSttId() + city.getCitId() + 3));
                                location.setName(city.getName());
                                location.setParent_id(Long.parseLong(country.getCouId() + city.getSttId() + 2));
                                location.setType(3);
                                location.setCountry_code(country.getCc2());
                                out.collect(location);
                            }
                            location.setId(Long.parseLong(country.getCouId() + state.getSttId() + 2));
                            location.setName(state.getName());
                            location.setParent_id(Long.parseLong(state.getCouId()));
                            location.setType(2);
                            location.setCountry_code(country.getCc2());
                            out.collect(location);
                        }
                        location.setId(Long.parseLong(country.getCouId()));
                        location.setName(country.getName());
                        location.setParent_id(0L);
                        location.setType(1);
                        location.setCountry_code(country.getCc2());
                        out.collect(location);
                    }
                }).returns(Location.class);

        countryOperator.map((MapFunction<Location, String>) JSON::toJSONString)
                .writeAsText("location.csv", FileSystem.WriteMode.OVERWRITE);

        /*
        countryOperator.addSink(StreamingFileSink.forRowFormat(
                        new Path("location.csv"), new SimpleStringEncoder<Location>("UTF-8")
                ).withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(5))
                                .withInactivityInterval(Duration.ofMinutes(5))
                                .withMaxPartSize(MemorySize.ofMebiBytes(1024 * 1024 * 1024))
                                .build())
                .build());

         */
//        countryOperator.print();

        env.execute(ReadJsonFlatMap.class.getName());
    }
}