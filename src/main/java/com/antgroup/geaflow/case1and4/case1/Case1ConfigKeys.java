package com.antgroup.geaflow.case1and4.case1;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;

public class Case1ConfigKeys {
    public static final ConfigKey SOURCE_PARALLELISM = ConfigKeys.key("geaflow.source.parallelism").defaultValue(16).description("job source parallelism");
    public static final ConfigKey SINK_PARALLELISM = ConfigKeys.key("geaflow.sink.parallelism").defaultValue(16).description("job sink parallelism");
    public static final ConfigKey MAP_PARALLELISM = ConfigKeys.key("geaflow.map.parallelism").defaultValue(1).description("job map parallelism");
    public static final ConfigKey REDUCE_PARALLELISM = ConfigKeys.key("geaflow.reduce.parallelism").defaultValue(1).description("job reduce parallelism");
    public static final ConfigKey ITERATOR_PARALLELISM = ConfigKeys.key("geaflow.iterator.parallelism").defaultValue(16).description("job iterator parallelism");
    public static final ConfigKey AGG_PARALLELISM = ConfigKeys.key("geaflow.agg.parallelism").defaultValue(1).description("job agg parallelism");
    public static final ConfigKey GEAFLOW_SINK_TYPE = ConfigKeys.key("geaflow.sink.type").defaultValue("console").description("job sink type, console or file");

}
