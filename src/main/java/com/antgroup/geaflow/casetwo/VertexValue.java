package com.antgroup.geaflow.casetwo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VertexValue {
    VertexValue() {
        in = new HashMap<>();
        out = new HashMap<>();
        ret = 0;
    }

    public Map<Long, Integer> in;
    public Map<Long, Integer> out;
    public int ret;
}
