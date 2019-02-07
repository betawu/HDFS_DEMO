package com.beta.server.hadoop.mr.project.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by beta on 2019/2/7.
 */
public class LogParser {
    public Map<String, String> parse(String log) {
        String[] arr = log.split("\001");

        String ip = arr[13];
        Map<String, String> map = new HashMap<>();

        IPParser.RegionInfo info = IPParser.getInstance().analyseIp(ip);

        map.put("ip",ip);
        map.put("country", info.getCountry() == null ? "-" : info.getCountry());
        map.put("province", info.getProvince() == null ? "-" : info.getProvince());
        map.put("city", info.getCity() == null ? "-" : info.getCity());

        return map;
    }
}
