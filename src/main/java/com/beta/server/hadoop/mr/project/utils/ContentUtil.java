package com.beta.server.hadoop.mr.project.utils;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by beta on 2019/2/7.
 */
public class ContentUtil {
    public static String getPageId(String url) {
        String pageId = "-";

        if (StringUtils.isNotBlank(url)) {
            Pattern compile = Pattern.compile("topicId=[0-9]+");
            Matcher matcher = compile.matcher(url);
            if (matcher.find()) {
                pageId = matcher.group().split("topicId=")[1];
            }
        }
        return pageId;
    }

    public static void main(String[] args) {
        String pageId = getPageId("http://www.yihaodian.com/cms/view.do?topicId=22331&cache");
        System.out.println(pageId);
    }
}
