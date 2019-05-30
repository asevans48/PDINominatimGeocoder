package com.si;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

public class TestSimpleJson {

    @Test
    public void shouldParseJson() throws IOException, ParseException {
        String json = "[{\"a\": 2}, {\"a\": 1, \"test\": \"hello\"}]";
        JSONParser parser = new JSONParser();
        Reader reader = new StringReader(json);
        JSONArray jarr = (JSONArray) parser.parse(reader);
        assert(jarr != null);
        assert(jarr.size() == 2);
        JSONObject jobject = (JSONObject) jarr.get(0);
        JSONObject jo2 = (JSONObject) jarr.get(1);
        assert(jobject != null);
        assert((Long)jobject.get("a") == 2);
        assert(jo2 != null);
        assert((Long)jo2.get("a") == 1);
    }
}
