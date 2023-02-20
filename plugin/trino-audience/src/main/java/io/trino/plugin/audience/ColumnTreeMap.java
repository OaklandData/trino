package io.trino.plugin.audience;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.TreeMap;

public class ColumnTreeMap
{
    public static HashMap<String, ArrayList<String>> strings = new LinkedHashMap<String, ArrayList<String>>();
    public static TreeMap<String, ArrayList<String>> treeMap = new TreeMap<>();
    private String metadata;
    private Set<String> setOfKeySet;

    public ColumnTreeMap(String metadata)
    {
        this.metadata = metadata;
    }

    public TreeMap<String, ArrayList<String>> getColumnTreeMap() throws IOException, InterruptedException, ParseException
    {
        String tokenResponse = BearerTokenGen.getBearerToken();

        URL url = new URL(metadata);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestProperty("Authorization", "Bearer " + tokenResponse);

        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestMethod("GET");

        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String inputLine;
        StringBuilder response = new StringBuilder();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        JSONParser parser = new JSONParser();
        Object obj = parser.parse(response.toString());
        JSONObject jsonObject = (JSONObject) obj;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(response.toString());
        printRec(jsonNode, "");
        treeMap.putAll(strings);
        return treeMap;
    }

    private static void printRec(JsonNode jsonNode, String key)
    {
        if (jsonNode.isValueNode()) {
            add(key, jsonNode.toString());
        }
        else if (jsonNode.isObject()) {
            jsonNode.fields().forEachRemaining(field -> printRec(field.getValue(), field.getKey()));
        }
        else if (jsonNode.isArray()) {
            for (int i = 0; i < jsonNode.size(); i++) {
                printRec(jsonNode.get(i), key + i);
            }
        }
    }

    private static void add(String key, String value)
    {
        ArrayList<String> values = strings.get(key);
        if (values == null) {
            values = new ArrayList<String>();
            strings.put(key, values);
        }

        values.add(value);
    }

    public String getTableName()
    {
        String metadataWithoutParameters = metadata.split("\\?")[0];
        String[] bits = metadataWithoutParameters.split("/");
        String finalTableName = bits[bits.length-1];
        return finalTableName;
    }
}
