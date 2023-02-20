package io.trino.plugin.audience;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;


public class BearerTokenGen
{
    private static HttpClient client =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(100)).build();

    public static String getBearerToken() throws IOException, InterruptedException
    {
        StringBuffer fileData = new StringBuffer();
        BufferedReader reader = new BufferedReader(new FileReader("/Users/Admin1/IdeaProjects/trino_bkp11feb/plugin/trino-audience/src/main/resources/file.json"));
        char[] buf = new char[1024];
        int numRead=0;
        while ((numRead=reader.read(buf)) != -1) {
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
        }
        reader.close();


        java.net.http.HttpRequest tokenRequest = java.net.http.HttpRequest.newBuilder()
                .uri(URI.create("https://dev.barb-api.co.uk/api/v1/auth/token/"))
                .GET()
                .headers("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(fileData.toString()))
                .build();

        HttpResponse<String> response = client.send(tokenRequest, BodyHandlers.ofString());
        String token = response.body();
        JSONObject jsonObject = new JSONObject(token);
        String tokenResponse = jsonObject.getString("access").replaceAll("\\s+", "");
        return tokenResponse;
    }
}
