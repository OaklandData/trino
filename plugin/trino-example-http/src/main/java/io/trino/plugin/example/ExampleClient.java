/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.example;


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;



public class ExampleClient
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Supplier<Map<String, Map<String, ExampleTable>>> schemas;

    @Inject
    public ExampleClient(ExampleConfig config, JsonCodec<Map<String, List<ExampleTable>>> catalogCodec)
    {
        requireNonNull(catalogCodec, "catalogCodec is null");
        schemas = Suppliers.memoize(schemasSupplier(catalogCodec, config.getMetadata()));
    }

    public Set<String> getSchemaNames()
    {
        return schemas.get().keySet();
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        Map<String, ExampleTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public ExampleTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, ExampleTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    private static Supplier<Map<String, Map<String, ExampleTable>>> schemasSupplier(JsonCodec<Map<String, List<ExampleTable>>> catalogCodec, URI metadataUri)
    {
        return () -> {
            try {
                return lookupSchemas(metadataUri, catalogCodec);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            catch (ParseException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static Map<String, Map<String, ExampleTable>> lookupSchemas(URI metadataUri, JsonCodec<Map<String, List<ExampleTable>>> catalogCodec)
            throws IOException, ParseException
    {
        URL result = metadataUri.toURL();

        URL url = new URL("https://dev.barb-api.co.uk/api/v1/stations");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestProperty("Authorization", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNjc2NDEwMDQyLCJpYXQiOjE2NzYzNjY4NDIsImp0aSI6ImU0YjI3NGQ1MWI4YzQxZjFiMTZmZmI3MmJjMmIzOTBjIiwidXNlcl9pZCI6IjljMTAzNmI2LTM1NTAtNDhhYS05YjkzLTBjNjU1NGVmMjcwZCJ9.KVfWzyYL941Ah6Z9PLPTcnVaC1VfH1OPbg4z4T3nkwY");

        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestMethod("GET");

        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        System.out.println(response.toString());


        JSONParser parser = new JSONParser();
        //JSONObject jsonObj = (JSONObject) parser.parse(response.toString());
        Object obj  = parser.parse(response.toString());
        JSONArray array = (JSONArray) obj;
        Iterator iter = array.iterator();
        String jsonString = "";

        //       while (iter.hasNext()) {
        JSONObject json = (JSONObject) iter.next();
        System.out.println(json);
        Iterator<String> keys = json.keySet().iterator();
        String record = "";


        while (keys.hasNext()) {
            String key = keys.next();
            record = "";
            record += "Key :" + key + "  Value :" + json.get(key);
            System.out.println("Key :" + key + "  Value :" + json.get(key));
            jsonString = "stations:[" + "{\"" + key + "\":\"" + json.get(key) + "\"}";
            //           }

        }
        record = "[" + record + "]";
   //     String json2 = Resources.toString(record, UTF_8);

 /*       URL result = metadataUri.toURL();
        String json = Resources.toString(result, UTF_8);
        System.out.println(json);*/
        List<ExampleTable> stationListTable = new ArrayList<>();
        ExampleTable stationTable = new ExampleTable("station",[],"")
        Map<String, List<ExampleTable>> catalog = catalogCodec.fromJson(record);

        return ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
    }

    private static Function<List<ExampleTable>, Map<String, ExampleTable>> resolveAndIndexTables(URI metadataUri)
    {
        return tables -> {
            Iterable<ExampleTable> resolvedTables = transform(tables, tableUriResolver(metadataUri));
            return ImmutableMap.copyOf(uniqueIndex(resolvedTables, ExampleTable::getName));
        };
    }

    private static Function<ExampleTable, ExampleTable> tableUriResolver(URI baseUri)
    {
        return table -> {
            List<URI> sources = ImmutableList.copyOf(transform(table.getSources(), baseUri::resolve));
            return new ExampleTable(table.getName(), table.getColumns(), sources);
        };
    }
}
