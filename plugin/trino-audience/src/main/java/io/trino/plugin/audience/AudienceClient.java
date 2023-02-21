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
package io.trino.plugin.audience;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.trino.spi.type.VarcharType;
import org.json.simple.parser.ParseException;

import javax.inject.Inject;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;

public class AudienceClient
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Supplier<Map<String, Map<String, AudienceTable>>> schemas;

    @Inject
    public AudienceClient(AudienceConfig config, JsonCodec<Map<String, List<AudienceTable>>> catalogCodec)
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
        Map<String, AudienceTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public AudienceTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, AudienceTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    private static Supplier<Map<String, Map<String, AudienceTable>>> schemasSupplier(JsonCodec<Map<String, List<AudienceTable>>> catalogCodec, URI metadataUri)
    {
        return () -> {
            try {
                return lookupSchemas(metadataUri, catalogCodec);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            catch (ParseException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static Map<String, Map<String, AudienceTable>> lookupSchemas(URI metadataUri, JsonCodec<Map<String, List<AudienceTable>>> catalogCodec)
            throws IOException, ParseException, InterruptedException
    {
        URL result = metadataUri.toURL();
        //String json = Resources.toString(result, UTF_8);
        ColumnTreeMap columnTreeMap = new ColumnTreeMap(result.toString());
        TreeMap<String, ArrayList<String>> treeMap = new TreeMap<>();
        treeMap = columnTreeMap.getColumnTreeMap();
        Set<String> setOfKeySet = treeMap.keySet();

        List<AudienceColumn> columnList = new ArrayList<AudienceColumn>();
        for (String key : setOfKeySet) {
            columnList.add(new AudienceColumn(key, VarcharType.createUnboundedVarcharType()));
        }

        String tableName = columnTreeMap.getTableName();

        List<AudienceTable> tableList = new ArrayList<AudienceTable>();
        tableList.add(new AudienceTable(tableName, columnList));
        Map<String, List<AudienceTable>> catalog = new HashMap<>();
        catalog.put("audienceschema", tableList);
        return ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
    }

    private static Function<List<AudienceTable>, Map<String, AudienceTable>> resolveAndIndexTables(URI metadataUri)
    {
        return tables -> {
            Iterable<AudienceTable> resolvedTables = transform(tables, tableUriResolver(metadataUri));
            return ImmutableMap.copyOf(uniqueIndex(resolvedTables, AudienceTable::getName));
        };
    }

    private static Function<AudienceTable, AudienceTable> tableUriResolver(URI baseUri)
    {
        return table -> {
           // List<URI> sources = ImmutableList.copyOf(transform(table.getSources(), baseUri::resolve));
            URL result = null;
            try {
                result = baseUri.toURL();
            }
            catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
            //String json = Resources.toString(result, UTF_8);
            ColumnTreeMap columnTreeMap = new ColumnTreeMap(result.toString());
            TreeMap<String, ArrayList<String>> treeMap = new TreeMap<>();
            try {
                treeMap = columnTreeMap.getColumnTreeMap();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            catch (ParseException e) {
                throw new RuntimeException(e);
            }
            Set<String> setOfKeySet = treeMap.keySet();

            List<AudienceColumn> columnList = new ArrayList<AudienceColumn>();
            for (String key : setOfKeySet) {
                columnList.add(new AudienceColumn(key, VarcharType.createUnboundedVarcharType()));
            }

            String tableName = columnTreeMap.getTableName();
            return new AudienceTable(tableName, columnList);
        };
    }
}
