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
package io.trino.plugin.barb;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;

public class BARBClient
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Supplier<Map<String, Map<String, BARBTable>>> schemas;

    @Inject
    public BARBClient(BARBConfig config, JsonCodec<Map<String, List<BARBTable>>> catalogCodec)
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
        Map<String, BARBTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public BARBTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, BARBTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    private static Supplier<Map<String, Map<String, BARBTable>>> schemasSupplier(JsonCodec<Map<String, List<BARBTable>>> catalogCodec, URI metadataUri)
    {
        return () -> {
            try {
                return lookupSchemas(metadataUri, catalogCodec);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private static Map<String, Map<String, BARBTable>> lookupSchemas(URI metadataUri, JsonCodec<Map<String, List<BARBTable>>> catalogCodec)
            throws IOException
    {
        URL result = metadataUri.toURL();
        //String json = Resources.toString(result, UTF_8);
        List<BARBColumn> columnList = new ArrayList<BARBColumn>();
        columnList.add(new BARBColumn("station_name", VarcharType.createUnboundedVarcharType()));
        columnList.add(new BARBColumn("station_code", VarcharType.createUnboundedVarcharType()));

        List<BARBTable> tableList = new ArrayList<BARBTable>();
        tableList.add(new BARBTable("stations", columnList));
        Map<String, List<BARBTable>> catalog = new HashMap<>();
        catalog.put("barbschema", tableList);

        return ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
    }

    private static Function<List<BARBTable>, Map<String, BARBTable>> resolveAndIndexTables(URI metadataUri)
    {
        return tables -> {
            Iterable<BARBTable> resolvedTables = transform(tables, tableUriResolver(metadataUri));
            return ImmutableMap.copyOf(uniqueIndex(resolvedTables, BARBTable::getName));
        };
    }

    private static Function<BARBTable, BARBTable> tableUriResolver(URI baseUri)
    {
        return table -> {
           // List<URI> sources = ImmutableList.copyOf(transform(baseUri::resolve));
            List<BARBColumn> columnList = new ArrayList<BARBColumn>();
            columnList.add(new BARBColumn("station_name", VarcharType.createUnboundedVarcharType()));
            columnList.add(new BARBColumn("station_code", VarcharType.createUnboundedVarcharType()));
            return new BARBTable("stations", columnList);
        };
    }
}