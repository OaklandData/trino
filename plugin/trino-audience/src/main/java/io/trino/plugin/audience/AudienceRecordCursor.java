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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.io.ByteSource;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public class AudienceRecordCursor
        implements RecordCursor
{
    private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();
    public static HashMap<String, ArrayList<String>> strings = new LinkedHashMap<String, ArrayList<String>>();
    public static TreeMap<String, ArrayList<String>> treeMap = new TreeMap<>();

    private final List<AudienceColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final Iterator<String> lines;
    private final long totalBytes;

    private List<String> fields;
    private static HttpClient client =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(100)).build();

    @SuppressWarnings("checkstyle:RegexpMultiline")
    public AudienceRecordCursor(List<AudienceColumnHandle> columnHandles, ByteSource byteSource) throws IOException, ParseException, InterruptedException
    {
        this.columnHandles = columnHandles;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            AudienceColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        ColumnTreeMap columnTreeMap = new ColumnTreeMap("https://dev.barb-api.co.uk/api/v1/audiences_by_time?min_transmission_date=2023-01-01&max_transmission_date=2023-12-31&time_period_length=15&viewing_status=live");
        treeMap = columnTreeMap.getColumnTreeMap();
        Set<String> setOfKeySet = treeMap.keySet();
        String fieldForColumnCount = setOfKeySet.iterator().next().toString();
        ArrayList al = new ArrayList();
        for (int i = 0; i < treeMap.get(fieldForColumnCount).size(); i++) {
            String record = "";
            for (String key : setOfKeySet) {
                record += treeMap.get(key).get(i) + ",";
            }
            record = record.substring(0, record.length() - 1);
            al.add(record);
        }

        lines = al.iterator();
        totalBytes = treeMap.toString().length();

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

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!lines.hasNext()) {
            return false;
        }
        String line = lines.next();
        fields = LINE_SPLITTER.splitToList(line);

        return true;
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        return fields.get(columnIndex);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }
}