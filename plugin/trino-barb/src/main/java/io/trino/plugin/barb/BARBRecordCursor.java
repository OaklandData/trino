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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.io.ByteSource;
import com.google.common.io.CountingInputStream;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.nio.charset.StandardCharsets.UTF_8;

public class BARBRecordCursor
        implements RecordCursor
{
    private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();

    private final List<BARBColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final Iterator<String> lines;
    private final long totalBytes;

    private List<String> fields;

    public BARBRecordCursor(List<BARBColumnHandle> columnHandles, ByteSource byteSource) throws IOException, ParseException
    {
        this.columnHandles = columnHandles;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            BARBColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        URL url = new URL("https://dev.barb-api.co.uk/api/v1/stations");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestProperty("Authorization", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNjc2NTE1NDIzLCJpYXQiOjE2NzY0NzIyMjMsImp0aSI6IjQ4NTBjZjYwYThjZTRlNTliYTE0MDJiMjNiNDU3ODg4IiwidXNlcl9pZCI6IjljMTAzNmI2LTM1NTAtNDhhYS05YjkzLTBjNjU1NGVmMjcwZCJ9.hoArvOuF928gjsd2ptX4EJtgjjD4LqmRUpKukpQq4gQ");

        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestMethod("GET");

        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        // System.out.println(response.toString());


        JSONParser parser = new JSONParser();
        //JSONObject jsonObj = (JSONObject) parser.parse(response.toString());
        Object obj = parser.parse(response.toString());
        JSONArray array = (JSONArray) obj;
        Iterator iter = array.iterator();

        ArrayList al = new ArrayList();

        while (iter.hasNext()) {
            JSONObject json = (JSONObject) iter.next();
            Iterator<String> keys = json.keySet().iterator();
            String record = "";
            int counter=0;
            while (keys.hasNext()) {
                String key = keys.next();
                if(counter == 0)
                    record += json.get(key) + ",";
                else {
                    record += json.get(key);
                    al.add(record);
                }
                counter++;
            }
        }
        lines = al.iterator();
        totalBytes = response.toString().length();
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
