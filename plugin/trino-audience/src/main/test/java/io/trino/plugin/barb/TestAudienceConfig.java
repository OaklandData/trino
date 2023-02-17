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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestAudienceConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AudienceConfig.class)
                .setMetadata(null));
    }

    @Test
    public void testExplicitPropertyMappings() throws URISyntaxException
    {
        Map<String, String> properties = ImmutableMap.of("metadata-uri", "file://test.json");

        AudienceConfig expected = new AudienceConfig()
                .setMetadata(URI.create("file://test.json"));

        assertFullMapping(properties, expected);
    }
}
