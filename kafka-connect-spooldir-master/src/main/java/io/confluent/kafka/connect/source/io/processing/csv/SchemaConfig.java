/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
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
package io.confluent.kafka.connect.source.io.processing.csv;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import io.confluent.kafka.connect.source.SpoolDirectoryConfig;
import io.confluent.kafka.connect.source.io.processing.FileMetadata;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SchemaConfig {
  public String name;
  public String keyName;
  public List<String> keys = new ArrayList<>();
  public List<FieldConfig> fields = new ArrayList<>();


  public Pair<ParserConfig, ParserConfig> parserConfigs(SpoolDirectoryConfig config) {
    Preconditions.checkNotNull(fields, "fields cannot be null.");
    Preconditions.checkNotNull(name, "SchemaConfig.name cannot be null");
    Preconditions.checkState(!name.isEmpty(), "SchemaConfig.name cannot be null");
    Preconditions.checkState(!name.equalsIgnoreCase(keyName), "SchemaConfig.name and SchemaConfig.keyName cannot be the same.");

    SchemaBuilder valueBuilder = SchemaBuilder.struct();
    valueBuilder.name(this.name);

    Preconditions.checkNotNull(keys, "keys cannot be null.");
    SchemaBuilder keyBuilder = SchemaBuilder.struct();

    if (null == this.keyName || this.keyName.isEmpty()) {
      String keySchemaName = (this.name == null ? "" : this.name) + "Key";
      keyBuilder.name(keySchemaName);
    } else {
      keyBuilder.name(this.keyName);
    }

    Set<String> keyLookup = new HashSet<>(this.keys);

    List<FieldMapping> valueMaps = new ArrayList<>();
    List<FieldMapping> keyMaps = new ArrayList<>();

    for (FieldConfig fieldConfig : this.fields) {
      valueBuilder.field(fieldConfig.name, fieldConfig.schema());

      FieldMapping mapping = new FieldMapping(fieldConfig.index, fieldConfig.name, fieldConfig.schema());
      valueMaps.add(mapping);

      if (keyLookup.contains(fieldConfig.name)) {
        keyMaps.add(mapping);
        keyBuilder.field(fieldConfig.name, fieldConfig.schema());
        keyLookup.remove(fieldConfig.name);
      }
    }

    Preconditions.checkState(keyLookup.isEmpty(), "Keys specified were not found in the schema. The following key field(s) " +
        "were not found: %s", Joiner.on(",").join(keyLookup));

    if (config.includeFileMetadata()) {
      FileMetadata.addFieldSchema(valueBuilder);
    }

    ParserConfig keyParserConfig = new ParserConfig(this.keys.isEmpty() ? null : keyBuilder.build(), keyMaps);
    ParserConfig valueParserConfig = new ParserConfig(valueBuilder.build(), valueMaps);


    return new ImmutablePair<>(keyParserConfig, valueParserConfig);
  }

  class FieldMapping {
    final int index;
    final String fieldName;
    final Schema schema;

    FieldMapping(int index, String fieldName, Schema schema) {
      this.index = index;
      this.fieldName = fieldName;
      this.schema = schema;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("index", this.index)
          .add("fieldName", this.fieldName)
          .add("schema", this.schema)
          .omitNullValues()
          .toString();
    }
  }


  public class ParserConfig {
    public final List<FieldMapping> mappings;
    public final Schema structSchema;

    public ParserConfig(Schema structSchema, List<FieldMapping> mappings) {
      this.structSchema = structSchema;
      this.mappings = mappings;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("mappings", this.mappings)
          .add("schema", this.structSchema)
          .omitNullValues()
          .toString();
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", this.name)
        .add("keys", this.keys)
        .add("fields", this.fields)
        .omitNullValues()
        .toString();
  }
}
