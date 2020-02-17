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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

// TODO: ElasticsearchColumnHandle vs ElasticsearchColumn ? 看代码，像是等价关系，因为有些connector就没有XXXColumn, 只有XXXColumnHandle
public final class ElasticsearchColumn
{
    private final String name;
    private final Type type;
    private final String jsonPath;
    private final String jsonType; // TODO: 这个字符串怎么用，形如：row(c5 bigint, c6 row(keyword varchar), c8 row(c10 row(keyword varchar), c9 bigint))
    // TODO: 啥意思？
    private final boolean isList;
    // TODO：啥意思？
    private final int ordinalPosition;

    @JsonCreator
    public ElasticsearchColumn(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("jsonPath") String jsonPath,
            @JsonProperty("jsonType") String jsonType,
            @JsonProperty("isList") boolean isList,
            @JsonProperty("ordinalPosition") int ordinalPosition)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.type = requireNonNull(type, "type is null");
        this.jsonPath = requireNonNull(jsonPath, "jsonPath is null");
        this.jsonType = requireNonNull(jsonType, "jsonType is null");
        this.isList = isList;
        this.ordinalPosition = ordinalPosition;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public String getJsonPath()
    {
        return jsonPath;
    }

    @JsonProperty
    public String getJsonType()
    {
        return jsonType;
    }

    @JsonProperty
    public boolean isList()
    {
        return isList;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, jsonPath, jsonType, isList, ordinalPosition);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ElasticsearchColumn other = (ElasticsearchColumn) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.jsonPath, other.jsonPath) &&
                Objects.equals(this.jsonType, other.jsonType) &&
                Objects.equals(this.isList, other.isList) &&
                Objects.equals(this.ordinalPosition, other.ordinalPosition);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("jsonPath", jsonPath)
                .add("jsonType", jsonType)
                .add("isList", isList)
                .add("ordinalPosition", ordinalPosition)
                .toString();
    }
}
