package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.druid.indexing.overlord.DataSourceMetadata;

import java.util.Objects;
import java.util.Set;


public class PubSubDataSourceMetadata implements DataSourceMetadata
{
    private final String baseDataSource;
    private final Set<String> dimensions;
    private final Set<String> metrics;

    @JsonCreator
    public PubSubDataSourceMetadata(
            @JsonProperty("baseDataSource") String baseDataSource,
            @JsonProperty("dimensions") Set<String> dimensions,
            @JsonProperty("metrics") Set<String> metrics
    )
    {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(baseDataSource), "baseDataSource cannot be null or empty. Please provide a baseDataSource.");
        this.baseDataSource = baseDataSource;

        this.dimensions = Preconditions.checkNotNull(dimensions, "dimensions cannot be null. This is not a valid DerivativeDataSourceMetadata.");
        this.metrics = Preconditions.checkNotNull(metrics, "metrics cannot be null. This is not a valid DerivativeDataSourceMetadata.");
    }

    @JsonProperty("baseDataSource")
    public String getBaseDataSource()
    {
        return baseDataSource;
    }

    @JsonProperty("dimensions")
    public Set<String> getDimensions()
    {
        return dimensions;
    }

    @JsonProperty("metrics")
    public Set<String> getMetrics()
    {
        return metrics;
    }

    @Override
    public boolean isValidStart()
    {
        return false;
    }

    @Override
    public DataSourceMetadata asStartMetadata()
    {
        return this;
    }

    @Override
    public boolean matches(DataSourceMetadata other)
    {
        return equals(other);
    }

    @Override
    public DataSourceMetadata plus(DataSourceMetadata other)
    {
        throw new UnsupportedOperationException("Derivative dataSource metadata is not allowed to plus");
    }

    @Override
    public DataSourceMetadata minus(DataSourceMetadata other)
    {
        throw new UnsupportedOperationException("Derivative dataSource metadata is not allowed to minus");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PubSubDataSourceMetadata that = (PubSubDataSourceMetadata) o;

        return baseDataSource.equals(that.getBaseDataSource()) &&
                dimensions.equals(that.getDimensions()) &&
                metrics.equals(that.getMetrics());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(baseDataSource, dimensions, metrics);
    }

    public Set<String> getColumns()
    {
        Set<String> fields = Sets.newHashSet(dimensions);
        fields.addAll(metrics);
        return fields;
    }

    @Override
    public String toString()
    {
        return "PubSubDataSourceMetadata{" +
                "baseDataSource=" + baseDataSource +
                ", dimensions=" + dimensions +
                ", metrics=" + metrics +
                '}';
    }
}