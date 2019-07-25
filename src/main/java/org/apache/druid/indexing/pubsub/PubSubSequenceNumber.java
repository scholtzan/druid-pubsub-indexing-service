package org.apache.druid.indexing.pubsub;

import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;

import javax.validation.constraints.NotNull;

public class PubSubSequenceNumber extends OrderedSequenceNumber<Long> {
    private PubSubSequenceNumber(Long sequenceNumber)
    {
        super(sequenceNumber, false);
    }

    public static PubSubSequenceNumber of(Long sequenceNumber)
    {
        return new PubSubSequenceNumber(sequenceNumber);
    }

    @Override
    public int compareTo(
            @NotNull OrderedSequenceNumber<Long> o
    )
    {
        return this.get().compareTo(o.get());
    }

}
