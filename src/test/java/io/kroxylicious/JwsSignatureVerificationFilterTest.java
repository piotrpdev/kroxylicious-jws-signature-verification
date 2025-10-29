package io.kroxylicious;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.config.JwsSignatureVerificationFilterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JwsSignatureVerificationFilterTest {

    private static final short API_VERSION = ApiMessageType.PRODUCE.highestSupportedVersion(true); // this is arbitrary for our filter
    private static final String PRE_TRANSFORM_VALUE = "eyJhbGciOiJFUzI1NiIsImtpZCI6InRoZSBrZXkifQ." +
            "UEFZTE9BRCE."+
            "Oq-H1lk5G0rl6oyNM3jR5S0-BZQgTlamIKMApq3RX8Hmh2d2XgB4scvsMzGvE-OlEmDY9Oy0YwNGArLpzXWyjw";
    private static final String CONFIG_JWKS =
            "{\"keys\":[{" +
                    "\"kty\":\"EC\"," +
                    "\"use\":\"sig\"," +
                    "\"kid\":\"the key\"," +
                    "\"x\":\"amuk6RkDZi-48mKrzgBN_zUZ_9qupIwTZHJjM03qL-4\"," +
                    "\"y\":\"ZOESj6_dpPiZZR-fJ-XVszQta28Cjgti7JudooQJ0co\"," +
                    "\"crv\":\"P-256\"" +
                    "}]}";

    @Mock
    private FilterContext context;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private RequestFilterResult requestFilterResult;

    @Captor
    private ArgumentCaptor<ApiMessage> apiMessageCaptor;
    @Captor
    private ArgumentCaptor<RequestHeaderData> requestHeaderDataCaptor;

    private JwsSignatureVerificationFilter filter;
    private RequestHeaderData headerData;

    @BeforeEach
    public void beforeEach() {
        setupContextMock();
        JwsSignatureVerificationFilterConfig config = new JwsSignatureVerificationFilterConfig(CONFIG_JWKS);
        this.filter = new JwsSignatureVerificationFilter(config);
        this.headerData = new RequestHeaderData();
    }

    @Test
    void willTransformProduceRequestTest() throws Exception {
        var requestData = buildProduceRequestData(PRE_TRANSFORM_VALUE);
        var stage = filter.onProduceRequest(API_VERSION, headerData, requestData, context);
        assertThat(stage).isCompleted();
        var forwardedRequest = stage.toCompletableFuture().get().message();
        var unpackedRequest = unpackProduceRequestData((ProduceRequestData) forwardedRequest);
        assertThat(unpackedRequest)
                .containsExactly(PRE_TRANSFORM_VALUE);
    }

    private void setupContextMock() {
        when(context.forwardRequest(requestHeaderDataCaptor.capture(), apiMessageCaptor.capture())).thenAnswer(
                invocation -> CompletableFuture.completedStage(requestFilterResult));
        when(requestFilterResult.message()).thenAnswer(invocation -> apiMessageCaptor.getValue());
        when(requestFilterResult.header()).thenAnswer(invocation -> requestHeaderDataCaptor.getValue());
    }

    private static ProduceRequestData buildProduceRequestData(String transformValue) {
        var requestData = new ProduceRequestData();
        // Build stream
        var stream = new ByteBufferOutputStream(ByteBuffer.wrap(transformValue.getBytes(StandardCharsets.UTF_8)));
        // Build records from stream
        var recordsBuilder = new MemoryRecordsBuilder(stream, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME, 0,
                RecordBatch.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, stream.remaining());
        // Create record Headers
        Header header = new RecordHeader("myKey", "myValue".getBytes());
        // Add transformValue as buffer to records
        recordsBuilder.append(RecordBatch.NO_TIMESTAMP, null, ByteBuffer.wrap(transformValue.getBytes(StandardCharsets.UTF_8)).position(0), new Header[]{ header });
        var records = recordsBuilder.build();
        // Build partitions from built records
        var partitions = new ArrayList<ProduceRequestData.PartitionProduceData>();
        var partitionData = new ProduceRequestData.PartitionProduceData();
        partitionData.setRecords(records);
        partitions.add(partitionData);
        // Build topics from built partitions
        var topics = new ProduceRequestData.TopicProduceDataCollection();
        var topicData = new ProduceRequestData.TopicProduceData();
        topicData.setPartitionData(partitions);
        topics.add(topicData);
        // Add built topics to ProduceRequestData object so that we can return it
        requestData.setTopicData(topics);
        return requestData;
    }

    private static List<String> unpackProduceRequestData(ProduceRequestData requestData) {
        var recordList = unpackRecords(requestData);
        var values = new ArrayList<String>();
        for (Record record : recordList) {
            values.add(new String(StandardCharsets.UTF_8.decode(record.value()).array()));
        }
        return values;
    }

    private static List<Record> unpackRecords(ProduceRequestData requestData) {
        List<Record> records = new ArrayList<>();
        for (ProduceRequestData.TopicProduceData topicProduceData : requestData.topicData()) {
            for (ProduceRequestData.PartitionProduceData partitionProduceData : topicProduceData.partitionData()) {
                for (Record record : ((Records) partitionProduceData.records()).records()) {
                    records.add(record);
                }
            }
        }
        return records;
    }
}
