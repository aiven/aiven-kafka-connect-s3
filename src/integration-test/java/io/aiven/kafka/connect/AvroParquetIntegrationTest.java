/*
 * Copyright 2021 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;
import io.aiven.kafka.connect.s3.SchemaRegistryContainer;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;

import cloud.localstack.Localstack;
import cloud.localstack.awssdkv1.TestUtils;
import cloud.localstack.docker.LocalstackDockerExtension;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(LocalstackDockerExtension.class)
@LocalstackDockerProperties(services = {"s3"})
@Testcontainers
class AvroParquetIntegrationTest implements KafkaIntegrationBase {
    private static final String S3_ACCESS_KEY_ID = "test-key-id0";
    private static final String S3_SECRET_ACCESS_KEY = "test_secret_key0";
    private static final String TEST_BUCKET_NAME = "test-bucket0";

    private static final String CONNECTOR_NAME = "aiven-s3-sink-connector";
    private static final String TEST_TOPIC_0 = "test-topic-0";
    private static final String COMMON_PREFIX = "aiven-kafka-connect-s3-test-";
    private static final int OFFSET_FLUSH_INTERVAL_MS = 5000;

    private static String s3Endpoint;
    private static String s3Prefix;
    private static BucketAccessor testBucketAccessor;
    private static File pluginDir;

    @Container
    private final KafkaContainer kafka = new KafkaContainer("5.2.1")
            .withExposedPorts(KafkaContainer.KAFKA_PORT, 9092)
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    @Container
    private final SchemaRegistryContainer schemaRegistry = new SchemaRegistryContainer(kafka);
    private AdminClient adminClient;
    private KafkaProducer<String, GenericRecord> producer;
    private ConnectRunner connectRunner;

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        s3Prefix = COMMON_PREFIX
                + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        final AmazonS3 s3 = TestUtils.getClientS3();
        s3Endpoint = Localstack.INSTANCE.getEndpointS3();
        testBucketAccessor = new BucketAccessor(s3, TEST_BUCKET_NAME);
        testBucketAccessor.createBucket();

        pluginDir = KafkaIntegrationBase.getPluginDir();
        KafkaIntegrationBase.extractConnectorPlugin(pluginDir);
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        adminClient = newAdminClient(kafka);
        producer = newProducer(kafka);

        final NewTopic newTopic0 = new NewTopic(TEST_TOPIC_0, 4, (short) 1);
        adminClient.createTopics(Arrays.asList(newTopic0)).all().get();

        connectRunner = newConnectRunner(kafka, pluginDir, OFFSET_FLUSH_INTERVAL_MS);
        connectRunner.start();
    }

    @AfterEach
    final void tearDown() {
        connectRunner.stop();
        adminClient.close();
        producer.close();

        connectRunner.awaitStop();
    }

    @Test
    final void parquetAllOutputFields(@TempDir final Path tmpDir)
            throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig());
        final String compression = "none";
        connectorConfig.put("format.output.fields", "key,value,offset,timestamp,headers");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("format.output.type", "parquet");
        connectRunner.createConnector(connectorConfig);

        final Schema valueSchema =
                SchemaBuilder.record("value")
                        .fields()
                        .name("name").type().stringType().noDefault()
                        .name("value").type().stringType().noDefault()
                        .endRecord();

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var key = "key-" + cnt;
                final GenericRecord value = new GenericData.Record(valueSchema);
                value.put("name", "user-" + cnt);
                value.put("value", "value-" + cnt);
                cnt += 1;
                sendFutures.add(sendMessageAsync(TEST_TOPIC_0, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final List<String> expectedBlobs = List.of(
                getBlobName(0, 0, compression),
                getBlobName(1, 0, compression),
                getBlobName(2, 0, compression),
                getBlobName(3, 0, compression));
        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            assertTrue(testBucketAccessor.doesObjectExist(blobName));
            final var records =
                    ParquetUtils.readRecords(
                            tmpDir.resolve(Paths.get(blobName)),
                            testBucketAccessor.readBytes(blobName)
                    );
            blobContents.put(blobName, records);
        }

        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var name = "user-" + cnt;
                final var value = "value-" + cnt;
                final String blobName = getBlobName(partition, 0, "none");
                final GenericRecord record = blobContents.get(blobName).get(i);
                final var expectedKey = "key-" + cnt;
                final var expectedValue = "{\"name\": \"" + name + "\", \"value\": \"" + value + "\"}";
                assertEquals(expectedKey, record.get("key").toString());
                assertEquals(expectedValue, record.get("value").toString());
                assertNotNull(record.get("offset"));
                assertNotNull(record.get("timestamp"));
                assertNull(record.get("headers"));
                cnt += 1;
            }
        }
    }

    @Test
    final void parquetValueComplexType(@TempDir final Path tmpDir)
            throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig());
        final String compression = "none";
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("format.output.type", "parquet");
        connectRunner.createConnector(connectorConfig);

        final Schema valueSchema =
                SchemaBuilder.record("value")
                        .fields()
                        .name("name").type().stringType().noDefault()
                        .name("value").type().stringType().noDefault()
                        .endRecord();

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var key = "key-" + cnt;
                final GenericRecord value = new GenericData.Record(valueSchema);
                value.put("name", "user-" + cnt);
                value.put("value", "value-" + cnt);
                cnt += 1;
                sendFutures.add(sendMessageAsync(TEST_TOPIC_0, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final List<String> expectedBlobs = List.of(
                getBlobName(0, 0, compression),
                getBlobName(1, 0, compression),
                getBlobName(2, 0, compression),
                getBlobName(3, 0, compression));
        final Map<String, List<GenericRecord>> blobContents = new HashMap<>();
        for (final String blobName : expectedBlobs) {
            final var records =
                    ParquetUtils.readRecords(
                            tmpDir.resolve(Paths.get(blobName)),
                            testBucketAccessor.readBytes(blobName)
                    );
            blobContents.put(blobName, records);
        }
        cnt = 0;
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var name = "user-" + cnt;
                final var value = "value-" + cnt;
                final String blobName = getBlobName(partition, 0, "none");
                final GenericRecord record = blobContents.get(blobName).get(i);
                assertEquals(name, record.get("name").toString());
                assertEquals(value, record.get("value").toString());
                cnt += 1;
            }
        }
    }

    @Test
    final void parquetOutputSchemaChanged(@TempDir final Path tmpDir)
            throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig());
        final String compression = "none";
        connectorConfig.put("format.output.fields", "value");
        connectorConfig.put("format.output.fields.value.encoding", "none");
        connectorConfig.put("file.compression.type", compression);
        connectorConfig.put("format.output.type", "parquet");
        connectRunner.createConnector(connectorConfig);

        final Schema valueSchema =
                SchemaBuilder.record("value")
                        .fields()
                        .name("name").type().stringType().noDefault()
                        .name("value").type().stringType().noDefault()
                        .endRecord();

        final Schema newValueSchema =
                SchemaBuilder.record("value")
                        .fields()
                        .name("name").type().stringType().noDefault()
                        .name("value").type().stringType().noDefault()
                        .name("blocked").type().booleanType().booleanDefault(false)
                        .endRecord();

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        final var expectedRecords = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < 4; partition++) {
                final var key = "key-" + cnt;
                final GenericRecord value;
                if (i < 5) {
                    value = new GenericData.Record(valueSchema);
                    value.put("name", "user-" + cnt);
                    value.put("value", "value-" + cnt);
                } else {
                    value = new GenericData.Record(newValueSchema);
                    value.put("name", "user-" + cnt);
                    value.put("value", "value-" + cnt);
                    value.put("blocked", true);
                }
                expectedRecords.add(value.toString());
                cnt += 1;
                sendFutures.add(sendMessageAsync(TEST_TOPIC_0, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        // TODO more robust way to detect that Connect finished processing
        Thread.sleep(OFFSET_FLUSH_INTERVAL_MS * 2);

        final List<String> expectedBlobs = Arrays.asList(
                getBlobName(0, 0, compression),
                getBlobName(0, 5, compression),
                getBlobName(1, 0, compression),
                getBlobName(1, 5, compression),
                getBlobName(2, 0, compression),
                getBlobName(2, 5, compression),
                getBlobName(3, 0, compression),
                getBlobName(3, 5, compression)
        );
        final var blobContents = new ArrayList<String>();
        for (final String blobName : expectedBlobs) {
            final var records =
                    ParquetUtils.readRecords(
                            tmpDir.resolve(Paths.get(blobName)),
                            testBucketAccessor.readBytes(blobName)
                    );
            blobContents.addAll(records.stream().map(GenericRecord::toString).collect(Collectors.toList()));
        }
        assertIterableEquals(
                expectedRecords.stream().sorted().collect(Collectors.toList()),
                blobContents.stream().sorted().collect(Collectors.toList())
        );
    }

    private KafkaProducer<String, GenericRecord> newProducer(final KafkaContainer kafka) {

        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("schema.registry.url", schemaRegistry.getSchemaRegistryUrl());
        return new KafkaProducer<>(producerProps);
    }

    private Future<RecordMetadata> sendMessageAsync(final String topicName,
                                                    final int partition,
                                                    final String key,
                                                    final GenericRecord value) {
        final ProducerRecord<String, GenericRecord> msg = new ProducerRecord<>(
                topicName, partition, key, value);
        return producer.send(msg);
    }

    private Map<String, String> basicConnectorConfig() {
        final Map<String, String> config = new HashMap<>();
        config.put("name", CONNECTOR_NAME);
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", schemaRegistry.getSchemaRegistryUrl());
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter.schema.registry.url", schemaRegistry.getSchemaRegistryUrl());
        config.put("tasks.max", "1");
        return config;
    }

    private Map<String, String> awsSpecificConfig(final Map<String, String> config) {
        config.put("connector.class", AivenKafkaConnectS3SinkConnector.class.getName());
        config.put("aws.access.key.id", S3_ACCESS_KEY_ID);
        config.put("aws.secret.access.key", S3_SECRET_ACCESS_KEY);
        config.put("aws.s3.endpoint", s3Endpoint);
        config.put("aws.s3.bucket.name", TEST_BUCKET_NAME);
        config.put("aws.s3.prefix", s3Prefix);
        config.put("topics", TEST_TOPIC_0);
        config.put("key.converter.schema.registry.url", schemaRegistry.getSchemaRegistryUrl());
        config.put("value.converter.schema.registry.url", schemaRegistry.getSchemaRegistryUrl());
        config.put("tasks.max", "1");
        return config;
    }

    // WARN: different from GCS
    private String getBlobName(final int partition, final int startOffset, final String compression) {
        final String result = String.format("%s%s-%d-%020d", s3Prefix, TEST_TOPIC_0, partition, startOffset);
        return result + CompressionType.forName(compression).extension();
    }

}
