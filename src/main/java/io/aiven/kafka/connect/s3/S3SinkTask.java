/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.s3;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.common.grouper.RecordGrouper;
import io.aiven.kafka.connect.common.grouper.RecordGrouperFactory;
import io.aiven.kafka.connect.common.output.OutputWriter;
import io.aiven.kafka.connect.common.output.jsonwriter.JsonLinesOutputWriter;
import io.aiven.kafka.connect.common.output.jsonwriter.JsonOutputWriter;
import io.aiven.kafka.connect.common.output.plainwriter.PlainOutputWriter;
import io.aiven.kafka.connect.common.templating.VariableTemplatePart;
import io.aiven.kafka.connect.s3.config.AwsCredentialProviderFactory;
import io.aiven.kafka.connect.s3.config.S3SinkConfig;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.github.luben.zstd.ZstdOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import static com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;

public class S3SinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(AivenKafkaConnectS3SinkConnector.class);

    private RecordGrouper recordGrouper;

    private S3SinkConfig config;

    private AmazonS3 s3Client;

    protected AwsCredentialProviderFactory credentialFactory = new AwsCredentialProviderFactory();

    // required by Connect
    public S3SinkTask() {

    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props, "props hasn't been set");
        config = new S3SinkConfig(props);

        final var awsEndpointConfig = newEndpointConfiguration(this.config);
        final var s3ClientBuilder =
            AmazonS3ClientBuilder
                .standard()
                .withCredentials(
                    credentialFactory.getProvider(config)
                );
        if (Objects.isNull(awsEndpointConfig)) {
            s3ClientBuilder.withRegion(config.getAwsS3Region());
        } else {
            s3ClientBuilder.withEndpointConfiguration(awsEndpointConfig).withPathStyleAccessEnabled(true);
        }
        s3Client = s3ClientBuilder.build();

        try {
            recordGrouper = RecordGrouperFactory.newRecordGrouper(config);
        } catch (final Exception e) {
            throw new ConnectException("Unsupported file name template " + config.getFilename(), e);
        }
    }

    private OutputWriter getOutputWriter(final OutputStream outputStream) {
        switch (this.config.getFormatType()) {
            case CSV:
                return new PlainOutputWriter(config.getOutputFields(), outputStream);
            case JSONL:
                return new JsonLinesOutputWriter(config.getOutputFields(), outputStream);
            case JSON:
                return new JsonOutputWriter(config.getOutputFields(), outputStream);
            default:
                throw new ConnectException("Unsupported format type " + config.getFormatType());
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) throws ConnectException {
        Objects.requireNonNull(records, "records cannot be null");
        LOGGER.info("Processing {} records", records.size());
        records.forEach(recordGrouper :: put);
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        recordGrouper.records().forEach(this::flushFile);
        recordGrouper.clear();
    }

    private void flushFile(final String filename, final List<SinkRecord> records) {
        Objects.requireNonNull(records, "records cannot be null");
        if (records.isEmpty()) {
            return;
        }
        final SinkRecord sinkRecord = records.get(0);
        try (final OutputStream compressedStream = newStreamFor(filename, sinkRecord)) {
            try (final OutputWriter outputWriter = getOutputWriter(compressedStream)) {
                for (final SinkRecord record: records) {
                    outputWriter.writeRecord(record);
                }
            }
        } catch (final IOException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public void stop() {
        s3Client.shutdown();
        LOGGER.info("Stop S3 Sink Task");
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    private OutputStream newStreamFor(final String filename, final SinkRecord record) {

        final var fullKey = config.usesFileNameTemplate() ? filename : oldFullKey(record);
        final var awsOutputStream = new S3OutputStream(s3Client, config.getAwsS3BucketName(), fullKey);
        try {
            return getCompressedStream(awsOutputStream);
        } catch (final IOException e) {
            throw new ConnectException(e);
        }
    }

    private EndpointConfiguration newEndpointConfiguration(final S3SinkConfig config) {
        if (Objects.isNull(config.getAwsS3EndPoint())) {
            return null;
        }
        return new EndpointConfiguration(config.getAwsS3EndPoint(), config.getAwsS3Region().getName());
    }

    private OutputStream getCompressedStream(final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(outputStream, "outputStream cannot be null");

        switch (config.getCompressionType()) {
            case ZSTD:
                return new ZstdOutputStream(outputStream);
            case GZIP:
                return new GZIPOutputStream(outputStream);
            case SNAPPY:
                return new SnappyOutputStream(outputStream);
            default:
                return outputStream;
        }
    }

    private String oldFullKey(final SinkRecord record) {
        final var prefix =
            config.getPrefixTemplate()
                .instance()
                .bindVariable(
                    FilenameTemplateVariable.TIMESTAMP.name,
                    parameter -> OldFullKeyFormatters.TIMESTAMP.apply(config.getTimestampSource(), parameter)
                )
                .bindVariable(
                    FilenameTemplateVariable.PARTITION.name,
                    () -> record.kafkaPartition().toString()
                )
                .bindVariable(
                    FilenameTemplateVariable.START_OFFSET.name,
                    parameter -> OldFullKeyFormatters.KAFKA_OFFSET.apply(record, parameter)
                )
                .bindVariable(FilenameTemplateVariable.TOPIC.name, record::topic)
                .bindVariable(
                    "utc_date",
                    () -> ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE)
                )
                .bindVariable(
                    "local_date",
                    () -> LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE)
                )
                .render();
        final var key =
            String.format(
                "%s-%s-%s",
                record.topic(),
                record.kafkaPartition(),
                OldFullKeyFormatters.KAFKA_OFFSET.apply(
                    record, VariableTemplatePart.Parameter.of("padding", "true")
                )
            );
        return prefix + key + config.getCompressionType().extension();
    }

}
