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

package io.aiven.kafka.connect.s3.testutils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import io.aiven.kafka.connect.common.config.CompressionType;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.github.luben.zstd.ZstdInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;


public class BucketAccessor {

    private final String bucketName;
    private final AmazonS3 s3;

    private static final Logger LOGGER = LoggerFactory.getLogger(BucketAccessor.class);

    public BucketAccessor(final AmazonS3 s3, final String bucketName) {
        this.bucketName = bucketName;
        this.s3 = s3;
    }

    public final void createBucket() {
        s3.createBucket(bucketName);
    }

    public final void removeBucket() {
        final var chunk = s3.listObjects(bucketName)
                                          .getObjectSummaries()
                                          .stream()
                                          .map(S3ObjectSummary::getKey)
                                          .toArray(String[]::new);

        final var deleteObjectsRequest =
            new DeleteObjectsRequest(bucketName).withKeys(chunk);
        try {
            s3.deleteObjects(deleteObjectsRequest);
        } catch (final MultiObjectDeleteException e) {
            for (final var err : e.getErrors()) {
                LOGGER.warn(String.format("Couldn't delete object: %s. Reason: [%s] %s",
                    err.getKey(), err.getCode(), err.getMessage()));
            }
        } catch (final AmazonClientException e) {
            LOGGER.error("Couldn't delete objects: "
                +   Arrays.stream(chunk).reduce(" ", String::concat) + e.getMessage());
        }
        s3.deleteBucket(bucketName);
    }

    public final Boolean doesObjectExist(final String objectName) {
        return s3.doesObjectExist(bucketName, objectName);
    }

    public final List<List<String>> readAndDecodeLines(final String blobName,
                                                       final String compression,
                                                       final int... fieldsToDecode) throws IOException {
        Objects.requireNonNull(blobName, "blobName cannot be null");
        Objects.requireNonNull(fieldsToDecode, "fieldsToDecode cannot be null");

        return readAndDecodeLines0(blobName, compression, fieldsToDecode);
    }

    private List<List<String>> readAndDecodeLines0(final String blobName,
                                                   final String compression,
                                                   final int[] fieldsToDecode) throws IOException {
        return readLines(blobName, compression).stream()
            .map(l -> l.split(","))
            .map(fields -> decodeRequiredFields(fields, fieldsToDecode))
            .collect(Collectors.toList());
    }

    public final byte[] readBytes(final String blobName) throws IOException {
        return s3.getObject(bucketName, blobName).getObjectContent().readAllBytes();
    }

    public final List<String> readLines(final String blobName, final String compression) throws IOException {
        Objects.requireNonNull(blobName, "blobName cannot be null");
        final byte[] blobBytes = s3.getObject(bucketName, blobName).getObjectContent().readAllBytes();

        try (final ByteArrayInputStream bais = new ByteArrayInputStream(blobBytes)) {
            final InputStream decompressedStream = getDecompressedStream(bais, compression);
            final InputStreamReader reader = new InputStreamReader(decompressedStream, StandardCharsets.UTF_8);
            final BufferedReader bufferedReader = new BufferedReader(reader);
            return bufferedReader.lines().collect(Collectors.toList());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private InputStream getDecompressedStream(final InputStream inputStream, final String compression)
        throws IOException {
        Objects.requireNonNull(inputStream, "inputStream cannot be null");
        Objects.requireNonNull(compression, "compression cannot be null");

        final CompressionType compressionType = CompressionType.forName(compression);
        switch (compressionType) {
            case ZSTD:
                return new ZstdInputStream(inputStream);
            case GZIP:
                return new GZIPInputStream(inputStream);
            case SNAPPY:
                return new SnappyInputStream(inputStream);
            default:
                return inputStream;
        }
    }

    private List<String> decodeRequiredFields(final String[] originalFields, final int[] fieldsToDecode) {
        Objects.requireNonNull(originalFields, "originalFields cannot be null");
        Objects.requireNonNull(fieldsToDecode, "fieldsToDecode cannot be null");

        final List<String> result = Arrays.asList(originalFields);
        for (final int fieldIdx : fieldsToDecode) {
            result.set(fieldIdx, b64Decode(result.get(fieldIdx)));
        }
        return result;
    }

    private String b64Decode(final String value) {
        Objects.requireNonNull(value, "value cannot be null");

        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
