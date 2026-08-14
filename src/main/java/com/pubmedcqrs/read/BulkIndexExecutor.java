package com.pubmedcqrs.read;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.faulttolerance.Retry;

import java.io.IOException;
import java.time.temporal.ChronoUnit;

/**
 * Isolado em bean próprio porque @Retry é um interceptor CDI: só funciona quando o método
 * é chamado de FORA da classe (via proxy). Se ficasse como método privado dentro do
 * ArticleProjector, a self-invocation ignoraria o interceptor silenciosamente.
 */
@ApplicationScoped
public class BulkIndexExecutor {

    @Inject
    ElasticsearchClient esClient;

    @Retry(
            maxRetries = 3,
            delay = 500,
            delayUnit = ChronoUnit.MILLIS,
            jitter = 200,
            retryOn = IOException.class
    )
    public BulkResponse executeBulkWithRetry(BulkRequest request) throws IOException {
        return esClient.bulk(request);
    }
}