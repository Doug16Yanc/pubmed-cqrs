package com.pubmedcqrs.read;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.TransportException;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;

@ApplicationScoped
public class ElasticsearchIndexInitializer {

    private static final String INDEX = "articles";
    private static final Logger LOG = Logger.getLogger(ElasticsearchIndexInitializer.class);

    @Inject
    ElasticsearchClient esClient;

    void onStart(@Observes StartupEvent event) throws IOException {
        try {
            boolean exists = esClient.indices().exists(e -> e.index(INDEX)).value();
            if (exists) {
                LOG.infof("Índice '%s' já existe, pulando criação", INDEX);
                return;
            }
        } catch (TransportException e) {
            LOG.warnf("Não foi possível verificar a existência do índice '%s' de forma limpa: %s. Tentando criar...", INDEX, e.getMessage());
        }

        try {
            esClient.indices().create(c -> c
                    .index(INDEX)
                    .mappings(m -> m
                            .properties("pmid", p -> p.keyword(k -> k))
                            .properties("title", p -> p.text(t -> t.analyzer("standard")))
                            .properties("abstractText", p -> p.text(t -> t.analyzer("standard")))
                            .properties("authors", p -> p.keyword(k -> k))
                            .properties("journal", p -> p.keyword(k -> k))
                            .properties("publicationDate", p -> p.date(d -> d.format("yyyy-MM-dd")))
                            .properties("meshTerms", p -> p.keyword(k -> k))
                            .properties("version", p -> p.long_(l -> l))
                            .properties("projectedAt", p -> p.date(d -> d))
                            // all-MiniLM-L6-v2 = 384 dim. Se trocar pro PubMedBERT
                            // (ver LocalEmbeddingModelProducer), mudar pra 768 aqui —
                            // e recriar o índice, dimensão não é mapping update.
                            .properties("embedding", p -> p.denseVector(dv -> dv
                                    .dims(384)
                                    .index(true)
                                    .similarity("cosine")))));

            LOG.infof("Índice '%s' criado com sucesso", INDEX);
        } catch (TransportException e) {
            if (e.statusCode() == 400 || e.statusCode() == 409) {
                LOG.infof("Índice '%s' já existe (capturado via create)", INDEX);
            } else {
                throw e;
            }
        }
    }
}