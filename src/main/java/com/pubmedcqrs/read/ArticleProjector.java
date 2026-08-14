package com.pubmedcqrs.read;

import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pubmedcqrs.events.ArticleEvent;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaRecordBatch;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class ArticleProjector {

    private static final Logger LOG = Logger.getLogger(ArticleProjector.class);
    private static final String INDEX = "articles";

    @Inject
    BulkIndexExecutor bulkIndexExecutor;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    EmbeddingModel embeddingModel;

    /**
     * Consome um lote de eventos (configurado via
     * mp.messaging.incoming.article-events-in.batch=true) e indexa todos de uma vez
     * via Bulk API. Cada mensagem do lote é reconhecida (ack) ou rejeitada (nack -> DLQ)
     * individualmente, com base no resultado item-a-item que o ES devolve — o lote em si
     * não é uma unidade atômica de sucesso/falha, só uma otimização de transporte.
     */
    @Incoming("article-events-in")
    @Blocking // o embedding agora roda in-process (ONNX/CPU), não é mais uma chamada de
    // rede — mas ainda é trabalho pesado o bastante pra não rodar no event loop.
    public CompletionStage<Void> onArticleEventBatch(KafkaRecordBatch<String, String> batch) {
        List<KafkaRecord<String, String>> records = new ArrayList<>();
        batch.forEach(records::add);

        if (records.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // Desserializa mantendo o vínculo evento <-> KafkaRecord original, pra poder ack/nack depois.
        List<ParsedEvent> parsed = new ArrayList<>(records.size());
        for (KafkaRecord<String, String> record : records) {
            try {
                ArticleEvent event = objectMapper.readValue(record.getPayload(), ArticleEvent.class);
                parsed.add(new ParsedEvent(record, event));
            } catch (Exception e) {
                // payload corrompido/ilegível: não dá nem pra tentar indexar, vai direto pra DLQ.
                LOG.errorf(e, "Evento ilegível no lote, enviando para DLQ sem tentar indexar");
                record.nack(e);
            }
        }

        if (parsed.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        List<float[]> embeddings = embedBatch(parsed);

        BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();
        for (int i = 0; i < parsed.size(); i++) {
            ArticleEvent event = parsed.get(i).event();
            float[] embedding = embeddings.get(i);
            bulkBuilder.operations(op -> op
                    .index(idx -> idx
                            .index(INDEX)
                            .id(event.pmid())
                            .versionType(VersionType.External)
                            .version(event.version())
                            .document(toDocument(event, embedding))
                    )
            );
        }

        try {
            long bulkStart = System.nanoTime();
            BulkResponse response = bulkIndexExecutor.executeBulkWithRetry(bulkBuilder.build());
            long bulkMs = (System.nanoTime() - bulkStart) / 1_000_000;
            LOG.infof("Bulk indexado: %d itens em %dms (%.1f itens/s)",
                    parsed.size(), bulkMs, parsed.size() / (bulkMs / 1000.0));
            return acknowledgeByResult(parsed, response);
        } catch (IOException e) {
            // Falha de transporte (ES fora do ar, timeout etc.) afeta o lote inteiro:
            // nesse caso sim, nack em todas as mensagens do lote -> todas vão pra DLQ.
            LOG.errorf(e, "Falha de I/O no bulk request, enviando lote inteiro (%d itens) para DLQ", parsed.size());
            for (ParsedEvent pe : parsed) {
                pe.record().nack(e);
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletionStage<Void> acknowledgeByResult(List<ParsedEvent> parsed, BulkResponse response) {
        List<BulkResponseItem> items = response.items();
        List<CompletionStage<Void>> acks = new ArrayList<>(parsed.size());

        for (int i = 0; i < parsed.size(); i++) {
            ParsedEvent pe = parsed.get(i);
            BulkResponseItem item = items.get(i);

            if (item.error() == null) {
                LOG.debugf("PMID %s indexado com sucesso (versão %d)", pe.event().pmid(), pe.event().version());
                acks.add(pe.record().ack());
            } else if (item.status() == 409) {
                // version conflict: não é falha, é a idempotência rejeitando um evento
                // duplicado/desatualizado. Trata como sucesso do ponto de vista do consumer.
                LOG.debugf("PMID %s ignorado por conflito de versão (idempotente, esperado)", pe.event().pmid());
                acks.add(pe.record().ack());
            } else {
                // falha real de indexação (mapping error, doc muito grande, etc.)
                RuntimeException cause = new RuntimeException(
                        "Falha ao indexar PMID " + pe.event().pmid() + ": " + item.error().reason());
                LOG.errorf(cause, "Enviando PMID %s para DLQ", pe.event().pmid());
                acks.add(pe.record().nack(cause));
            }
        }

        return CompletableFuture.allOf(
                acks.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new)
        );
    }

    /**
     * Embeda o lote inteiro numa única chamada (embedAll) em vez de artigo por artigo.
     * Mesmo local/in-process, o modelo ONNX paraleliza internamente entre os
     * TextSegments de uma chamada embedAll (um thread por segmento, thread pool
     * cacheado) — chamar em lote aproveita esse paralelismo em vez de serializar
     * uma inferência de cada vez.
     *
     * Título + abstract concatenados: embedding de abstract sozinho perde contexto do
     * título, e são poucos tokens a mais.
     */
    private List<float[]> embedBatch(List<ParsedEvent> parsed) {
        List<TextSegment> segments = new ArrayList<>(parsed.size());
        for (ParsedEvent pe : parsed) {
            ArticleEvent event = pe.event();
            String title = event.title() == null ? "" : event.title();
            String abstractText = event.abstractText() == null ? "" : event.abstractText();
            String text = (title + ". " + abstractText).trim();
            // embedAll não aceita segmento vazio; artigo sem título nem abstract
            // (raro, mas existe no baseline) ainda precisa de um vetor pra não
            // quebrar o alinhamento posicional da lista de resultados.
            segments.add(TextSegment.from(text.isBlank() ? "[sem conteúdo]" : text));
        }

        long embedStart = System.nanoTime();
        Response<List<Embedding>> response = embeddingModel.embedAll(segments);
        long embedMs = (System.nanoTime() - embedStart) / 1_000_000;
        LOG.infof("Embeddings gerados: %d artigos em %dms (%.1f artigos/s)",
                parsed.size(), embedMs, parsed.size() / Math.max(1.0, embedMs / 1000.0));

        List<float[]> vectors = new ArrayList<>(parsed.size());
        for (Embedding embedding : response.content()) {
            vectors.add(embedding.vector());
        }
        return vectors;
    }

    private Object toDocument(ArticleEvent event, float[] embedding) {
        Instant projectedAt = Instant.now();
        long lagMs = Duration.between(event.occurredAt(), projectedAt).toMillis();
        LOG.debugf("PMID %s — lag de projeção: %dms", event.pmid(), lagMs);

        return new ArticleDocument(
                event.pmid(),
                event.title(),
                event.abstractText(),
                event.authors(),
                event.journal(),
                event.publicationDate(),
                event.meshTerms(),
                event.version(),
                projectedAt.toString(),
                embedding
        );
    }

    private record ParsedEvent(KafkaRecord<String, String> record, ArticleEvent event) {}

    private record ArticleDocument(
            String pmid,
            String title,
            String abstractText,
            List<String> authors,
            String journal,
            String publicationDate,
            List<String> meshTerms,
            long version,
            String projectedAt,
            float[] embedding
    ) {}
}