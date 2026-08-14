package com.pubmedcqrs.events;

import java.time.Instant;
import java.util.List;

/**
 * Evento de domínio publicado no tópico "article-events" (particionado por PMID,
 * ver key.serializer em application.properties — garante ordem por artigo).
 *
 * O campo "version" é o que permite ao read side (projector) detectar e descartar
 * reentregas/duplicatas do Kafka (garantias at-least-once) sem corromper o índice.
 */
public record ArticleEvent(
        String eventId,
        ArticleEventType type,
        String pmid,
        String title,
        String abstractText,
        List<String> authors,
        String journal,
        String publicationDate,
        List<String> meshTerms,
        long version,
        Instant occurredAt
) {
}
