package com.pubmedcqrs.write;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.pubmedcqrs.events.ArticleEvent;
import com.pubmedcqrs.events.ArticleEventType;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@ApplicationScoped
public class ArticleCommandService {

    private static final Logger LOG = Logger.getLogger(ArticleCommandService.class);

    @Inject
    ArticleRepository articleRepository;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    @Channel("article-events-out")
    Emitter<String> eventEmitter;

    /**
     * Resultado interno de preparar um lote pra persistência: os artigos prontos pra
     * upsert, o tipo de evento de cada um (mesma ordem/índice que bulkOps), e as
     * operações de bulk write em si. Os três índices ficam alinhados 1:1 — se essa
     * invariante quebrar, o mapeamento de erro parcial do bulk (ver applyBulkWrite)
     * aponta pro artigo errado.
     */
    private record PreparedBatch(
            List<Article> articles,
            List<ArticleEventType> types,
            List<WriteModel<Article>> bulkOps
    ) {
        boolean isEmpty() {
            return bulkOps.isEmpty();
        }
    }

    public Article ingest(ArticleIngestCommand command) {
        List<Article> result = ingestBatch(List.of(command));
        return result.isEmpty() ? articleRepository.find("pmid", command.pmid()).firstResult() : result.get(0);
    }

    public List<Article> ingestBatch(List<ArticleIngestCommand> commands) {
        if (commands.isEmpty()) return List.of();

        long start = System.nanoTime();
        try {
            List<ArticleIngestCommand> deduped = dedupeByPmid(commands);
            Map<String, Article> existingByPmid = findExistingByPmid(deduped);
            PreparedBatch batch = prepareBatch(deduped, existingByPmid);

            List<Article> persisted = persistBatch(batch);
            publishAll(persisted, batch);

            return persisted;
        } finally {
            logBatchTiming(commands.size(), start);
        }
    }

    /**
     * Remove PMIDs duplicados dentro do mesmo lote, mantendo a ocorrência mais recente.
     * Necessário porque o find de existentes é feito uma vez só pro lote inteiro — sem
     * isso, duas ocorrências do mesmo PMID no mesmo buffer geram dois upserts pro
     * mesmo _id dentro do mesmo bulkWrite, o que o Mongo rejeita como duplicate key.
     */
    private List<ArticleIngestCommand> dedupeByPmid(List<ArticleIngestCommand> commands) {
        Map<String, ArticleIngestCommand> dedupedByPmid = new LinkedHashMap<>();
        for (ArticleIngestCommand cmd : commands) {
            if (dedupedByPmid.put(cmd.pmid(), cmd) != null) {
                LOG.debugf("PMID %s duplicado no lote, mantendo a ocorrência mais recente", cmd.pmid());
            }
        }
        return new ArrayList<>(dedupedByPmid.values());
    }

    private Map<String, Article> findExistingByPmid(List<ArticleIngestCommand> deduped) {
        List<String> pmids = deduped.stream().map(ArticleIngestCommand::pmid).toList();
        return articleRepository
                .find("pmid in ?1", pmids)
                .<Article>stream()
                .collect(Collectors.toMap(a -> a.pmid, a -> a));
    }

    /**
     * Monta os artigos atualizados e as operações de bulk write correspondentes,
     * pulando os que não têm mudança real (idempotência).
     */
    private PreparedBatch prepareBatch(List<ArticleIngestCommand> deduped, Map<String, Article> existingByPmid) {
        Instant now = Instant.now();
        List<Article> articles = new ArrayList<>();
        List<ArticleEventType> types = new ArrayList<>();
        List<WriteModel<Article>> bulkOps = new ArrayList<>();

        for (ArticleIngestCommand command : deduped) {
            Article existing = existingByPmid.get(command.pmid());

            if (existing != null && !hasChanges(existing, command)) {
                LOG.debugf("PMID %s sem alterações, ignorando (idempotente)", command.pmid());
                continue;
            }

            Article article = buildArticle(command, existing, now);

            // pmid é o @BsonId da entidade Article — no documento Mongo ele é
            // armazenado como _id, não como um campo "pmid" separado. Filtrar por
            // "pmid" aqui nunca encontra o doc existente e faz todo upsert de update
            // virar tentativa de insert, colidindo em duplicate key no _id.
            bulkOps.add(new ReplaceOneModel<>(
                    Filters.eq("_id", article.pmid),
                    article,
                    new ReplaceOptions().upsert(true)
            ));
            articles.add(article);
            types.add(existing == null ? ArticleEventType.INGESTED : ArticleEventType.UPDATED);
        }

        return new PreparedBatch(articles, types, bulkOps);
    }

    private Article buildArticle(ArticleIngestCommand command, Article existing, Instant now) {
        Article article = existing != null ? existing : new Article();
        article.pmid = command.pmid();
        article.title = command.title();
        article.abstractText = command.abstractText();
        article.authors = command.authors();
        article.journal = command.journal();
        article.publicationDate = command.publicationDate();
        article.meshTerms = command.meshTerms();
        article.updatedAt = now;
        article.version = existing != null ? existing.version + 1 : 1;
        if (existing == null) {
            article.ingestedAt = now;
        }
        return article;
    }

    /**
     * Executa o bulk write com ordered=false, pra que um item com falha (ex: colisão
     * de _id) não derrube os demais do lote. Retorna só os artigos que de fato foram
     * persistidos — os que falharam são removidos e logados, não publicados no Kafka.
     */
    private List<Article> persistBatch(PreparedBatch batch) {
        if (batch.isEmpty()) return List.of();

        try {
            articleRepository.mongoCollection().bulkWrite(
                    batch.bulkOps(),
                    new BulkWriteOptions().ordered(false)
            );
            return batch.articles();
        } catch (MongoBulkWriteException e) {
            return removeFailedFromBatch(batch, e);
        }
    }

    private List<Article> removeFailedFromBatch(PreparedBatch batch, MongoBulkWriteException e) {
        Set<Integer> failedIndexes = new HashSet<>();
        for (BulkWriteError err : e.getWriteErrors()) {
            failedIndexes.add(err.getIndex());
            String pmid = batch.articles().get(err.getIndex()).pmid;
            LOG.errorf("Falha ao persistir PMID %s no bulk: %s", pmid, err.getMessage());
        }

        List<Article> succeeded = new ArrayList<>();
        for (int i = 0; i < batch.articles().size(); i++) {
            if (!failedIndexes.contains(i)) {
                succeeded.add(batch.articles().get(i));
            }
        }
        return succeeded;
    }

    /**
     * Publica eventos só pros artigos que sobreviveram ao persistBatch — usa o pmid
     * pra reencontrar o tipo (INGESTED/UPDATED) original em batch.types(), já que
     * persisted pode ter itens removidos por falha parcial.
     */
    private void publishAll(List<Article> persisted, PreparedBatch batch) {
        Map<String, ArticleEventType> typeByPmid = new HashMap<>();
        for (int i = 0; i < batch.articles().size(); i++) {
            typeByPmid.put(batch.articles().get(i).pmid, batch.types().get(i));
        }
        for (Article article : persisted) {
            publishEvent(article, typeByPmid.get(article.pmid));
        }
    }

    private void logBatchTiming(int commandCount, long startNanos) {
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        double rate = elapsedMs == 0 ? 0 : commandCount * 1000.0 / elapsedMs;
        LOG.debug(String.format("Ingestão em lote: %d comandos em %dms (%.1f/s)",
                commandCount, elapsedMs, rate));
    }

    private boolean hasChanges(Article existing, ArticleIngestCommand command) {
        return !Objects.equals(existing.title, command.title())
                || !Objects.equals(existing.abstractText, command.abstractText())
                || !Objects.equals(existing.authors, command.authors())
                || !Objects.equals(existing.journal, command.journal())
                || !Objects.equals(existing.publicationDate, command.publicationDate())
                || !Objects.equals(existing.meshTerms, command.meshTerms());
    }

    private void publishEvent(Article article, ArticleEventType type) {
        ArticleEvent event = new ArticleEvent(
                UUID.randomUUID().toString(),
                type,
                article.pmid,
                article.title,
                article.abstractText,
                article.authors,
                article.journal,
                article.publicationDate,
                article.meshTerms,
                article.version,
                Instant.now()
        );

        try {
            String payload = objectMapper.writeValueAsString(event);
            Message<String> message = Message.of(payload)
                    .addMetadata(OutgoingKafkaRecordMetadata.<String>builder()
                            .withKey(article.pmid)
                            .build());
            eventEmitter.send(message);
            LOG.debugf("Evento %s publicado para PMID %s (versão %d)", type, article.pmid, article.version);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Falha ao serializar evento do artigo " + article.pmid, e);
        }
    }
}