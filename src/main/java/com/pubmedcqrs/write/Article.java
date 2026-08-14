package com.pubmedcqrs.write;

import io.quarkus.mongodb.panache.PanacheMongoEntityBase;
import org.bson.codecs.pojo.annotations.BsonId;

import java.time.Instant;
import java.util.List;

/**
 * Write model (fonte da verdade) — documento fiel à estrutura do artigo do PubMed.
 * Usamos o PMID como _id do Mongo: isso torna a reingestão do mesmo artigo naturalmente
 * idempotente (upsert), sem precisar de lógica extra de deduplicação no write side.
 */
public class Article extends PanacheMongoEntityBase {

    @BsonId
    public String pmid;

    public String title;
    public String abstractText;
    public List<String> authors;
    public String journal;
    public String publicationDate; // yyyy-MM-dd, conforme normalizado a partir do PubMed
    public List<String> meshTerms;

    public Instant ingestedAt;
    public Instant updatedAt;

    // Incrementado a cada persistOrUpdate. Vira o "version" do evento publicado no Kafka
    // e é usado no read side (Elasticsearch, versionamento externo) para descartar
    // eventos duplicados ou entregues fora de ordem.
    public long version;
}
