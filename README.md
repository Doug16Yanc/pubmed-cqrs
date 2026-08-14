# PubMed CQRS POC

POC de CQRS: MongoDB (write model) → Kafka/Redpanda (eventos) → Elasticsearch (read model),
usando artigos do PubMed como massa de dados.

## Stack

- Java 25 + Quarkus 3.33 LTS
- MongoDB (write model, fonte da verdade)
- Redpanda (compatível com protocolo Kafka)
- Elasticsearch (read model, busca full-text/facetas)

## Como rodar

1. Suba a infraestrutura:

   ```bash
   docker compose up -d
   ```

   Isso sobe Mongo (27017), Redpanda (9092, console em http://localhost:8090)
   e Elasticsearch (9200, Kibana em http://localhost:5601).

2. Rode a aplicação em modo dev:

   ```bash
   ./mvnw quarkus:dev
   ```

   > Nota: este esqueleto foi gerado sem acesso à internet no momento da criação,
   > então a primeira build vai precisar baixar as dependências do Maven Central
   > normalmente. Ajuste `quarkus.platform.version` no `pom.xml` para a última
   > patch da 3.33 LTS disponível quando for buildar.

## Testando o fluxo

**Ingerir um artigo (write side):**

```bash
curl -X POST http://localhost:8080/articles \
  -H "Content-Type: application/json" \
  -d '{
    "pmid": "12345678",
    "title": "Machine learning applications in cheminformatics",
    "abstractText": "This study explores...",
    "authors": ["Silva J", "Souza M"],
    "journal": "Journal of Cheminformatics",
    "publicationDate": "2024-03-15",
    "meshTerms": ["Machine Learning", "Cheminformatics", "Drug Discovery"]
  }'
```

**Consultar (read side, via Elasticsearch):**

```bash
curl "http://localhost:8080/articles/search?q=cheminformatics"
curl "http://localhost:8080/articles/search/12345678"
```

Repare que existe uma pequena defasagem entre o POST (grava no Mongo + publica evento)
e o artigo ficar pesquisável no ES (consistência eventual) — essa é uma métrica
interessante para medir no POC.

## Roteiro sugerido de experimentos

1. **Idempotência / reentrega**: publique manualmente o mesmo evento duas vezes no
   tópico `article-events` (via Redpanda Console, http://localhost:8090) com o mesmo
   `version` — o segundo deve ser rejeitado pelo Elasticsearch (409, versionamento
   externo) e logado como no-op no `ArticleProjector`.
2. **Fora de ordem**: publique um evento com `version` menor que o já indexado e
   confirme que ele é descartado.
3. **Rebuild do read model**: zere o índice do ES (`DELETE /articles`) e reprocesse
   o tópico do zero (resetando o `group.id` do consumer) para reconstruir o read
   model inteiro a partir do Kafka — um mini "event sourcing replay".
4. **Carga**: use o endpoint `/articles/batch` para ingerir um lote de artigos
   (baixados via PubMed E-utilities ou baseline files) e meça:
   - Lag entre gravação no Mongo e disponibilidade no ES
   - Latência de `search` no ES vs. uma query equivalente direto no Mongo
   - Throughput do consumer sob carga (via métricas do Redpanda Console)

## Dados do PubMed

Para volume de teste, uns 10k–50k artigos de um baseline file do PubMed já são
suficientes para observar diferenças de performance sem precisar baixar milhões
de registros. Se for usar a API E-utilities (esearch/efetch) em vez dos baseline
dumps, respeite o rate limit do NCBI (3 req/s sem API key, 10 req/s com key).

## Próximos passos (não incluídos neste esqueleto)

- Script de importação em lote a partir de arquivos XML do PubMed baseline
- Dead-letter queue para eventos malformados no projector
- Testes de integração com Testcontainers (Mongo + Redpanda + ES)
- Métricas (Micrometer) para medir lag de projeção e latência de query
