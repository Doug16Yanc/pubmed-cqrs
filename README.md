# PubMed CQRS PoC

PoC de arquitetura CQRS event-driven: MongoDB como write model, Kafka (Redpanda)
como espinha dorsal de eventos, Elasticsearch como read model otimizado para
busca full-text. Massa de dados real do baseline público do PubMed.

## Stack

- Java 25 + Quarkus 3.33 LTS.
- MongoDB — write model, fonte da verdade.
- Kafka — event bus entre write e read side.
- Elasticsearch — read model, busca full-text e facetada.
- LangChain4j + ONNX Runtime — embeddings in-process, sem chamada de rede.
- all-MiniLM-L6-v2 (quantizado) — modelo de sentence embedding, 384 dimensões.
- MicroProfile Fault Tolerance (`@Retry`) — resiliência na indexação.

## Arquitetura

![Diagrama](https://github.com/user-attachments/assets/fc071142-31c7-4363-9fe8-eb4bdcc3ed12)

Write e read side não se conhecem diretamente, a única ponte é o tópico Kafka.
O read model pode ser reconstruído do zero a qualquer momento reprocessando o
tópico desde o início, sem tocar no write model.

## Busca semântica

Além do full-text tradicional, o `ArticleProjector` gera um embedding
(all-MiniLM-L6-v2, via LangChain4j + ONNX Runtime, rodando in-process na JVM)
para cada artigo no momento da indexação, armazenado como campo
`dense_vector` (384 dim, similaridade cosseno) no Elasticsearch.

```http
GET /articles/search?q=...&mode=text      # BM25 puro
GET /articles/search?q=...&mode=semantic  # kNN sobre o dense_vector
GET /articles/search?q=...&mode=hybrid    # combina os dois scores
```

`hybrid` costuma ser o modo mais robusto na prática: o componente léxico
(BM25) corrige casos onde o embedding aproxima demais textos que só
compartilham vocabulário genérico do domínio biomédico, sem perder a
capacidade do semântico de casar sinônimos e paráfrases que o full-text
sozinho não pega.

- Sem métricas exportadas (Micrometer/Prometheus) — números de performance
   acima vieram de logs instrumentados manualmente, não de um painel.
- Modelo de embedding fixo em 384 dimensões (all-MiniLM-L6-v2) — trocar por
  um modelo maior/domain-specific exige recriar o índice do zero
  (dimensão de vetor não é um mapping update no Elasticsearch) e tem custo de CPU
  proporcionalmente maior, sem garantia de ganho de relevância no volume
  atual de dados.
  
## Decisões de design

**Idempotência via versionamento externo + diff de conteúdo.** Cada evento
carrega um `version` incremental. O `ArticleProjector` indexa no ES com
`version_type=external`: o Elasticsearch rejeita nativamente (`409`) qualquer
versão igual ou anterior à já indexada, sem lógica de deduplicação na
aplicação. No write side, `ArticleCommandService` só incrementa a versão e
publica evento novo se o conteúdo de fato mudou (comparação campo a campo) —
reimportar o mesmo artigo sem alteração é um no-op silencioso, não gera
ruído no Kafka.

**Falha parcial não derruba o consumer.** O read side consome em lote
(`KafkaRecordBatch`) e indexa via Bulk API, mas cada mensagem é reconhecida
(`ack`/`nack`) individualmente com base no resultado item-a-item que o
Elasticsearch devolve. Um `409` de conflito de versão é tratado como sucesso
(idempotência esperada); uma falha real de indexação vai para uma dead-letter
queue (`article-events-dlq`) sem bloquear o restante do lote. Falha de
transporte (Elasticsearch indisponível) aciona retry com backoff
(`@Retry`, 3 tentativas) antes de escalar para DLQ.


**Parsing XML em streaming.** O baseline do PubMed é distribuído em arquivos
`.xml.gz` de dezenas de milhares de artigos. O parser usa StAX
(`XMLStreamReader`) para nunca materializar o arquivo inteiro em memória —
o custo de memória do import é O(1) em relação ao tamanho do arquivo.

## Executando

```bash
docker compose up -d   # Mongo :27017, Redpanda :9092 (console :8090), ES :9200 (Kibana :5601)
./mvnw quarkus:dev
```

### Massa de dados

```bash
./scripts/download-baseline.sh ./pubmed-data 1
```

Baixa um arquivo do baseline (~20-30 mil artigos) com verificação de checksum
e retomada em caso de falha.

### Import

```bash
curl -X POST "http://localhost:8080/api/import/pubmed?dir=./pubmed-data&file=pubmed26n0001.xml.gz&limit=2000"
```

`dir`/`file` mira um arquivo específico; omitir `file` processa todo o
diretório. `limit` é um teto por arquivo, não uma garantia — artigos sem
PMID ou título são descartados no parsing e não contam para o limite alcançado
em termos de posição no XML.

## Performance observada

Import completo de um arquivo baseline (30.000 artigos, hardware local,
infraestrutura em Docker na mesma máquina):

| Métrica | Valor |
|---|---|
| Tempo total | 425s |
| Throughput médio | ~70 artigos/s |
| Lag de projeção (Kafka → ES) | ~6ms por evento |
| Latência de bulk index (ES) | ~3ms por lote |

O gargalo estava no write side: parsing StAX + `find`/`persistOrUpdate` no
Mongo + publish no Kafka, artigo a artigo, a ~22ms por artigo — um teto de
~45 artigos/s. O read side, com consumo em lote e Bulk API, processava mais
rápido do que o write side conseguia alimentar; os lotes chegavam com poucos
itens porque o Kafka nunca acumulava backlog suficiente entre polls.
 
**Depois (write-side batchado via `bulkWrite` no Mongo):** o gargalo foi
eliminado — a escrita deixou de ser o teto do pipeline.
 
Lag de projeção (Kafka → ES) e latência de bulk index no Elasticsearch se
mantiveram nos mesmos ~6ms e ~3ms por lote observados antes; a mudança não
afetou o read side, só removeu o represamento anterior.

## Limitações conhecidas / débito técnico

- `hasChanges()` compara documento completo a cada reimport — correto para o
  volume atual, torna-se um custo relevante em lotes muito maiores.
- Sem testes de integração automatizados (Testcontainers) — validação até
  aqui foi manual, via logs e consultas diretas ao Mongo/Elasticsearch.
- Sem métricas exportadas (Micrometer/Prometheus) — números de performance
  acima vieram de logs instrumentados manualmente, não de um painel.
