package com.pubmedcqrs.read;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.EmbeddingModel;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.util.List;

@Path("/articles/search")
@Produces(MediaType.APPLICATION_JSON)
public class ArticleSearchResource {

    private static final String INDEX = "articles";

    @Inject
    ElasticsearchClient esClient;

    @Inject
    EmbeddingModel embeddingModel;

    /**
     * mode=text (padrão): BM25 tradicional, com minimum_should_match pra evitar
     *   que uma única palavra genérica em comum puxe documento irrelevante pro
     *   resultado — era exatamente esse ruído que trazia holografia/diabetes
     *   pra query sobre metástase.
     * mode=semantic: kNN puro sobre o campo "embedding" — o texto da query é
     *   convertido em vetor pelo mesmo modelo (all-MiniLM-L6-v2) usado na
     *   indexação, e o Elasticsearch busca os vizinhos mais próximos por
     *   similaridade de cosseno. Não olha palavra nenhuma, só significado.
     * mode=hybrid: combina os dois na mesma chamada — full-text entra como
     *   query normal, kNN roda em paralelo, o ES soma os scores.
     */
    @GET
    public List<ArticleSearchHit> search(@QueryParam("q") String query,
                                         @QueryParam("size") @DefaultValue("20") int size,
                                         @QueryParam("mode") @DefaultValue("text") String mode) throws IOException {

        SearchResponse<ArticleDocument> response = switch (mode) {
            case "semantic" -> searchSemantic(query, size);
            case "hybrid" -> searchHybrid(query, size);
            default -> searchFullText(query, size);
        };

        return response.hits().hits().stream()
                .map(this::toHit)
                .toList();
    }

    private SearchResponse<ArticleDocument> searchFullText(String query, int size) throws IOException {
        return esClient.search(s -> s
                        .index(INDEX)
                        .size(size)
                        .source(src -> src.filter(f -> f.excludes("embedding")))
                        .query(q -> q.multiMatch(m -> m
                                .fields("title^3", "abstractText", "meshTerms", "authors")
                                .query(query)
                                // sem isso, UMA palavra em comum já qualifica o documento (operador OR
                                // padrão). Com 75%, exige que a maioria dos termos da query casem —
                                // reduz drasticamente ruído de coincidência léxica isolada.
                                .minimumShouldMatch("75%"))),
                ArticleDocument.class);
    }

    private SearchResponse<ArticleDocument> searchSemantic(String query, int size) throws IOException {
        float[] queryVector = embedQuery(query);

        return esClient.search(s -> s
                        .index(INDEX)
                        .size(size)
                        .source(src -> src.filter(f -> f.excludes("embedding")))
                        .knn(k -> k
                                .field("embedding")
                                .queryVector(toList(queryVector))
                                .k(size)
                                .numCandidates(Math.max(size * 5, 50))),
                ArticleDocument.class);
    }

    private SearchResponse<ArticleDocument> searchHybrid(String query, int size) throws IOException {
        float[] queryVector = embedQuery(query);

        return esClient.search(s -> s
                        .index(INDEX)
                        .size(size)
                        .source(src -> src.filter(f -> f.excludes("embedding")))
                        .query(q -> q.multiMatch(m -> m
                                .fields("title^3", "abstractText", "meshTerms", "authors")
                                .query(query)
                                .minimumShouldMatch("75%")))
                        .knn(k -> k
                                .field("embedding")
                                .queryVector(toList(queryVector))
                                .k(size)
                                .numCandidates(Math.max(size * 5, 50))
                                // boost relativo entre o score do full-text e o score do kNN.
                                // ambos entram na mesma soma final — ajuste conforme observar
                                // qual dos dois está dominando o ranking na prática.
                                .boost(1.0f)),
                ArticleDocument.class);
    }

    private float[] embedQuery(String query) {
        Embedding embedding = embeddingModel.embed(query).content();
        return embedding.vector();
    }

    private List<Float> toList(float[] vector) {
        List<Float> list = new java.util.ArrayList<>(vector.length);
        for (float v : vector) list.add(v);
        return list;
    }

    private ArticleSearchHit toHit(Hit<ArticleDocument> hit) {
        return new ArticleSearchHit(hit.source(), hit.score());
    }

    @GET
    @Path("/{pmid}")
    public Response getByPmid(@PathParam("pmid") String pmid) throws IOException {
        GetResponse<ArticleDocument> response = esClient.get(g -> g.index(INDEX).id(pmid), ArticleDocument.class);
        if (!response.found()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(response.source()).build();
    }

    public record ArticleSearchHit(ArticleDocument article, Double score) {}
}