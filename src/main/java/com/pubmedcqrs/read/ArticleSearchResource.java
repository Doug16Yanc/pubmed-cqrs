package com.pubmedcqrs.read;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
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

    @GET
    public List<ArticleDocument> search(@QueryParam("q") String query,
                                         @QueryParam("size") @DefaultValue("20") int size) throws IOException {
        SearchResponse<ArticleDocument> response = esClient.search(s -> s
                        .index(INDEX)
                        .size(size)
                        .query(q -> q.multiMatch(m -> m
                                .fields("title^3", "abstractText", "meshTerms", "authors")
                                .query(query))),
                ArticleDocument.class);

        return response.hits().hits().stream()
                .map(hit -> hit.source())
                .toList();
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
}
