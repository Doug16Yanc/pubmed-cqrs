package com.pubmedcqrs.write;

import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.List;
import java.util.Map;

@Path("/articles")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ArticleCommandResource {

    @Inject
    ArticleCommandService commandService;

    @POST
    public Response ingest(@Valid ArticleIngestCommand command) {
        Article article = commandService.ingest(command);
        return Response.status(Response.Status.ACCEPTED)
                .entity(Map.of("pmid", article.pmid, "version", article.version))
                .build();
    }

    @POST
    @Path("/batch")
    public Response ingestBatch(@Valid List<ArticleIngestCommand> commands) {
        int count = 0;
        for (ArticleIngestCommand command : commands) {
            commandService.ingest(command);
            count++;
        }
        return Response.status(Response.Status.ACCEPTED)
                .entity(Map.of("ingested", count))
                .build();
    }
}
