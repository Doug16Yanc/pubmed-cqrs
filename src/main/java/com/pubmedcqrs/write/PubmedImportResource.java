package com.pubmedcqrs.write;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@Path("/import/pubmed")
public class PubmedImportResource {

    private static final Logger LOG = Logger.getLogger(PubmedImportResource.class);

    @Inject
    PubmedImporterService importerService;

    @Inject
    Executor managedExecutor;


    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response triggerImport(
            @QueryParam("dir") @DefaultValue("./pubmed-data") String dir,
            @QueryParam("file") String file,
            @QueryParam("limit") @DefaultValue("500") int limit) {

        java.nio.file.Path dirPath = java.nio.file.Path.of(dir);

        CompletableFuture.runAsync(() -> {
            try {
                if (file != null && !file.isBlank()) {
                    importerService.importSingleFile(dirPath.resolve(file), limit);
                } else {
                    importerService.importFilesFromDirectory(dirPath, limit);
                }
            } catch (Exception e) {
                LOG.error("Erro durante import assíncrono", e);
            }
        }, managedExecutor);

        String target = (file != null && !file.isBlank()) ? dir + "/" + file : dir + " (diretório inteiro)";
        return Response.accepted()
                .entity("{\"status\":\"started\",\"target\":\"" + target + "\",\"limitPerFile\":" + limit + "}")
                .build();
    }
}