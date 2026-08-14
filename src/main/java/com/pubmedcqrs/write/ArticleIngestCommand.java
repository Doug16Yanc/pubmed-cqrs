package com.pubmedcqrs.write;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public record ArticleIngestCommand(
        @NotBlank String pmid,
        @NotBlank String title,
        String abstractText,
        @NotNull List<String> authors,
        String journal,
        String publicationDate,
        @NotNull List<String> meshTerms
) {
}
