package com.pubmedcqrs.read;

import com.pubmedcqrs.events.ArticleEvent;

import java.time.Instant;
import java.util.List;

/**
 * Read model — desnormalizado e otimizado para busca full-text/facetas.
 * Projetado a partir do ArticleEvent pelo ArticleProjector.
 */
public class ArticleDocument {

    public String pmid;
    public String title;
    public String abstractText;
    public List<String> authors;
    public String journal;
    public String publicationDate;
    public List<String> meshTerms;
    public long version;
    public Instant projectedAt;

    public static ArticleDocument fromEvent(ArticleEvent event) {
        ArticleDocument doc = new ArticleDocument();
        doc.pmid = event.pmid();
        doc.title = event.title();
        doc.abstractText = event.abstractText();
        doc.authors = event.authors();
        doc.journal = event.journal();
        doc.publicationDate = event.publicationDate();
        doc.meshTerms = event.meshTerms();
        doc.version = event.version();
        doc.projectedAt = Instant.now();
        return doc;
    }
}
