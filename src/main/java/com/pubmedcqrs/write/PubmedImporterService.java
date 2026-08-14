package com.pubmedcqrs.write;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

@ApplicationScoped
public class PubmedImporterService {

    private static final Logger LOG = Logger.getLogger(PubmedImporterService.class);
    private static final int BATCH_SIZE = 200;

    @Inject
    ArticleCommandService articleCommandService;

    public void importSingleFile(Path filePath, int maxArticlesPerFile) {
        if (!Files.exists(filePath)) {
            LOG.warnf("Arquivo não encontrado: %s", filePath);
            return;
        }
        if (!filePath.toString().endsWith(".xml.gz")) {
            LOG.warnf("Arquivo não é .xml.gz, ignorando: %s", filePath);
            return;
        }
        processGzipFile(filePath, maxArticlesPerFile);
    }

    public void importFilesFromDirectory(Path dir, int maxArticlesPerFile) {
        try {
            if (!Files.exists(dir)) {
                LOG.warnf("Diretório de dados não encontrado: %s", dir);
                return;
            }

            Files.list(dir)
                    .filter(path -> path.toString().endsWith(".xml.gz"))
                    .forEach(file -> processGzipFile(file, maxArticlesPerFile));

        } catch (Exception e) {
            LOG.error("Erro ao varrer diretório de importação", e);
        }
    }

    private void processGzipFile(Path filePath, int limit) {
        long startNanos = System.nanoTime();
        LOG.infof("INÍCIO IMPORTAÇÃO PubMed — arquivo: %s", filePath.getFileName());

        int count = 0;
        List<ArticleIngestCommand> buffer = new ArrayList<>();

        try (InputStream fileStream = new FileInputStream(filePath.toFile());
             InputStream gzipStream = new GZIPInputStream(fileStream)) {

            XMLInputFactory factory = XMLInputFactory.newInstance();
            factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
            factory.setProperty("javax.xml.stream.isSupportingExternalEntities", false);
            XMLStreamReader reader = factory.createXMLStreamReader(gzipStream);

            String currentPmid = null;
            String currentTitle = null;
            StringBuilder currentAbstract = new StringBuilder();
            List<String> currentAuthors = new ArrayList<>();
            String currentJournal = null;
            String pubYear = null, pubMonth = null, pubDay = null;

            boolean insideArticle = false;
            boolean pmidCaptured = false;
            String currentLastName = null;
            String currentForeName = null;
            java.util.ArrayDeque<String> elementStack = new java.util.ArrayDeque<>();

            while (reader.hasNext()) {
                int event = reader.next();

                switch (event) {
                    case XMLStreamConstants.START_ELEMENT: {
                        String elementName = reader.getLocalName();
                        elementStack.push(elementName);

                        if ("PubmedArticle".equals(elementName)) {
                            insideArticle = true;
                            pmidCaptured = false;
                            currentPmid = null;
                            currentTitle = null;
                            currentAbstract.setLength(0);
                            currentAuthors.clear();
                            currentJournal = null;
                            pubYear = pubMonth = pubDay = null;
                        } else if ("Author".equals(elementName)) {
                            currentLastName = null;
                            currentForeName = null;
                        }
                        break;
                    }

                    case XMLStreamConstants.CHARACTERS: {
                        if (!insideArticle || reader.isWhiteSpace()) break;
                        String text = reader.getText().trim();
                        if (text.isEmpty()) break;

                        String parent = elementStack.peek();

                        switch (parent) {
                            case "PMID":
                                if (!pmidCaptured) {
                                    currentPmid = text;
                                    pmidCaptured = true;
                                }
                                break;
                            case "ArticleTitle":
                                currentTitle = (currentTitle == null) ? text : currentTitle + text;
                                break;
                            case "AbstractText":
                                if (currentAbstract.length() > 0) currentAbstract.append(" ");
                                currentAbstract.append(text);
                                break;
                            case "LastName":
                                currentLastName = text;
                                break;
                            case "ForeName":
                                currentForeName = text;
                                break;
                            case "Title":
                                if ("Title".equals(elementStack.peek())) {
                                    currentJournal = text;
                                }
                                break;
                            case "Year":
                                pubYear = text;
                                break;
                            case "Month":
                                pubMonth = text;
                                break;
                            case "Day":
                                pubDay = text;
                                break;
                            default:
                                break;
                        }
                        break;
                    }

                    case XMLStreamConstants.END_ELEMENT: {
                        String endName = reader.getLocalName();

                        if ("Author".equals(endName)) {
                            if (currentLastName != null || currentForeName != null) {
                                String full = ((currentForeName != null ? currentForeName + " " : "")
                                        + (currentLastName != null ? currentLastName : "")).trim();
                                if (!full.isEmpty()) currentAuthors.add(full);
                            }
                        } else if ("PubmedArticle".equals(endName)) {
                            insideArticle = false;

                            if (currentPmid != null && currentTitle != null) {
                                String pubDate = buildDate(pubYear, pubMonth, pubDay);

                                ArticleIngestCommand cmd = new ArticleIngestCommand(
                                        currentPmid,
                                        currentTitle,
                                        currentAbstract.toString(),
                                        List.copyOf(currentAuthors),
                                        currentJournal != null ? currentJournal : "Unknown Journal",
                                        pubDate,
                                        List.of("Biomedical Research")
                                );

                                try {
                                    buffer.add(cmd);
                                    // Só envia o lote ao Mongo/Kafka quando ele encher —
                                    // nunca a cada artigo. Enviar a cada iteração (mesmo
                                    // com buffer parcial) reenvia o mesmo conteúdo repetidas
                                    // vezes e transforma o import em O(n²) chamadas.
                                    if (buffer.size() >= BATCH_SIZE) {
                                        flushBuffer(buffer);
                                    }
                                    count++;
                                } catch (Exception e) {
                                    LOG.errorf(e, "Falha ao ingerir PMID %s, pulando artigo", currentPmid);
                                }

                                if (count >= limit) {
                                    LOG.infof("Limite de %d artigos atingido para o arquivo %s", limit, filePath.getFileName());
                                    flushBuffer(buffer);
                                    return;
                                }
                            } else {
                                LOG.warnf("Artigo descartado por falta de PMID/título em %s", filePath.getFileName());
                            }
                        }

                        if (!elementStack.isEmpty()) elementStack.pop();
                        break;
                    }
                }
            }

            // Flush final: o que sobrou no buffer sem completar um lote de BATCH_SIZE.
            flushBuffer(buffer);

            LOG.infof("Importação concluída para o arquivo: %s. Total importados: %d", filePath.getFileName(), count);

        } catch (Exception e) {
            LOG.errorf(e, "Erro ao processar o arquivo XML.gz: %s", filePath.getFileName());
        } finally {
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
            LOG.infof("FIM IMPORTAÇÃO PubMed — arquivo: %s — total importados: %d — tempo: %dms",
                    filePath.getFileName(), count, elapsedMs);
        }
    }

    /**
     * Envia o buffer atual para ingestBatch e limpa, sempre que houver conteúdo.
     * Chamado tanto ao atingir BATCH_SIZE quanto no flush final/por limite —
     * único ponto que dispara ingestBatch, pra evitar reenvio duplicado do buffer.
     */
    private void flushBuffer(List<ArticleIngestCommand> buffer) {
        if (buffer.isEmpty()) return;
        articleCommandService.ingestBatch(buffer);
        buffer.clear();
    }

    private String buildDate(String year, String month, String day) {
        if (year == null) return java.time.LocalDate.now().toString();
        String y = year;
        String m = normalizeMonth(month);
        String d = (day != null && day.matches("\\d+")) ? String.format("%02d", Integer.parseInt(day)) : "01";
        return y + "-" + m + "-" + d;
    }

    private String normalizeMonth(String month) {
        if (month == null) return "01";
        if (month.matches("\\d+")) return String.format("%02d", Integer.parseInt(month));
        String[] names = {"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};
        for (int i = 0; i < names.length; i++) {
            if (names[i].equalsIgnoreCase(month.substring(0, Math.min(3, month.length())))) {
                return String.format("%02d", i + 1);
            }
        }
        return "01";
    }
}