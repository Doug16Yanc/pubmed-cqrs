package com.pubmedcqrs.read;

import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2q.AllMiniLmL6V2QuantizedEmbeddingModel;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/**
 * Expõe o EmbeddingModel como bean CDI. Não usamos uma extensão Quarkus LangChain4j
 * aqui de propósito — o modelo roda in-process via ONNX Runtime, dentro da própria
 * JVM, então basta instanciar a classe diretamente. Sem chave de API, sem chamada
 * de rede, sem servidor externo.
 *
 * ---- Upgrade pra qualidade melhor em texto biomédico ----
 * O all-MiniLM-L6-v2 é um baseline genérico. Pro domínio do PubMed especificamente,
 * o NeuML/pubmedbert-base-embeddings (fine-tunado em pares título/abstract do PubMed)
 * performa melhor nos benchmarks do próprio autor (PubMed QA, PubMed Subset, PubMed
 * Summary) — ver https://huggingface.co/NeuML/pubmedbert-base-embeddings.
 *
 * Pra trocar:
 *   1. Converter o modelo pra ONNX (passo único, feito localmente com Python):
 *        pip install optimum[exporters]
 *        optimum-cli export onnx --model NeuML/pubmedbert-base-embeddings ./pubmedbert-onnx
 *      Isso gera model.onnx + tokenizer.json em ./pubmedbert-onnx.
 *   2. Colocar os dois arquivos em src/main/resources/models/pubmedbert/ (ou fora do
 *      classpath, se preferir não versionar ~440MB no Git).
 *   3. Trocar o corpo deste método por:
 *        String modelPath = "/caminho/pro/model.onnx";
 *        String tokenizerPath = "/caminho/pro/tokenizer.json";
 *        return new OnnxEmbeddingModel(modelPath, tokenizerPath, PoolingMode.MEAN);
 *      (requer a dependência dev.langchain4j:langchain4j-embeddings no lugar da
 *      all-minilm-l6-v2-q no pom.xml)
 *   4. IMPORTANTE: mudar dims(384) pra dims(768) no ElasticsearchIndexInitializer —
 *      dimensão errada quebra a indexação, e o índice existente precisaria ser
 *      recriado (dimensão de campo não é mapping update).
 */
@ApplicationScoped
public class LocalEmbeddingModelProducer {

    @Produces
    @ApplicationScoped
    public EmbeddingModel embeddingModel() {
        return new AllMiniLmL6V2QuantizedEmbeddingModel();
    }
}
