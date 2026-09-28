import os

import requests
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

from vector_search import DOCUMENTS, search


# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

OLLAMA_URL = os.getenv(
    "OLLAMA_URL",
    "http://localhost:11434/api/generate",
)

OLLAMA_MODEL = os.getenv(
    "OLLAMA_MODEL",
    "llama3.2:3b",
)

EMBEDDING_MODEL = (
    "paraphrase-multilingual-MiniLM-L12-v2"
)


# ============================================================
# GENERATION
# ============================================================

def generate_answer(
    question: str,
    context: str,
) -> str:
    """
    Génère une réponse avec Llama via Ollama
    à partir du contexte récupéré par le RAG.
    """

    prompt = f"""
Tu es un assistant spécialisé dans les équipements industriels.

Réponds uniquement à partir du contexte fourni.

N'invente aucune information.

Si le contexte ne permet pas de répondre à la question,
indique que tu ne disposes pas de suffisamment d'informations.

CONTEXTE :

{context}

QUESTION :

{question}

RÉPONSE :
"""

    response = requests.post(
        OLLAMA_URL,
        json={
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
        },
        timeout=120,
    )

    response.raise_for_status()

    return response.json()["response"]


# ============================================================
# PROGRAMME RAG
# ============================================================

def main() -> None:

    # -------------------------
    # 1. MODELE D'EMBEDDING
    # -------------------------

    print(
        "Chargement du modèle "
        "d'embedding multilingue..."
    )

    embedding_model = SentenceTransformer(
        EMBEDDING_MODEL
    )

    # -------------------------
    # 2. INDEXATION
    # -------------------------

    print("Indexation des documents...")

    texts = [
        document["text"]
        for document in DOCUMENTS
    ]

    document_embeddings = embedding_model.encode(
        texts
    )

    print(
        f"{len(DOCUMENTS)} documents indexés."
    )

    # -------------------------
    # 3. QUESTION
    # -------------------------

    question = input(
        "\nPose ta question : "
    )

    # -------------------------
    # 4. RETRIEVAL
    # -------------------------

    results = search(
        question=question,
        model=embedding_model,
        document_embeddings=document_embeddings,
        top_k=3,
    )

    print("\nDocuments récupérés :")

    for result in results:
        print(
            f"- {result['id']} "
            f"(score={result['score']:.3f})"
        )

        print(
            f"  {result['text']}"
        )

    # -------------------------
    # 5. CONTEXTE
    # -------------------------

    context = "\n\n".join(
        (
            f"{result['id']} : "
            f"{result['text']}"
        )
        for result in results
    )

    # -------------------------
    # 6. GENERATION
    # -------------------------

    print("\nGénération de la réponse...")

    answer = generate_answer(
        question=question,
        context=context,
    )

    print("\nRéponse du RAG:")

    print(answer)


if __name__ == "__main__":
    main()