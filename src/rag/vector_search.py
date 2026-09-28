from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity


# ============================================================
# DOCUMENTS
# ============================================================

DOCUMENTS = [
    {
        "id": "DOC001",
        "text": (
            "La pompe EQ001 présente une température anormalement élevée. "
            "Une inspection du système de refroidissement est recommandée."
        ),
    },
    {
        "id": "DOC002",
        "text": (
            "Le moteur EQ002 fonctionne normalement. "
            "Aucune anomalie n'a été détectée."
        ),
    },
    {
        "id": "DOC003",
        "text": (
            "La pompe EQ003 nécessite une opération de maintenance "
            "préventive dans les prochaines semaines."
        ),
    },
    {
        "id": "DOC004",
        "text": (
            "Les données des équipements industriels sont stockées "
            "dans le Data Lake."
        ),
    },
    {
        "id": "DOC005",
        "text": (
            "Une température excessive peut indiquer une surchauffe "
            "d'un équipement industriel."
        ),
    },
]


# ============================================================
# VECTOR SEARCH
# ============================================================

def search(
    question: str,
    model: SentenceTransformer,
    document_embeddings,
    top_k: int = 3,
) -> list[dict]:
    """
    Recherche les documents les plus proches sémantiquement
    de la question.
    """

    # --------------------------------------------------------
    # 1. EMBEDDING DE LA QUESTION
    # --------------------------------------------------------

    question_embedding = model.encode([question])

    # --------------------------------------------------------
    # 2. SIMILARITÉ COSINUS
    # --------------------------------------------------------

    similarities = cosine_similarity(
        question_embedding,
        document_embeddings,
    )[0]

    # --------------------------------------------------------
    # 3. CLASSEMENT
    # --------------------------------------------------------

    best_indices = similarities.argsort()[::-1][:top_k]

    # --------------------------------------------------------
    # 4. CONSTRUCTION DES RÉSULTATS
    # --------------------------------------------------------

    results = []

    for index in best_indices:

        results.append(
            {
                "id": DOCUMENTS[index]["id"],
                "text": DOCUMENTS[index]["text"],
                "score": float(similarities[index]),
            }
        )

    return results


# ============================================================
# PROGRAMME PRINCIPAL
# ============================================================

def main() -> None:

    # --------------------------------------------------------
    # 1. MODÈLE D'EMBEDDING MULTILINGUE
    # --------------------------------------------------------

    print("Chargement du modèle d'embedding multilingue...")

    model = SentenceTransformer(
        "paraphrase-multilingual-MiniLM-L12-v2"
    )

    # --------------------------------------------------------
    # 2. INDEXATION
    # --------------------------------------------------------

    texts = [
        document["text"]
        for document in DOCUMENTS
    ]

    document_embeddings = model.encode(texts)

    print(
        f"{len(DOCUMENTS)} documents indexés "
        f"avec des vecteurs de dimension "
        f"{document_embeddings.shape[1]}"
    )

    # --------------------------------------------------------
    # 3. QUESTION
    # --------------------------------------------------------

    question = (
        "Quel équipement semble présenter "
        "un problème de surchauffe ?"
    )

    # --------------------------------------------------------
    # 4. VECTOR SEARCH
    # --------------------------------------------------------

    results = search(
        question=question,
        model=model,
        document_embeddings=document_embeddings,
        top_k=3,
    )

    # --------------------------------------------------------
    # 5. AFFICHAGE
    # --------------------------------------------------------

    print(f"\nQuestion : {question}")

    print("\nTop 3 documents :")

    for rank, result in enumerate(
        results,
        start=1,
    ):

        print(
            f"\n{rank}. {result['id']} "
            f"(score={result['score']:.3f})"
        )

        print(result["text"])


if __name__ == "__main__":
    main()