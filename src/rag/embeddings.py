from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity


def main() -> None:

    # Modèle qui transforme du texte en vecteurs
    model = SentenceTransformer("all-MiniLM-L6-v2")

    documents = [
        "La pompe EQ001 présente une température trop élevée.",
        "Le moteur EQ002 fonctionne normalement.",
        "La pompe EQ003 nécessite une opération de maintenance.",
        "Les données industrielles sont stockées dans le Data Lake.",
    ]

    question = "Quel équipement présente un problème de surchauffe ?"

    # Transformation des textes en embeddings
    document_embeddings = model.encode(documents)
    question_embedding = model.encode([question])

    print("Nombre de documents :", len(documents))
    print("Dimension d'un embedding :", document_embeddings.shape[1])

    # Similarité entre la question et chaque document
    similarities = cosine_similarity(
        question_embedding,
        document_embeddings,
    )[0]

    print("\nQuestion :", question)

    print("\nSimilarités :")

    for document, score in zip(documents, similarities):
        print(f"{score:.3f} -> {document}")

    # Document ayant le meilleur score
    best_index = similarities.argmax()

    print("\nDocument le plus pertinent :")
    print(documents[best_index])


if __name__ == "__main__":
    main()