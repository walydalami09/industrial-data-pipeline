import numpy as np
import pandas as pd
import joblib
from pathlib import Path

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score


def generate_dataset(n_rows: int = 1000) -> pd.DataFrame:
    """
    Génère des mesures industrielles synthétiques pour notre exercice ML.
    """

    rng = np.random.default_rng(42)

    outside_temperature = rng.uniform(5, 35, n_rows)
    equipment_age = rng.integers(1, 20, n_rows)
    load = rng.uniform(20, 100, n_rows)

    # Température de l'équipement simulée à partir de plusieurs facteurs.
    temperature = (
        30
        + 0.7 * outside_temperature
        + 0.25 * load
        + 0.4 * equipment_age
        + rng.normal(0, 3, n_rows)
    )

    return pd.DataFrame(
        {
            "outside_temperature": outside_temperature,
            "equipment_age": equipment_age,
            "load": load,
            "temperature": temperature,
        }
    )


def main() -> None:

    # 1. Dataset
    df = generate_dataset()

    print("Dataset :")
    print(df.head())

    # 2. Features X et target y
    X = df[
        [
            "outside_temperature",
            "equipment_age",
            "load",
        ]
    ]

    y = df["temperature"]

    # 3. Séparation entraînement / test
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
    )

    print(f"\nTrain : {len(X_train)} lignes")
    print(f"Test  : {len(X_test)} lignes")

    # 4. Création du modèle
    model = RandomForestRegressor(
        n_estimators=100,
        random_state=42,
    )

    # 5. Entraînement
    model.fit(X_train, y_train)

    # Sauvegarde du modèle entraîné
    MODEL_PATH = Path("models/temperature_model.joblib")

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)

    joblib.dump(model, MODEL_PATH)

    print(f"\nModèle sauvegardé : {MODEL_PATH}")

    # 6. Prédictions
    predictions = model.predict(X_test)

    # 7. Évaluation
    mae = mean_absolute_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)

    print("\nRésultats du modèle")
    print(f"MAE : {mae:.2f} °C")
    print(f"R²  : {r2:.3f}")


if __name__ == "__main__":
    main()