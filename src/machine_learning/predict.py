import joblib
import pandas as pd
from pathlib import Path


MODEL_PATH = Path("models/temperature_model.joblib")


def main() -> None:

    # 1. Charger le modèle déjà entraîné
    model = joblib.load(MODEL_PATH)

    # 2. Nouvelle donnée jamais vue
    new_data = pd.DataFrame(
        [
            {
                "outside_temperature": 30,
                "equipment_age": 8,
                "load": 75,
            }
        ]
    )

    print("Nouvelle donnée :")
    print(new_data)

    # 3. Prédiction
    prediction = model.predict(new_data)

    print(
        f"\nTempérature prédite : "
        f"{prediction[0]:.2f} °C"
    )


if __name__ == "__main__":
    main()