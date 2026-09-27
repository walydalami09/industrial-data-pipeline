import numpy as np

# Températures de nos équipements industriels.
temperatures = np.array([72.4, 91.8, 68.1, 87.3, 95.2])

print(temperatures)

# Informations sur le tableau.
print(temperatures.ndim)
print(temperatures.shape)
print(temperatures.dtype)

# Calculer la moyenne des températures.
print(np.mean(temperatures))

# Récupérer la température maximale.
print(np.max(temperatures))

high_temperatures = temperatures[(temperatures >= 70 )& (temperatures <= 90) ]

print(high_temperatures)