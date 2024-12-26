import pandas as pd
from datetime import datetime

# Lecture du fichier
df = pd.read_csv('fin_juin.csv')

# Conversion de la colonne date en datetime
df['date'] = pd.to_datetime(df['ts_event'], unit='ns')

# Définition de la plage de dates
date_start = datetime(2019, 6, 21, 13, 29)
date_end = datetime(2019, 6, 30, 23, 59)

# Filtrage des données
filtered_data = df[(df['date'] >= date_start) & (df['date'] <= date_end)]

# Création du nouveau fichier CSV
filtered_data.to_csv('fin_juin_filtered.csv', index=False)