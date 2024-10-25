import pandas as pd
import requests
from io import BytesIO, StringIO

def download_data(url):
    """
    Télécharge et retourne les données d'une URL, en gérant les fichiers Excel et CSV de manière appropriée.
    Si le fichier est un Excel, il est converti en DataFrame directement, puis traité comme un CSV.
    """
    # Détecter le type de fichier à partir de l'URL
    if url.endswith('.xls') or url.endswith('.xlsx'):
        skip_rows = 4  # Fichiers d'origine Excel : ignorer les 4 premières lignes
        
        # Télécharger et charger l'Excel
        response = requests.get(url)
        response.raise_for_status()
        data = pd.read_excel(BytesIO(response.content), skiprows=skip_rows)

    else:
        skip_rows = 0  # Pas de saut de lignes par défaut pour les fichiers CSV
        # Télécharger et charger le CSV
        response = requests.get(url)
        response.raise_for_status()
        csv_data = response.content.decode('utf-8')
        
        try:
            data = pd.read_csv(StringIO(csv_data), skiprows=skip_rows)
        except pd.errors.ParserError:
            try:
                data = pd.read_csv(StringIO(csv_data), delimiter=';', skiprows=skip_rows)
            except pd.errors.ParserError:
                # Si les deux méthodes échouent, sauter 10 lignes supplémentaires
                data = pd.read_csv(StringIO(csv_data), skiprows=9 + skip_rows)

    # Si aucune colonne 'Weight' ou 'Weight (%)' n'est trouvée, sauter 10 premières lignes
    if not any('weight' in col.lower() for col in data.columns):
        data = pd.read_csv(StringIO(csv_data), skiprows=9 + skip_rows)

    return data

def calculate_total_weights(url, etf_weight):
    """
    Calcule le poids total pour chaque action d'un fichier CSV ou Excel.
    """
    data = download_data(url)
    
    # Identifier la colonne de poids
    weight_column = next((col for col in data.columns if 'weight' in col.lower()), None)
    if weight_column is None:
        print("Colonne 'Weight' ou 'Weight (%)' introuvable dans le fichier.")
        return None

    ticker_column = next((col for col in data.columns if 'ticker' in col.lower()), None)
    if ticker_column is None:
        print("Colonne 'Ticker' introuvable dans le fichier.")
        return None

    # Calculer le poids pondéré pour chaque action
    data['Weighted_Weight'] = data[weight_column] * etf_weight

    # Agréger les poids par action
    total_weights = data.groupby(['Name', ticker_column])['Weighted_Weight'].sum().reset_index()
    total_weights.columns = ['Name', 'Ticker', 'Total_Weight']
    
    # Exclure les poids nuls et trier les données
    total_weights = total_weights[total_weights['Total_Weight'] > 0].sort_values(by='Total_Weight', ascending=False)
    
    return total_weights

# Exemple d'utilisation du programme combiné avec plusieurs URLs et poids d'ETF
urls = [
    "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&action=download&ticker=SPHQ",
    "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&action=download&ticker=SPLV",
    "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&action=download&ticker=SPMO",
    "https://www.ishares.com/us/products/239728/ishares-sp-500-value-etf/1467271812596.ajax?fileType=csv&fileName=IVE_holdings&dataType=fund",
    "https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spyd.xlsx"
]
weights = [0.1, 0.4, 0.1, 0.3, 0.1]   # poids assigné à chaque ETF
#SPHQ - SPLV - SPMO - IVE - SPYD


final_data = pd.DataFrame()

# Pour chaque URL, télécharger et traiter les données en appliquant le poids
for url, weight in zip(urls, weights):
    etf_data = calculate_total_weights(url, weight)
    if etf_data is not None:
        final_data = pd.concat([final_data, etf_data])

# Agréger les poids pour obtenir le poids total par action et trier le résultat
final_data = final_data.groupby(['Name', 'Ticker'])['Total_Weight'].sum().reset_index()
final_data.sort_values(by='Total_Weight', ascending=False, inplace=True)

# Affiche le résultat final sans index
print(final_data.to_string(index=False))

# Enregistrer le DataFrame final dans un fichier CSV
final_data.to_csv("poids_actions_etf.csv", index=False)
print("Les données complètes ont été enregistrées dans 'poids_actions_etf.csv'.")
