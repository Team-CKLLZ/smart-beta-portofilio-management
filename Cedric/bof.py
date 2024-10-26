import pandas as pd
import requests
from io import BytesIO, StringIO

def download_data(url):
    """
    Télécharge et retourne les données d'une URL, en gérant les fichiers Excel et CSV de manière appropriée.
    """
    if url.endswith('.xls') or url.endswith('.xlsx'):
        skip_rows = 4
        response = requests.get(url)
        response.raise_for_status()
        data = pd.read_excel(BytesIO(response.content), skiprows=skip_rows)
    else:
        skip_rows = 0
        response = requests.get(url)
        response.raise_for_status()
        csv_data = response.content.decode('utf-8')
        try:
            data = pd.read_csv(StringIO(csv_data), skiprows=skip_rows)
        except pd.errors.ParserError:
            try:
                data = pd.read_csv(StringIO(csv_data), delimiter=';', skiprows=skip_rows)
            except pd.errors.ParserError:
                data = pd.read_csv(StringIO(csv_data), skiprows=9 + skip_rows)

    if not any('weight' in col.lower() for col in data.columns):
        data = pd.read_csv(StringIO(csv_data), skiprows=9 + skip_rows)
    return data

def normalize_ticker(ticker):
    """
    Normalise le ticker en supprimant les espaces et les caractères inutiles.
    """
    if pd.isna(ticker):  # Vérifier si ticker est NaN
        return ''
    return ticker.replace('/', '').strip().upper()

def calculate_total_weights(url, etf_weight):
    """
    Calcule le poids total pour chaque action d'un fichier CSV ou Excel.
    """
    data = download_data(url)
    weight_column = next((col for col in data.columns if 'weight' in col.lower()), None)
    if weight_column is None:
        print("Colonne 'Weight' ou 'Weight (%)' introuvable dans le fichier.")
        return None

    holding_ticker_column = next((col for col in data.columns if 'holding ticker' in col.lower()), None)
    ticker_column = next((col for col in data.columns if 'ticker' in col.lower()), None)
    if holding_ticker_column:
        ticker_column = holding_ticker_column
    elif not ticker_column:
        print("Aucune colonne 'Ticker' ou 'Holding Ticker' introuvable dans le fichier.")
        return None

    data['Weighted_Weight'] = data[weight_column] * etf_weight

    if 'Name' in data.columns:
        data = data[~data['Name'].isin(["US DOLLAR", "Cash/Receivables/Payables"])]

    # Normaliser les tickers
    data[ticker_column] = data[ticker_column].apply(normalize_ticker)
    
    total_weights = data.groupby(ticker_column)['Weighted_Weight'].sum().reset_index()
    total_weights.columns = ['Ticker', 'Total_Weight']
    total_weights = total_weights[total_weights['Total_Weight'] > 0]
    return total_weights

# Téléchargement et traitement des données pour chaque URL
urls = [
    "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&action=download&ticker=SPHQ",
    "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&action=download&ticker=SPLV",
    "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&action=download&ticker=SPMO",
    "https://www.ishares.com/us/products/239728/ishares-sp-500-value-etf/1467271812596.ajax?fileType=csv&fileName=IVE_holdings&dataType=fund",
    "https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spyd.xlsx"
]
weights = [0.1, 0.4, 0.1, 0.3, 0.1]

final_data = pd.DataFrame()

# Appliquer chaque URL et poids d'ETF
for url, weight in zip(urls, weights):
    etf_data = calculate_total_weights(url, weight)
    if etf_data is not None:
        final_data = pd.concat([final_data, etf_data])

# Étape finale : Regrouper par ticker exact et sommer les poids
final_data = final_data.groupby('Ticker', as_index=False)['Total_Weight'].sum()

# Trier les données en ordre décroissant de poids
final_data.sort_values(by='Total_Weight', ascending=False, inplace=True)

# Vérification des doublons
duplicate_tickers = final_data[final_data.duplicated(subset='Ticker', keep=False)]
if not duplicate_tickers.empty:
    print("Doublons trouvés dans les tickers :")
    print(duplicate_tickers)
else:
    print("Aucun doublon trouvé dans les tickers.")

# Affichage du résultat final
print(final_data.to_string(index=False))

# Enregistrer dans un fichier CSV
final_data.to_csv("poids_actions_etf.csv", index=False)
print("Les données complètes ont été enregistrées dans 'poids_actions_etf.csv'.")
