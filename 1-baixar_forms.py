import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# === Configurações ===
CREDENTIALS_FILE = "exemplo.json"   # Arquivo exemplo de credenciais
SHEET_ID = "SEU_SHEET_ID_AQUI"          # Coloque aqui o ID da URL do Google Sheets
OUTPUT_FILE = "dados_forms/respostas-brutas.csv"

# Escopo necessário
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Autenticação
creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)

# Abre a planilha pelo ID
spreadsheet = gc.open_by_key(SHEET_ID)

# Seleciona a primeira aba automaticamente
worksheet = spreadsheet.get_worksheet(0)

# Pega todos os valores (sem filtrar)
data = worksheet.get_all_values()

# Transforma em DataFrame
df = pd.DataFrame(data[1:], columns=data[0])  # primeira linha = cabeçalho

# Salva como CSV
df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

print(f"✅ Respostas baixadas ({len(df)} registros) e salvas em {OUTPUT_FILE}")
