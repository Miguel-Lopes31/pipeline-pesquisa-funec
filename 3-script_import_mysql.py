import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import unidecode

# ============================
# CONFIGURAÇÕES DO BANCO
# ============================
USER = "postgres"       # usuário do PostgreSQL
PASSWORD = "123456"     # senha do PostgreSQL
HOST = "localhost"
PORT = "5432"
DB = "pesquisa_funec"

engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}")

# ============================
# FUNÇÃO PARA LIMPAR NOMES DE COLUNAS
# ============================
def limpar_colunas(colunas):
    return [
        unidecode.unidecode(c).strip().lower().replace(" ", "_").replace("-", "_")
        for c in colunas
    ]

# ============================
# IMPORTAÇÃO DOS CSV
# ============================
PASTA = "dados_forms"

for arquivo in os.listdir(PASTA):
    if arquivo.endswith(".csv"):
        caminho = os.path.join(PASTA, arquivo)
        tabela = os.path.splitext(arquivo)[0]  # nome do arquivo sem .csv

        try:
            print(f"📂 Importando {arquivo} para a tabela '{tabela}'...")

            # Ler CSV (UTF-8 com BOM se existir)
            df = pd.read_csv(caminho, encoding="utf-8-sig")

            # Limpar nomes de colunas
            df.columns = limpar_colunas(df.columns)

            print(f"  📝 {len(df)} registros carregados.")

            # Escolha: "replace" para recriar a tabela / "append" para acumular
            MODO = "replace"   # 🔄 troque para "append" se quiser acumular dados

            # Enviar para o banco
            df.to_sql(
                tabela,
                engine,
                if_exists=MODO,
                index=False,
                method="multi",
                chunksize=1000
            )

            print(f"✅ Tabela '{tabela}' importada com sucesso! (modo: {MODO})\n")

        except UnicodeDecodeError as e:
            print(f"❌ Erro de codificação em {arquivo}: {e}")
        except SQLAlchemyError as e:
            print(f"❌ Erro SQL na tabela {tabela}: {e}")
        except Exception as e:
            print(f"❌ Erro inesperado com {arquivo}: {e}")

print("🎉 Todas as importações foram concluídas!")
