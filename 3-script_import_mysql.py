import pandas as pd
import mysql.connector
from mysql.connector import Error

# === Configuração do MySQL ===
config = {
    "host": "localhost",     # Altere se necessário
    "user": "root",          # Seu usuário MySQL
    "password": "123456.",   # Sua senha MySQL
    "database": "pesquisa_funec"
}

try:
    # === 1. Conectar ao banco ===
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    print("✅ Conexão com o MySQL realizada com sucesso!")

    # === 2. Ler o CSV tratado ===
    df = pd.read_csv("dados_forms/respostas_tratadas_mysql.csv")
    print(f"📂 CSV carregado com {len(df)} registros.")

    # === 3. Preparar INSERT IGNORE ===
    cols = df.columns.tolist()
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join([f"`{c}`" for c in cols])

    sql = f"""
        INSERT IGNORE INTO respostas ({col_names})
        VALUES ({placeholders});
    """

    # === 4. Criar lista de valores ===
    values_list = [
        tuple(x if pd.notna(x) else None for x in row)
        for _, row in df.iterrows()
    ]

    # === 5. Inserir em lote ===
    cursor.executemany(sql, values_list)

    # Contar quantos foram inseridos e quantos ignorados
    novos_inseridos = cursor.rowcount
    ignorados = len(df) - novos_inseridos

    # === 6. Confirmar ===
    conn.commit()
    print("✅ Importação concluída com sucesso!")
    print(f"📥 Novos registros inseridos: {novos_inseridos}")
    print(f"⚠️ Registros ignorados (duplicados): {ignorados}")

    # === 7. Contar registros totais ===
    cursor.execute("SELECT COUNT(*) FROM respostas;")
    total_registros = cursor.fetchone()[0]
    print(f"📊 Total de registros na tabela agora: {total_registros}")

except Error as e:
    print(f"❌ Erro de conexão ou execução: {e}")

finally:
    if 'cursor' in locals() and cursor:
        cursor.close()
    if 'conn' in locals() and conn and conn.is_connected():
        conn.close()
        print("🔌 Conexão com MySQL encerrada.")
