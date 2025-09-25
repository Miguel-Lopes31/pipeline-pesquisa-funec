import pandas as pd
import unidecode  

# === Função para limpar nomes de colunas ===
def limpar_nome(col):
    col = unidecode.unidecode(col)  
    col = col.strip().lower()
    col = col.replace(" ", "_").replace("?", "").replace(".", "")
    col = col.replace("(", "").replace(")", "").replace("-", "_")
    col = col.replace("/", "_").replace(",", "_")
    return col

def processar_dados():
    try:
        # === Configurações ===
        input_file_original = "dados_forms/respostas-brutas.csv"
        output_principal = "dados_forms/respostas_principal.csv"
        output_q8 = "dados_forms/respostas_q8.csv"
        output_q10 = "dados_forms/respostas_q10.csv"
        output_q18 = "dados_forms/respostas_q18.csv"

        print("🚀 Iniciando processamento de dados...")

        # === 1. Carregar dados originais ===
        print("📥 Lendo arquivo CSV original...")
        df = pd.read_csv(input_file_original)

        # Excluir a coluna de data, se existir
        if "Carimbo de data/hora" in df.columns:
            df = df.drop(columns=["Carimbo de data/hora", "Pontuação"])
            print("🗑️ Coluna de data removida.")

        # Criar identificador sequencial
        df.insert(0, "id_aluno", range(1, len(df) + 1))
        print("🔑 Coluna id_aluno criada (sequencial).")

        # === 2. Separar múltiplas escolhas ===
        multi_cols = {
            "8. Como você estuda para o ENEM? ( No máximo 3)": output_q8,
            "10. Quais fatores mais influenciam sua decisão de fazer ou não o ENEM? (Máximo 3 alternativas)": output_q10,
            "18. Qual fator você considera mais importante para conseguir um bom emprego? (Máximo 3 alternativas)": output_q18
        }

        print("📊 Processando perguntas de múltipla escolha...")
        for col, output_file in multi_cols.items():
            if col in df.columns:
                # Padronizar respostas
                df[col] = (
                    df[col]
                    .fillna("")
                    .str.replace("\n", " ", regex=True)
                    .str.replace("\r", " ", regex=True)
                    .str.strip()
                )

                # Remover duplicações dentro da mesma célula
                df[col] = df[col].apply(
                    lambda x: ",".join(
                        sorted(set([resp.strip() for resp in x.split(",") if resp.strip()]))
                    )
                )

                # Criar dummies (0/1)
                dummies = df[col].str.get_dummies(sep=",").astype(int)

                # Extrair apenas o número da pergunta (antes do ponto)
                num_pergunta = col.split(".")[0].strip()

                # Padronizar nomes com prefixo q{numero_pergunta}
                dummies.columns = [
                    f"q{num_pergunta}_{limpar_nome(c.strip())}" for c in dummies.columns
                ]

                # Adicionar ID
                dummies.insert(0, "id_aluno", df["id_aluno"])

                # Salvar tabela separada
                dummies.to_csv(output_file, index=False, encoding="utf-8-sig")
                print(f"✅ Tabela salva: {output_file}")

        # === 3. Criar tabela principal ===
        df_principal = df.drop(columns=list(multi_cols.keys()))
        df_principal.columns = [limpar_nome(c) for c in df_principal.columns]

        # === 4. Renomear colunas para nomes amigáveis ===
        print("🏷️ Renomeando colunas principais...")
        mapa_colunas = {
            "1_qual_e_a_sua_serie_atual": "serie",
            "2_em_qual_turno_voce_estuda_na_funec_inconfidentes": "turno",
            "qual_a_sua_sala": "sala",
            "3_qual_e_a_sua_idade": "idade",
            "4_voce_pretende_fazer_o_enem": "pretende_enem",
            "5_se_pretende_fazer__quando": "quando_enem",
            "6_o_quanto_voce_se_sente_preparado_para_o_enem": "preparo_enem",
            "7_com_qual_frequencia_voce_estuda_ou_se_prepara_para_o_enem": "frequencia_estudo",
            "9_voce_avalia_que_a_funec_inconfidentes_contribui_com_a_sua_preparacao_para_o_enem": "avaliacao_funec",
            "11_voce_pretende_ingressar_no_ensino_superior_apos_o_ensino_medio": "pretende_faculdade",
            "12_qual_opcao_mais_representa_seu_plano_principal_apos_o_ensino_medio": "plano_principal",
            "13_qual_dos_desafios_abaixo_mais_atrapalha_ou_dificulta_o_seu_principal_plano_para_depois_do_ensino_medio": "desafio_principal",
            "14_voce_ja_decidiu_a_area_profissional_que_deseja_seguir": "decidiu_area",
            "15_qual_destas_areas_voce_pretende_seguir_no_futuro": "area_desejada",
            "16_voce_se_sente_preparadoa_para_entrar_no_mercado_de_trabalho": "preparado_trabalho",
            "17_voce_ja_participou_ou_participa_de_algum_curso_tecnico__estagio_ou_formacao_profissional": "experiencia_prof",
            "19__voce_conhece_bem_as_competencias_e_as_habilidades_necessarias_para_a_area_que_voce_deseja": "conhece_competencias",
            "20_o_que_o_trabalho_representa_para_voce": "significado_trabalho",
            "qual_o_seu_sexo": "sexo"
        }
        df_principal.rename(columns=mapa_colunas, inplace=True)

        # === 5. Tratar valores nulos ===
        print("🧹 Tratando valores nulos...")
        df_principal = df_principal.where(pd.notnull(df_principal), None)

        # === 6. Salvar tabela principal ===
        df_principal.to_csv(output_principal, index=False, encoding="utf-8-sig")
        print(f"💾 Tabela principal salva em: {output_principal}")

        print("🎉 Processo concluído com sucesso!")
        return df_principal

    except Exception as e:
        print(f"❌ Erro durante o processamento: {e}")

if __name__ == "__main__":
    processar_dados()
