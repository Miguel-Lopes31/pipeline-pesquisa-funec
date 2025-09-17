import pandas as pd
import unidecode
import uuid   # <--- adicionado para gerar UUIDs

def processar_dados():
    # === CONFIGURAÇÕES INICIAIS ===
    input_file_original = "dados_forms/respostas-brutas.csv"
    output_file_final = "dados_forms/respostas_tratadas_mysql.csv"
    
    # === 1. PRIMEIRA ETAPA DE TRATAMENTO ===
    print("🔧 Iniciando primeira etapa de tratamento...")
    
    # Carregar dados originais
    df = pd.read_csv(input_file_original)
    
    # Excluir a coluna de data
    if "Carimbo de data/hora" in df.columns:
        df = df.drop(columns=["Carimbo de data/hora"])
    
    # Colunas com múltiplas respostas
    multi_cols = [
        "8. Como você estuda para o ENEM? ( No máximo 3)", 
        "10. Quais fatores mais influenciam sua decisão de fazer ou não o ENEM? (Máximo 3 alternativas)", 
        "18. Qual fator você considera mais importante para conseguir um bom emprego? (Máximo 3 alternativas)"
    ]
    
    # Criar variáveis dummies
    for col in multi_cols:
        if col in df.columns:
            dummies = df[col].str.get_dummies(sep=",").astype(int)
            dummies.columns = [
                f"{col.split('.')[0]}_" + c.strip().replace("\n", " ").replace("  ", " ")
                for c in dummies.columns
            ]
            dummies = dummies.loc[:, ~dummies.columns.duplicated()]
            df = pd.concat([df, dummies], axis=1)
    
    # === 2. LIMPEZA DE NOMES DE COLUNAS ===
    print("🧹 Limpando nomes de colunas...")
    
    def limpar_nome(col):
        col = unidecode.unidecode(col)  # remove acentos
        col = col.strip().lower()
        col = col.replace(" ", "_")
        col = col.replace("?", "")
        col = col.replace(".", "")
        col = col.replace("(", "").replace(")", "")
        col = col.replace("-", "_")
        col = col.replace("/", "_")
        col = col.replace(",", "_")
        return col
    
    df.columns = [limpar_nome(c) for c in df.columns]
    
    # === 3. CRIAR IDENTIFICADOR ÚNICO COM UUID ===
    print("🔑 Criando identificadores únicos com UUID...")
    df["id_hash"] = [str(uuid.uuid4()) for _ in range(len(df))]
    
    # === 4. RENOMEAR COLUNAS PARA NOMES DO BANCO DE DADOS ===
    print("🏷️ Renomeando colunas para MySQL...")
    
    mapa_colunas = {
        "1_qual_e_a_sua_serie_atual": "serie",
        "2_em_qual_turno_voce_estuda_na_funec_inconfidentes": "turno",
        "qual_a_sua_sala": "sala",
        "3_qual_e_a_sua_idade": "idade",
        "4_voce_pretende_fazer_o_enem": "pretende_enem",
        "5_se_pretende_fazer__quando": "quando_enem",
        "6_o_quanto_voce_se_sente_preparado_para_o_enem": "preparo_enem",
        "7_com_qual_frequencia_voce_estuda_ou_se_prepara_para_o_enem": "frequencia_estudo",
        "8_como_voce_estuda_para_o_enem__no_maximo_3": "como_estuda",
        "9_voce_avalia_que_a_funec_inconfidentes_contribui_com_a_sua_preparacao_para_o_enem": "avaliacao_funec",
        "10_quais_fatores_mais_influenciam_sua_decisao_de_fazer_ou_nao_o_enem_maximo_3_alternativas": "fatores_decisao",
        "11_voce_pretende_ingressar_no_ensino_superior_apos_o_ensino_medio": "pretende_faculdade",
        "12_qual_opcao_mais_representa_seu_plano_principal_apos_o_ensino_medio": "plano_principal",
        "13_qual_dos_desafios_abaixo_mais_atrapalha_ou_dificulta_o_seu_principal_plano_para_depois_do_ensino_medio": "desafio_principal",
        "14_voce_ja_decidiu_a_area_profissional_que_deseja_seguir": "decidiu_area",
        "15_qual_destas_areas_voce_pretende_seguir_no_futuro": "area_desejada",
        "16_voce_se_sente_preparadoa_para_entrar_no_mercado_de_trabalho": "preparado_trabalho",
        "17_voce_ja_participou_ou_participa_de_algum_curso_tecnico__estagio_ou_formacao_profissional": "experiencia_prof",
        "18_qual_fator_voce_considera_mais_importante_para_conseguir_um_bom_emprego_maximo_3_alternativas": "fator_emprego",
        "19__voce_conhece_bem_as_competencias_e_as_habilidades_necessarias_para_a_area_que_voce_deseja": "conhece_competencias",
        "20_o_que_o_trabalho_representa_para_voce": "significado_trabalho",
        "qual_o_seu_sexo": "sexo",
        # múltipla escolha
        "8_cursinhos_presenciais": "cursinhos_presenciais",
        "8_materiais_e_livros_por_conta_propria": "materiais_proprios",
        "8_nao_estudo_para_o_enem": "nao_estuda_enem",
        "8_nao_gosto_de_estudar": "nao_gosta_estudar",
        "8_plataformas_online": "plataformas_online",
        "8_apenas_com_o_conteudo_das_aulas_na_escola": "conteudo_aulas_escola",
        "10_falta_de_confianca_na_propria_preparacao": "falta_confianca",
        "10_falta_de_recursos_financeiros": "falta_recursos",
        "10_necessidade_de_trabalhar": "necessidade_trabalho",
        "10_oportunidade_de_ter_uma_vida_melhor": "oportunidade_melhor_vida",
        "10_pressao_familiar": "pressao_familiar",
        "10_desejo_de_cursar_ensino_superior": "desejo_ensino_superior",
        "18_ter_as_competencias_e_habilidades_exigidas_para_o_desempenho_da_funcao": "competencias_exigidas",
        "18_ter_boas_conexoes_indicacoes": "boas_conexoes",
        "18_ter_cursos_tecnicos_profissionalizantes": "cursos_tecnicos",
        "18_ter_experiencia_profissional": "experiencia_profissional",
        "18_ter_habilidades_interpessoais": "habilidades_interpessoais",
        "18_ter_diploma_de_ensino_superior": "diploma_superior"
    }
    
    df.rename(columns=mapa_colunas, inplace=True)
    
    # === 5. TRATAR VALORES NULOS ===
    print("📊 Tratando valores nulos...")
    df = df.where(pd.notnull(df), None)
    
    # === 6. SALVAR ARQUIVO FINAL ===
    print("💾 Salvando arquivo final...")
    df.to_csv(output_file_final, index=False, encoding="utf-8-sig")
    
    print(f"✅ Processo concluído! Arquivo final salvo em: {output_file_final}")
    return df

if __name__ == "__main__":
    processar_dados()
