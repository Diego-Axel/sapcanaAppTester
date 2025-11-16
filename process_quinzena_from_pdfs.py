import os
import re
import calendar
import datetime as dt

import pandas as pd
from sqlalchemy import create_engine, text
import PyPDF2


# ⚠️ AJUSTE A SUA DATABASE_URL AQUI
# Exemplo local: "postgresql+psycopg2://postgres:sua_senha@localhost:5432/sapcana"
DATABASE_URL = "postgresql+psycopg2://postgres:palmeiras123@localhost:5432/sapcana"
engine = create_engine(DATABASE_URL)


# ---------- Funções de parsing dos PDFs ----------

def extract_full_text(pdf_path):
    """Extrai todo o texto de um PDF e normaliza espaços."""
    reader = PyPDF2.PdfReader(open(pdf_path, "rb"))
    full = ""
    for page in reader.pages:
        full += page.extract_text() + "\n"
    # normalizar espaços (substituir múltiplos espaços/tabs por um único espaço)
    full = re.sub(r"[ \t]+", " ", full)
    return full


def parse_header(full_text):
    """Extrai Safra, Período e Produtor do cabeçalho do PDF."""
    # Safra: padrão "2025/2026Safra:"
    safra_match = re.search(r"([0-9]{4}/[0-9]{4})\s*Safra:", full_text)
    safra = safra_match.group(1) if safra_match else None

    # Período de lançamento: "Periodo de Lançamento: 2025/10-Quinz.02"
    per_match = re.search(r"Periodo de Lançamento:\s*([0-9]{4}/[0-9]{2}-Quinz\.0[12])", full_text)
    periodo_raw = per_match.group(1) if per_match else None

    # Produtor: "Produtor: 13737 - JAPUNGU AGROINDUSTRIAL LTDA"
    # Captura o código e o nome antes do texto "Matéria prima"
    prod_match = re.search(r"Produtor:\s*([0-9]+)\s*-\s*(.+?)\s*Matéria prima", full_text)
    produtor_cod = prod_match.group(1) if prod_match else None
    produtor_nome = prod_match.group(2).strip() if prod_match else None

    return safra, periodo_raw, produtor_cod, produtor_nome


def decode_periodo(periodo_raw):
    """
    Decodifica o período raw (ex: "2025/10-Quinz.02") para:
    periodo_codigo (2025/10-Q2), periodo_desc, e data_referencia (fim da quinzena).
    """
    if not periodo_raw:
        return None, None, None

    m = re.match(r"([0-9]{4})/([0-9]{2})-Quinz\.0([12])", periodo_raw)
    if not m:
        return None, None, None

    year = int(m.group(1))
    month = int(m.group(2))
    q = int(m.group(3))

    # Calcula o dia de referência (15 para Q1, último dia do mês para Q2)
    if q == 1:
        day = 15
    else:
        day = calendar.monthrange(year, month)[1]

    data_ref = dt.date(year, month, day)
    periodo_codigo = f"{year}/{month:02d}-Q{q}"
    periodo_desc = f"{q}ª quinzena de {month:02d}/{year}"

    return periodo_codigo, periodo_desc, data_ref


def parse_metrics(full_text):
    """
    Percorre os blocos de produtos no texto do PDF e extrai as métricas.
    """
    result = {
        "cana_propria_t": 0.0,
        "cana_terceiros_t": 0.0,
        "etanol_anidro_prod": 0.0,
        "etanol_anidro_estoque": 0.0,
        "etanol_hidratado_prod": 0.0,
        "etanol_hidratado_estoque": 0.0,
        "acucar_total_t": 0.0, # Soma da produção de todos os açúcares
        "estoque_acucar_total_t": 0.0, # Soma do estoque de todos os açúcares
    }

    chunks = full_text.split("Matéria prima / Produto / Subproduto")
    # O primeiro pedaço é apenas o cabeçalho, ignoramos
    for chunk in chunks[1:]:
        parts = chunk.split("Tipo Lançamento Valor")
        prod = parts[0].strip().replace("\n", " ")
        body = parts[1] if len(parts) > 1 else ""
        lines = [ln.strip() for ln in body.split("\n") if ln.strip()]

        def clean_value(match):
            """Remove pontos de milhar e substitui vírgula decimal por ponto."""
            return float(match.group(1).replace(".", "").replace(",", "."))

        # --- Cana moída ---
        if "Cana moída - Própria" in prod:
            for ln in lines:
                m = re.search(r"Entrada t ?([0-9\.\,]+) Produção", ln)
                if m:
                    result["cana_propria_t"] += clean_value(m)

        elif "Cana moída - Terceiros" in prod:
            for ln in lines:
                m = re.search(r"Entrada t ?([0-9\.\,]+) Produção", ln)
                if m:
                    result["cana_terceiros_t"] += clean_value(m)

        # --- Etanol ---
        elif "Etanol - Anidro" in prod:
            for ln in lines:
                # Produção
                if "Entrada m³" in ln and "Produção" in ln:
                    m = re.search(r"Entrada m³ ?([0-9\.\,]+) Produção", ln)
                    if m:
                        result["etanol_anidro_prod"] += clean_value(m)
                # Estoque
                if "Estoque físico do período atual m³" in ln:
                    m = re.search(r"Estoque físico do período atual m³ ?([0-9\.\,]+)", ln)
                    if m:
                        result["etanol_anidro_estoque"] = clean_value(m)

        elif "Etanol - Hidratado" in prod:
            for ln in lines:
                # Produção
                if "Entrada m³" in ln and "Produção" in ln:
                    m = re.search(r"Entrada m³ ?([0-9\.\,]+) Produção", ln)
                    if m:
                        result["etanol_hidratado_prod"] += clean_value(m)
                # Estoque
                if "Estoque físico do período atual m³" in ln:
                    m = re.search(r"Estoque físico do período atual m³ ?([0-9\.\,]+)", ln)
                    if m:
                        result["etanol_hidratado_estoque"] = clean_value(m)

        # --- Açúcares (Cristal, VHP, etc.) ---
        # Usa 'Açúcar -' pois pode ser Cristal, VHP, etc.
        elif "Açúcar -" in prod:
            for ln in lines:
                # Produção de Açúcar (Soma a produção de todos os tipos)
                if "Entrada t" in ln and "Produção" in ln:
                    m = re.search(r"Entrada t ?([0-9\.\,]+) Produção", ln)
                    if m:
                        result["acucar_total_t"] += clean_value(m)
                # Estoque de Açúcar (Soma o estoque de todos os tipos)
                if "Estoque físico do período atual t" in ln:
                    m = re.search(r"Estoque físico do período atual t ?([0-9\.\,]+)", ln)
                    if m:
                        result["estoque_acucar_total_t"] += clean_value(m)

    return result


def parse_pdf(pdf_path):
    """Função principal para extrair todos os dados de um PDF e formatar a linha."""
    full = extract_full_text(pdf_path)
    safra, periodo_raw, produtor_cod, produtor_nome = parse_header(full)
    metrics = parse_metrics(full)

    periodo_codigo, periodo_desc, data_ref = decode_periodo(periodo_raw)

    # Cria o apelido para a usina (usando a última palavra do nome, como sugerido)
    apelido = produtor_nome.split()[-1] if produtor_nome else None

    # Monta a linha de dados
    row = {
        "safra": safra,
        "periodo_codigo": periodo_codigo,
        "periodo_desc": periodo_desc,
        "data_referencia": data_ref,
        "unidade_apelido": apelido,
        "produtor_cod": produtor_cod,
        "produtor_nome": produtor_nome,
    }
    row.update(metrics)

    # Deriva os totais (conforme formato CSV/DB)
    row["cana_total_t"] = row["cana_propria_t"] + row["cana_terceiros_t"]
    row["etanol_total_m3"] = (
        row["etanol_anidro_prod"] + row["etanol_hidratado_prod"]
    )
    row["estoque_etanol_total_m3"] = (
        row["etanol_anidro_estoque"] + row["etanol_hidratado_estoque"]
    )

    return row


# ---------- Funções para gravar no PostgreSQL (Lógica UPSERT) ----------

def get_or_create_safra_periodo(row, conn):
    """Insere ou atualiza (UPSERT) a safra/período e retorna o ID."""
    sql = text("""
        INSERT INTO safra_periodo (safra, periodo_codigo, periodo_desc, data_referencia)
        VALUES (:safra, :periodo_codigo, :periodo_desc, :data_referencia)
        ON CONFLICT (safra, periodo_codigo) DO UPDATE
        SET periodo_desc = EXCLUDED.periodo_desc,
            data_referencia = EXCLUDED.data_referencia
        RETURNING id;
    """)
    result = conn.execute(sql, {
        "safra": row["safra"],
        "periodo_codigo": row["periodo_codigo"],
        "periodo_desc": row["periodo_desc"],
        "data_referencia": row["data_referencia"],
    })
    return result.scalar()


def get_or_create_unidade(row, conn):
    """Insere ou atualiza (UPSERT) a unidade produtora e retorna o ID."""
    sql = text("""
        INSERT INTO unidade_produtora (cod_mapa, nome, apelido)
        VALUES (:cod_mapa, :nome, :apelido)
        ON CONFLICT (apelido) DO UPDATE
        SET cod_mapa = EXCLUDED.cod_mapa,
            nome = EXCLUDED.nome
        RETURNING id;
    """)
    # Converte o código do produtor para inteiro, se existir
    cod_mapa = int(row["produtor_cod"]) if row["produtor_cod"] else None

    result = conn.execute(sql, {
        "cod_mapa": cod_mapa,
        "nome": row["produtor_nome"] if row["produtor_nome"] else row["unidade_apelido"],
        "apelido": row["unidade_apelido"],
    })
    return result.scalar()


def upsert_resumo(row, safra_periodo_id, unidade_id, conn):
    """Insere ou atualiza (UPSERT) a linha na tabela fato_resumo_quinzena."""
    sql = text("""
        INSERT INTO fato_resumo_quinzena (
            safra_periodo_id, unidade_id,
            cana_propria_t, cana_terceiros_t, cana_total_t,
            acucar_total_t, etanol_total_m3,
            estoque_acucar_total_t, estoque_etanol_total_m3
        ) VALUES (
            :safra_periodo_id, :unidade_id,
            :cana_propria_t, :cana_terceiros_t, :cana_total_t,
            :acucar_total_t, :etanol_total_m3,
            :estoque_acucar_total_t, :estoque_etanol_total_m3
        )
        ON CONFLICT (safra_periodo_id, unidade_id) DO UPDATE SET
            cana_propria_t = EXCLUDED.cana_propria_t,
            cana_terceiros_t = EXCLUDED.cana_terceiros_t,
            cana_total_t = EXCLUDED.cana_total_t,
            acucar_total_t = EXCLUDED.acucar_total_t,
            etanol_total_m3 = EXCLUDED.etanol_total_m3,
            estoque_acucar_total_t = EXCLUDED.estoque_acucar_total_t,
            estoque_etanol_total_m3 = EXCLUDED.estoque_etanol_total_m3,
            updated_at = now();
    """)
    conn.execute(sql, {
        "safra_periodo_id": safra_periodo_id,
        "unidade_id": unidade_id,
        "cana_propria_t": row["cana_propria_t"],
        "cana_terceiros_t": row["cana_terceiros_t"],
        "cana_total_t": row["cana_total_t"],
        "acucar_total_t": row["acucar_total_t"],
        "etanol_total_m3": row["etanol_total_m3"],
        "estoque_acucar_total_t": row["estoque_acucar_total_t"],
        "estoque_etanol_total_m3": row["estoque_etanol_total_m3"],
    })


# ---------- Pipeline principal: pasta PDFs -> CSV -> DB ----------

def process_folder(pdf_folder, csv_output_path):
    """
    Processa todos os PDFs de uma pasta, gera o CSV e carrega no banco de dados.
    """
    rows = []

    # 1. Extração e Transformação (PDFs -> Rows)
    for fname in os.listdir(pdf_folder):
        if not fname.lower().endswith(".pdf"):
            continue
        pdf_path = os.path.join(pdf_folder, fname)
        print(f"Processando {pdf_path} ...")
        
        try:
            row = parse_pdf(pdf_path)
            rows.append(row)
        except Exception as e:
            print(f"ERRO ao processar o PDF {fname}: {e}. Pulando...")
            continue

    if not rows:
        print("Nenhum dado válido extraído. Verifique a pasta e os PDFs.")
        return

    df = pd.DataFrame(rows)

    # 2. Geração do CSV consolidado
    df_to_save = df[[
        "safra", "periodo_codigo", "periodo_desc", "data_referencia", "unidade_apelido",
        "cana_propria_t", "cana_terceiros_t", "cana_total_t",
        "acucar_total_t", "etanol_total_m3",
        "estoque_acucar_total_t", "estoque_etanol_total_m3"
    ]]
    df_to_save.to_csv(csv_output_path, index=False, encoding="utf-8", decimal=',')
    print(f"CSV consolidado salvo em {csv_output_path}")

    # 3. Carga no PostgreSQL (Load)
    with engine.begin() as conn:
        for index, r in df.iterrows():
            # Pular se houver dados essenciais faltando (embora o parse_pdf tente ser robusto)
            if not r["safra"] or not r["unidade_apelido"]:
                print(f"Aviso: Linha {index} sem Safra ou Apelido da Unidade. Pulando.")
                continue

            safra_periodo_id = get_or_create_safra_periodo(r, conn)
            unidade_id = get_or_create_unidade(r, conn)
            upsert_resumo(r, safra_periodo_id, unidade_id, conn)

    print("Carga no PostgreSQL concluída com sucesso.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Uso: python process_quinzena_from_pdfs.py <pasta_pdfs> <arquivo_csv_saida>")
        sys.exit(1)

    folder = sys.argv[1]
    csv_out = sys.argv[2]
    process_folder(folder, csv_out)