import sys
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Date,
    Numeric, DateTime, func, UniqueConstraint, ForeignKey
)

DATABASE_URL = "postgresql+psycopg2://postgres:palmeiras123@localhost:5432/sapcana"

engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Tabela: safra_periodo
# Armazena as informações de tempo (quinzena)
safra_periodo = Table(
    "safra_periodo", metadata,
    Column("id", Integer, primary_key=True),
    Column("safra", String(10), nullable=False),
    Column("periodo_codigo", String(20), nullable=False),
    Column("periodo_desc", String(100), nullable=False),
    Column("data_referencia", Date, nullable=False),
    UniqueConstraint("safra", "periodo_codigo", name="uq_safra_periodo")
)

# Tabela: unidade_produtora
# Armazena as informações das usinas
unidade_produtora = Table(
    "unidade_produtora", metadata,
    Column("id", Integer, primary_key=True),
    Column("cod_mapa", Integer),
    Column("nome", String(100), nullable=False),
    Column("apelido", String(50), nullable=False, unique=True)
)

# Tabela: fato_resumo_quinzena
# Tabela principal (fato) que guarda os indicadores quinzenais
fato_resumo_quinzena = Table(
    "fato_resumo_quinzena", metadata,
    Column("id", Integer, primary_key=True),
    Column("safra_periodo_id", Integer, ForeignKey("safra_periodo.id"), nullable=False),
    Column("unidade_id", Integer, ForeignKey("unidade_produtora.id"), nullable=False),

    # Indicadores de Cana (t)
    Column("cana_propria_t", Numeric(15, 3), default=0),
    Column("cana_terceiros_t", Numeric(15, 3), default=0),
    Column("cana_total_t", Numeric(15, 3), default=0),
    
    # Produção (t/m³)
    Column("acucar_total_t", Numeric(15, 3), default=0),
    Column("etanol_total_m3", Numeric(15, 3), default=0),

    # Estoques (t/m³)
    Column("estoque_acucar_total_t", Numeric(15, 3), default=0),
    Column("estoque_etanol_total_m3", Numeric(15, 3), default=0),

    # Timestamps
    Column("created_at", DateTime, server_default=func.now()),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),
    
    # Chave única: uma linha por quinzena e por unidade
    UniqueConstraint("safra_periodo_id", "unidade_id", name="uq_resumo_safra_unidade")
)

def main():
    """Cria todas as tabelas definidas no metadata."""
    try:
        metadata.create_all(engine)
        print("Tabelas criadas com sucesso.")
    except Exception as e:
        print(f"Erro ao criar as tabelas: {e}")
        print("Verifique se a DATABASE_URL está correta e se o banco de dados 'sapcana' existe.")
        sys.exit(1)


if __name__ == "__main__":
    main()