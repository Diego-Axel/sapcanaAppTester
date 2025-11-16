from dash import Dash, dcc, html, Input, Output, dash_table
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os
from dotenv import load_dotenv

# ⬅️ NOVO: Carrega as variáveis do arquivo .env (deve estar na raiz do projeto)
load_dotenv() 

# ⬅️ NOVO: Tenta ler a DATABASE_URL da variável de ambiente.
# Se não encontrar, assume um valor dummy para forçar um erro controlado
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERRO: Variável DATABASE_URL não encontrada no arquivo .env. Verifique o arquivo.")
    DATABASE_URL = "postgresql+psycopg2://dummy:dummy@localhost:5432/dummy" 

# Cria o engine de conexão (agora usando a variável do .env)
try:
    engine = create_engine(DATABASE_URL)
except Exception as e:
    print(f"ERRO: Não foi possível criar o engine do banco de dados: {e}")
    engine = None 


app = Dash(__name__)
app.title = "SapCana – Produção Quinzenal"


# Mapeamento de métricas para nomes amigáveis
METRICAS = {
    "cana_total_t": "Cana total (t) - Moída",
    "acucar_total_t": "Açúcar total (t) - Produção",
    "etanol_total_m3": "Etanol total (m³) - Produção",
    "estoque_acucar_total_t": "Estoque açúcar (t)",
    "estoque_etanol_total_m3": "Estoque etanol (m³)",
}
UNIDADE_DEFAULT = "N/A" # Usado como placeholder antes de carregar dados


def get_data():
    """
    Carrega todos os dados consolidados do PostgreSQL.
    """
    if engine is None:
        return pd.DataFrame()
        
    query = """
        SELECT
            sp.data_referencia,
            sp.safra,
            sp.periodo_codigo,
            u.apelido   AS unidade,
            frq.cana_propria_t,
            frq.cana_terceiros_t,
            frq.cana_total_t,
            frq.acucar_total_t,
            frq.etanol_total_m3,
            frq.estoque_acucar_total_t,
            frq.estoque_etanol_total_m3
        FROM fato_resumo_quinzena frq
        JOIN safra_periodo sp   ON sp.id = frq.safra_periodo_id
        JOIN unidade_produtora u ON u.id = frq.unidade_id
        ORDER BY sp.data_referencia;
    """
    try:
        df = pd.read_sql(query, engine)
        df["data_referencia"] = pd.to_datetime(df["data_referencia"])
        return df
    except Exception as e:
        print(f"ERRO ao carregar dados do banco de dados: {e}")
        return pd.DataFrame()


# Carrega os dados na inicialização do app
df_completo = get_data()

# Determina a última quinzena disponível para o Relatório Gerencial
if not df_completo.empty:
    unidades = sorted(df_completo["unidade"].unique())
    ultima_quinzena = df_completo["data_referencia"].max()
    quinzenas = sorted(df_completo["data_referencia"].unique(), reverse=True)
else:
    unidades = [UNIDADE_DEFAULT]
    ultima_quinzena = None
    quinzenas = []


# --- Layout: Abas para separar Evolução (Gráfico) e Relatório (Tabela/Boletim) ---

app.layout = html.Div(style={'font-family': 'Arial, sans-serif', 'padding': '20px', 'maxWidth': '1200px', 'margin': '0 auto'}, children=[
    html.H1("SapCana – Análise de Produção Quinzenal", style={'textAlign': 'center', 'color': '#005f73'}),
    
    html.Div(id="status-mensagem", style={'textAlign': 'center', 'marginTop': '20px', 'color': 'red', 'fontWeight': 'bold'}),

    dcc.Tabs(id="tabs-principal", value='tab-boletim', children=[
        dcc.Tab(label='1. Evolução Histórica (Gráfico)', value='tab-evolucao', style={'padding': '10px'}, children=[
            html.H2("Evolução Quinzenal por Unidade", style={'marginTop': '20px', 'borderBottom': '1px solid #ccc', 'paddingBottom': '10px'}),
            html.Div([
                # Dropdown Unidade
                html.Div([
                    html.Label("Unidade Produtora:", style={'fontWeight': 'bold', 'display': 'block', 'marginBottom': '5px'}),
                    dcc.Dropdown(
                        id="evolucao-unidade-dropdown",
                        options=[{"label": u, "value": u} for u in unidades],
                        value=unidades[0] if unidades and unidades[0] != UNIDADE_DEFAULT else None,
                        clearable=False,
                        style={'borderRadius': '8px', 'border': '1px solid #ccc'}
                    ),
                ], style={"width": "45%", "display": "inline-block", "marginRight": "10%"}),

                # Dropdown Métrica
                html.Div([
                    html.Label("Métrica de Análise:", style={'fontWeight': 'bold', 'display': 'block', 'marginBottom': '5px'}),
                    dcc.Dropdown(
                        id="evolucao-metrica-dropdown",
                        options=[{"label": nome, "value": col} for col, nome in METRICAS.items()],
                        value="cana_total_t",
                        clearable=False,
                        style={'borderRadius': '8px', 'border': '1px solid #ccc'}
                    ),
                ], style={"width": "45%", "display": "inline-block"}),
            ], style={'display': 'flex', 'justifyContent': 'center', 'marginBottom': '30px', 'marginTop': '20px'}),
            dcc.Graph(id="grafico-evolucao", style={'borderRadius': '10px', 'boxShadow': '0 4px 8px rgba(0,0,0,0.1)'}),
        ]),

        dcc.Tab(label='2. Boletim Quinzenal (Todas as Unidades)', value='tab-boletim', style={'padding': '10px'}, children=[
            html.H2("Boletim de Produção - Comparativo por Unidade", style={'marginTop': '20px', 'borderBottom': '1px solid #ccc', 'paddingBottom': '10px'}),
            
            # Dropdown para selecionar a Quinzena
            html.Div([
                html.Label("Selecione a Quinzena:", style={'fontWeight': 'bold', 'display': 'block', 'marginBottom': '5px'}),
                dcc.Dropdown(
                    id="boletim-quinzena-dropdown",
                    options=[
                        {'label': d.strftime('%d/%m/%Y'), 'value': str(d)} 
                        for d in quinzenas
                    ],
                    value=str(ultima_quinzena) if ultima_quinzena else None,
                    clearable=False,
                    style={'width': '300px', 'borderRadius': '8px', 'border': '1px solid #ccc'}
                ),
            ], style={'marginBottom': '20px', 'textAlign': 'center'}),
            
            # Tabela de Dados (Boletim)
            html.Div(id='boletim-output'),

            # Gráfico de Barras Comparativo
            dcc.Graph(id="grafico-comparativo", style={'marginTop': '40px', 'borderRadius': '10px', 'boxShadow': '0 4px 8px rgba(0,0,0,0.1)'})
        ]),
    ])
])

# --- Callback para a Aba 1: Evolução Histórica (Gráfico de Linha) ---

@app.callback(
    Output("grafico-evolucao", "figure"),
    [Input("evolucao-unidade-dropdown", "value"),
     Input("evolucao-metrica-dropdown", "value")],
    prevent_initial_call=True
)
def update_grafico_evolucao(unidade, metrica):
    """Filtra e gera o gráfico de evolução histórica para uma unidade."""
    if df_completo.empty:
        return {}

    if unidade is None or metrica is None:
        return {}

    dff = df_completo[df_completo["unidade"] == unidade].copy()
    dff = dff.sort_values("data_referencia")

    fig = px.line(
        dff,
        x="data_referencia",
        y=metrica,
        markers=True,
        title=f"{METRICAS[metrica]} – Evolução Quinzenal da Usina {unidade}",
        height=500,
    )
    
    fig.update_layout(
        xaxis_title="Data de Referência (Quinzena)",
        yaxis_title=METRICAS[metrica],
        plot_bgcolor='#ffffff',
        paper_bgcolor='#f8f9fa',
        margin=dict(l=40, r=40, t=80, b=40),
        title_font_size=16,
    )
    
    fig.update_traces(hovertemplate='%{y:,.2f} <br>Quinzena: %{x|%Y-%m-%d}<extra></extra>')
    
    return fig

# --- Callback para a Aba 2: Boletim Quinzenal (Tabela e Gráfico de Barras) ---

@app.callback(
    [Output("boletim-output", "children"),
     Output("grafico-comparativo", "figure")],
    [Input("boletim-quinzena-dropdown", "value")],
    prevent_initial_call=True
)
def update_boletim_quinzenal(data_quinzena_str):
    """
    Filtra os dados para a quinzena selecionada e gera a Tabela e o Gráfico Comparativo.
    """
    if df_completo.empty or not data_quinzena_str:
        # Retorna um texto de aviso se não houver dados
        return html.P("Selecione uma quinzena ou verifique se há dados."), {}
    
    # Converte a string de volta para datetime
    data_quinzena = pd.to_datetime(data_quinzena_str)
    
    # Filtra os dados para a quinzena selecionada
    df_boletim = df_completo[df_completo["data_referencia"] == data_quinzena].copy()
    
    if df_boletim.empty:
        return html.P(f"Nenhum dado encontrado para a quinzena de {data_quinzena.strftime('%d/%m/%Y')}."), {}

    # 1. Preparar os dados e calcular o TOTAL GERAL
    
    # Adicionar linha TOTAL GERAL (como no seu PDF)
    total_geral = df_boletim[[
        "cana_propria_t", "cana_terceiros_t", "cana_total_t", 
        "acucar_total_t", "etanol_total_m3"
    ]].sum()
    
    total_geral_row = {
        'unidade': 'TOTAL GERAL',
        'cana_propria_t': total_geral['cana_propria_t'],
        'cana_terceiros_t': total_geral['cana_terceiros_t'],
        'cana_total_t': total_geral['cana_total_t'],
        'acucar_total_t': total_geral['acucar_total_t'],
        'etanol_total_m3': total_geral['etanol_total_m3'],
    }
    
    # Cria um DataFrame com os dados das usinas e a linha de Total Geral
    df_tabela = pd.concat([df_boletim, pd.Series(total_geral_row).to_frame().T], ignore_index=True)
    
    # 2. Gerar a Tabela (Dash DataTable)
    tabela = dash_table.DataTable(
        id='datatable-boletim',
        columns=[
            {"name": "Un. Prod.", "id": "unidade", "type": "text"},
            {"name": "Própria (t)", "id": "cana_propria_t", "type": "numeric", "format": {"specifier": ",.0f"}},
            {"name": "Terceiros (t)", "id": "cana_terceiros_t", "type": "numeric", "format": {"specifier": ",.0f"}},
            {"name": "Cana Total (t)", "id": "cana_total_t", "type": "numeric", "format": {"specifier": ",.0f"}},
            {"name": "Açúcar (t)", "id": "acucar_total_t", "type": "numeric", "format": {"specifier": ",.0f"}},
            {"name": "Etanol (m³)", "id": "etanol_total_m3", "type": "numeric", "format": {"specifier": ",.0f"}},
        ],
        data=df_tabela[[
            "unidade", "cana_propria_t", "cana_terceiros_t", "cana_total_t", 
            "acucar_total_t", "etanol_total_m3"
        ]].to_dict('records'),
        style_header={
            'backgroundColor': '#005f73',
            'color': 'white',
            'fontWeight': 'bold',
            'textAlign': 'center'
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            },
            # Estilo para a linha TOTAL GERAL
            {
                'if': {'filter_query': '{unidade} = "TOTAL GERAL"'},
                'backgroundColor': '#e8f0fe',
                'fontWeight': 'bold',
                'fontSize': '110%'
            }
        ],
        style_cell={'textAlign': 'right', 'padding': '10px'},
        export_format='csv',
    )
    
    # 3. Gerar Gráfico de Barras Comparativo (Cana Moída)
    # Exclui a linha 'TOTAL GERAL' do gráfico para não distorcer a comparação
    df_plot = df_boletim[df_boletim['unidade'] != 'TOTAL GERAL']
    
    fig_bar = px.bar(
        df_plot,
        x="unidade", 
        y="cana_total_t", 
        title=f"Cana Moída Total (t) - Comparativo Quinzena {data_quinzena.strftime('%d/%m/%Y')}",
        color="unidade",
        text_auto=".2s", # Mostra o valor em cima da barra, formatado (ex: 400k)
        height=500
    )
    
    fig_bar.update_layout(
        xaxis_title="Unidade Produtora",
        yaxis_title="Cana Moída Total (t)",
        plot_bgcolor='#ffffff',
        paper_bgcolor='#f8f9fa',
    )
    
    fig_bar.update_traces(textposition='outside')
    
    return html.Div([tabela]), fig_bar

# --- Callback para Mensagem de Status (Vazio/Erro) ---

@app.callback(
    Output("status-mensagem", "children"),
    Input("tabs-principal", "value")
)
def update_status_message(tab):
    if df_completo.empty:
        return "⚠️ ERRO: Não foi possível carregar os dados do PostgreSQL. Verifique a conexão e se as tabelas contêm dados."
    return ""

if __name__ == "__main__":
    # Mudança de app.run_server para app.run conforme o erro resolvido anteriormente
    app.run(debug=True)