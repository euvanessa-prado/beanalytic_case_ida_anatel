"""Streamlit dashboard para o Data Mart IDA (Anatel).

Exibe:
- Benchmark competitivo (diferença vs média de mercado)
- Série histórica do indicador IDA por serviço e operadora
- KPIs resumindo período, variação média, operadoras e serviços
"""
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
import re
from collections import defaultdict
from config import DB_CONFIG
from pathlib import Path
import numpy as np
from PIL import Image
def _pick_logo():
    bases = [Path("assets")]
    exts = {".png", ".jpg", ".jpeg", ".svg", ".webp"}
    for base in bases:
        if base.exists():
            files = [f for f in base.glob("*.*") if f.suffix.lower() in exts]
            if files:
                prefer = [f for f in files if "anatel" in f.name.lower()]
                if prefer:
                    return prefer[0]
                try:
                    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
                except Exception:
                    pass
                return files[0]
    return None

def _remove_white_bg(src: Path, dst: Path, threshold: int = 240):
    try:
        img = Image.open(src).convert("RGBA")
        arr = np.array(img)
        r, g, b, a = arr.T
        mask = (r > threshold) & (g > threshold) & (b > threshold)
        arr[..., 3][mask.T] = 0
        Image.fromarray(arr).save(dst)
        return dst
    except Exception:
        return src

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Painel de Qualidade - Anatel",
    page_icon="🇧🇷",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS PERSONALIZADO (Design System Profissional/Gov) ---
st.markdown("""
    <style>
    :root {
        --primary-color: #90caf9;
        --secondary-color: #f8c300;
        --bg-dark: #121212;
        --panel-dark: #1c1f26;
        --panel-darker: #16181d;
        --border-dark: #2a2e35;
        --text-light: #e0e0e0;
        --text-muted: #b0b0b0;
    }
    .stApp { background-color: var(--bg-dark); color: var(--text-light); }
    section[data-testid="stSidebar"] { background-color: var(--panel-darker); border-right: 1px solid var(--border-dark); }
    section[data-testid="stSidebar"] img { background-color: transparent; border-radius: 10px; padding: 0; box-shadow: none; }
    .header-container {
        background-color: var(--panel-dark);
        padding: 1.5rem 2rem;
        border-radius: 12px;
        box-shadow: none;
        margin-bottom: 2rem;
        border-left: 6px solid var(--primary-color);
        display: flex; align-items: center; justify-content: space-between;
    }
    .header-title { color: var(--primary-color); font-family: 'Segoe UI', sans-serif; font-size: 2.2rem; font-weight: 700; margin: 0; }
    .header-subtitle { color: var(--text-muted); font-size: 1.1rem; margin-top: 0.5rem; }
    div[data-testid="stMetric"] { background-color: var(--panel-dark); padding: 1.2rem; border-radius: 10px; border: 1px solid var(--border-dark); }
    [data-testid="stMetricLabel"] { color: var(--text-muted); font-size: 0.9rem; font-weight: 600; }
    [data-testid="stMetricValue"] { color: var(--primary-color); font-size: 2rem !important; font-weight: 700; }
    .stTabs [data-baseweb="tab-list"] { gap: 1rem; background-color: transparent; }
    .stTabs [data-baseweb="tab"] { height: 50px; background-color: var(--panel-dark); border-radius: 8px 8px 0 0; padding: 0 24px; font-weight: 600; border: 1px solid var(--border-dark); border-bottom: none; color: var(--text-light); }
    .stTabs [aria-selected="true"] { background-color: var(--panel-dark); border-top: 4px solid var(--primary-color); color: var(--primary-color) !important; }
    div[data-testid="stExpander"] { background-color: var(--panel-dark); border: 1px solid var(--border-dark); color: var(--text-light); }
    div[data-testid="stDataFrame"] { background-color: var(--panel-dark); border: 1px solid var(--border-dark); color: var(--text-light); }
    .footer { position: fixed; left: 0; bottom: 0; width: 100%; background-color: var(--panel-darker); color: var(--text-muted); text-align: center; padding: 12px; font-size: 0.85rem; border-top: 1px solid var(--border-dark); z-index: 999; }
    .block-container { padding-bottom: 5rem; padding-top: 2rem; }
    .stAlert { background-color: #1f2a37 !important; color: var(--text-light) !important; border: 1px solid #0ea5e9 !important; }
    </style>
""", unsafe_allow_html=True)

# --- FUNÇÕES DE CARGA DE DADOS ---
@st.cache_data(ttl=300)
def load_data():
    try:
        conn = psycopg2.connect(**DB_CONFIG, options='-c search_path=ida,public')
        
        df = None
        for v in ["view_taxa_resolucao_5_dias", "pivot_variacao"]:
            try:
                df = pd.read_sql(f"SELECT * FROM {v} ORDER BY 1", conn)
                if not df.empty or df is not None:
                    break
            except Exception as e:
                if "does not exist" in str(e) or "não existe" in str(e):
                    continue
                raise
        if df is None:
            conn.close()
            return None, None
        
        # Tratamento de Dados da View
        cols_numericas = df.columns.drop(['Mes', 'Taxa de Variação Média'])
        
        # Inversão de Sinal: (Individual - Mercado)
        # Positivo = Melhor que a média | Negativo = Pior que a média
        df[cols_numericas] = df[cols_numericas] * -1
        
        # Filtro de Outliers (Erros matemáticos de divisão por zero)
        mask = (df[cols_numericas] > 200) | (df[cols_numericas] < -200)
        df[cols_numericas] = df[cols_numericas].mask(mask)
        
        def _clean_group(n: str) -> str:
            x = re.sub(r"\s*\([^)]*\)", "", n or "")
            x = re.sub(r"\*+", "", x)
            x = re.sub(r"\s+", " ", x).strip().upper()
            if x.startswith("SERCOMTEL"):
                x = "SERCOMTEL"
            return x
        new_map = defaultdict(list)
        for c in cols_numericas:
            new_map[_clean_group(c)].append(c)
        df_clean = pd.DataFrame()
        df_clean["Mes"] = df["Mes"]
        df_clean["Taxa de Variação Média"] = df["Taxa de Variação Média"]
        for k, lst in new_map.items():
            df_clean[k] = df[lst].mean(axis=1)
        df = df_clean
        
        df_trend = None
        # Tenta com modelo dbt (campos: f.nome_grupo, f.id_tempo, f.codigo_servico)
        try:
            query_trend_dbt = """
                SELECT 
                    dt.ano_mes,
                    f.nome_grupo,
                    ds.nome_servico,
                    f.taxa_solicitacoes_resolvidas_5dias AS ida
                FROM fato_ida f
                JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
                JOIN dim_servico ds ON f.codigo_servico = ds.codigo_servico
                ORDER BY dt.ano_mes
            """
            df_trend = pd.read_sql(query_trend_dbt, conn)
        except Exception:
            # Fallback para modelo original (campos: f.id_grupo, f.id_servico)
            query_trend_orig = """
                SELECT 
                    dt.ano_mes,
                    dg.nome_grupo,
                    ds.nome_servico,
                    f.taxa_solicitacoes_resolvidas_5dias AS ida
                FROM fato_ida f
                JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
                JOIN dim_grupo_economico dg ON f.id_grupo = dg.id_grupo
                JOIN dim_servico ds ON f.id_servico = ds.id_servico
                ORDER BY dt.ano_mes
            """
            df_trend = pd.read_sql(query_trend_orig, conn)
        
        df_trend["nome_grupo"] = (
            df_trend["nome_grupo"]
            .map(lambda x: re.sub(r"\s*\([^)]*\)", "", str(x)))
            .map(lambda x: re.sub(r"\*+", "", str(x)))
            .map(lambda x: re.sub(r"\s+", " ", str(x)).strip().upper())
            .map(lambda x: "SERCOMTEL" if x.startswith("SERCOMTEL") else x)
        )
        
        conn.close()
        return df, df_trend
    except Exception as e:
        if "does not exist" in str(e) or "não existe" in str(e):
             return None, None
        st.error(f"Erro ao conectar no banco de dados: {e}")
        return pd.DataFrame(), pd.DataFrame()

# --- SIDEBAR ---
with st.sidebar:
    logo = _pick_logo()
    if logo:
        processed = Path("assets/_logo_transparent.png")
        try:
            if not processed.exists() or logo.stat().st_mtime > processed.stat().st_mtime:
                _remove_white_bg(logo, processed)
            st.image(str(processed if processed.exists() else logo), use_column_width=True)
        except Exception:
            st.image(str(logo), use_column_width=True)
    else:
        st.markdown("""
            <div style="text-align: center; margin-bottom: 20px;">
                <svg width="160" height="100" viewBox="0 0 160 100" xmlns="http://www.w3.org/2000/svg">
                    <circle cx="52" cy="34" r="12" fill="#1e88e5"/>
                    <path d="M105 18 C70 8 35 48 95 78" stroke="#f8c300" stroke-width="20" fill="none" stroke-linecap="round"/>
                </svg>
                <div style="margin-top: 6px;">
                    <div style="color:#0f7d32; font-weight:800; font-size:1.2rem; letter-spacing:1px;">ANATEL</div>
                    <div style="color:#2e7d32; font-size:0.85rem;">Agência Nacional de Telecomunicações</div>
                </div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("### ⚙️ Painel de Controle")
    st.info(
        """
        **IDA - Índice de Desempenho no Atendimento**
        
        Indicador oficial que mede a qualidade do atendimento das operadoras de telecomunicações, baseado na resolução de demandas em até 5 dias úteis.
        """
    )
    
    st.markdown("---")
    st.markdown("**Versão:** 1.0.2 (Estável)")
    st.markdown("**Ambiente:** Produção")

# --- MAIN CONTENT ---

# Carregamento com feedback visual limpo
with st.spinner('Sincronizando dados com o Data Mart...'):
    df_view, df_trend = load_data()

# Validação se dados existem
if df_view is None:
    st.cache_data.clear()
    df_view, df_trend = load_data()
    if df_view is None:
        st.warning("⚠️ Inicialização do Banco de Dados em andamento. Por favor, aguarde e recarregue a página.")
        st.stop()

if df_view.empty:
    st.warning("📭 Base de dados vazia. Execute o pipeline de carga para visualizar o dashboard.")
    st.stop()

# --- FILTROS NA LATERAL ---
df_view['Ano'] = df_view['Mes'].str.slice(0, 4)
anos = sorted(df_view['Ano'].unique())
operadoras = [c for c in df_view.columns if c not in ['Mes', 'Taxa de Variação Média', 'Ano']]

with st.sidebar:
    st.markdown("### 🎛️ Filtros")
    sel_anos = st.multiselect("Ano de Referência:", anos, default=anos[-1:] if len(anos) else [])
    selected_ops = st.multiselect(
        "Operadoras:", 
        operadoras, 
        default=operadoras[:4] if len(operadoras) >= 4 else operadoras
    )
    df_month_basis = df_view[df_view['Ano'].isin(sel_anos)] if sel_anos else df_view
    meses = sorted(df_month_basis['Mes'].unique())
    sel_meses = st.multiselect("Meses:", meses, default=meses[-3:] if len(meses) >= 3 else meses)
    df_view_plot_sidebar = df_month_basis[df_month_basis['Mes'].isin(sel_meses)] if sel_meses else df_month_basis
    if selected_ops:
        medias = df_view_plot_sidebar[selected_ops].mean().sort_values(ascending=False)
        top_op = medias.index[0]
        top_val = round(medias.iloc[0], 2)
        bottom_op = medias.index[-1]
        bottom_val = round(medias.iloc[-1], 2)
    st.markdown("---")
    st.markdown("### 📚 Fonte dos Dados")
    st.write("""
    As notas derivam do indicador **IDA (taxa de solicitações resolvidas em até 5 dias)**,
    obtido dos dados públicos da **Anatel** e persistido no **Data Mart (PostgreSQL)**.
    
    - Tabela base: `fato_ida` unida às dimensões (`dim_tempo`, `dim_grupo_economico`, `dim_servico`).
    - Série histórica no gráfico: campo `taxa_solicitacoes_resolvidas_5dias` (renomeado para `ida`).
    - Comparativo (Δ vs mercado): view `view_taxa_resolucao_5_dias` com pivot por operadora.
    """)

# Header Principal
st.markdown("""
    <div class="header-container">
        <div>
            <h1 class="header-title">Painel de Monitoramento da Qualidade</h1>
            <p class="header-subtitle">Análise Técnica de Desempenho das Operadoras - Case beAnalytic</p>
        </div>
        <div style="text-align: right;">
            <span style="background-color: #e3f2fd; color: #0d47a1; padding: 5px 10px; border-radius: 15px; font-size: 0.8rem; font-weight: bold;">Dados Públicos</span>
        </div>
    </div>
""", unsafe_allow_html=True)

# KPIs Globais
st.markdown("### 📈 Resumo Executivo")
col1, col2, col3, col4 = st.columns(4)
try:
    kpi_df = df_view_plot_sidebar
except NameError:
    kpi_df = df_view
ultimo_mes = kpi_df.iloc[-1]
delta_kpi = None
try:
    # Cálculo da variação média do mercado respeitando filtro de anos
    df_trend_base = df_trend.copy()
    try:
        df_trend_base['Ano'] = df_trend_base['ano_mes'].str.slice(0, 4)
        if sel_anos:
            df_trend_base = df_trend_base[df_trend_base['Ano'].isin(sel_anos)]
        if sel_meses:
            df_trend_base = df_trend_base[df_trend_base['ano_mes'].isin(sel_meses)]
    except Exception:
        pass
    market = df_trend_base.groupby('ano_mes', as_index=False)['ida'].mean().sort_values('ano_mes')
    market['var_mercado'] = market['ida'].pct_change() * 100
    var_last = market['var_mercado'].iloc[-1]
    var_prev = market['var_mercado'].iloc[-2] if len(market) >= 2 else None
    delta_kpi = round(var_last - var_prev, 2) if var_prev is not None else None
except Exception:
    if len(kpi_df) >= 2:
        delta_kpi = round(ultimo_mes['Taxa de Variação Média'] - kpi_df['Taxa de Variação Média'].iloc[-2], 2)

with col1:
    st.metric("📅 Última Referência", ultimo_mes['Mes'], delta_color="off")
with col2:
    try:
        val_mercado_fmt = f"{round(float(var_last), 1)}%"
    except Exception:
        # Fallback para a coluna da view caso algo falhe
        val_mercado = ultimo_mes['Taxa de Variação Média']
        val_mercado_fmt = f"{round(float(val_mercado), 1)}%" if str(val_mercado).replace('.','',1).isdigit() else f"{val_mercado}%"
    st.metric("📉 Variação Média (Mercado)", val_mercado_fmt, delta=f"{delta_kpi} pp" if delta_kpi is not None else None, delta_color="normal")
with col3:
    try:
        ops_count = len(selected_ops) if selected_ops else len(df_view.columns) - 2
    except NameError:
        ops_count = len(df_view.columns) - 2
    st.metric("🏢 Operadoras Monitoradas", ops_count)
with col4:
    df_trend_kpi = df_trend.copy()
    df_trend_kpi['Ano'] = df_trend_kpi['ano_mes'].str.slice(0, 4)
    try:
        serv_count = len(df_trend_kpi[df_trend_kpi['Ano'].isin(sel_anos)]['nome_servico'].unique()) if sel_anos else len(df_trend_kpi['nome_servico'].unique())
    except NameError:
        serv_count = len(df_trend_kpi['nome_servico'].unique())
    st.metric("🛠️ Serviços Analisados", serv_count)

# KPIs de Benchmark (com base nos filtros)
try:
    base_kpi = df_view[df_view['Ano'].isin(sel_anos)] if sel_anos else df_view
    df_view_plot_kpi = base_kpi[base_kpi['Mes'].isin(sel_meses)] if sel_meses else base_kpi
    k_ops = selected_ops if selected_ops else [c for c in df_view_plot_kpi.columns if c not in ['Mes','Taxa de Variação Média','Ano']]
    if k_ops:
        medias_kpi = df_view_plot_kpi[k_ops].mean().sort_values(ascending=False)
        k_col1, k_col2, k_col3 = st.columns(3)
        with k_col1:
            st.metric("🏆 Melhor Operadora (Δ médio)", f"{medias_kpi.index[0]} | {round(medias_kpi.iloc[0],2)}%")
        with k_col2:
            st.metric("⚠️ Pior Operadora (Δ médio)", f"{medias_kpi.index[-1]} | {round(medias_kpi.iloc[-1],2)}%")
        with k_col3:
            st.metric("🗓️ Períodos Selecionados", f"{len(df_view_plot_kpi)} meses")
except Exception:
    pass

st.markdown("---")

# Abas de Análise
tab_benchmark, tab_historico, tab_sobre = st.tabs([
    "📊 Benchmark Competitivo", 
    "📉 Série Histórica (IDA)", 
    "ℹ️ Metodologia e Detalhes"
])

# --- ABA 1: BENCHMARK ---
with tab_benchmark:
    # Filtros aplicados
    base_plot = df_view[df_view['Ano'].isin(sel_anos)] if sel_anos else df_view
    df_view_plot = base_plot[base_plot['Mes'].isin(sel_meses)] if sel_meses else base_plot
    
    if not selected_ops:
        st.warning("👈 Selecione pelo menos uma operadora para visualizar a análise.")
    else:
        # Layout principal: Gráfico de Linha + Descrição
        col_chart, col_desc = st.columns([3, 1])
        
        with col_desc:
            st.markdown("#### 🔍 Análise de Delta")
            st.info("""
            **Como interpretar o gráfico**
            - **Delta de Performance**: Diferença entre a nota da operadora e a média do mercado.
            - **Linha Vermelha (0)**: Média do mercado.
            - **Positivo**: Desempenho superior.
            - **Negativo**: Desempenho inferior.
            """)
            
            # Ranking Rápido (Barras) na lateral
            ranking_df = df_view_plot[selected_ops].mean().sort_values(ascending=True).reset_index()
            ranking_df.columns = ['Operadora', 'Delta Médio']
            
            fig_bar = px.bar(
                ranking_df,
                x='Delta Médio',
                y='Operadora',
                orientation='h',
                text='Delta Médio',
                title="<b>Ranking Médio (Período)</b>",
                color='Delta Médio',
                color_continuous_scale='RdYlGn',
                height=350
            )
            fig_bar.update_traces(texttemplate='%{text:.2f}', textposition='inside')
            fig_bar.update_layout(
                template="plotly_dark",
                plot_bgcolor="#1c1f26",
                margin=dict(t=40, l=0, r=0, b=0),
                xaxis=dict(showgrid=False, showticklabels=False),
                yaxis=dict(title=None),
                coloraxis_showscale=False,
                font=dict(family="Segoe UI, sans-serif", size=11, color="#e0e0e0")
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        with col_chart:
            # Preparar dados para plotagem com labels (Long Format)
            df_long = df_view_plot.melt(
                id_vars=['Mes'], 
                value_vars=selected_ops, 
                var_name='Operadora', 
                value_name='Delta'
            )
            
            fig = px.line(
                df_long, 
                x="Mes", 
                y="Delta",
                color="Operadora",
                text="Delta",  # Adiciona os valores nas junções
                title="<b>Performance Relativa (Diferença vs Média de Mercado)</b>",
                markers=True,
                height=550
            )
            # Adicionar linha de referência
            fig.add_hline(y=0, line_dash="dash", line_color="#d32f2f", annotation_text="Média (0)")
            
            # Layout Profissional
            fig.update_traces(textposition="top center", texttemplate='%{text:.1f}')
            fig.update_layout(
                xaxis_title="Mês de Referência",
                yaxis_title="Delta Percentual (%)",
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=None),
                template="plotly_dark",
                plot_bgcolor="#1c1f26",
                margin=dict(t=50, l=20, r=20, b=20),
                font=dict(family="Segoe UI, sans-serif", size=12, color="#e0e0e0")
            )
            fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#2a2e35', color="#e0e0e0")
            fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#2a2e35', color="#e0e0e0")
            
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        
        # Novos Gráficos: Heatmap e Dispersão (Lado a Lado)
        c_heat, c_extra = st.columns(2)
        
        with c_heat:
            st.markdown("#### 🔥 Mapa de Calor de Performance")
            # Heatmap
            fig_heat = px.imshow(
                df_view_plot.set_index('Mes')[selected_ops].T,
                labels=dict(x="Mês", y="Operadora", color="Delta"),
                x=df_view_plot['Mes'],
                y=selected_ops,
                color_continuous_scale='RdYlGn',
                text_auto='.1f',
                aspect="auto",
                height=400
            )
            fig_heat.update_layout(
                template="plotly_dark",
                plot_bgcolor="#1c1f26",
                margin=dict(t=20, l=0, r=0, b=0),
                font=dict(family="Segoe UI, sans-serif", size=11, color="#e0e0e0")
            )
            st.plotly_chart(fig_heat, use_container_width=True)
            
        with c_extra:
            st.markdown("#### 📊 Distribuição de Performance")
            # Boxplot para ver variabilidade
            fig_box = px.box(
                df_long,
                x="Operadora",
                y="Delta",
                color="Operadora",
                points="all",
                title="Variabilidade do Desempenho por Operadora",
                height=400
            )
            fig_box.update_layout(
                template="plotly_dark",
                plot_bgcolor="#1c1f26",
                showlegend=False,
                margin=dict(t=40, l=0, r=0, b=0),
                font=dict(family="Segoe UI, sans-serif", size=11, color="#e0e0e0")
            )
            fig_box.add_hline(y=0, line_dash="dash", line_color="#d32f2f")
            st.plotly_chart(fig_box, use_container_width=True)

    # Tabela de Dados (Expansível)
    with st.expander("📋 Visualizar Dados Tabulares", expanded=True):
        st.markdown("""
        **Legenda do Heatmap:**
        *   <span style='color: green'>**Verde**</span>: Desempenho **SUPERIOR** à média do mercado.
        *   <span style='color: #f8c300'>**Amarelo**</span>: Desempenho **PRÓXIMO** à média.
        *   <span style='color: red'>**Vermelho**</span>: Desempenho **INFERIOR** à média do mercado.
        
        **O que significam os números?**
        Os valores representam a **diferença percentual** entre a nota da operadora e a média do mercado.
        *   Exemplo: `+7.90` indica que a operadora teve nota **7.90 pontos ACIMA** da média geral.
        *   Exemplo: `-12.60` indica que a operadora ficou **12.60 pontos ABAIXO** da média.
        """, unsafe_allow_html=True)
        
        st.dataframe(
            df_view_plot[["Mes"] + selected_ops].style.background_gradient(cmap="RdYlGn", vmin=-10, vmax=10).format(precision=2),
            use_container_width=True
        )

# --- ABA 2: HISTÓRICO ---
with tab_historico:
    st.markdown("#### 📉 Evolução do Indicador IDA (Absoluto)")
    
    # Filtros em linha para maximizar espaço do gráfico
    c1, c2, c3 = st.columns([1, 1, 2])
    
    with c1:
        servicos = df_trend['nome_servico'].unique()
        sel_servico = st.selectbox("Serviço:", servicos)
    
    # Filtragem preliminar para obter operadoras do serviço selecionado
    # CORREÇÃO: Usando .copy() para evitar SettingWithCopyWarning
    df_filtered_trend = df_trend[df_trend['nome_servico'] == sel_servico].copy()
    
    with c2:
        ops_trend = df_filtered_trend['nome_grupo'].unique()
        sel_ops_trend = st.multiselect(
            "Operadoras:", 
            ops_trend, 
            default=ops_trend[:5] if len(ops_trend) >= 5 else ops_trend
        )
        
    with c3:
        df_filtered_trend['Ano'] = df_filtered_trend['ano_mes'].str.slice(0, 4)
        anos_hist = sorted(df_filtered_trend['Ano'].unique())
        sel_anos_hist = st.multiselect("Anos:", anos_hist, default=anos_hist[-2:] if len(anos_hist) > 1 else anos_hist)
    
    if sel_ops_trend:
        # Filtragem final
        df_chart = df_filtered_trend[
            df_filtered_trend['nome_grupo'].isin(sel_ops_trend) & 
            (df_filtered_trend['Ano'].isin(sel_anos_hist) if sel_anos_hist else True)
        ]
        
        fig2 = px.line(
            df_chart,
            x="ano_mes",
            y="ida",
            color="nome_grupo",
            text="ida",
            title=f"<b>Evolução Histórica - {sel_servico}</b>",
            markers=True,
            height=500
        )
        fig2.update_traces(textposition="top center", texttemplate='%{text:.1f}')
        fig2.update_layout(
            xaxis_title="Período",
            yaxis_title="IDA (Taxa de Resolução %)",
            hovermode="x unified",
            template="plotly_dark",
            plot_bgcolor="#1c1f26",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=None),
            font=dict(family="Segoe UI, sans-serif", size=12, color="#e0e0e0")
        )
        fig2.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#2a2e35', color="#e0e0e0")
        fig2.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#2a2e35', range=[0, 105], color="#e0e0e0")
        
        st.plotly_chart(fig2, use_container_width=True)
        st.caption("""
        Detalhe técnico:
        - Origem: SELECT dt.ano_mes, dg.nome_grupo, ds.nome_servico, f.taxa_solicitacoes_resolvidas_5dias AS ida
          FROM fato_ida f JOIN dim_tempo dt JOIN dim_grupo_economico dg JOIN dim_servico ds.
        - Métrica exibida: **IDA (%)** = taxa de solicitações resolvidas em até 5 dias (valor absoluto por mês).
        - Transformações no painel: apenas filtros por **Operadoras** e **Anos**; sem normalização adicional.
        - Interpretação: linhas mostram a evolução mensal do IDA por operadora dentro do serviço selecionado.
        """)
    else:
        st.info("Selecione operadoras acima para gerar o gráfico.")

# --- ABA 3: SOBRE ---
with tab_sobre:
    c_info, c_tec = st.columns(2)
    
    with c_info:
        st.markdown("### 🎯 Contexto do Projeto")
        st.write("""
        Este dashboard foi desenvolvido como parte de um **Case Técnico de Engenharia de Dados**.
        O objetivo é demonstrar competências em:
        
        *   **Extração de Dados (ETL):** Coleta automatizada de fontes públicas (Anatel).
        *   **Modelagem de Dados:** Construção de Data Mart em Star Schema (PostgreSQL).
        *   **Visualização:** Criação de dashboards interativos e storytelling com dados.
        *   **Infraestrutura:** Uso de Docker e Docker Compose.
        """)
        
    with c_tec:
        st.markdown("### 🏗️ Stack Tecnológica")
        st.code("""
        Python 3.12
        Streamlit (Frontend)
        Plotly (Visualização)
        PostgreSQL (Database)
        Docker (Containerization)
        Playwright (Web Scraping)
        """, language="text")
        st.markdown("### 📐 Métricas e Regras de Cálculo")
        st.write("""
        - IDA (%): taxa de resoluções em 5 dias. Fonte: **fato_ida**.
        - Mercado (%): média mensal do IDA entre operadoras (benchmark).
        - Variação (%): diferença percentual mês-a-mês contra o mês anterior.
        - Delta (Individual − Mercado): leitura positiva indica desempenho superior ao benchmark.
        - View analítica: **view_taxa_resolucao_5_dias** e alternativa dbt **pivot_variacao**.
        - Tratamentos: remoção de outliers (> |200%|), limpeza e padronização de nomes de grupos.
        """)

# --- FOOTER ---
st.markdown("""
    <div class="footer">
        <p>Desenvolvido por <b>Vanessa Prado Rocha Aida</b> | Case Técnico beAnalytic | Dados Públicos da Anatel (2024)</p>
    </div>
""", unsafe_allow_html=True)
