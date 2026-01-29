"""Utilitários de processamento e normalização ODS para o conjunto de dados IDA da Anatel.

Este módulo fornece classes para processar planilhas ODS heterogêneas, convertendo-as
em DataFrames normalizados no formato longo (long-format).

MODERNIZAÇÃO (2026):
Refatorado para utilizar **Polars** como engine de transformação, garantindo
maior performance e sintaxe expressiva moderna, conforme tendências de mercado.
"""

import pandas as pd
import polars as pl
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

class DataNormalizer:
    """Normaliza tabelas ODS (formato wide) para DataFrame (formato long) usando Polars.
    
    Responsabilidades:
    - Converter Pandas DF (leitura ODS) para Polars LazyFrame/DataFrame.
    - Identificar colunas dinamicamente.
    - Realizar unpivot (melt) eficiente.
    - Processar datas e limpar dados com expressões nativas do Polars.
    """
    
    MONTH_MAP = {
        'JAN': 1, 'FEV': 2, 'MAR': 3, 'ABR': 4, 'MAI': 5, 'JUN': 6,
        'JUL': 7, 'AGO': 8, 'SET': 9, 'OUT': 10, 'NOV': 11, 'DEZ': 12
    }

    def __init__(self, target_year=None):
        """Inicializa com ano alvo opcional para filtragem."""
        self.target_year = target_year

    def normalize(self, df_pandas: pd.DataFrame) -> pd.DataFrame:
        """Converte um DataFrame bruto ODS para o esquema normalizado via Polars.
        
        Args:
            df_pandas: DataFrame lido de uma planilha ODS (via pandas/odfpy).
        
        Returns:
            DataFrame (Pandas) normalizado para compatibilidade com loaders existentes.
        """
        # 1. Pré-processamento leve no Pandas para achar o cabeçalho
        # (Ainda mais eficiente fazer essa busca linear simples no Pandas antes de converter)
        start_row = -1
        for i, row in df_pandas.iterrows():
            # Busca robusta por "Grupo Econômico"
            if any(str(v).strip().upper() == 'GRUPO ECONÔMICO' for v in row):
                start_row = i + 1
                break
        
        if start_row == -1: 
            return pd.DataFrame()

        # Extração de cabeçalhos e corte do DF
        headers = [str(h).strip() if pd.notna(h) else f"c{i}" for i, h in enumerate(df_pandas.iloc[start_row-1])]
        df_clean_pd = df_pandas.iloc[start_row:].copy()
        df_clean_pd.columns = headers
        
        # 2. Conversão para Polars para processamento pesado
        # Usamos pl.from_pandas para migrar para a engine Rust
        try:
            # Força conversão para string para evitar erros de inferência (mixed types)
            # Isso resolve falhas como "tried to convert to double" em colunas com comentários de texto
            df_clean_pd = df_clean_pd.astype(str)
            lf = pl.from_pandas(df_clean_pd).lazy()
        except Exception as e:
            logger.error(f"Erro ao converter para Polars: {e}")
            return pd.DataFrame()

        # Identificação de colunas de período vs ID
        # (Lógica mantida em Python puro pois depende dos nomes das colunas)
        all_cols = lf.columns
        period_cols = [c for c in all_cols if re.search(r'\d{4}', c) or any(m in c.upper() for m in self.MONTH_MAP)]
        id_cols = [c for c in all_cols if c not in period_cols and "Unnamed" not in c and c.strip() != ""]

        if not id_cols or not period_cols:
            logger.warning(f"Estrutura irreconhecida. IDs: {id_cols}, Períodos: {len(period_cols)}")
            return pd.DataFrame()

        # 3. Pipeline de Transformação Polars
        # - Rename dinâmico
        # - Unpivot (Melt)
        # - Casting e Limpeza
        
        # Mapa de renomeação para garantir consistência
        rename_map = {id_cols[0]: 'grupo_economico'}
        if len(id_cols) > 1:
            rename_map[id_cols[1]] = 'variavel'
            
        # Expressões para parsing de data
        # Tenta extrair YYYY-MM ou MMM/YYYY
        
        # Preparação do LazyFrame
        processed_lf = (
            lf
            .rename(rename_map)
            .with_columns([
                pl.col('grupo_economico').cast(pl.Utf8),
                pl.lit('Valor Único').alias('variavel') if 'variavel' not in rename_map.values() else pl.col('variavel').cast(pl.Utf8)
            ])
            .select(['grupo_economico', 'variavel'] + period_cols)
            .melt(
                id_vars=['grupo_economico', 'variavel'],
                value_vars=period_cols,
                variable_name='periodo',
                value_name='valor'
            )
            .with_columns([
                # Limpeza e conversão do valor
                pl.col('valor').cast(pl.Utf8).str.replace(r',', '.').cast(pl.Float64, strict=False)
            ])
            .filter(pl.col('valor').is_not_null())
        )

        # Materializa para processar datas com lógica complexa (regex customizado)
        # Polars tem suporte a regex, mas mapear Mês PT-BR para Int exige um map
        try:
            df_pl = processed_lf.collect()
        except Exception as e:
            logger.error(f"Erro ao coletar LazyFrame Polars: {e}")
            return pd.DataFrame()
        
        # Parsing de Datas (Uso de map_elements para flexibilidade com dicionário Python, 
        # já que Polars nativo não tem dicionário de meses PT-BR embutido)
        # Para performance máxima, poderíamos usar .str.replace_all com um map gigante, mas map_elements é aceitável aqui.
        
        def parse_date_pl(val: str):
            if not val: return None
            txt = str(val).upper().strip()
            
            # YYYY-MM
            m1 = re.search(r'(\d{4})[.-](\d{1,2})', txt)
            if m1: return (int(m1.group(1)), int(m1.group(2)))
            
            # MMM/YYYY
            m2 = re.search(r'([A-Z]{3})/(\d{4})', txt)
            if m2: return (int(m2.group(2)), self.MONTH_MAP.get(m2.group(1), 1))
            
            # Fallback
            if self.target_year and txt in self.MONTH_MAP:
                return (self.target_year, self.MONTH_MAP[txt])
            
            return None

        # Aplicação do parser e criação de colunas ano/mes
        # Nota: Polars Structs são ótimas para retornar tuplas
        parsed_dates = df_pl['periodo'].map_elements(parse_date_pl, return_dtype=pl.Object)
        
        # Como o retorno é tupla, vamos separar em colunas no Pandas ou aqui.
        # Vamos fazer um trque simples: converter para Pandas agora e finalizar.
        # (Manter o retorno em Pandas é requisito do orquestrador atual)
        
        df_final_pd = df_pl.to_pandas()
        
        # Finalização no Pandas (rápida pós-redução de dados)
        df_final_pd['parsed'] = parsed_dates
        df_final_pd = df_final_pd.dropna(subset=['parsed'])
        df_final_pd[['ano', 'mes']] = pd.DataFrame(df_final_pd['parsed'].tolist(), index=df_final_pd.index)
        
        # Filtro de ano alvo
        if self.target_year:
            df_final_pd = df_final_pd[df_final_pd['ano'] == self.target_year]

        # Limpeza final
        cols_final = ['grupo_economico', 'variavel', 'periodo', 'valor', 'ano', 'mes']
        return df_final_pd[cols_final].drop_duplicates(subset=['ano', 'mes', 'grupo_economico', 'variavel'])

class ODSProcessor:
    """Processador em lote de ODS que concatena datasets normalizados."""
    
    def __init__(self, data_path: str):
        """Inicializa com o caminho do diretório contendo arquivos ODS."""
        self.path = Path(data_path)

    def process_all(self, export_parquet: bool = False) -> pd.DataFrame:
        """Lê todos os arquivos ODS e retorna um DataFrame único concatenado.
        
        Args:
            export_parquet: Se True, salva cada arquivo processado em .parquet na pasta 'dados_ida/parquet'.
        """
        files = list(self.path.glob('*.ods'))
        results = []

        # Setup da pasta de saída
        if export_parquet:
            parquet_dir = self.path / "parquet"
            parquet_dir.mkdir(exist_ok=True)
        
        for f in files:
            parquet_path = self.path / "parquet" / f.with_suffix('.parquet').name if export_parquet else None
            
            # Se já existir parquet, lê dele (Cache simples)
            if export_parquet and parquet_path and parquet_path.exists():
                try:
                    df_norm = pd.read_parquet(parquet_path)
                    results.append(df_norm)
                    logger.info(f"Lido do cache Parquet: {parquet_path.name}")
                    continue
                except Exception:
                    pass # Se falhar, reprocessa do ODS

            svc = re.sub(r'\d+', '', f.stem).upper()
            yr_match = re.search(r'\d{4}', f.stem)
            yr = int(yr_match.group()) if yr_match else None
            
            # Leitura com Pandas (ODS support)
            try:
                df_raw = pd.read_excel(f, engine='odf')
                # Normalização via Polars
                df_norm = DataNormalizer(target_year=yr).normalize(df_raw)
                
                if not df_norm.empty:
                    df_norm['servico'] = svc
                    df_norm['arquivo_origem'] = f.name
                    df_norm['ano_mes'] = df_norm.apply(lambda r: f"{int(r.ano)}-{int(r.mes):02d}", axis=1)
                    
                    if export_parquet and parquet_path:
                        df_norm.to_parquet(parquet_path, index=False)
                        logger.info(f"Salvo em Parquet: {parquet_path.name}")
                        
                    results.append(df_norm)
            except Exception as e:
                logger.error(f"Falha ao processar arquivo {f.name}: {e}")

        return pd.concat(results, ignore_index=True) if results else pd.DataFrame()
