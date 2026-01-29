"""Utilitários de processamento e normalização ODS para o conjunto de dados IDA da Anatel.

Este módulo fornece classes para processar planilhas ODS heterogêneas, convertendo-as
em DataFrames normalizados no formato longo (long-format), lidando com extração de
mês/ano, conversão numérica e deduplicação. Projetado para processamento em lote e pipelines ETL.
"""

import pandas as pd
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

class DataNormalizer:
    """Normaliza tabelas ODS (formato wide) para DataFrame (formato long).
    
    Responsabilidades:
    - Localizar linhas de cabeçalho e derivar nomes de colunas.
    - Identificar colunas de identificação vs. colunas de período.
    - Realizar 'melt' para formato longo e processar período em (ano, mes).
    - Aplicar conversão numérica e filtragem opcional por ano.
    """
    
    MONTH_MAP = {
        'JAN': 1, 'FEV': 2, 'MAR': 3, 'ABR': 4, 'MAI': 5, 'JUN': 6,
        'JUL': 7, 'AGO': 8, 'SET': 9, 'OUT': 10, 'NOV': 11, 'DEZ': 12
    }

    def __init__(self, target_year=None):
        """Inicializa com ano alvo opcional para filtragem."""
        self.target_year = target_year

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Converte um DataFrame bruto ODS para o esquema normalizado.
        
        Args:
            df: DataFrame lido de uma planilha ODS.
        
        Returns:
            DataFrame com colunas:
            - grupo_economico, variavel, periodo, valor, ano, mes
        """
        start_row = -1
        for i, row in df.iterrows():
            if any(str(v).strip().upper() == 'GRUPO ECONÔMICO' for v in row):
                start_row = i + 1
                break
        
        if start_row == -1: 
            return pd.DataFrame()

        headers = [str(h).strip() if pd.notna(h) else f"c{i}" for i, h in enumerate(df.iloc[start_row-1])]
        df_clean = df.iloc[start_row:].copy()
        df_clean.columns = headers
        
        # Identificação de ID e Período
        id_cols = [headers[0], headers[1]] 
        period_cols = [c for c in headers if re.search(r'\d{4}', c) or any(m in c.upper() for m in self.MONTH_MAP)]
        
        df_long = pd.melt(df_clean, id_vars=id_cols, value_vars=period_cols, var_name='periodo', value_name='valor')
        df_long.columns = ['grupo_economico', 'variavel', 'periodo', 'valor']

        # Parsing de data e Conversão numérica
        df_long[['ano', 'mes']] = df_long['periodo'].apply(lambda x: pd.Series(self._parse_date(x)))
        df_long['valor'] = pd.to_numeric(df_long['valor'], errors='coerce')
        df_long = df_long.dropna(subset=['valor'])
        
        # Filtragem rigorosa por ano do arquivo
        if self.target_year:
            df_long = df_long[df_long['ano'] == self.target_year]
            
        return df_long.drop_duplicates(subset=['ano', 'mes', 'grupo_economico', 'variavel'])

    def _parse_date(self, val) -> tuple:
        """Extrai (ano, mes) de múltiplos formatos de período.
        
        Suporta objetos date, padrões 'YYYY-MM' e 'MON/YYYY'.
        Retorna (target_year, 1) ou (2015, 1) em caso de falha no parsing.
        """
        if hasattr(val, 'year'): 
            return int(val.year), int(val.month)
            
        txt = str(val).upper()
        # Formato: YYYY-MM
        m = re.search(r'(\d{4})[.-](\d{1,2})', txt)
        if m: 
            return int(m.group(1)), int(m.group(2))
        
        # Formato: MES/YYYY
        m = re.search(r'([A-Z]{3})/(\d{4})', txt)
        if m: 
            return int(m.group(2)), self.MONTH_MAP.get(m.group(1), 1)
            
        return self.target_year or 2015, 1

class ODSProcessor:
    """Processador em lote de ODS que concatena datasets normalizados."""
    
    def __init__(self, data_path: str):
        """Inicializa com o caminho do diretório contendo arquivos ODS."""
        self.path = Path(data_path)

    def process_all(self) -> pd.DataFrame:
        """Lê todos os arquivos ODS e retorna um DataFrame único concatenado."""
        files = list(self.path.glob('*.ods'))
        results = []

        for f in files:
            svc = re.sub(r'\d+', '', f.stem).upper()
            yr_match = re.search(r'\d{4}', f.stem)
            yr = int(yr_match.group()) if yr_match else None
            
            df_raw = pd.read_excel(f, engine='odf')
            df_norm = DataNormalizer(target_year=yr).normalize(df_raw)
            
            if not df_norm.empty:
                df_norm['servico'] = svc
                df_norm['arquivo_origem'] = f.name
                df_norm['ano_mes'] = df_norm.apply(lambda r: f"{int(r.ano)}-{int(r.mes):02d}", axis=1)
                results.append(df_norm)

        return pd.concat(results, ignore_index=True) if results else pd.DataFrame()
