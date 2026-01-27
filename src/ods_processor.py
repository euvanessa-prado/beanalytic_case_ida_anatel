"""
Módulo para processamento de arquivos ODS do portal dados.gov.br.

Este módulo implementa classes para leitura, normalização e transformação
de arquivos ODS contendo dados de desempenho no atendimento de operadoras.

Classes:
    ODSProcessor: Processador principal de arquivos ODS
    DataNormalizer: Normalizador de dados para formato padrão do Data Mart

Exemplo de uso:
    >>> processor = ODSProcessor(data_dir='dados_ida')
    >>> df = processor.processar_todos()
    >>> print(f"Registros: {len(df)}")
"""

import pandas as pd
import glob
import logging
from pathlib import Path
from typing import Optional, Tuple, List
import re

logger = logging.getLogger(__name__)


class DataNormalizer:
    """
    Normalizador de dados de arquivos ODS para formato padrão.
    
    Esta classe é responsável por transformar dados do formato wide (colunas
    representando períodos) para formato long (linhas representando períodos),
    adequado para análise e carga em banco de dados.
    
    Attributes:
        MESES_MAP (dict): Mapeamento de abreviações de meses para números
    """
    
    MESES_MAP = {
        'JAN': 1, 'FEV': 2, 'MAR': 3, 'ABR': 4,
        'MAI': 5, 'JUN': 6, 'JUL': 7, 'AGO': 8,
        'SET': 9, 'OUT': 10, 'NOV': 11, 'DEZ': 12
    }
    
    def __init__(self):
        """Inicializa o normalizador de dados."""
        pass
    
    def extrair_periodo(self, df: pd.DataFrame) -> Tuple[Optional[int], Optional[int]]:
        """
        Extrai período (ano/mês) do cabeçalho da planilha.
        
        Procura por linhas contendo "PERÍODO:" e extrai informações de data
        no formato "MÊS/ANO" (ex: "OUT/2015").
        
        Args:
            df (pd.DataFrame): DataFrame bruto do arquivo ODS
        
        Returns:
            tuple: (ano, mes) onde ano e mes são int, ou (None, None) se não encontrar
        
        Examples:
            >>> normalizer = DataNormalizer()
            >>> normalizer.extrair_periodo(df)
            (2015, 10)
        """
        try:
            for idx, row in df.iterrows():
                row_str = ' '.join([str(val) for val in row if pd.notna(val)])
                if 'PERÍODO' in row_str.upper() or 'PERIODO' in row_str.upper():
                    match = re.search(r'(\w{3})/(\d{4})', row_str)
                    if match:
                        mes_str, ano = match.groups()
                        mes = self.MESES_MAP.get(mes_str.upper(), 1)
                        return int(ano), mes
        except Exception as e:
            logger.warning(f"Erro ao extrair período: {e}")
        
        return None, None
    
    def encontrar_linha_dados(self, df: pd.DataFrame) -> int:
        """
        Encontra a linha onde começam os dados (após cabeçalhos).
        
        Procura por linhas contendo "GRUPO ECONÔMICO" ou "VARIÁVEL" que
        indicam o início da tabela de dados.
        
        Args:
            df (pd.DataFrame): DataFrame bruto
        
        Returns:
            int: Índice da linha inicial de dados (0 se não encontrar)
        """
        for idx, row in df.iterrows():
            row_str = ' '.join([str(val) for val in row if pd.notna(val)])
            if 'GRUPO ECONÔMICO' in row_str.upper() or 'VARIAVEL' in row_str.upper():
                return idx + 1
        
        return 0
    
    def identificar_colunas_periodo(self, df: pd.DataFrame) -> List[str]:
        """
        Identifica colunas que representam períodos temporais.
        
        Procura por colunas com formato YYYY-MM, YYYY.MM ou YYYY_MM.
        
        Args:
            df (pd.DataFrame): DataFrame com dados
        
        Returns:
            list: Lista de nomes de colunas identificadas como períodos
        """
        period_cols = []
        for col in df.columns:
            col_str = str(col)
            if any(sep in col_str for sep in ['-', '.', '_']) and any(char.isdigit() for char in col_str):
                period_cols.append(col)
        
        if not period_cols:
            logger.warning("Nenhuma coluna de período identificada")
            period_cols = [col for col in df.columns if df[col].dtype in ['float64', 'int64']]
        
        return period_cols
    
    def parsear_periodo(self, period_str: str, ano_default: Optional[int] = None, 
                       mes_default: Optional[int] = None) -> Tuple[int, int]:
        """
        Extrai ano e mês de string de período.
        
        Args:
            period_str (str): String contendo período (ex: "2015-01", "2015.10")
            ano_default (int, optional): Ano padrão se não conseguir extrair
            mes_default (int, optional): Mês padrão se não conseguir extrair
        
        Returns:
            tuple: (ano, mes) como inteiros
        """
        period_str = str(period_str)
        
        # Tentar formato YYYY-MM ou YYYY.MM
        match = re.search(r'(\d{4})[.-_](\d{1,2})', period_str)
        if match:
            return int(match.group(1)), int(match.group(2))
        
        return ano_default or 2015, mes_default or 1
    
    def normalizar(self, df: pd.DataFrame, ano: Optional[int] = None, 
                   mes: Optional[int] = None) -> pd.DataFrame:
        """
        Normaliza dados para formato padrão do Data Mart.
        
        Transforma dados do formato wide (colunas = períodos) para formato long
        (linhas = períodos), adequado para análise dimensional.
        
        Estrutura esperada do ODS:
            - Coluna A: Grupo Econômico (operadora)
            - Coluna B: Variável/Indicador
            - Colunas seguintes: Valores por período
        
        Args:
            df (pd.DataFrame): DataFrame com dados brutos
            ano (int, optional): Ano de referência
            mes (int, optional): Mês de referência
        
        Returns:
            pd.DataFrame: DataFrame normalizado com colunas:
                         [ano, mes, grupo_economico, variavel, valor]
        """
        logger.info("Normalizando dados...")
        
        # Extrair período se não fornecido
        if ano is None or mes is None:
            ano, mes = self.extrair_periodo(df)
        
        # Encontrar linha inicial dos dados
        start_row = self.encontrar_linha_dados(df)
        
        if start_row > 0:
            headers = df.iloc[start_row - 1].tolist()
            df_data = df.iloc[start_row:].copy()
            df_data.columns = headers
        else:
            df_data = df.copy()
        
        # Limpar dados
        df_data = df_data.dropna(how='all')
        
        # Identificar colunas de período
        period_cols = self.identificar_colunas_periodo(df_data)
        logger.info(f"Colunas de período identificadas: {len(period_cols)}")
        
        # Identificar colunas ID
        id_cols = [col for col in df_data.columns if col not in period_cols]
        if not id_cols:
            id_cols = df_data.columns[:2].tolist()
        
        logger.info(f"Colunas identificadoras: {id_cols}")
        
        # Transformar para formato long
        df_long = pd.melt(
            df_data,
            id_vars=id_cols,
            value_vars=period_cols,
            var_name='periodo',
            value_name='valor'
        )
        
        # Extrair ano e mês
        df_long[['ano', 'mes']] = df_long['periodo'].apply(
            lambda x: pd.Series(self.parsear_periodo(x, ano, mes))
        )
        
        # Renomear colunas para padrão
        df_long = self._renomear_colunas(df_long, id_cols)
        
        # Limpar valores
        df_long['valor'] = pd.to_numeric(df_long['valor'], errors='coerce')
        df_long = df_long.dropna(subset=['valor'])
        
        # Selecionar colunas finais
        final_cols = ['ano', 'mes', 'grupo_economico', 'variavel', 'valor']
        df_long = df_long[[col for col in final_cols if col in df_long.columns]]
        
        logger.info(f"Dados normalizados: {len(df_long)} registros")
        
        return df_long
    
    def _renomear_colunas(self, df: pd.DataFrame, id_cols: List[str]) -> pd.DataFrame:
        """
        Renomeia colunas para padrão do Data Mart.
        
        Args:
            df (pd.DataFrame): DataFrame a ser renomeado
            id_cols (list): Lista de colunas identificadoras
        
        Returns:
            pd.DataFrame: DataFrame com colunas renomeadas
        """
        rename_map = {}
        for col in id_cols:
            col_lower = str(col).lower()
            if 'grupo' in col_lower or 'econômico' in col_lower or 'economico' in col_lower:
                rename_map[col] = 'grupo_economico'
            elif 'variável' in col_lower or 'variavel' in col_lower or 'indicador' in col_lower:
                rename_map[col] = 'variavel'
        
        df = df.rename(columns=rename_map)
        
        # Garantir colunas necessárias
        if 'grupo_economico' not in df.columns and len(id_cols) > 0:
            df['grupo_economico'] = df[id_cols[0]]
        
        if 'variavel' not in df.columns and len(id_cols) > 1:
            df['variavel'] = df[id_cols[1]]
        
        return df


class ODSProcessor:
    """
    Processador principal de arquivos ODS.
    
    Esta classe coordena o processamento de múltiplos arquivos ODS,
    realizando leitura, normalização e consolidação dos dados.
    
    Attributes:
        data_dir (str): Diretório contendo arquivos ODS
        normalizer (DataNormalizer): Instância do normalizador de dados
    
    Example:
        >>> processor = ODSProcessor(data_dir='dados_ida')
        >>> df_consolidado = processor.processar_todos()
        >>> print(f"Total: {len(df_consolidado)} registros")
    """
    
    def __init__(self, data_dir: str = 'dados_ida'):
        """
        Inicializa o processador.
        
        Args:
            data_dir (str): Caminho do diretório com arquivos ODS
        """
        self.data_dir = data_dir
        self.normalizer = DataNormalizer()
    
    def processar_todos(self) -> Optional[pd.DataFrame]:
        """
        Processa todos os arquivos ODS do diretório.
        
        Returns:
            pd.DataFrame: DataFrame consolidado com todos os dados,
                         ou None se nenhum arquivo for processado
        """
        logger.info(f"Buscando arquivos ODS em: {self.data_dir}")
        
        ods_files = glob.glob(f"{self.data_dir}/*.ods")
        
        if not ods_files:
            logger.error(f"Nenhum arquivo ODS encontrado em {self.data_dir}/")
            logger.error("Baixe os arquivos do portal e coloque nesta pasta!")
            return None
        
        logger.info(f"Encontrados {len(ods_files)} arquivos ODS")
        
        all_data = []
        
        for file_path in ods_files:
            df_normalized = self.processar_arquivo(file_path)
            if df_normalized is not None and len(df_normalized) > 0:
                all_data.append(df_normalized)
        
        if not all_data:
            logger.error("Nenhum arquivo foi processado com sucesso")
            return None
        
        combined = pd.concat(all_data, ignore_index=True)
        logger.info(f"Total de registros combinados: {len(combined)}")
        
        return combined
    
    def processar_arquivo(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Processa um único arquivo ODS.
        
        Args:
            file_path (str): Caminho completo do arquivo ODS
        
        Returns:
            pd.DataFrame: DataFrame normalizado, ou None se houver erro
        """
        try:
            logger.info(f"Processando: {Path(file_path).name}")
            
            df = pd.read_excel(file_path, engine='odf')
            logger.info(f"  Linhas: {len(df)}, Colunas: {len(df.columns)}")
            
            df_normalized = self.normalizer.normalizar(df)
            
            if df_normalized is not None and len(df_normalized) > 0:
                logger.info(f"  Normalizado: {len(df_normalized)} registros")
                return df_normalized
            else:
                logger.warning(f"  Nenhum dado normalizado")
                return None
            
        except Exception as e:
            logger.error(f"Erro ao processar {file_path}: {e}")
            return None


def process_ods_files(data_dir: str = 'dados_ida') -> Optional[pd.DataFrame]:
    """
    Função de conveniência para processar arquivos ODS.
    
    Mantida para compatibilidade com código existente.
    
    Args:
        data_dir (str): Diretório com arquivos ODS
    
    Returns:
        pd.DataFrame: DataFrame consolidado ou None
    """
    processor = ODSProcessor(data_dir)
    return processor.processar_todos()


# Manter funções antigas para compatibilidade
def extract_period_from_sheet(df: pd.DataFrame) -> Tuple[Optional[int], Optional[int]]:
    """Função legada - usar DataNormalizer.extrair_periodo()"""
    normalizer = DataNormalizer()
    return normalizer.extrair_periodo(df)


def find_data_start_row(df: pd.DataFrame) -> int:
    """Função legada - usar DataNormalizer.encontrar_linha_dados()"""
    normalizer = DataNormalizer()
    return normalizer.encontrar_linha_dados(df)


def normalize_data(df: pd.DataFrame, ano: Optional[int] = None, 
                   mes: Optional[int] = None) -> pd.DataFrame:
    """Função legada - usar DataNormalizer.normalizar()"""
    normalizer = DataNormalizer()
    return normalizer.normalizar(df, ano, mes)

