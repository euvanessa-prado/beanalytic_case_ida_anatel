"""
Módulo de transformação de dados para o Data Mart IDA.

Este módulo transforma dados brutos normalizados em métricas
agregadas de IDA (Índice de Desempenho no Atendimento).

Classes:
    IDATransformer: Transformador de dados para métricas de IDA

Exemplo de uso:
    >>> transformer = IDATransformer()
    >>> df_metricas = transformer.transformar(df_raw)
"""

import pandas as pd
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class IDATransformer:
    """
    Transformador de dados para métricas de IDA.
    
    Esta classe transforma dados brutos normalizados em métricas
    agregadas por período, grupo econômico e serviço.
    
    Attributes:
        variaveis_ida (list): Lista de variáveis relacionadas ao IDA
    """
    
    # Variáveis que compõem o IDA
    VARIAVEIS_IDA = [
        'Indicador de Desempenho no Atendimento (IDA)',
        'Índice de Reclamações',
        'Quantidade de acessos em serviço',
        'Quantidade de reabertas',
        'Quantidade de reclamações',
        'Quantidade de solicitações',
        'Solicitações resolvidas em até 5 dias úteis'
    ]
    
    def __init__(self):
        """Inicializa o transformador."""
        self.variaveis_ida = self.VARIAVEIS_IDA
    
    def transformar(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Transforma dados brutos em métricas de IDA.
        
        Args:
            df (pd.DataFrame): DataFrame com dados normalizados
                              Colunas esperadas: ano, mes, ano_mes, servico,
                              grupo_economico, variavel, valor
        
        Returns:
            pd.DataFrame: DataFrame com métricas agregadas ou None
        """
        logger.info("Transformando dados para métricas de IDA...")
        
        if df is None or len(df) == 0:
            logger.error("DataFrame vazio")
            return None
        
        # Filtrar apenas variáveis relevantes para IDA
        df_ida = df[df['variavel'].isin(self.variaveis_ida)].copy()
        
        if len(df_ida) == 0:
            logger.warning("Nenhuma variável de IDA encontrada")
            return None
        
        logger.info(f"Registros com variáveis IDA: {len(df_ida)}")
        
        # Pivotar dados: cada variável vira uma coluna
        df_pivot = df_ida.pivot_table(
            index=['ano', 'mes', 'ano_mes', 'servico', 'grupo_economico'],
            columns='variavel',
            values='valor',
            aggfunc='first'
        ).reset_index()
        
        # Renomear colunas para padrão do Data Mart
        colunas_rename = {
            'Indicador de Desempenho no Atendimento (IDA)': 'ida_percentual',
            'Índice de Reclamações': 'indice_reclamacoes',
            'Quantidade de acessos em serviço': 'total_acessos',
            'Quantidade de reabertas': 'qtd_reabertas',
            'Quantidade de reclamações': 'qtd_reclamacoes',
            'Quantidade de solicitações': 'total_solicitacoes',
            'Solicitações resolvidas em até 5 dias úteis': 'solicitacoes_resolvidas_5dias'
        }
        
        df_pivot = df_pivot.rename(columns=colunas_rename)
        
        # Calcular métricas derivadas
        df_pivot = self._calcular_metricas(df_pivot)
        
        logger.info(f"Métricas transformadas: {len(df_pivot)} registros")
        
        return df_pivot
    
    def _calcular_metricas(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula métricas derivadas.
        
        Args:
            df (pd.DataFrame): DataFrame com métricas base
        
        Returns:
            pd.DataFrame: DataFrame com métricas calculadas
        """
        # Taxa de resolução em 5 dias (se não existir)
        if 'solicitacoes_resolvidas_5dias' in df.columns and 'total_solicitacoes' in df.columns:
            df['taxa_resolvidas_5dias'] = (
                (df['solicitacoes_resolvidas_5dias'] / df['total_solicitacoes']) * 100
            ).fillna(0)
        elif 'ida_percentual' in df.columns:
            # Usar IDA como proxy para taxa de resolução
            df['taxa_resolvidas_5dias'] = df['ida_percentual']
        else:
            df['taxa_resolvidas_5dias'] = 0
        
        # Garantir que total_solicitacoes existe
        if 'total_solicitacoes' not in df.columns:
            df['total_solicitacoes'] = 0
        
        # Calcular solicitações resolvidas se não existir
        if 'solicitacoes_resolvidas_5dias' not in df.columns:
            df['solicitacoes_resolvidas_5dias'] = (
                df['total_solicitacoes'] * df['taxa_resolvidas_5dias'] / 100
            ).fillna(0)
        
        # Arredondar valores
        df['taxa_resolvidas_5dias'] = df['taxa_resolvidas_5dias'].round(2)
        df['total_solicitacoes'] = df['total_solicitacoes'].fillna(0).astype(int)
        df['solicitacoes_resolvidas_5dias'] = df['solicitacoes_resolvidas_5dias'].fillna(0).astype(int)
        
        return df
    
    def preparar_para_carga(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepara DataFrame para carga no Data Mart.
        
        Seleciona apenas colunas necessárias para a tabela fato.
        
        Args:
            df (pd.DataFrame): DataFrame com métricas
        
        Returns:
            pd.DataFrame: DataFrame pronto para carga
        """
        colunas_fato = [
            'ano', 'mes', 'ano_mes', 'servico', 'grupo_economico',
            'taxa_resolvidas_5dias', 'total_solicitacoes', 'solicitacoes_resolvidas_5dias'
        ]
        
        # Selecionar apenas colunas que existem
        colunas_disponiveis = [col for col in colunas_fato if col in df.columns]
        
        df_final = df[colunas_disponiveis].copy()
        
        # Renomear para padrão esperado pelo loader
        df_final = df_final.rename(columns={
            'solicitacoes_resolvidas_5dias': 'solicitacoes_resolvidas'
        })
        
        logger.info(f"DataFrame preparado para carga: {len(df_final)} registros")
        
        return df_final
