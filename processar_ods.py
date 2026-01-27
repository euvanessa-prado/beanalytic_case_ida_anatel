"""
Script para processar e visualizar arquivos ODS já baixados.

Este script processa arquivos ODS do diretório dados_ida/ e exibe
estatísticas e amostras dos dados processados.

Classes:
    DataAnalyzer: Analisador de dados processados com estatísticas

Exemplo de uso:
    $ python processar_ods.py
"""

import logging
from typing import Optional
import pandas as pd
from src.ods_processor import ODSProcessor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class DataAnalyzer:
    """
    Analisador de dados processados de arquivos ODS.
    
    Esta classe fornece métodos para análise exploratória dos dados
    processados, incluindo estatísticas descritivas e visualizações.
    
    Attributes:
        df (pd.DataFrame): DataFrame com dados processados
    """
    
    def __init__(self, df: pd.DataFrame):
        """
        Inicializa o analisador.
        
        Args:
            df (pd.DataFrame): DataFrame com dados processados
        """
        self.df = df
    
    def exibir_resumo(self) -> None:
        """Exibe resumo geral dos dados."""
        print(f"\nDados processados com sucesso!")
        print(f"   Total de registros: {len(self.df)}")
        print(f"   Colunas: {list(self.df.columns)}")
    
    def exibir_amostra(self, n: int = 20) -> None:
        """
        Exibe amostra dos dados.
        
        Args:
            n (int): Número de linhas a exibir
        """
        print("\n" + "="*80)
        print(f"AMOSTRA DOS DADOS (primeiras {n} linhas):")
        print("="*80)
        print(self.df.head(n).to_string())
    
    def exibir_estatisticas(self) -> None:
        """Exibe estatísticas descritivas dos dados."""
        print("\n" + "="*80)
        print("ESTATÍSTICAS:")
        print("="*80)
        print(self.df.describe())
    
    def exibir_periodo(self) -> None:
        """Exibe informações sobre o período dos dados."""
        if 'ano' in self.df.columns and 'mes' in self.df.columns:
            print("\n" + "="*80)
            print("PERÍODO DOS DADOS:")
            print("="*80)
            print(f"   Início: {self.df['ano'].min()}/{self.df['mes'].min():02d}")
            print(f"   Fim: {self.df['ano'].max()}/{self.df['mes'].max():02d}")
    
    def exibir_grupos_economicos(self) -> None:
        """Exibe informações sobre grupos econômicos (operadoras)."""
        if 'grupo_economico' in self.df.columns:
            print("\n" + "="*80)
            print("GRUPOS ECONÔMICOS (Operadoras):")
            print("="*80)
            grupos = self.df['grupo_economico'].value_counts()
            for grupo, count in grupos.items():
                print(f"   • {grupo}: {count} registros")
    
    def exibir_analise_completa(self) -> None:
        """Exibe análise completa dos dados."""
        self.exibir_resumo()
        self.exibir_amostra()
        self.exibir_estatisticas()
        self.exibir_periodo()
        self.exibir_grupos_economicos()


class ProcessadorODSCLI:
    """
    Interface de linha de comando para processamento de arquivos ODS.
    
    Esta classe coordena o processamento e análise de arquivos ODS,
    fornecendo uma interface amigável para o usuário.
    
    Attributes:
        data_dir (str): Diretório com arquivos ODS
        processor (ODSProcessor): Processador de arquivos ODS
    """
    
    def __init__(self, data_dir: str = 'dados_ida'):
        """
        Inicializa o processador CLI.
        
        Args:
            data_dir (str): Diretório contendo arquivos ODS
        """
        self.data_dir = data_dir
        self.processor = ODSProcessor(data_dir)
    
    def executar(self) -> Optional[pd.DataFrame]:
        """
        Executa o processamento completo.
        
        Returns:
            pd.DataFrame: DataFrame processado, ou None se houver erro
        """
        self._exibir_cabecalho()
        
        # Processar arquivos
        df = self.processor.processar_todos()
        
        if df is None or len(df) == 0:
            print("Nenhum dado foi processado")
            return None
        
        # Analisar dados
        analyzer = DataAnalyzer(df)
        analyzer.exibir_analise_completa()
        
        self._exibir_rodape()
        
        return df
    
    def _exibir_cabecalho(self) -> None:
        """Exibe cabeçalho do programa."""
        print("\n" + "="*80)
        print("PROCESSAMENTO DE ARQUIVOS ODS")
        print("="*80 + "\n")
    
    def _exibir_rodape(self) -> None:
        """Exibe rodapé com próximos passos."""
        print("\n" + "="*80)
        print("PROCESSAMENTO CONCLUIDO!")
        print("="*80)
        print("\nProximo passo:")
        print("   python src/main.py  (carregar no PostgreSQL)")
        print("="*80 + "\n")


def main():
    """
    Função principal do script.
    
    Cria uma instância do ProcessadorODSCLI e executa o processamento.
    """
    cli = ProcessadorODSCLI(data_dir='dados_ida')
    cli.executar()


if __name__ == '__main__':
    main()
