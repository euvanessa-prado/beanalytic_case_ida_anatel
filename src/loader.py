"""
Módulo de carga de dados no PostgreSQL.

Este módulo implementa classes para carregar dados processados no
Data Mart PostgreSQL, incluindo dimensões e tabela fato.

Classes:
    DimensionLoader: Carregador de tabelas dimensão
    FactLoader: Carregador de tabela fato
    DataLoader: Orquestrador principal de carga

Exemplo de uso:
    >>> from loader import DataLoader
    >>> from config import DB_CONFIG
    >>> loader = DataLoader(DB_CONFIG)
    >>> loader.connect()
    >>> loader.load_all(df)
    >>> loader.close()
"""

import psycopg2
import pandas as pd
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """
    Gerenciador de conexão com PostgreSQL.
    
    Esta classe encapsula a lógica de conexão e desconexão do banco,
    implementando context manager para uso seguro.
    
    Attributes:
        db_config (dict): Configurações de conexão
        conn: Objeto de conexão psycopg2
        cursor: Cursor do banco de dados
    """
    
    def __init__(self, db_config: Dict[str, str]):
        """
        Inicializa gerenciador de conexão.
        
        Args:
            db_config (dict): Dicionário com parâmetros de conexão
        """
        self.db_config = db_config
        self.conn = None
        self.cursor = None
    
    def connect(self) -> None:
        """
        Estabelece conexão com o banco de dados.
        
        Raises:
            psycopg2.Error: Se houver erro na conexão
        """
        self.conn = psycopg2.connect(**self.db_config)
        self.cursor = self.conn.cursor()
        self.cursor.execute("SET search_path TO ida, public")
        logger.info("Conectado ao PostgreSQL")
    
    def close(self) -> None:
        """Fecha conexão com o banco de dados."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Conexão fechada")
    
    def commit(self) -> None:
        """Confirma transação."""
        if self.conn:
            self.conn.commit()
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


class DimensionLoader:
    """
    Carregador de tabelas dimensão.
    
    Esta classe é responsável por carregar dados nas tabelas dimensão
    do Data Mart (tempo, grupo econômico, região, serviço).
    
    Attributes:
        cursor: Cursor do banco de dados
        conn: Conexão com banco de dados
    """
    
    def __init__(self, cursor, conn):
        """
        Inicializa o carregador de dimensões.
        
        Args:
            cursor: Cursor do psycopg2
            conn: Conexão do psycopg2
        """
        self.cursor = cursor
        self.conn = conn
    
    def carregar_tempo(self, df: pd.DataFrame) -> int:
        """
        Carrega dimensão tempo.
        
        Extrai períodos únicos do DataFrame e insere na tabela dim_tempo,
        calculando trimestre e semestre automaticamente.
        
        Args:
            df (pd.DataFrame): DataFrame com colunas ano, mes, ano_mes
        
        Returns:
            int: Número de registros carregados
        """
        tempos = df[['ano', 'mes', 'ano_mes']].drop_duplicates()
        
        for _, row in tempos.iterrows():
            trimestre = (row['mes'] - 1) // 3 + 1
            semestre = (row['mes'] - 1) // 6 + 1
            
            self.cursor.execute("""
                INSERT INTO dim_tempo (ano, mes, ano_mes, trimestre, semestre)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (ano_mes) DO NOTHING
            """, (row['ano'], row['mes'], row['ano_mes'], trimestre, semestre))
        
        self.conn.commit()
        logger.info(f"Dimensão tempo carregada: {len(tempos)} registros")
        return len(tempos)
    
    def carregar_grupos(self, df: pd.DataFrame) -> int:
        """
        Carrega dimensão grupo econômico.
        
        Args:
            df (pd.DataFrame): DataFrame com coluna grupo_economico
        
        Returns:
            int: Número de registros carregados
        """
        grupos = df['grupo_economico'].unique()
        
        for grupo in grupos:
            self.cursor.execute("""
                INSERT INTO dim_grupo_economico (nome_grupo)
                VALUES (%s)
                ON CONFLICT (nome_grupo) DO NOTHING
            """, (grupo,))
        
        self.conn.commit()
        logger.info(f"Dimensão grupos carregada: {len(grupos)} registros")
        return len(grupos)
    
    def carregar_regioes(self, df: pd.DataFrame) -> int:
        """
        Carrega dimensão região.
        
        Args:
            df (pd.DataFrame): DataFrame com coluna uf
        
        Returns:
            int: Número de registros carregados
        """
        ufs = df['uf'].unique()
        
        for uf in ufs:
            self.cursor.execute("""
                INSERT INTO dim_regiao (uf)
                VALUES (%s)
                ON CONFLICT (uf) DO NOTHING
            """, (uf,))
        
        self.conn.commit()
        logger.info(f"Dimensão regiões carregada: {len(ufs)} registros")
        return len(ufs)


class FactLoader:
    """
    Carregador de tabela fato.
    
    Esta classe é responsável por carregar dados na tabela fato do Data Mart,
    realizando lookup de chaves estrangeiras nas dimensões.
    
    Attributes:
        cursor: Cursor do banco de dados
        conn: Conexão com banco de dados
        batch_size (int): Tamanho do lote para commit
    """
    
    def __init__(self, cursor, conn, batch_size: int = 100):
        """
        Inicializa o carregador de fatos.
        
        Args:
            cursor: Cursor do psycopg2
            conn: Conexão do psycopg2
            batch_size (int): Número de registros por commit
        """
        self.cursor = cursor
        self.conn = conn
        self.batch_size = batch_size
    
    def carregar_fato(self, df: pd.DataFrame) -> int:
        """
        Carrega tabela fato.
        
        Para cada registro, busca IDs das dimensões e insere na tabela fato.
        Realiza commits em lotes para melhor performance.
        
        Args:
            df (pd.DataFrame): DataFrame com dados a carregar
        
        Returns:
            int: Número de registros carregados
        """
        count = 0
        
        for _, row in df.iterrows():
            # Buscar IDs das dimensões
            id_tempo = self._buscar_id_tempo(row['ano_mes'])
            id_grupo = self._buscar_id_grupo(row['grupo_economico'])
            id_servico = self._buscar_id_servico(row['servico'])
            id_regiao = self._buscar_id_regiao(row['uf'])
            
            # Inserir fato
            self.cursor.execute("""
                INSERT INTO fato_ida (id_tempo, id_grupo, id_servico, id_regiao, 
                                     taxa_resolvidas_5dias, total_solicitacoes, solicitacoes_resolvidas)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (id_tempo, id_grupo, id_servico, id_regiao,
                  row['taxa_resolvidas_5dias'], row['total_solicitacoes'], row['solicitacoes_resolvidas']))
            
            count += 1
            
            if count % self.batch_size == 0:
                self.conn.commit()
                logger.info(f"Carregados {count} registros...")
        
        self.conn.commit()
        logger.info(f"Tabela fato carregada: {count} registros")
        return count
    
    def _buscar_id_tempo(self, ano_mes: str) -> int:
        """Busca ID da dimensão tempo."""
        self.cursor.execute("SELECT id_tempo FROM dim_tempo WHERE ano_mes = %s", (ano_mes,))
        return self.cursor.fetchone()[0]
    
    def _buscar_id_grupo(self, nome_grupo: str) -> int:
        """Busca ID da dimensão grupo econômico."""
        self.cursor.execute("SELECT id_grupo FROM dim_grupo_economico WHERE nome_grupo = %s", (nome_grupo,))
        return self.cursor.fetchone()[0]
    
    def _buscar_id_servico(self, codigo_servico: str) -> int:
        """Busca ID da dimensão serviço."""
        self.cursor.execute("SELECT id_servico FROM dim_servico WHERE codigo_servico = %s", (codigo_servico,))
        return self.cursor.fetchone()[0]
    
    def _buscar_id_regiao(self, uf: str) -> int:
        """Busca ID da dimensão região."""
        self.cursor.execute("SELECT id_regiao FROM dim_regiao WHERE uf = %s", (uf,))
        return self.cursor.fetchone()[0]


class DataLoader:
    """
    Orquestrador principal de carga de dados.
    
    Esta classe coordena o processo completo de carga de dados no Data Mart,
    incluindo dimensões e tabela fato.
    
    Attributes:
        db_config (dict): Configurações de conexão
        connection (DatabaseConnection): Gerenciador de conexão
        dimension_loader (DimensionLoader): Carregador de dimensões
        fact_loader (FactLoader): Carregador de fatos
    
    Example:
        >>> loader = DataLoader(DB_CONFIG)
        >>> loader.connect()
        >>> try:
        ...     loader.load_all(df)
        ... finally:
        ...     loader.close()
    """
    
    def __init__(self, db_config: Dict[str, str]):
        """
        Inicializa o loader.
        
        Args:
            db_config (dict): Dicionário com configurações do banco
        """
        self.db_config = db_config
        self.connection = DatabaseConnection(db_config)
        self.dimension_loader = None
        self.fact_loader = None
    
    def connect(self) -> None:
        """Conecta ao banco de dados e inicializa loaders."""
        self.connection.connect()
        self.dimension_loader = DimensionLoader(self.connection.cursor, self.connection.conn)
        self.fact_loader = FactLoader(self.connection.cursor, self.connection.conn)
    
    def close(self) -> None:
        """Fecha conexão."""
        self.connection.close()
    
    def load_tempo(self, df: pd.DataFrame) -> int:
        """
        Carrega dimensão tempo.
        
        Args:
            df (pd.DataFrame): DataFrame com dados
        
        Returns:
            int: Número de registros carregados
        """
        return self.dimension_loader.carregar_tempo(df)
    
    def load_grupos(self, df: pd.DataFrame) -> int:
        """
        Carrega dimensão grupo econômico.
        
        Args:
            df (pd.DataFrame): DataFrame com dados
        
        Returns:
            int: Número de registros carregados
        """
        return self.dimension_loader.carregar_grupos(df)
    
    def load_regioes(self, df: pd.DataFrame) -> int:
        """
        Carrega dimensão região.
        
        Args:
            df (pd.DataFrame): DataFrame com dados
        
        Returns:
            int: Número de registros carregados
        """
        return self.dimension_loader.carregar_regioes(df)
    
    def load_fato(self, df: pd.DataFrame) -> int:
        """
        Carrega tabela fato.
        
        Args:
            df (pd.DataFrame): DataFrame com dados
        
        Returns:
            int: Número de registros carregados
        """
        return self.fact_loader.carregar_fato(df)
    
    def load_all(self, df: pd.DataFrame) -> None:
        """
        Carrega todas as tabelas (dimensões e fato).
        
        Args:
            df (pd.DataFrame): DataFrame com dados processados
        """
        logger.info("Iniciando carga de dados...")
        
        self.load_tempo(df)
        self.load_grupos(df)
        self.load_regioes(df)
        self.load_fato(df)
        
        logger.info("Carga concluída com sucesso!")

