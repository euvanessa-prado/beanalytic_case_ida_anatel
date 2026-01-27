"""
Módulo de configuração do projeto.

Este módulo centraliza todas as configurações do projeto, incluindo
parâmetros de conexão com banco de dados e diretórios de dados.

Attributes:
    DB_CONFIG (dict): Configurações de conexão com PostgreSQL
    DATA_DIR (str): Diretório contendo arquivos ODS

Exemplo de uso:
    >>> from config import DB_CONFIG, DATA_DIR
    >>> print(DB_CONFIG['database'])
    'ida_datamart'
"""

import os
from typing import Dict


class DatabaseConfig:
    """
    Configurações de conexão com banco de dados PostgreSQL.
    
    Esta classe encapsula as configurações de conexão, permitindo
    sobrescrita através de variáveis de ambiente.
    
    Attributes:
        host (str): Endereço do servidor PostgreSQL
        port (str): Porta de conexão
        database (str): Nome do banco de dados
        user (str): Usuário do banco
        password (str): Senha do usuário
    """
    
    def __init__(self):
        """Inicializa configurações a partir de variáveis de ambiente."""
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = os.getenv('DB_PORT', '5432')
        self.database = os.getenv('DB_NAME', 'ida_datamart')
        self.user = os.getenv('DB_USER', 'postgres')
        self.password = os.getenv('DB_PASSWORD', 'postgres')
    
    def to_dict(self) -> Dict[str, str]:
        """
        Converte configurações para dicionário.
        
        Returns:
            dict: Dicionário com parâmetros de conexão
        """
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password
        }
    
    def __repr__(self) -> str:
        """Representação string da configuração (sem senha)."""
        return (f"DatabaseConfig(host='{self.host}', port='{self.port}', "
                f"database='{self.database}', user='{self.user}')")


class ProjectConfig:
    """
    Configurações gerais do projeto.
    
    Attributes:
        data_dir (str): Diretório com arquivos ODS
        db_config (DatabaseConfig): Configurações do banco de dados
    """
    
    def __init__(self):
        """Inicializa configurações do projeto."""
        self.data_dir = os.getenv('DATA_DIR', 'dados_ida')
        self.db_config = DatabaseConfig()
    
    def get_db_config(self) -> Dict[str, str]:
        """
        Obtém configurações do banco como dicionário.
        
        Returns:
            dict: Configurações de conexão
        """
        return self.db_config.to_dict()


# Instância global de configuração
_config = ProjectConfig()

# Exportar configurações para compatibilidade com código existente
DB_CONFIG = _config.get_db_config()
DATA_DIR = _config.data_dir

