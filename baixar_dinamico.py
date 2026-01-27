"""
Módulo para extração dinâmica de arquivos ODS do portal dados.gov.br.

Este módulo implementa um extrator que lê dinamicamente os recursos disponíveis
no portal ANATEL através de web scraping, identificando automaticamente os arquivos
ODS disponíveis e realizando o download.

Classes:
    RecursoPortal: Representa um recurso identificado no portal
    PortalExtractor: Extrator de recursos do portal usando Playwright
    ODSDownloader: Gerenciador de download de arquivos ODS
    AnatelScraper: Orquestrador principal do processo de extração

Exemplo de uso:
    >>> scraper = AnatelScraper(output_dir='dados_ida')
    >>> arquivos = scraper.executar()
    >>> print(f"Baixados: {len(arquivos)} arquivos")
"""

from playwright.sync_api import sync_playwright
import requests
import logging
from pathlib import Path
from typing import List, Optional, Tuple
from dataclasses import dataclass
import re
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class RecursoPortal:
    """
    Representa um recurso identificado no portal dados.gov.br.
    
    Attributes:
        titulo (str): Título completo do recurso
        servico (str): Tipo de serviço (SCM, SMP ou STFC)
        ano (int): Ano de referência dos dados
        filename (str): Nome do arquivo ODS correspondente
    """
    titulo: str
    servico: str
    ano: int
    filename: str


class PortalExtractor:
    """
    Extrator de recursos do portal dados.gov.br usando Playwright.
    
    Esta classe é responsável por acessar o portal, expandir a seção de recursos
    e extrair informações dos títulos disponíveis.
    
    Attributes:
        url_portal (str): URL do portal de dados
        pattern (re.Pattern): Padrão regex para extração de serviço e ano
    """
    
    URL_PORTAL = "https://dados.gov.br/dados/conjuntos-dados/indice-desempenho-atendimento"
    TITULO_PATTERN = re.compile(r'(SCM|SMP|STFC)\s*-\s*(\d{4})')
    
    def __init__(self):
        """Inicializa o extrator de portal."""
        self.url_portal = self.URL_PORTAL
        self.pattern = self.TITULO_PATTERN
    
    def extrair_info_titulo(self, titulo: str) -> Tuple[Optional[str], Optional[int]]:
        """
        Extrai tipo de serviço e ano do título do recurso.
        
        Args:
            titulo (str): Título do recurso no formato 
                         "Índice de Desempenho no Atendimento - SCM - 2015"
        
        Returns:
            tuple: (servico, ano) onde servico é str e ano é int,
                   ou (None, None) se não conseguir extrair
        
        Examples:
            >>> extractor = PortalExtractor()
            >>> extractor.extrair_info_titulo("IDA - SCM - 2015")
            ('SCM', 2015)
        """
        try:
            match = self.pattern.search(titulo)
            if match:
                servico = match.group(1)
                ano = int(match.group(2))
                return servico, ano
            return None, None
        except Exception as e:
            logger.error(f"Erro ao extrair info do título: {e}")
            return None, None
    
    def extrair_recursos(self) -> List[RecursoPortal]:
        """
        Acessa o portal e extrai a lista de recursos disponíveis.
        
        Utiliza Playwright para navegar no portal, expandir a seção de recursos
        e extrair informações dos títulos h4 disponíveis.
        
        Returns:
            list: Lista de objetos RecursoPortal identificados
        
        Raises:
            Exception: Se houver erro ao acessar o portal
        """
        recursos = []
        
        logger.info("="*80)
        logger.info("EXTRAÇÃO DINÂMICA DE RECURSOS - PORTAL DADOS.GOV.BR")
        logger.info("="*80)
        
        with sync_playwright() as p:
            logger.info("\n🌐 Abrindo navegador...")
            
            browser = p.chromium.launch(headless=False)
            context = browser.new_context()
            page = context.new_page()
            
            try:
                # Acessar portal
                logger.info(f"\n📄 Acessando: {self.url_portal}")
                page.goto(self.url_portal, wait_until='domcontentloaded', timeout=60000)
                time.sleep(5)
                
                # Expandir recursos
                self._expandir_recursos(page)
                
                # Buscar títulos
                titulos = page.locator('h4[data-v-444d5111]').all()
                logger.info(f"✅ Encontrados {len(titulos)} títulos\n")
                
                # Processar cada título
                recursos = self._processar_titulos(titulos)
                
                logger.info(f"\n✅ Total de recursos identificados: {len(recursos)}")
                
            except Exception as e:
                logger.error(f"❌ Erro ao acessar portal: {e}")
                
            finally:
                context.close()
                browser.close()
        
        return recursos
    
    def _expandir_recursos(self, page) -> None:
        """
        Expande a seção 'Recursos' no portal.
        
        Args:
            page: Objeto Page do Playwright
        """
        logger.info("\n📂 Expandindo 'Recursos'...")
        try:
            recursos_btn = page.locator('button:has-text("Recursos")').first
            if recursos_btn.count() > 0:
                recursos_btn.click()
                time.sleep(3)
                logger.info("✅ Recursos expandido")
        except:
            logger.info("ℹ️ Recursos já expandido")
    
    def _processar_titulos(self, titulos) -> List[RecursoPortal]:
        """
        Processa lista de elementos de título e extrai informações.
        
        Args:
            titulos: Lista de elementos Locator do Playwright
        
        Returns:
            list: Lista de objetos RecursoPortal
        """
        recursos = []
        
        logger.info("\n🔍 Processando títulos dos recursos...")
        
        for i, titulo_elem in enumerate(titulos, 1):
            try:
                titulo_texto = titulo_elem.inner_text()
                logger.info(f"[{i}] {titulo_texto}")
                
                servico, ano = self.extrair_info_titulo(titulo_texto)
                
                if servico and ano:
                    recurso = RecursoPortal(
                        titulo=titulo_texto,
                        servico=servico,
                        ano=ano,
                        filename=f"{servico}{ano}.ods"
                    )
                    recursos.append(recurso)
                    logger.info(f"    → {recurso.filename}")
                else:
                    logger.warning(f"    ⚠️ Não conseguiu extrair info")
                
            except Exception as e:
                logger.error(f"    ❌ Erro: {e}")
                continue
        
        return recursos


class ODSDownloader:
    """
    Gerenciador de download de arquivos ODS do portal ANATEL.
    
    Esta classe é responsável por realizar o download dos arquivos ODS
    identificados, validar o formato e salvar no diretório de destino.
    
    Attributes:
        base_url (str): URL base para download dos arquivos
        output_dir (Path): Diretório de destino dos arquivos
    """
    
    BASE_URL = "https://www.anatel.gov.br/dadosabertos/PDA/IDA/"
    ODS_SIGNATURE = b'PK'  # Arquivos ODS são ZIP (começam com PK)
    
    def __init__(self, output_dir: str = 'dados_ida'):
        """
        Inicializa o downloader.
        
        Args:
            output_dir (str): Caminho do diretório de destino
        """
        self.base_url = self.BASE_URL
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def baixar_arquivo(self, recurso: RecursoPortal) -> Optional[str]:
        """
        Baixa um arquivo ODS do portal.
        
        Args:
            recurso (RecursoPortal): Objeto com informações do recurso
        
        Returns:
            str: Nome do arquivo baixado, ou None se falhar
        
        Raises:
            requests.RequestException: Se houver erro na requisição HTTP
        """
        try:
            url = f"{self.base_url}{recurso.filename}"
            
            logger.info(f"   📥 Baixando: {recurso.filename}")
            logger.info(f"      URL: {url}")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Validar formato ODS
            if not self._validar_ods(response.content):
                logger.warning(f"   ⚠️ Não é ODS válido")
                return None
            
            # Salvar arquivo
            filepath = self.output_dir / recurso.filename
            self._salvar_arquivo(filepath, response.content)
            
            size_mb = len(response.content) / (1024 * 1024)
            logger.info(f"   ✅ Salvo: {recurso.filename} ({size_mb:.2f} MB)")
            
            return recurso.filename
            
        except Exception as e:
            logger.error(f"   ❌ Erro: {str(e)[:50]}")
            return None
    
    def _validar_ods(self, content: bytes) -> bool:
        """
        Valida se o conteúdo é um arquivo ODS válido.
        
        Args:
            content (bytes): Conteúdo do arquivo
        
        Returns:
            bool: True se for ODS válido, False caso contrário
        """
        return content.startswith(self.ODS_SIGNATURE)
    
    def _salvar_arquivo(self, filepath: Path, content: bytes) -> None:
        """
        Salva conteúdo em arquivo.
        
        Args:
            filepath (Path): Caminho completo do arquivo
            content (bytes): Conteúdo a ser salvo
        """
        with open(filepath, 'wb') as f:
            f.write(content)


class AnatelScraper:
    """
    Orquestrador principal do processo de extração de dados ANATEL.
    
    Esta classe coordena o processo completo de extração: identificação de
    recursos no portal e download dos arquivos ODS correspondentes.
    
    Attributes:
        extractor (PortalExtractor): Extrator de recursos do portal
        downloader (ODSDownloader): Gerenciador de downloads
    
    Example:
        >>> scraper = AnatelScraper(output_dir='dados_ida')
        >>> arquivos = scraper.executar()
        >>> print(f"Total baixado: {len(arquivos)}")
    """
    
    def __init__(self, output_dir: str = 'dados_ida'):
        """
        Inicializa o scraper.
        
        Args:
            output_dir (str): Diretório de destino dos arquivos
        """
        self.extractor = PortalExtractor()
        self.downloader = ODSDownloader(output_dir)
    
    def executar(self) -> List[str]:
        """
        Executa o processo completo de extração e download.
        
        Returns:
            list: Lista com nomes dos arquivos baixados com sucesso
        """
        # Extrair recursos do portal
        recursos = self.extractor.extrair_recursos()
        
        if not recursos:
            logger.error("\n❌ Nenhum recurso foi identificado!")
            return []
        
        # Baixar arquivos
        logger.info("\n" + "="*80)
        logger.info("BAIXANDO ARQUIVOS")
        logger.info("="*80 + "\n")
        
        arquivos_baixados = []
        
        for i, recurso in enumerate(recursos, 1):
            logger.info(f"\n[{i}/{len(recursos)}] {recurso.titulo}")
            logger.info("-"*60)
            
            filename = self.downloader.baixar_arquivo(recurso)
            
            if filename:
                arquivos_baixados.append(filename)
            
            time.sleep(0.5)
        
        # Resultado
        logger.info("\n" + "="*80)
        logger.info(f"✅ DOWNLOAD CONCLUÍDO!")
        logger.info(f"   Arquivos baixados: {len(arquivos_baixados)}/{len(recursos)}")
        logger.info("="*80)
        
        return arquivos_baixados


def main():
    """
    Função principal para execução do script.
    
    Cria uma instância do AnatelScraper e executa o processo de extração.
    
    Returns:
        list: Lista de arquivos baixados
    """
    scraper = AnatelScraper(output_dir='dados_ida')
    return scraper.executar()


if __name__ == '__main__':
    print("\n" + "="*80)
    print("EXTRATOR DINÂMICO DE ARQUIVOS ODS - ANATEL")
    print("Lê recursos disponíveis no portal automaticamente")
    print("="*80 + "\n")
    
    arquivos = main()
    
    print("\n" + "="*80)
    if arquivos:
        print(f"✅ SUCESSO! {len(arquivos)} ARQUIVOS BAIXADOS")
        print("="*80)
        print("\n📁 Arquivos baixados:\n")
        for i, arq in enumerate(sorted(arquivos), 1):
            print(f"  {i:2d}. {arq}")
        print(f"\n📂 Pasta: dados_ida/")
        print("\n💡 Próximos passos:")
        print("   1. python processar_ods.py")
        print("   2. python src/main.py")
    else:
        print("❌ NENHUM ARQUIVO FOI BAIXADO")
    print("="*80 + "\n")
