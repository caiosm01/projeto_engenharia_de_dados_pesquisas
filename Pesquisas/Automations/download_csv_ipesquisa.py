from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
import time
import os

class PesquisaDownloader:
    def __init__(self, driver):
        self.driver = driver
        self.usuario_ipesquisa = os.getenv("usuario_ipesquisa")
        self.senha_ipesquisa = os.getenv("senha_ipesquisa")
        self.contador = 0

    def realizar_login(self):
        """
        Realiza o login no sistema de gestão de pesquisa.
        """
        try:
            self.driver.get("https://sistema.gestaopesquisa.com.br/account/login")
            time.sleep(1)

            WebDriverWait(self.driver, timeout=100).until(lambda d: d.find_element(By.ID, 'UserName')).send_keys(Keys.CONTROL + "a", Keys.BACK_SPACE)
            self.driver.find_element(By.ID, 'UserName').send_keys(self.usuario_ipesquisa)

            WebDriverWait(self.driver, timeout=100).until(lambda d: d.find_element(By.ID, 'Password')).send_keys(Keys.CONTROL + "a", Keys.BACK_SPACE)
            self.driver.find_element(By.ID, 'Password').send_keys(self.senha_ipesquisa)

            WebDriverWait(self.driver, timeout=100).until(lambda d: d.find_element(By.CSS_SELECTOR, 'button.btn')).click()
            print('LOGIN CONCLUÍDO!')

        except Exception as e:
            return f'Erro ao realizar login: {e}'

    def baixar_arquivo(self, id_pesquisa, Arquivo_pasta):
        """
        Baixa um arquivo específico de pesquisa.

        :param id_pesquisa: Nome da pesquisa a ser baixada.
        :param Arquivo_pasta: Caminho do arquivo a ser baixado.
        """
        # Código para baixar o arquivo
        try:

            if os.path.exists(Arquivo_pasta):
                os.remove(Arquivo_pasta)

            tempo_inicial = time.time()
            WebDriverWait(self.driver, timeout=100).until(lambda d: d.find_element(By.XPATH, '//*[@id="side-menu"]/li[3]/a')).click()

            print('ÁREA DAS PESQUISAS ACESSADA COM SUCESSO!')

            WebDriverWait(self.driver, timeout=100).until( lambda d: d.find_element(By.XPATH, '//*[@id="DataTables_Table_0_filter"]/label/input')).send_keys(id_pesquisa)

            WebDriverWait(self.driver, timeout=100).until(lambda d: d.find_element(By.XPATH, '//*[@id="DataTables_Table_0"]/tbody/tr/td[5]/div/a')).click()

            print(f'{self.contador} - ÁREA DE EXPORTAÇÃO ACESSADA COM SUCESSO!')

            WebDriverWait(self.driver, timeout=100).until(lambda d: d.find_element(By.XPATH,'//*[@id="formDashboard"]/div/div/div[1]/div[1]/div/div/div[2]/div[2]/a[1]/span')).click()

            print(f'{self.contador} - ARQUIVO SENDO EXPORTADO!')

            WebDriverWait(self.driver, timeout=100).until(lambda d: d.find_element(By.XPATH,'//*[@id="formDashboard"]/div/div/div/div/div[2]/div[2]/div/button[1]')).click()

            while os.path.exists(Arquivo_pasta) == False:
                time.sleep(1)
                tempo_atual = time.time()
                if tempo_atual - tempo_inicial >= 500:
                    return 'Erro na página'

            print(f'{self.contador} - ARQUIVO EXPORTADO COM SUCESSO!')

            # Retornar um status ou mensagem conforme necessário
        except Exception as e:
            return f'Erro ao baixar arquivo: {e}'
