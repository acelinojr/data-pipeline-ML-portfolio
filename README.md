

Início do projeto: realizarei testes e diagnósticos, usando scripts python, na API **yfinance** para visualizar o formato dos dados e determinar como será a estrutura do scraper e do DW no MySQL.

Passo 1: Criação da DW no MYSQL, elaboração do docker-compose e do script scraper, **yahoo_scraper.py**, para a coleta contínua (hourly) e inserção dos dados transacionais de Cryptocurrency na tabela de dados crus **raw_crypto**. Optei por usar a API pública do **Yahoo Finance** para o scrape dos dados. Considerei o **investing.com** mas como eles não possuem API pública, diferente do Yahoo Finance, escolhi este.




