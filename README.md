Passo 1: Criação das Tabelas no MYSQL e do script yahoo_scraper.py para a coleta contínua de dados.

Passo 2: Popular a tabela dim_time_hourly com o script populate_dim_time.py, para facilitar análises temporais.

Passo 3: Rodar o yahoo_scraper.py UMA vez para separar o backfill (dados dos últimos 360 dias) dos dados fresh, já que o script principal foi feito para rodar de forma contínua.
