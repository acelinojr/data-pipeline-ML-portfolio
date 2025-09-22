

Início do projeto: realizarei testes e diagnósticos, usando scripts python, na API **yfinance** para determinar como será a estrutura do scraper e do DW no MySQL.

Passo 1: Criação da DW no MYSQL, elaboração do docker-compose e do script scraper, **yahoo_scraper.py**, para a coleta contínua (hourly) e inserção dos dados transacionais de Cryptocurrency na tabela de dados crus **raw_crypto**. Optei por usar a API pública do **Yahoo Finance** para o scrape dos dados. Considerei o **investing.com** mas como eles não possuem API pública, diferente do Yahoo Finance, escolhi esse.

Passo 2: Popular a tabela dim_time_hourly com o script populate_dim_time.py, para facilitar análises temporais.

Passo 3: Rodar o yahoo_scraper.py **UMA** vez para separar o backfill (dados dos últimos 360 dias) dos dados fresh, já que o script principal foi feito para rodar de forma contínua.

**Problemas encontrados:** 

1. Cálculo Incorreto do change_24h_percent - df["change_24h_percent"] = df["Close"].pct_change(24) * 100
Solução: Foi criado o mapeamento INTERVAL_TO_PERIODS_24H para calcular corretamente quantos períodos equivalem a 24h - calculate_24h_change() - que usa o intervalo correto.

2. Não há validação para dados inválidos (NaN, inf, etc.).
Solução: Uso das funções - safe_float() e safe_int() - para tratar NaN e valores inválidos.

3. BTC (Bitcoin) era a única cryptocurrency que estava sendo coletada pelo scraper.
Solução: alteração no docker-compose para incluir mais bitcoins: **TICKERS=BTC-USD,ETH-USD,XRP-USD...etc.**

Diversas análises de estrutura e de logs foram feitas no script scraper, usando prompt engineering; testes rápidos com um run_once foram muito úteis para encontrar problemas:

```from yahoo_scraper import run_once

if __name__ == "__main__":
    
  success = run_once()
    
  print("run_cycle returned:", success)
```
Após corrigir diversos erros no script os dados finalmente chegaram no DW nos formatos esperados.

<img width="871" height="394" alt="image" src="https://github.com/user-attachments/assets/df1f7459-9f6e-497f-bf07-83eea0f9c3c1" />


