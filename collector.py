from flask import Flask, request, Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST, make_wsgi_app
from werkzeug.middleware.dispatcher import DispatcherMiddleware
import json
import time

app = Flask(__name__)

# === MÉTRICAS ESPECÍFICAS PARA NI-FI + SCRAPER ===
NIFI_RECORDS_TOTAL = Counter(
    'nifi_records_total',
    'Total records processados no NiFi',
    ['flow_name', 'status', 'symbol']
)

NIFI_ERRORS_TOTAL = Counter(
    'nifi_errors_total',
    'Erros no NiFi ETL',
    ['flow_name', 'error_type', 'symbol']
)

NIFI_LATENCY_SEC = Histogram(
    'nifi_flow_latency_seconds',
    'Latência de flows NiFi',
    ['flow_name', 'symbol'],
    buckets=(0.1, 0.5, 1.0, 5.0, 10.0, 30.0)
)

# === MÉTRICAS GENÉRICAS ===
API_LATENCY = Histogram(
    'api_transaction_latency_seconds',
    'Latência geral',
    ['endpoint', 'region'],
    buckets=(0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.5, 5.0)
)

REQ_COUNT = Counter(
    'api_transactions_total',
    'Requisições totais',
    ['endpoint', 'status', 'region']
)

@app.route('/ingest', methods=['POST'])
def ingest_nifi_metrics():
    try:
        data = request.get_json(force=True)

        # Campos obrigatórios ou com fallback
        flow_name = data.get('flow_name', 'unknown')
        status = data.get('status', 'unknown')
        region = data.get('regiao_origem', 'NA')
        symbol = data.get('symbol', 'unknown')  # <-- ESSENCIAL PARA CRIPTO

        # Latência (convertida de ms para segundos)
        lat_ms = float(data.get('latencia_ms', 0.0))
        lat_s = lat_ms / 1000.0

        # Contadores
        records_total = int(data.get('records_total', 0))
        errors = int(data.get('errors', 0))
        error_type = data.get('error_type', 'none')

        # Labels comuns para métricas específicas
        labels_nifi = {
            'flow_name': flow_name,
            'symbol': symbol
        }

        # Incrementa métricas específicas
        if records_total > 0:
            NIFI_RECORDS_TOTAL.labels(
                flow_name=flow_name,
                status=status,
                symbol=symbol
            ).inc(records_total)

        if errors > 0:
            NIFI_ERRORS_TOTAL.labels(
                flow_name=flow_name,
                error_type=error_type,
                symbol=symbol
            ).inc(errors)

        NIFI_LATENCY_SEC.labels(
            flow_name=flow_name,
            symbol=symbol
        ).observe(lat_s)

        # Métricas genéricas (opcional, mantém compatibilidade)
        endpoint = data.get('endpoint', '/nifi_ingest')
        API_LATENCY.labels(endpoint=endpoint, region=region).observe(lat_s)
        REQ_COUNT.labels(endpoint=endpoint, status=status, region=region).inc()

        return {'status': 'ok', 'timestamp': time.time()}, 200

    except Exception as e:
        # Registra erro de parsing
        NIFI_ERRORS_TOTAL.labels(
            flow_name='ingest',
            error_type='parse_error',
            symbol='unknown'
        ).inc()
        return {'error': str(e)}, 400


@app.route('/health')
def health():
    return {'status': 'healthy'}, 200


@app.route('/metrics')
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


# Monta /metrics via DispatcherMiddleware (opcional, mas útil se for expandir)
app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
    '/metrics': make_wsgi_app()
})


if __name__ == '__main__':
    # CORRIGIDO: host='0.0.0.0' (não '0.0.0')
    app.run(host='0.0.0.0', port=9000, debug=False)