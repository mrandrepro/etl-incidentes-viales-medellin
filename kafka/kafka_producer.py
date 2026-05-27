"""
KAFKA PRODUCER — Metricas de incidentes viales
"""
import socket
_orig = socket.getaddrinfo
def _ipv4(h, p, f=0, t=0, pr=0, fl=0): return _orig(h, p, socket.AF_INET, t, pr, fl)
socket.getaddrinfo = _ipv4

from kafka import KafkaProducer
from sqlalchemy import create_engine, text
import json
import time
from datetime import datetime

DB_URL       = 'mysql+pymysql://root:@localhost:3306/dw_incidentes_v2'
KAFKA_SERVER = 'localhost:9092'
TOPIC        = 'metricas_incidentes'

if __name__ == '__main__':
    print("=" * 50)
    print("KAFKA PRODUCER iniciado")
    print(f"Topic: {TOPIC} | Servidor: {KAFKA_SERVER}")
    print("=" * 50)

    engine = create_engine(DB_URL)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        max_block_ms=5000
    )
    print("Kafka y MySQL OK — publicando cada 10s")
    print("=" * 50)

    while True:
        try:
            ts = datetime.now().isoformat()
            with engine.connect() as conn:
                total = int(conn.execute(text("SELECT COUNT(*) FROM fact_incidente")).fetchone()[0])
                producer.send(TOPIC, value={'metrica': 'total_incidentes', 'valor': total, 'timestamp': ts})
                producer.flush(timeout=5)
                print(f"[{ts}] total_incidentes enviado")

                rows = conn.execute(text("""
                    SELECT t.es_festivo, COUNT(*) as total FROM fact_incidente f
                    JOIN dim_tiempo t ON f.sk_tiempo = t.sk_tiempo GROUP BY t.es_festivo
                """)).fetchall()
                data = {str(r[0]): int(r[1]) for r in rows}
                producer.send(TOPIC, value={'metrica': 'festivos_vs_normales',
                    'dias_festivos': data.get('1', 0), 'dias_normales': data.get('0', 0), 'timestamp': ts})
                producer.flush(timeout=5)
                print(f"[{ts}] festivos_vs_normales enviado")

                rows = conn.execute(text("""
                    SELECT u.comuna, COUNT(*) as total FROM fact_incidente f
                    JOIN dim_ubicacion u ON f.sk_ubicacion = u.sk_ubicacion
                    WHERE u.comuna != 'DESCONOCIDO'
                    GROUP BY u.comuna ORDER BY total DESC LIMIT 5
                """)).fetchall()
                producer.send(TOPIC, value={'metrica': 'top_comunas',
                    'ranking': [{'comuna': r[0], 'total': int(r[1])} for r in rows], 'timestamp': ts})
                producer.flush(timeout=5)
                print(f"[{ts}] top_comunas enviado")

                rows = conn.execute(text("""
                    SELECT a.gravedad_accidente, COUNT(*) as total FROM fact_incidente f
                    JOIN dim_accidente a ON f.sk_accidente = a.sk_accidente
                    GROUP BY a.gravedad_accidente ORDER BY total DESC
                """)).fetchall()
                producer.send(TOPIC, value={'metrica': 'distribucion_gravedad',
                    'data': [{'gravedad': r[0], 'total': int(r[1])} for r in rows], 'timestamp': ts})
                producer.flush(timeout=5)
                print(f"[{ts}] distribucion_gravedad enviado")

            print(f"--- 4 metricas publicadas. Esperando 10s ---")
            time.sleep(10)

        except KeyboardInterrupt:
            print("\nProducer detenido.")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)
