"""
KAFKA CONSUMER — Monitoreo en tiempo real
Fuerza IPv4 para compatibilidad con Kafka en Docker
"""
import socket
_orig = socket.getaddrinfo
def _ipv4_only(host, port, family=0, type=0, proto=0, flags=0):
    return _orig(host, port, socket.AF_INET, type, proto, flags)
socket.getaddrinfo = _ipv4_only

from kafka import KafkaConsumer
import json

KAFKA_SERVER = 'localhost:9092'
TOPIC        = 'metricas_incidentes'

def formatear_metrica(msg):
    m = msg['metrica']
    if m == 'total_incidentes':
        print(f"\n{'='*50}")
        print(f"TOTAL INCIDENTES EN EL DW: {msg['valor']:,}")
        print(f"Timestamp: {msg['timestamp']}")
    elif m == 'festivos_vs_normales':
        total = msg['dias_festivos'] + msg['dias_normales']
        pct = (msg['dias_festivos'] / total * 100) if total > 0 else 0
        print(f"\n{'='*50}")
        print(f"FESTIVOS vs NORMALES")
        print(f"  Festivos: {msg['dias_festivos']:,} ({pct:.1f}%)")
        print(f"  Normales: {msg['dias_normales']:,} ({100-pct:.1f}%)")
    elif m == 'top_comunas':
        print(f"\n{'='*50}")
        print(f"TOP 5 COMUNAS")
        for i, item in enumerate(msg['ranking'], 1):
            print(f"  {i}. {item['comuna']}: {item['total']:,}")
    elif m == 'distribucion_gravedad':
        print(f"\n{'='*50}")
        print(f"DISTRIBUCION POR GRAVEDAD")
        for item in msg['data']:
            print(f"  {item['gravedad']}: {item['total']:,}")

if __name__ == '__main__':
    print("=" * 50)
    print("KAFKA CONSUMER iniciado (IPv4 forzado)")
    print(f"Topic: {TOPIC}")
    print("Presiona Ctrl+C para detener")
    print("=" * 50)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='monitor_v2'
    )

    try:
        for message in consumer:
            if 'metrica' in message.value:
                formatear_metrica(message.value)
    except KeyboardInterrupt:
        print("\nConsumer detenido.")
    finally:
        consumer.close()
