import asyncio
import json
import logging
import websockets
from kafka import KafkaProducer

# 1. LOGLAMA YAPILANDIRMASI
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 2. SABİT DEĞERLER (Bybit Public Linear Stream)
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'crypto-market-data'
BYBIT_WS_URL = 'wss://stream.bybit.com/v5/public/linear'

def create_kafka_producer():
    """Kafka bağlantısını kurar ve producer objesini döndürür."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info(f"Kafka'ya baglanildi: {KAFKA_BROKER}")
        return producer
    except Exception as e:
        logger.error(f"Kafka baglanti hatasi: {e}")
        raise

async def bybit_to_kafka(producer):
    """Bybit WebSocket'i dinler ve veriyi Kafka'ya basar."""
    while True:
        try:
            async with websockets.connect(BYBIT_WS_URL) as ws:
                logger.info(f"Bybit WebSocket'e baglanildi: {BYBIT_WS_URL}")
                
                # Bybit bizden hangi veriyi istediğimizi söylememizi (Subscribe) bekler
                subscribe_msg = {
                    "op": "subscribe",
                    "args": ["publicTrade.BTCUSDT"]
                }
                await ws.send(json.dumps(subscribe_msg))
                logger.info("BTCUSDT Trade kanalina abone olundu, veri bekleniyor...")
                
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    
                    # Gelen mesaj bir trade verisi ise (Ping/Pong veya Info mesajı değilse)
                    if "topic" in data and data["topic"] == "publicTrade.BTCUSDT":
                        for trade in data["data"]:
                            # Bybit JSON Formatını Standartlaştırıyoruz
                            payload = {
                                "symbol": trade["s"],
                                "price": float(trade["p"]),
                                "quantity": float(trade["v"]),
                                "timestamp": trade["T"]
                            }
                            
                            producer.send(KAFKA_TOPIC, value=payload)
                            logger.info(f"Kafka'ya Gonderildi -> {payload['symbol']} | Fiyat: {payload['price']} | Hacim: {payload['quantity']}")
                            
        except websockets.exceptions.ConnectionClosed:
            logger.warning("Baglanti koptu! 5 saniye icinde yeniden baglaniliyor...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Beklenmeyen bir hata olustu: {e}. 5 saniye icinde yeniden deneniyor...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    logger.info("Kripto Veri Ureticisi (Producer) baslatiliyor...")
    try:
        kafka_producer = create_kafka_producer()
        asyncio.run(bybit_to_kafka(kafka_producer))
    except KeyboardInterrupt:
        logger.info("Kullanici tarafindan sistem durduruldu.")
    except Exception as e:
        logger.critical(f"Sistem kritik hata ile coktu: {e}")
