# IoT Greenhouse MVP (CSV → MQTT → Edge → SQLite → Plot)

Bu repo, greenhouse.csv içeriğini bir IoT cihazı gibi MQTT üzerinden yayınlamak için bir **simulator** içerir.

## Kurulum
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate

pip install -r requirements.txt
```
## Veri tabanını silme
Veri tabanını silelim temiz bir görüntü için.

Komut
```bash
rm -f storage/iot.db storage/iot.db-wal storage/iot.db-shm
```

## Edge Layer
Edge işlemini başlatıp DB oluşturma işlemini tamamalayalım.

Komut:
```bash
python -m edge.edge_processor --db storage/iot.db --achp-threshold 50.0
```

## Simulator çalıştırma(Publisher)
```bash
python simulator/simulator.py --interval 1
```
MQTT’ye her 1 saniyede bir JSON mesajı gönderir
Edge terminalinde insert logları akmaya başlar

## MQTT Broker (Mosquitto)
Broker ile akışı görüntüleriz.

Abone (mesajları görmek için):
```bash
mosquitto_sub -t greenhouse/telemetry -v
```

## Terminal Sekmesi — Live Dashboard (SQLite canlı tablo/KPI)
Bu sekme sunumda “en görünür” çıktıyı verir: veri aktıkça tablo/KPI güncellenir.

```bash
python visualize/live_dashboard.py --db storage/iot.db --device-id gh_01 --refresh 1
```

## Terminal Sekmesi — Plot (SQLite → Matplotlib)
Plot’u, en az 50–100 satır biriktikten sonra açmak daha anlamlıdır.
```bash
python visualize/plot.py --db storage/iot.db --limit 300
```
