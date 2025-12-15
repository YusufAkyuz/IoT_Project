# IoT Greenhouse MVP (CSV → MQTT → Edge → SQLite → Plot)

Bu repo, greenhouse.csv içeriğini bir IoT cihazı gibi MQTT üzerinden yayınlamak için bir **simulator** içerir.

## Kurulum
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate

pip install -r requirements.txt
```

## MQTT Broker (Mosquitto)
Broker zaten yoksa, yerelde Mosquitto çalıştırın (veya Docker ile 1883 portunu açın).

Abone (mesajları görmek için):
```bash
mosquitto_sub -t greenhouse/telemetry -v
```

## Simulator çalıştırma
```bash
python simulator/simulator.py --host localhost --port 1883 --interval 1
```

İlk 20 satır ile hızlı test:
```bash
python simulator/simulator.py --rows 20 --interval 0.2
```

Not: CSV'deki `Class` alanı, stabil bir şekilde tamsayıya encode edilir (örn. SA→0).
