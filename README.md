# IoT Greenhouse MVP (CSV → MQTT → Edge → SQLite → Plot)

Bu repo, greenhouse.csv içeriğini bir IoT cihazı gibi MQTT üzerinden yayınlamak için bir **simulator** içerir.

## Hızlı Başlangıç (Otomatik)

Proje yönetimini kolaylaştırmak için `runner.py` scripti hazırlanmıştır.

### 1. Kurulum ve Hazırlık
Sanal ortam oluşturup bağlantıları yükleyin:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
python runner.py install
```

### 2. Veri Tabanını Temizleme (Opsiyonel)
Temiz bir başlangıç yapmak isterseniz eski veritabanını silebilirsiniz:
```bash
python runner.py clean
```

### 3. Çalıştırma
Projenin farklı parçalarını çalıştırmak için aşağıdaki komutları ayrı terminallerde çalıştırabilirsiniz.

**Adım 1: Edge Processor (Veriyi işler ve kayerder)**
MQTT'den gelen veriyi dinler ve veritabanına yazar.
```bash
python runner.py edge
```

**Adım 2: Simulator (Veri üretir)**
Sensör verisi üretip MQTT'ye gönderir.
```bash
python runner.py sim
```

**Adım 3: Live Dashboard (Canlı İzleme)**
Veritabanına yazılan veriyi anlık olarak gösterir.
```bash
python runner.py dash
```

**Adım 4: Plot (Grafik)**
Biriken veriyi grafik olarak çizer (En az 50-100 veri biriktikten sonra çalıştırın).
```bash
python runner.py plot
```

**Adım 5: Web Dashboard (Streamlit)**
Modern ve interaktif web arayüzü. Tarayıcıda açılır.
```bash
python runner.py web
```

---

## Manuel Çalıştırma (Eski Yöntem)
Dilerseniz script kullanmadan da çalıştırabilirsiniz:

**Kurulum:** `pip install -r requirements.txt`

**Temizleme:** `rm -f storage/iot.db*`

**Edge:** `python -m edge.edge_processor --db storage/iot.db --achp-threshold 50.0`

**Sim:** `python simulator/simulator.py --interval 1`

**Dash:** `python visualize/live_dashboard.py --db storage/iot.db --device-id gh_01 --refresh 1`
