# Satış Raporlama ve LLM Analiz Uygulaması

Bu proje, çok formatlı satış raporlarını normalize edip DuckDB üzerinde saklayan, hızlı analitik hesaplamalar yapan ve LLM destekli içgörüler sunan uçtan uca bir uygulamadır.

## Dizim
```
sales-llm-app/
├─ app.py
├─ backend/
│  ├─ main.py
│  ├─ routers/
│  ├─ services/
│  ├─ db/
│  └─ models/
├─ prompts/
├─ data/
│  ├─ uploads/
│  └─ cache/
├─ logs/
├─ tests/
├─ Dockerfile
└─ docker-compose.yml
```

## Kurulum & Çalıştırma

1. Depoyu klonlayın ve bağımlılıkları kurun:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r sales-llm-app/requirements.txt
   ```
2. Ortam değişkenleri için `.env.example` dosyasını `.env` olarak kopyalayın ve gerekli anahtarları doldurun.
3. FastAPI arka ucunu çalıştırın:
   ```bash
   uvicorn backend.main:app --host 0.0.0.0 --port 8000
   ```
4. Ayrı bir terminalde Streamlit arayüzünü başlatın:
   ```bash
   streamlit run app.py
   ```
5. Alternatif olarak Docker kullanın:
   ```bash
   cd sales-llm-app
   docker compose up --build
   ```

## Testler
```bash
pytest sales-llm-app/tests
```

## Notlar
- LLM entegrasyonu sağlayıcıdan bağımsızdır; `LLM_PROVIDER` değişkenini kullanarak OpenAI, Anthropic veya mock modları seçebilirsiniz.
- Uygulama, yüklenen dosyaları `data/uploads/` içinde saklar ve analiz sonuçlarını `logs/app.log` dosyasına yazar.

