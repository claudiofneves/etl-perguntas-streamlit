# ETL Perguntas — Streamlit (Aula)

Projeto simples para demonstração de **ETL** em sala:
- **E (Extract):** coleta respostas via Streamlit e grava em `data/raw/respostas_raw.csv`
- **T (Transform):** limpeza, padronização, deduplicação e métricas rápidas
- **L (Load):** salva dataset tratado em `data/curated/respostas_curadas.csv` e exibe resultados no app

## Como rodar

### 1) Ambiente
```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
