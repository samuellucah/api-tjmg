# Usa a imagem oficial do Playwright (já inclui Python e dependências de sistema)
FROM mcr.microsoft.com/playwright/python:v1.49.0-jammy

# Define diretório de trabalho
WORKDIR /app

# Copia os arquivos do projeto
COPY . .

# Instala as bibliotecas Python (FastAPI, Uvicorn)
RUN pip install --no-cache-dir -r requirements.txt

# Instala o navegador Chromium dentro do container
RUN playwright install chromium

# Expõe a porta 8000 (mesma usada no seu código)
EXPOSE 8000

# Comando para rodar a API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]