# 1. Usa a imagem oficial que JÁ CONTÉM Python, Chromium e dependências
# Isso evita o download gigante na hora do deploy
FROM mcr.microsoft.com/playwright/python:v1.49.0-jammy

# 2. Define a pasta de trabalho
WORKDIR /app

# 3. Copia APENAS o requirements primeiro (para o cache ser rápido)
COPY requirements.txt .

# 4. Instala as bibliotecas Python
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copia o restante do código
COPY . .

# 6. Define a variável para o Playwright achar o navegador nativo
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# 7. Expõe a porta 8000
EXPOSE 8000

# 8. Inicia a API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
