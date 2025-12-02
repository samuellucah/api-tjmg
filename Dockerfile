# Usa a versão 1.49.0 que é estável e compatível com o requirements.txt
FROM mcr.microsoft.com/playwright/python:v1.49.0-jammy

# Define a pasta de trabalho
WORKDIR /app

# Copia APENAS o requirements primeiro (para o cache ser rápido)
COPY requirements.txt .

# Instala as bibliotecas Python (vai instalar a 1.49.0 travada)
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do código
COPY . .

# Variável de ambiente OBRIGATÓRIA para o Playwright achar o navegador nativo
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Expõe a porta 8000
EXPOSE 8000

# Inicia a API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
