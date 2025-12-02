# ATUALIZADO PARA v1.56.0 (Conforme pedido pelo erro)
FROM mcr.microsoft.com/playwright/python:v1.56.0-jammy

# Define a pasta de trabalho
WORKDIR /app

# Copia APENAS o requirements primeiro (para o cache ser rápido)
COPY requirements.txt .

# Instala as bibliotecas Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do código
COPY . .

# Variável de ambiente para o Playwright achar o navegador nativo da imagem
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Expõe a porta 8000
EXPOSE 8000

# Inicia a API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
