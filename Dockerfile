# 1. Usa a imagem base oficial do Playwright (já tem Python e muita coisa de sistema)
FROM mcr.microsoft.com/playwright/python:v1.49.0-jammy

# 2. Define a pasta de trabalho
WORKDIR /app

# 3. Copia seus arquivos para dentro do servidor
COPY . .

# 4. Instala as dependências do Python (O equivalente ao !pip install)
RUN pip install --no-cache-dir -r requirements.txt

# 5. Instala as dependências de SISTEMA (O equivalente aos seus !apt-get do Colab)
# Estamos forçando a instalação daquelas bibliotecas que você listou para garantir.
RUN apt-get update && apt-get install -y \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libatspi2.0-0 \
    libxcomposite1 \
    && rm -rf /var/lib/apt/lists/*

# 6. Instala o navegador Chromium (Equivalente ao !playwright install chromium)
RUN playwright install chromium

# 7. Expõe a porta 8000
EXPOSE 8000

# 8. Comando para iniciar a API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
