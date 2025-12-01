import re
import time
import asyncio
import nest_asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query, HTTPException
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError, Page

# URL TJMG
URL = "https://pje-consulta-publica.tjmg.jus.br/"

# Regex CNJ (Formato: 0000000-00.0000.0.00.0000)
CNJ_RE = re.compile(r"\b\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}\b")

# Filtro de ruídos
UNWANTED_RE = re.compile(
    r"(documentos?\s+juntados|documento\b|certid[aã]o|visualizar|"
    r"pjeoffice|indispon[ií]vel|aplicativo\s+pjeoffice|"
    r"página\b|resultados?\s+encontrados|recibo)",
    re.IGNORECASE,
)

def _norm(txt: str) -> str:
    return re.sub(r"\s+", " ", (txt or "")).strip()

def sanitize_id(val: str) -> str:
    return re.sub(r"\D+", "", val or "")

SEMA = asyncio.Semaphore(1)
CACHE_TTL = 300
_cache: Dict[str, Dict[str, Any]] = {}

app = FastAPI(title="PJe TJMG - Consulta Pública (Fixed Input)")

# ==============================================================================
# FUNÇÕES DE NAVEGAÇÃO
# ==============================================================================

async def wait_pje_loading(page: Page):
    """Aguarda os carregamentos chatos do PJe (RichFaces/JSF)."""
    try:
        # Espera inicial para animações
        await page.wait_for_timeout(500)
        
        # Lista de bloqueadores visuais
        blockers = [
            ".ui-widget-overlay", 
            ".ui-blockui", 
            "[class*='loading' i]", 
            "[class*='spinner' i]",
            "div[id*='status']" # Status Ajax
        ]
        
        for sel in blockers:
            if await page.locator(sel).count() > 0:
                try:
                    el = page.locator(sel).first
                    if await el.is_visible():
                        await el.wait_for(state="hidden", timeout=10000)
                except: pass
    except: pass

async def open_process_popup(page: Page, clickable):
    """Clica no link e pega a popup."""
    try:
        async with page.expect_popup(timeout=15000) as pop:
            try:
                # Tenta scrollar até o elemento para garantir visibilidade
                await clickable.scroll_into_view_if_needed()
                await clickable.click(timeout=3000)
            except:
                await clickable.dispatch_event("click")
        
        popup = await pop.value
        await popup.wait_for_load_state("domcontentloaded")
        return popup
    except:
        return None

async def extract_data(popup: Page, numero: str) -> Dict[str, Any]:
    """Extrai dados do popup."""
    # 1. Clica na aba de movimentações se existir
    try:
        tab = popup.locator("text=/Movimenta(ç|c)ões/i").first
        if await tab.is_visible():
            await tab.click()
            await popup.wait_for_timeout(500)
    except: pass

    # 2. Metadados
    meta = {
        "assunto": None, "classe_judicial": None, "data_distribuicao": None, 
        "orgao_julgador": None, "jurisdicao": None
    }
    try:
        body = await popup.locator("body").inner_text()
        lines = [_norm(ln) for ln in body.split('\n') if ln.strip()]
        
        map_keys = {
            "assunto": ["assunto", "assunto(s)"],
            "classe_judicial": ["classe judicial", "classe"],
            "data_distribuicao": ["data da distribuição", "distribuição"],
            "orgao_julgador": ["órgão julgador", "orgao julgador"],
            "jurisdicao": ["jurisdição", "comarca"]
        }

        for field, keywords in map_keys.items():
            for i, line in enumerate(lines):
                lower_line = line.lower()
                if any(k in lower_line for k in keywords):
                    parts = re.split(r"[:\-]\s*", line, maxsplit=1)
                    if len(parts) > 1 and len(parts[1]) > 3:
                        meta[field] = parts[1].strip()
                        break
                    if i + 1 < len(lines):
                        next_line = lines[i+1]
                        if len(next_line) > 3 and not UNWANTED_RE.search(next_line):
                            meta[field] = next_line
                            break
    except: pass

    # 3. Movimentações
    movs = []
    seen = set()
    selectors = ["tbody[id*='moviment'] tr", "table[class*='moviment'] tr", ".rich-table-row"]
    
    for sel in selectors:
        rows = popup.locator(sel)
        if await rows.count() > 0:
            count = await rows.count()
            for i in range(min(count, 50)): # Limite de 50 movs
                txt = _norm(await rows.nth(i).inner_text())
                if txt and not UNWANTED_RE.search(txt) and txt not in seen:
                    seen.add(txt)
                    movs.append(txt)
            break
            
    return {"numero": numero, **meta, "movimentacoes": movs}

# ==============================================================================
# LÓGICA PRINCIPAL (SCRAPER)
# ==============================================================================

async def scrape_pje(doc_digits: str, doc_type: str) -> Dict[str, Any]:
    result = {
        "documento": doc_digits,
        "tipo": doc_type,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "processos": [],
    }

    async with async_playwright() as p:
        # User-Agent é vital para não ser bloqueado
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768}
        )
        page = await context.new_page()

        try:
            print(f"Acessando TJMG para {doc_type} {doc_digits}...")
            await page.goto(URL, wait_until="domcontentloaded")
            await wait_pje_loading(page)

            # === 1. SELEÇÃO DO TIPO (CRÍTICO) ===
            # Clica no Radio e ESPERA o input recarregar
            try:
                if doc_type.upper() == "CNPJ":
                    await page.locator("input[type='radio'][value='CNPJ'], label:has-text('CNPJ')").first.click()
                else:
                    await page.locator("input[type='radio'][value='CPF'], label:has-text('CPF')").first.click()
                
                # Espera crítica para o site trocar o campo de texto
                await page.wait_for_timeout(1000)
            except Exception as e:
                print(f"Erro ao selecionar radio: {e}")

            # === 2. LOCALIZAR O INPUT CORRETO ===
            # Estratégia: O input correto é aquele que está visível E não é de 'nome'/'oab'
            target_input = None
            
            inputs = page.locator("input[type='text']:visible")
            count = await inputs.count()
            
            for i in range(count):
                inp = inputs.nth(i)
                id_attr = (await inp.get_attribute("id") or "").lower()
                placeholder = (await inp.get_attribute("placeholder") or "").lower()
                
                # Blacklist: Se tiver essas palavras, NÃO é o campo que queremos
                blacklist = ["nome", "advogado", "oab", "classe", "vara", "processo"]
                if any(b in id_attr for b in blacklist) or any(b in placeholder for b in blacklist):
                    continue
                
                # Se achou um input limpo, usa ele
                target_input = inp
                break
            
            if not target_input:
                # Fallback: Tenta achar pelo label próximo
                target_input = page.locator("td:has-text('CPF'), td:has-text('CNPJ')").locator("xpath=..//input").first

            if not target_input:
                print("Input não encontrado!")
                raise HTTPException(status_code=500, detail="input_nao_encontrado_na_tela")

            # === 3. PREENCHER E PESQUISAR ===
            await target_input.click()
            await target_input.fill("")
            await target_input.type(doc_digits, delay=50) # Digita devagar
            
            # Clica no botão Pesquisar
            # Tenta vários seletores para o botão
            btn = page.locator("input[value='PESQUISAR'], button:has-text('PESQUISAR')").first
            await btn.click()
            
            # Espera o resultado
            await wait_pje_loading(page)
            
            # Espera explícita para garantir que a tabela carregou
            try:
                # Espera ou aparecer um link de processo OU a mensagem de "nenhum resultado"
                await page.wait_for_selector("a.btn-detalhes, a[href*='Processo'], .rich-messages", timeout=8000)
            except: pass

            # === 4. EXTRAIR RESULTADOS ===
            # Pega todos os links que parecem CNJ
            links = page.locator("a").filter(has_text=CNJ_RE)
            count = await links.count()
            print(f"Encontrados {count} processos.")

            for i in range(count):
                link = links.nth(i)
                if not await link.is_visible(): continue
                
                txt = await link.inner_text()
                m = CNJ_RE.search(txt)
                if not m: continue
                
                numero = m.group(0)
                
                # Tenta achar o ícone de popup na mesma linha (mais seguro que clicar no texto)
                clickable = link
                try:
                    row = link.locator("xpath=./ancestor::tr").first
                    icon = row.locator("a[title*='Abrir'], a[title*='Detalhes']").first
                    if await icon.count() > 0:
                        clickable = icon
                except: pass

                popup = await open_process_popup(page, clickable)
                
                if popup is None:
                    # Se não abrir popup, salva o erro mas continua
                    result["processos"].append({
                        "numero": numero,
                        "erro": "nao_abriu_popup",
                        "movimentacoes": []
                    })
                    continue

                await popup.wait_for_timeout(1000)
                meta_data = await extract_data(popup, numero)
                result["processos"].append(meta_data)
                
                await popup.close()

        except Exception as e:
            print(f"Erro geral: {e}")
            await browser.close()
            raise HTTPException(status_code=500, detail=str(e))

        await browser.close()
    return result

@app.get("/health")
def health():
    return {"ok": True, "status": "online"}

@app.get("/consulta")
async def consulta(
    doc: str = Query(..., description="Número do documento (CPF ou CNPJ)"),
    tipo: str = Query("CPF", description="Tipo do documento: 'CPF' ou 'CNPJ'")
):
    """
    Recebe ?doc=123&tipo=CNPJ
    """
    doc_clean = sanitize_id(doc)
    doc_type = tipo.upper() if tipo else "CPF"
    
    # Validações simples
    if doc_type == "CNPJ" and len(doc_clean) < 14:
        raise HTTPException(status_code=400, detail="CNPJ incompleto")
    if doc_type == "CPF" and len(doc_clean) < 11:
        raise HTTPException(status_code=400, detail="CPF incompleto")

    cache_key = f"{doc_type}_{doc_clean}"
    now = time.time()
    
    # Cache
    cached = _cache.get(cache_key)
    if cached and (now - cached["ts"]) < CACHE_TTL:
        return cached["data"]

    async with SEMA:
        cached = _cache.get(cache_key)
        if cached and (time.time() - cached["ts"]) < CACHE_TTL:
            return cached["data"]
            
        try:
            # Timeout de 3 minutos
            data = await asyncio.wait_for(scrape_pje(doc_clean, doc_type), timeout=180)
            _cache[cache_key] = {"ts": time.time(), "data": data}
            return data
        except asyncio.TimeoutError:
             raise HTTPException(status_code=504, detail="Timeout: Tribunal demorou muito")
