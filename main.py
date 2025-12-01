import re
import time
import asyncio
import nest_asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Literal

from fastapi import FastAPI, Query, HTTPException
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError, Page

# URLs
URL_TJMG = "https://pje-consulta-publica.tjmg.jus.br/"
URL_TRF6 = "https://pje1g.trf6.jus.br/consultapublica/ConsultaPublica/listView.seam"

# Regex CNJ
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

# Concorrência
SEMA = asyncio.Semaphore(1)
CACHE_TTL = 300
_cache: Dict[str, Dict[str, Any]] = {}

app = FastAPI(title="API PJe Multi-Tribunal")

# ==============================================================================
# FUNÇÕES DE EXTRAÇÃO (Mantidas iguais para ambos, pois é PJe)
# ==============================================================================

async def wait_spinner_or_delay(page: Page):
    """Aguarda carregamento."""
    candidates = ".ui-widget-overlay, .ui-blockui, .ui-progressbar, [class*='loading' i], [class*='spinner' i]"
    try:
        loc = page.locator(candidates)
        if await loc.count() > 0:
            if await loc.first.is_visible():
                await loc.first.wait_for(state="hidden", timeout=15000)
    except:
        await page.wait_for_timeout(2000)

async def open_process_popup(page: Page, clickable):
    """Abre o popup do processo."""
    try:
        async with page.expect_popup(timeout=15000) as pop:
            # Tenta clique normal, se falhar, força via JS
            try:
                await clickable.click(timeout=5000)
            except:
                await clickable.dispatch_event("click")
        
        popup = await pop.value
        await popup.wait_for_load_state("domcontentloaded")
        return popup
    except:
        return None

async def extract_data(popup: Page, numero: str, tribunal: str) -> Dict[str, Any]:
    """Extrai dados de dentro do popup (Metadados + Movimentações)."""
    
    # 1. Tenta ir para aba de movimentações
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
        body_text = await popup.locator("body").inner_text()
        lines = [_norm(ln) for ln in body_text.split('\n') if ln.strip()]
        
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
            for i in range(min(count, 50)):
                txt = _norm(await rows.nth(i).inner_text())
                if txt and not UNWANTED_RE.search(txt) and txt not in seen:
                    seen.add(txt)
                    movs.append(txt)
            break
    
    return {"tribunal": tribunal, "numero": numero, **meta, "movimentacoes": movs}

# ==============================================================================
# LÓGICA ESPECÍFICA DE CADA SITE (O que muda de verdade)
# ==============================================================================

async def _scrape_tjmg(context, doc_value: str, doc_type: str) -> List[Dict]:
    """Abre TJMG, seleciona CPF/CNPJ, digita e busca."""
    results = []
    page = await context.new_page()
    try:
        await page.goto(URL_TJMG, wait_until="domcontentloaded")
        await page.wait_for_timeout(1000)

        # 1. SELECIONAR RADIO BUTTON (CPF ou CNPJ)
        # Procura pelo valor do input ou texto do label
        if doc_type == "cnpj":
            await page.locator("input[type='radio'][value='CNPJ'], label:has-text('CNPJ')").first.click()
        else:
            await page.locator("input[type='radio'][value='CPF'], label:has-text('CPF')").first.click()
        
        await page.wait_for_timeout(500) # Espera a máscara mudar

        # 2. ENCONTRAR O INPUT CORRETO
        # Ignora inputs de 'Nome', 'Processo', 'Advogado'
        inputs = page.locator("input[type='text']:visible")
        count = await inputs.count()
        target = None
        for i in range(count):
            inp = inputs.nth(i)
            id_attr = (await inp.get_attribute("id") or "").lower()
            if "nome" in id_attr or "processo" in id_attr or "advogado" in id_attr or "oab" in id_attr:
                continue
            target = inp
            break # Assume o primeiro visível que sobrou
        
        if target:
            await target.click()
            await target.fill(doc_value)
            await page.get_by_role("button", name="PESQUISAR").first.click()
            await wait_spinner_or_delay(page)

            # 3. EXTRAIR RESULTADOS
            links = page.locator("a").filter(has_text=CNJ_RE)
            count = await links.count()
            for i in range(count):
                link = links.nth(i)
                txt = await link.inner_text()
                m = CNJ_RE.search(txt)
                if m:
                    popup = await open_process_popup(page, link)
                    if popup:
                        data = await extract_data(popup, m.group(0), "TJMG")
                        results.append(data)
                        await popup.close()
    except Exception as e:
        print(f"[TJMG Error] {e}")
    finally:
        await page.close()
    return results

async def _scrape_trf6(context, doc_value: str, doc_type: str) -> List[Dict]:
    """Abre TRF6, seleciona CPF/CNPJ, digita e busca."""
    results = []
    page = await context.new_page()
    try:
        await page.goto(URL_TRF6, wait_until="domcontentloaded")
        await page.wait_for_timeout(1000)

        # 1. SELECIONAR RADIO BUTTON
        # No TRF6 geralmente é label clicável
        if doc_type == "cnpj":
            await page.locator("label:has-text('CNPJ')").first.click()
        else:
            await page.locator("label:has-text('CPF')").first.click()
        
        await page.wait_for_timeout(500)

        # 2. ENCONTRAR INPUT
        # No TRF6 o input costuma ter 'nrDocumentoInput' no ID
        target = page.locator("input[id*='nrDocumentoInput']").first
        if not await target.count():
            # Fallback genérico
            target = page.locator("input[type='text']:visible").nth(0)

        await target.click()
        await target.fill(doc_value)
        await page.get_by_role("button", name="Pesquisar").first.click()
        await wait_spinner_or_delay(page)

        # 3. EXTRAIR RESULTADOS
        links = page.locator("a").filter(has_text=CNJ_RE)
        count = await links.count()
        for i in range(count):
            link = links.nth(i)
            txt = await link.inner_text()
            m = CNJ_RE.search(txt)
            if m:
                popup = await open_process_popup(page, link)
                if popup:
                    data = await extract_data(popup, m.group(0), "TRF6")
                    results.append(data)
                    await popup.close()
    except Exception as e:
        print(f"[TRF6 Error] {e}")
    finally:
        await page.close()
    return results

# ==============================================================================
# ORQUESTRADOR E API
# ==============================================================================

async def scrape_all(doc_value: str, doc_type: str) -> Dict[str, Any]:
    result = {
        "documento": doc_value,
        "tipo": doc_type,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "processos": [],
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720}
        )

        # Executa SEQUENCIAL (Um depois do outro) para não travar a memória da VPS
        try:
            print(f"Buscando TJMG para {doc_value} ({doc_type})...")
            p_tjmg = await _scrape_tjmg(context, doc_value, doc_type)
            result["processos"].extend(p_tjmg)
        except: pass

        try:
            print(f"Buscando TRF6 para {doc_value} ({doc_type})...")
            p_trf6 = await _scrape_trf6(context, doc_value, doc_type)
            result["processos"].extend(p_trf6)
        except: pass

        await context.close()
        await browser.close()
    
    return result

@app.get("/health")
def health():
    return {"ok": True, "status": "online"}

@app.get("/consulta")
async def consulta(
    cpf: Optional[str] = Query(None),
    cnpj: Optional[str] = Query(None),
    tipo: Literal["cpf", "cnpj"] = Query(..., description="Obrigatório: 'cpf' ou 'cnpj'")
):
    # Pega o valor de qualquer campo que veio preenchido
    raw_val = cpf or cnpj
    if not raw_val:
        raise HTTPException(status_code=400, detail="Informe o numero no parametro cpf ou cnpj")

    doc_clean = sanitize_id(raw_val)
    
    # Validações básicas
    if tipo == "cpf" and len(doc_clean) < 11:
        raise HTTPException(status_code=400, detail="CPF curto demais")
    if tipo == "cnpj" and len(doc_clean) < 14:
        raise HTTPException(status_code=400, detail="CNPJ curto demais")

    cache_key = f"{tipo}_{doc_clean}"
    now = time.time()
    
    # Cache Check
    cached = _cache.get(cache_key)
    if cached and (now - cached["ts"]) < CACHE_TTL:
        return cached["data"]

    async with SEMA:
        # Double check inside lock
        cached = _cache.get(cache_key)
        if cached and (time.time() - cached["ts"]) < CACHE_TTL:
            return cached["data"]
            
        try:
            # Timeout de 4 minutos pois são 2 tribunais lentos
            data = await asyncio.wait_for(scrape_all(doc_clean, tipo), timeout=240)
            _cache[cache_key] = {"ts": time.time(), "data": data}
            return data
        except asyncio.TimeoutError:
            raise HTTPException(status_code=504, detail="Timeout: Tribunais demoraram muito")
