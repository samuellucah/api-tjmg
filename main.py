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

SEMA = asyncio.Semaphore(1)
CACHE_TTL = 300
_cache: Dict[str, Dict[str, Any]] = {}

app = FastAPI(title="PJe TJMG - Consulta Pública (Final Fix)")

# ==============================================================================
# FUNÇÕES DE NAVEGAÇÃO
# ==============================================================================

async def find_input_any_frame(page):
    """Procura o input de documento e retorna também o FRAME onde ele está."""
    frames = [page.main_frame] + [f for f in page.frames if f != page.main_frame]
    
    for fr in frames:
        try:
            inputs = fr.locator("input[type='text']:visible, input[type='tel']:visible")
            count = await inputs.count()
            
            for i in range(count):
                inp = inputs.nth(i)
                id_attr = (await inp.get_attribute("id") or "").lower()
                placeholder = (await inp.get_attribute("placeholder") or "").lower()
                
                # Lista negra: ignora campos que NÃO são de documento
                blacklist = ["nome", "processo", "advogado", "oab", "classe", "vara"]
                if any(bad in id_attr for bad in blacklist) or any(bad in placeholder for bad in blacklist):
                    continue
                
                return fr, inp
        except:
            continue
    return None, None

async def wait_loading(page: Page):
    """Espera carregamentos do PJe."""
    try:
        await page.wait_for_timeout(500)
        blockers = [".ui-widget-overlay", ".ui-blockui", "[class*='loading' i]", "[class*='spinner' i]"]
        for sel in blockers:
            if await page.locator(sel).count() > 0:
                if await page.locator(sel).first.is_visible():
                    try: await page.locator(sel).first.wait_for(state="hidden", timeout=5000)
                    except: pass
    except: pass

async def open_process_popup(page: Page, clickable):
    try:
        async with page.expect_popup(timeout=15000) as pop:
            try:
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
    try:
        tab = popup.locator("text=/Movimenta(ç|c)ões/i").first
        if await tab.is_visible():
            await tab.click()
            await popup.wait_for_timeout(500)
    except: pass

    meta = {"assunto": None, "classe_judicial": None, "data_distribuicao": None, "orgao_julgador": None, "jurisdicao": None}
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
            
    return {"numero": numero, **meta, "movimentacoes": movs}

# ==============================================================================
# SCRAPER PRINCIPAL
# ==============================================================================

async def scrape_pje(doc_digits: str, doc_type: str) -> Dict[str, Any]:
    result = {
        "documento": doc_digits,
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
            viewport={"width": 1366, "height": 768}
        )
        page = await context.new_page()

        try:
            print(f"Acessando TJMG para {doc_type} {doc_digits}...")
            await page.goto(URL, wait_until="domcontentloaded")
            await wait_loading(page)

            # === 1. SELEÇÃO DO TIPO ===
            # Clica no radio e espera o site reagir
            try:
                if doc_type.upper() == "CNPJ":
                    await page.locator("input[type='radio'][value='CNPJ'], label:has-text('CNPJ')").first.click()
                else:
                    await page.locator("input[type='radio'][value='CPF'], label:has-text('CPF')").first.click()
                await page.wait_for_timeout(1000)
            except: 
                print("Erro ao clicar no radio (pode já estar selecionado)")

            # === 2. LOCALIZAR O INPUT E O FRAME CORRETO ===
            fr, target_input = await find_input_any_frame(page)

            if not target_input:
                # Fallback: tenta buscar diretamente pelo label
                target_input = page.locator("td:has-text('CPF'), td:has-text('CNPJ')").locator("xpath=..//input").first
                fr = page

            if not target_input:
                raise HTTPException(status_code=500, detail="input_nao_encontrado")

            # === 3. PREENCHER ===
            await target_input.click()
            await target_input.fill("")
            await target_input.type(doc_digits, delay=50)
            
            # === 4. CLICAR NO BOTÃO ===
            search_context = fr if fr else page
            btn_selectors = [
                "input[value='PESQUISAR']", 
                "button:has-text('PESQUISAR')", 
                "a:has-text('PESQUISAR')",
                "input[type='submit']"
            ]
            
            btn = None
            for sel in btn_selectors:
                possible_btn = search_context.locator(sel).first
                if await possible_btn.is_visible():
                    btn = possible_btn
                    break
            
            if not btn:
                btn = page.get_by_role("button", name="PESQUISAR").first
            
            await btn.click()
            
            await wait_loading(page)
            
            # Espera tabela ou erro
            try:
                await page.wait_for_selector("a.btn-detalhes, a[href*='Processo'], .rich-messages", timeout=8000)
            except: pass

            # === 5. EXTRAIR DADOS ===
            links = page.locator("a").filter(has_text=CNJ_RE)
            count = await links.count()
            print(f"Links encontrados: {count}")

            for i in range(count):
                link = links.nth(i)
                if not await link.is_visible(): continue
                
                txt = _norm(await link.inner_text())
                m = CNJ_RE.search(txt)
                if not m: continue
                
                numero = m.group(0)
                
                clickable = link
                try:
                    row = link.locator("xpath=./ancestor::tr").first
                    icon = row.locator("a[title*='Abrir'], a[title*='Detalhes']").first
                    if await icon.count() > 0: clickable = icon
                except: pass

                popup = await open_process_popup(page, clickable)
                
                if popup is None:
                    result["processos"].append({"numero": numero, "erro": "popup_bloqueado", "movimentacoes": []})
                    continue

                await popup.wait_for_timeout(1000)
                meta_data = await extract_data(popup, numero)
                result["processos"].append(meta_data)
                await popup.close()

        except Exception as e:
            print(f"Erro geral: {e}")
            await browser.close() # <--- CORRIGIDO AQUI
            raise HTTPException(status_code=500, detail=str(e))

        await browser.close() # <--- CORRIGIDO AQUI TAMBÉM
    return result

@app.get("/health")
def health():
    return {"ok": True, "status": "online"}

@app.get("/consulta")
async def consulta(
    doc: str = Query(..., description="CPF/CNPJ"),
    tipo: str = Query("CPF", description="CPF ou CNPJ")
):
    doc_clean = sanitize_id(doc)
    doc_type = tipo.upper() if tipo else "CPF"
    
    if doc_type == "CNPJ" and len(doc_clean) < 14:
        raise HTTPException(status_code=400, detail="CNPJ incompleto")
    if doc_type == "CPF" and len(doc_clean) < 11:
        raise HTTPException(status_code=400, detail="CPF incompleto")

    cache_key = f"{doc_type}_{doc_clean}"
    now = time.time()
    
    cached = _cache.get(cache_key)
    if cached and (now - cached["ts"]) < CACHE_TTL:
        return cached["data"]

    async with SEMA:
        cached = _cache.get(cache_key)
        if cached and (time.time() - cached["ts"]) < CACHE_TTL:
            return cached["data"]
            
        try:
            data = await asyncio.wait_for(scrape_pje(doc_clean, doc_type), timeout=180)
            _cache[cache_key] = {"ts": time.time(), "data": data}
            return data
        except asyncio.TimeoutError:
             raise HTTPException(status_code=504, detail="Timeout no tribunal")
