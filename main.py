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

app = FastAPI(title="PJe TJMG - Consulta Pública (Frame Scan)")

# ==============================================================================
# FUNÇÕES DE APOIO
# ==============================================================================

async def wait_loading(page: Page):
    """Aguarda carregamento visual do PJe."""
    try:
        await page.wait_for_timeout(800)
        blockers = [".ui-widget-overlay", ".ui-blockui", "[class*='loading' i]", "[class*='spinner' i]"]
        for sel in blockers:
            if await page.locator(sel).count() > 0:
                if await page.locator(sel).first.is_visible():
                    try: await page.locator(sel).first.wait_for(state="hidden", timeout=8000)
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
    # Tenta aba movimentações
    try:
        tab = popup.locator("text=/Movimenta(ç|c)ões/i").first
        if await tab.is_visible():
            await tab.click()
            await popup.wait_for_timeout(500)
    except: pass

    # Metadados
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

    # Movimentações
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
            # Clica e espera a máscara mudar
            try:
                if doc_type.upper() == "CNPJ":
                    await page.locator("input[type='radio'][value='CNPJ'], label:has-text('CNPJ')").first.click()
                else:
                    await page.locator("input[type='radio'][value='CPF'], label:has-text('CPF')").first.click()
                await page.wait_for_timeout(1500) 
            except: 
                print("Erro ao clicar radio")

            # === 2. LOCALIZAR O INPUT CERTO (Varredura de Frames) ===
            target_input = None
            target_frame = None
            
            # Pega lista de todos os frames (principal + iframes)
            frames = [page.main_frame] + page.frames
            
            for fr in frames:
                # Procura inputs visíveis neste frame
                inputs = fr.locator("input[type='text']:visible")
                count = await inputs.count()
                
                for i in range(count):
                    inp = inputs.nth(i)
                    id_attr = (await inp.get_attribute("id") or "").lower()
                    
                    # Filtra inputs errados
                    blacklist = ["nome", "processo", "advogado", "oab", "classe", "vara"]
                    if any(b in id_attr for b in blacklist):
                        continue
                        
                    # Se achou um input que não é os de cima, assume que é o Doc
                    target_input = inp
                    target_frame = fr
                    break
                if target_input: break

            if not target_input:
                raise HTTPException(status_code=500, detail="input_nao_encontrado_na_tela")

            # === 3. PREENCHER E PESQUISAR ===
            await target_input.click()
            await target_input.fill("")
            await target_input.type(doc_digits, delay=50)
            
            # Clica PESQUISAR (no mesmo frame do input)
            btn = None
            btn_selectors = ["input[value='PESQUISAR']", "button:has-text('PESQUISAR')", "input[type='submit']"]
            for sel in btn_selectors:
                if await target_frame.locator(sel).count() > 0:
                    btn = target_frame.locator(sel).first
                    break
            
            if btn:
                await btn.click()
            else:
                # Fallback: Tenta na página inteira
                await page.locator("input[value='PESQUISAR']").click()
            
            await wait_loading(page)
            
            # Espera tabela carregar
            try:
                await page.wait_for_selector("a.btn-detalhes, a[href*='Processo'], .rich-messages, .rich-table", timeout=8000)
            except: pass

            # === 4. EXTRAIR RESULTADOS (VARREDURA GLOBAL) ===
            # Aqui estava o erro: Os resultados podem estar em um frame diferente.
            # Vamos procurar links CNJ em TODOS os frames.
            
            total_links = []
            
            # Recarrega a lista de frames pois pode ter mudado após a pesquisa
            all_frames = [page.main_frame] + page.frames
            
            for fr in all_frames:
                try:
                    # Procura links neste frame
                    links = fr.locator("a").filter(has_text=CNJ_RE)
                    count = await links.count()
                    if count > 0:
                        print(f"Encontrados {count} links no frame {fr.name}")
                        for i in range(count):
                            total_links.append(links.nth(i))
                except: continue

            print(f"Total de links processáveis: {len(total_links)}")

            processed_numbers = set()

            for link in total_links:
                try:
                    if not await link.is_visible(): continue
                    
                    txt = _norm(await link.inner_text())
                    m = CNJ_RE.search(txt)
                    if not m: continue
                    
                    numero = m.group(0)
                    if numero in processed_numbers: continue
                    processed_numbers.add(numero)
                    
                    # Tenta achar ícone de popup (dentro do mesmo frame do link)
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
                except: continue

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
