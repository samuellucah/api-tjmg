import re
import time
import asyncio
import nest_asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query, HTTPException
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# URL Fixa (apenas TJMG como solicitado)
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
    """Limpa CPF ou CNPJ (mantém apenas números)"""
    return re.sub(r"\D+", "", val or "")

# ===== Concurrency + Cache =====
SEMA = asyncio.Semaphore(1)
CACHE_TTL = 300
_cache: Dict[str, Dict[str, Any]] = {}

app = FastAPI(title="PJe TJMG - Consulta Pública (Single URL)")

async def find_input_any_frame(page):
    """Procura o input de documento (CPF/CNPJ) em qualquer frame."""
    frames = [page.main_frame] + [f for f in page.frames if f != page.main_frame]
    
    # Estratégia: Procura inputs de texto visíveis que não sejam de 'Nome' ou 'Processo'
    for fr in frames:
        try:
            inputs = fr.locator("input[type='text']:visible, input[type='tel']:visible")
            count = await inputs.count()
            
            for i in range(count):
                inp = inputs.nth(i)
                # Verifica atributos para fugir de campos errados
                id_attr = (await inp.get_attribute("id") or "").lower()
                placeholder = (await inp.get_attribute("placeholder") or "").lower()
                
                # Lista de palavras para ignorar (campos que NÃO são o de CPF/CNPJ)
                blacklist = ["nome", "processo", "advogado", "oab", "classe", "vara"]
                
                if any(bad in id_attr for bad in blacklist) or any(bad in placeholder for bad in blacklist):
                    continue
                
                # Se passou, é o nosso candidato
                return fr, inp
        except:
            continue
            
    return None, None

async def wait_spinner_or_delay(page):
    """Aguarda carregamentos do sistema."""
    blockers = [".ui-widget-overlay", ".ui-blockui", "[class*='loading' i]", "[class*='spinner' i]"]
    try:
        await page.wait_for_timeout(500)
        for sel in blockers:
            if await page.locator(sel).count() > 0:
                if await page.locator(sel).first.is_visible():
                    try:
                        await page.locator(sel).first.wait_for(state="hidden", timeout=10000)
                    except: pass
    except:
        pass

async def open_process_popup(page, clickable):
    try:
        async with page.expect_popup(timeout=15000) as pop:
            try:
                await clickable.click(timeout=5000)
            except:
                await clickable.dispatch_event("click")
        popup = await pop.value
        await popup.wait_for_load_state("domcontentloaded")
        return popup
    except:
        return None

async def try_click_movements_tab(popup):
    candidates = [
        popup.locator("text=/Movimenta(ç|c)ões/i"),
        popup.get_by_role("tab", name=re.compile(r"Movimenta", re.I))
    ]
    for c in candidates:
        try:
            if await c.count() > 0 and await c.first.is_visible():
                await c.first.click(timeout=3000)
                await popup.wait_for_timeout(500)
                return
        except: pass

async def extract_metadata(popup) -> Dict[str, Optional[str]]:
    try:
        body = await popup.locator("body").inner_text()
    except:
        return {}

    lines = [_norm(ln) for ln in body.replace("\r", "").split("\n")]
    meta = {
        "assunto": None, "classe_judicial": None, "data_distribuicao": None,
        "orgao_julgador": None, "jurisdicao": None
    }
    
    map_keys = {
        "assunto": ["assunto", "assunto(s)"],
        "classe_judicial": ["classe judicial", "classe"],
        "data_distribuicao": ["data da distribuição", "distribuição"],
        "orgao_julgador": ["órgão julgador", "orgao julgador"],
        "jurisdicao": ["jurisdição", "comarca"]
    }

    for field, keywords in map_keys.items():
        for i, line in enumerate(lines):
            low = line.lower()
            if any(k in low for k in keywords):
                parts = re.split(r"[:\-]\s*", line, maxsplit=1)
                if len(parts) > 1 and len(parts[1]) > 3:
                    val = parts[1].strip()
                    if not UNWANTED_RE.search(val): meta[field] = val
                    break
                if i+1 < len(lines):
                    val = lines[i+1]
                    if len(val) > 3 and not UNWANTED_RE.search(val): 
                        meta[field] = val
                        break
    return meta

async def extract_movements(popup) -> List[str]:
    await try_click_movements_tab(popup)
    texts = []
    seen = set()
    selectors = ["tbody[id*='moviment'] tr", "table[class*='moviment'] tr", ".rich-table-row"]
    
    for sel in selectors:
        rows = popup.locator(sel)
        if await rows.count() > 0:
            count = await rows.count()
            for i in range(min(count, 50)):
                t = _norm(await rows.nth(i).inner_text())
                if t and not UNWANTED_RE.search(t) and t not in seen:
                    seen.add(t)
                    texts.append(t)
            break
    return texts

async def scrape_pje(doc_digits: str, doc_type: str) -> Dict[str, Any]:
    result = {
        "documento": doc_digits,
        "tipo": doc_type,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "processos": [],
    }

    async with async_playwright() as p:
        # Configuração para evitar detecção de robô
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720}
        )
        page = await context.new_page()

        try:
            await page.goto(URL, wait_until="domcontentloaded")
            await page.wait_for_timeout(1500)

            # === PASSO CRUCIAL: Selecionar CPF ou CNPJ ===
            # Isso adapta a máscara do campo e evita erros de validação
            if doc_type.upper() == "CNPJ":
                # Tenta clicar no radio button ou label de CNPJ
                try:
                    await page.locator("input[type='radio'][value='CNPJ'], label:has-text('CNPJ')").first.click()
                except: 
                    print("Erro ao clicar em CNPJ")
            else:
                # Garante que CPF está selecionado
                try:
                    await page.locator("input[type='radio'][value='CPF'], label:has-text('CPF')").first.click()
                except: pass
            
            await page.wait_for_timeout(800) # Espera a tela atualizar o campo

            # Encontra e preenche o campo
            fr, input_el = await find_input_any_frame(page)
            if input_el is None:
                raise HTTPException(status_code=500, detail="input_nao_encontrado")

            await input_el.click()
            await input_el.fill("")
            await input_el.type(doc_digits, delay=50) # Digita com delay humano

            # Clica em Pesquisar
            try:
                await page.get_by_role("button", name="PESQUISAR").first.click()
            except:
                # Fallback se o botão tiver outro nome ou for um input type=submit
                await page.locator("input[type='submit'], button:has-text('Pesquisar')").first.click()

            await wait_spinner_or_delay(page)

            # Extrai Links
            try:
                await page.wait_for_selector("a[href*='Processo'], .rich-table", timeout=8000)
            except: pass

            proc_links = page.locator("a").filter(has_text=CNJ_RE)
            count = await proc_links.count()

            for i in range(count):
                link = proc_links.nth(i)
                if not await link.is_visible(): continue
                
                txt = _norm(await link.inner_text())
                m = CNJ_RE.search(txt)
                if not m: continue
                
                numero = m.group(0)
                
                # Tenta clicar no ícone ao lado do link para garantir popup
                clickable = link
                try:
                    icon = link.locator("xpath=./ancestor::tr//a[contains(@title,'Abrir') or contains(@title,'Detalhes')]")
                    if await icon.count() > 0:
                        clickable = icon.first
                except: pass

                popup = await open_process_popup(page, clickable)
                
                if popup is None:
                    result["processos"].append({
                        "numero": numero,
                        "erro": "nao_abriu_popup",
                        "movimentacoes": []
                    })
                    continue

                await popup.wait_for_timeout(1000)
                meta = await extract_metadata(popup)
                movs = await extract_movements(popup)
                
                result["processos"].append({
                    "numero": numero,
                    **meta,
                    "movimentacoes": movs
                })
                await popup.close()

        except Exception as e:
            await browser.close()
            raise HTTPException(status_code=500, detail=str(e))

        await browser.close()
    return result

@app.get("/health")
def health():
    return {"ok": True, "status": "online"}

# === Endpoint Ajustado para sua Regra ===
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
    
    # Validação Simples
    if doc_type == "CNPJ" and len(doc_clean) < 14:
        raise HTTPException(status_code=400, detail="CNPJ incompleto")
    if doc_type == "CPF" and len(doc_clean) < 11:
        raise HTTPException(status_code=400, detail="CPF incompleto")

    # Cache
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
             raise HTTPException(status_code=504, detail="timeout_no_tribunal")
