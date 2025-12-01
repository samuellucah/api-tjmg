import re
import time
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query, HTTPException
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# Configuração
URL = "https://pje-consulta-publica.tjmg.jus.br/"
CNJ_RE = re.compile(r"\b\d{7}-\\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}\b")

# Filtro para NÃO retornar ruídos
UNWANTED_RE = re.compile(
    r"(documentos?\s+juntados|documento\b|certid[aã]o|visualizar|"
    r"pjeoffice|indispon[ií]vel|aplicativo\s+pjeoffice|"
    r"página\b|resultados?\s+encontrados|recibo)",
    re.IGNORECASE,
)

def _norm(txt: str) -> str:
    return re.sub(r"\s+", " ", (txt or "")).strip()

def sanitize_cpf(cpf: str) -> str:
    return re.sub(r"\D+", "", cpf or "")

# ===== Concurrency + Cache =====
SEMA = asyncio.Semaphore(1)          # 1 request por vez para não travar a VPS
CACHE_TTL = 300                      # 5 minutos
_cache: Dict[str, Dict[str, Any]] = {}

app = FastAPI(title="PJe TJMG - Consulta Pública API")

async def find_cpf_input_any_frame(page):
    frames = [page.main_frame] + [f for f in page.frames if f != page.main_frame]
    anchor_xpaths = [
        "xpath=//*[contains(.,'CPF') and contains(.,'CNPJ')][1]",
        "xpath=//label[contains(normalize-space(.),'CPF')][1]/parent::*",
        "xpath=//*[contains(normalize-space(.),'CPF')][1]",
    ]
    input_after = "xpath=following::input[(not(@type) or @type='text' or @type='tel') and not(@disabled)][1]"

    for fr in frames:
        for ax in anchor_xpaths:
            try:
                anchor = fr.locator(ax)
                if await anchor.count() == 0:
                    continue
                candidate = anchor.first.locator(input_after).first
                if await candidate.count() > 0 and await candidate.is_visible():
                    return fr, candidate
            except:
                pass
    return None, None

async def wait_spinner_or_delay(page):
    candidates = ".ui-widget-overlay, .ui-blockui, .ui-progressbar, [class*='loading' i], [class*='spinner' i]"
    loc = page.locator(candidates)
    try:
        await loc.first.wait_for(state="visible", timeout=2000)
        await loc.first.wait_for(state="hidden", timeout=25000)
    except PlaywrightTimeoutError:
        await page.wait_for_timeout(8000)

async def open_process_popup(page, clickable):
    try:
        async with page.expect_popup(timeout=20000) as pop:
            await clickable.click(timeout=60000)
        popup = await pop.value
        await popup.wait_for_load_state("domcontentloaded")
        return popup
    except PlaywrightTimeoutError:
        return None

async def try_click_movements_tab(popup):
    candidates = [
        popup.get_by_role("tab", name=re.compile(r"Movimenta", re.I)),
        popup.get_by_role("button", name=re.compile(r"Movimenta", re.I)),
        popup.get_by_role("link", name=re.compile(r"Movimenta", re.I)),
        popup.locator("text=/Movimenta(ç|c)ões/i"),
        popup.locator("text=/Movimenta(ç|c)ões do Processo/i"),
    ]
    for c in candidates:
        try:
            if await c.count() > 0 and await c.first.is_visible():
                await c.first.click(timeout=4000)
                await popup.wait_for_timeout(800)
                return
        except:
            pass

async def extract_metadata(popup) -> Dict[str, Optional[str]]:
    try:
        body = await popup.locator("body").inner_text()
    except:
        return {"assunto": None, "classe_judicial": None, "data_distribuicao": None, "orgao_julgador": None, "jurisdicao": None}

    lines = [_norm(ln) for ln in body.replace("\r", "").split("\n")]
    lines = [ln for ln in lines if ln]

    def find_value(keys: List[str]) -> Optional[str]:
        keys_l = [k.lower() for k in keys]
        for i, ln in enumerate(lines):
            low = ln.lower()
            if any(k in low for k in keys_l):
                parts = re.split(r"[:\-]\s*", ln, maxsplit=1)
                if len(parts) == 2 and parts[1].strip():
                    val = parts[1].strip()
                    if not UNWANTED_RE.search(val): return val
                if i + 1 < len(lines) and lines[i + 1]:
                    val = lines[i + 1]
                    if not UNWANTED_RE.search(val): return val
        return None

    return {
        "assunto": find_value(["assunto", "assunto(s)"]),
        "classe_judicial": find_value(["classe judicial", "classe"]),
        "data_distribuicao": find_value(["data da distribuição", "data de distribuição", "distribuição"]),
        "orgao_julgador": find_value(["órgão julgador", "orgao julgador"]),
        "jurisdicao": find_value(["jurisdição", "jurisdicao", "comarca"]),
    }

async def extract_movements(popup) -> List[str]:
    await try_click_movements_tab(popup)
    texts: List[str] = []
    seen = set()
    selectors = [
        "css=[id*='moviment' i] tr", "css=[class*='moviment' i] tr",
        "css=[id*='moviment' i] li", "css=[class*='moviment' i] li",
        "xpath=//table[.//*[contains(translate(.,'MOVIMENTACOESÇÃ','movimentacoesca'),'moviment')]]//tr",
        "xpath=//ul[.//*[contains(translate(.,'MOVIMENTACOESÇÃ','movimentacoesca'),'moviment')]]//li",
    ]

    for sel in selectors:
        try:
            loc = popup.locator(sel)
            cnt = await loc.count()
            if cnt == 0: continue
            for i in range(min(cnt, 500)):
                t = _norm(await loc.nth(i).inner_text())
                if not t or UNWANTED_RE.search(t) or t in seen: continue
                seen.add(t)
                texts.append(t)
            if len(texts) >= 5: break
        except: pass

    if not texts:
        try:
            body = await popup.locator("body").inner_text()
            for ln in body.split("\n"):
                t = _norm(ln)
                if not t or UNWANTED_RE.search(t) or t in seen: continue
                seen.add(t)
                texts.append(t)
        except: pass
    return texts

async def scrape_pje(cpf_digits: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "cpf": cpf_digits,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "processos": [],
    }
    async with async_playwright() as p:
        # headless=True é obrigatório em VPS
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"])
        page = await browser.new_page(viewport={"width": 1280, "height": 720})

        try:
            await page.goto(URL, wait_until="domcontentloaded")
            await page.wait_for_timeout(1200)

            fr, cpf_input = await find_cpf_input_any_frame(page)
            if cpf_input is None:
                raise HTTPException(status_code=500, detail="nao_encontrei_campo_cpf")

            await cpf_input.click(timeout=60000)
            await cpf_input.fill("")
            await cpf_input.type(cpf_digits, delay=40)

            if not (await cpf_input.input_value()).strip():
                raise HTTPException(status_code=500, detail="cpf_nao_preencheu")

            btn = fr.get_by_role("button", name="PESQUISAR")
            if await btn.count() == 0:
                btn = page.get_by_role("button", name="PESQUISAR")
            await btn.first.click(timeout=60000)

            await wait_spinner_or_delay(page)

            proc_links = page.locator("a").filter(has_text=CNJ_RE)
            count = await proc_links.count()

            for i in range(count):
                link = proc_links.nth(i)
                txt = _norm(await link.inner_text())
                m = CNJ_RE.search(txt)
                if not m: continue
                numero = m.group(0)

                popup = await open_process_popup(page, link)
                if popup is None:
                    icon = link.locator("xpath=ancestor::*[self::tr or self::div][1]//a[1]")
                    if await icon.count() > 0:
                        popup = await open_process_popup(page, icon.first)

                if popup is None:
                    result["processos"].append({"numero": numero, "erro": "nao_abriu_popup", "movimentacoes": []})
                    continue

                await popup.wait_for_timeout(1200)
                meta = await extract_metadata(popup)
                movs = await extract_movements(popup)
                result["processos"].append({"numero": numero, **meta, "movimentacoes": movs})
                await popup.close()
        except Exception as e:
             raise HTTPException(status_code=500, detail=str(e))
        finally:
            await browser.close()
    return result

@app.get("/health")
def health():
    return {"ok": True, "status": "online"}

@app.get("/consulta")
async def consulta(cpf: str = Query(..., description="CPF")):
    cpf_digits = sanitize_cpf(cpf)
    if not cpf_digits or len(cpf_digits) < 11:
        raise HTTPException(status_code=400, detail="cpf_invalido")

    now = time.time()
    cached = _cache.get(cpf_digits)
    if cached and (now - cached["ts"]) < CACHE_TTL:
        return cached["data"]

    async with SEMA:
        cached = _cache.get(cpf_digits)
        if cached and (time.time() - cached["ts"]) < CACHE_TTL:
            return cached["data"]
        
        # Timeout aumentado para 3 minutos pois scraping judicial é lento
        try:
            data = await asyncio.wait_for(scrape_pje(cpf_digits), timeout=180)
            _cache[cpf_digits] = {"ts": time.time(), "data": data}
            return data
        except asyncio.TimeoutError:
             raise HTTPException(status_code=504, detail="timeout_no_tribunal")