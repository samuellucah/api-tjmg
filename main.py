import re
import time
import asyncio
import nest_asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query, HTTPException
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# Permite loops aninhados
nest_asyncio.apply()

URL = "https://pje-consulta-publica.tjmg.jus.br/"

# Regex CNJ: 0000000-00.0000.0.00.0000
CNJ_RE = re.compile(r"\b\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}\b")

# Filtra ruídos
UNWANTED_RE = re.compile(
    r"(documentos?\s+juntados|documento\b|certid[aã]o|visualizar|"
    r"pjeoffice|indispon[ií]vel|aplicativo\s+pjeoffice|"
    r"página\b|resultados?\s+encontrados|recibo)",
    re.IGNORECASE,
)

def _norm(txt: str) -> str:
    return re.sub(r"\s+", " ", (txt or "")).strip()

def sanitize_doc(doc: str) -> str:
    return re.sub(r"\D+", "", doc or "")

SEMA = asyncio.Semaphore(1)          
CACHE_TTL = 300                      
_cache: Dict[str, Dict[str, Any]] = {} 

app = FastAPI(title="PJe TJMG - Scraper")

# --- FUNÇÕES AUXILIARES ---

async def find_input_any_frame(page):
    """Procura o campo de input em todos os frames."""
    frames = [page.main_frame] + [f for f in page.frames if f != page.main_frame]
    
    anchor_xpaths = [
        "xpath=//*[contains(normalize-space(.),'CPF') and contains(normalize-space(.),'CNPJ')][1]",
        "xpath=//label[contains(normalize-space(.),'CPF')][1]/parent::*",
        "xpath=//label[contains(normalize-space(.),'CNPJ')][1]/parent::*",
    ]
    input_after = "xpath=following::input[(not(@type) or @type='text' or @type='tel') and not(@disabled)][1]"

    for fr in frames:
        for ax in anchor_xpaths:
            try:
                anchor = fr.locator(ax)
                if await anchor.count() > 0:
                    candidate = anchor.first.locator(input_after).first
                    if await candidate.count() > 0 and await candidate.is_visible():
                        return fr, candidate
            except:
                pass
    return None, None

async def force_set_doc_type_radio(page, frame, doc_type: str) -> bool:
    """Força a seleção do Radio Button."""
    target = (doc_type or "").upper().strip()
    
    locators = [
        frame.get_by_label(target, exact=True),
        frame.locator(f"input[type='radio'][value='{target}']"),
        frame.locator(f"xpath=//label[contains(normalize-space(.), '{target}')]//input[@type='radio']"),
        frame.get_by_text(target, exact=True)
    ]

    for loc in locators:
        try:
            if await loc.count() > 0:
                if await loc.first.is_visible():
                    await loc.first.check(force=True, timeout=1000)
                else:
                    await loc.first.evaluate("el => el.click()")
                await page.wait_for_timeout(1000)
                return True
        except:
            continue
    return False

async def ensure_input_match(page, input_locator, expected_digits: str):
    """
    GARANTIA DE PREENCHIMENTO:
    1. Tenta digitar normalmente.
    2. Se falhar, INJETA o valor via JavaScript.
    """
    for attempt in range(2):
        try:
            await input_locator.click()
            await input_locator.press("Control+A")
            await input_locator.press("Backspace")
            await page.wait_for_timeout(300)
            await input_locator.type(expected_digits, delay=80)
            await page.wait_for_timeout(500)
            
            raw_val = await input_locator.input_value()
            clean_val = re.sub(r"\D+", "", raw_val)
            
            if clean_val == expected_digits:
                return True 
            
            await page.keyboard.press("Tab")
            await page.wait_for_timeout(500)
        except:
            pass

    print(f"Digitação falhou (Lido: {clean_val} vs Esperado: {expected_digits}). Forçando JS...")
    try:
        await input_locator.evaluate(f"""(el) => {{
            el.value = '{expected_digits}';
            el.dispatchEvent(new Event('input', {{ bubbles: true }}));
            el.dispatchEvent(new Event('change', {{ bubbles: true }}));
            el.dispatchEvent(new Event('blur', {{ bubbles: true }}));
        }}""")
        await page.wait_for_timeout(500)
        
        raw_val = await input_locator.input_value()
        clean_val = re.sub(r"\D+", "", raw_val)
        return clean_val == expected_digits
    except:
        return False

async def open_process_popup(page, clickable):
    try:
        async with page.expect_popup(timeout=10000) as pop:
            await clickable.click(timeout=8000)
        popup = await pop.value
        await popup.wait_for_load_state("domcontentloaded")
        return popup
    except:
        return None

async def extract_metadata(popup) -> Dict[str, Optional[str]]:
    try:
        body = await popup.locator("body").inner_text()
    except:
        return {}
    lines = [_norm(ln) for ln in body.split("\n") if ln.strip()]
    
    def find(keys):
        keys_l = [k.lower() for k in keys]
        for i, ln in enumerate(lines):
            low = ln.lower()
            if any(k in low for k in keys_l):
                parts = ln.split(":", 1)
                if len(parts) == 2 and parts[1].strip():
                    val = parts[1].strip()
                    if not UNWANTED_RE.search(val): return val
                if i + 1 < len(lines):
                    val = lines[i+1]
                    if not UNWANTED_RE.search(val): return val
        return None

    return {
        "assunto": find(["assunto"]),
        "classe_judicial": find(["classe judicial", "classe"]),
        "data_distribuicao": find(["distribuição"]),
        "orgao_julgador": find(["órgão julgador"]),
        "jurisdicao": find(["jurisdição", "comarca"]),
    }

async def extract_movements(popup) -> List[str]:
    texts = []
    seen = set()
    try:
        tab = popup.locator("text=/Movimenta(ç|c)ões/i")
        if await tab.count() > 0:
            await tab.first.click(timeout=2000)
            await popup.wait_for_timeout(500)
    except:
        pass

    rows = popup.locator("tr")
    count = await rows.count()
    for i in range(min(count, 100)):
        try:
            txt = _norm(await rows.nth(i).inner_text())
            if len(txt) > 10 and not UNWANTED_RE.search(txt) and txt not in seen:
                seen.add(txt)
                texts.append(txt)
        except:
            continue
    return texts[:10]

# --- BUSCA INTELIGENTE DE RESULTADOS ---
async def wait_and_find_results(page):
    """
    Aguarda explicitamente por sinais de sucesso ou falha na pesquisa em TODOS os frames.
    """
    start_time = time.time()
    while (time.time() - start_time) < 30: # Espera até 30 segundos
        frames = [page.main_frame] + page.frames
        for fr in frames:
            try:
                # 1. Sucesso: Links de Processo
                links = fr.locator("a").filter(has_text=CNJ_RE)
                if await links.count() > 0:
                    return fr, links
                
                # 2. Sucesso: Tabela de Resultados (fallback)
                rows = fr.locator("tr").filter(has_text=CNJ_RE)
                if await rows.count() > 0:
                    return fr, rows
                
                # 3. Aviso: Mensagem de erro ou "Nenhum registro"
                msg_el = fr.locator(".ui-messages-error, .ui-messages-info, .ui-messages-warn")
                if await msg_el.count() > 0:
                    txt = await msg_el.first.inner_text()
                    if "encontrado" in txt.lower() or "registro" in txt.lower():
                        return fr, None # Encontrou aviso de que não tem nada
            except:
                continue
        
        await page.wait_for_timeout(1000)
    
    return None, None

async def scrape_pje(doc_digits: str, doc_type: str) -> Dict[str, Any]:
    result = {
        "documento": doc_digits,
        "tipo": doc_type,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "processos": []
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720}
        )
        
        try:
            page = await context.new_page()
            await page.goto(URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(2000)

            fr, doc_input = await find_input_any_frame(page)
            if not doc_input:
                raise Exception("Input de CPF/CNPJ não encontrado.")

            # 1. Troca o Radio Button
            print(f"Selecionando tipo {doc_type}...")
            await force_set_doc_type_radio(page, fr, doc_type)
            await page.wait_for_timeout(1500)
            
            fr, doc_input = await find_input_any_frame(page)
            
            # 2. Digita
            print(f"Digitando documento {doc_digits}...")
            match = await ensure_input_match(page, doc_input, doc_digits)
            
            if not match:
                print("Injeção falhou. Tentando Toggle...")
                other_type = "CPF" if doc_type == "CNPJ" else "CNPJ"
                await force_set_doc_type_radio(page, fr, other_type)
                await page.wait_for_timeout(1000)
                await force_set_doc_type_radio(page, fr, doc_type)
                await page.wait_for_timeout(2000)
                
                fr, doc_input = await find_input_any_frame(page)
                match = await ensure_input_match(page, doc_input, doc_digits)

            if not match:
                result["aviso_site"] = f"Não foi possível digitar o documento completo ({len(doc_digits)} dígitos)."
            
            # 3. Pesquisar
            print("Clicando em Pesquisar...")
            btn = fr.locator("button:has-text('PESQUISAR'), input[type='submit'][value*='PESQUISAR' i]").first
            if await btn.count() == 0:
                btn = page.locator("button:has-text('PESQUISAR')").first
            
            if await btn.count() > 0:
                await btn.click()
            else:
                await doc_input.press("Enter")
            
            # 4. Aguarda e Captura Resultados
            print("Aguardando resultados...")
            res_frame, links = await wait_and_find_results(page)
            
            if not links or await links.count() == 0:
                # Procura aviso final
                msg = await page.locator(".ui-messages-error, .ui-messages-info").all_inner_texts()
                if not msg and res_frame:
                    msg = await res_frame.locator(".ui-messages-error, .ui-messages-info").all_inner_texts()
                
                if msg: 
                    result["aviso_site"] = msg
                    print(f"Aviso do site encontrado: {msg}")
                else:
                    print("Timeout: Nenhum resultado ou aviso encontrado.")
                
                return result

            count = await links.count()
            print(f"Encontrados {count} processos.")
            seen = set()
            
            for i in range(count):
                item = links.nth(i)
                txt = await item.inner_text()
                m = CNJ_RE.search(txt)
                if not m: continue
                numero = m.group(0)
                
                if numero in seen: continue
                seen.add(numero)

                clickable = item
                if await item.evaluate("el => el.tagName !== 'A'"):
                    clickable = item.locator("a").first

                if await clickable.count() > 0:
                    popup = await open_process_popup(page, clickable)
                    if popup:
                        meta = await extract_metadata(popup)
                        movs = await extract_movements(popup)
                        result["processos"].append({
                            "numero": numero,
                            **meta,
                            "movimentacoes": movs
                        })
                        await popup.close()
                    else:
                        result["processos"].append({"numero": numero, "erro": "popup_bloqueado"})

        except Exception as e:
            print(f"ERRO GERAL: {e}")
            result["erro_interno"] = str(e)
        finally:
            await browser.close()

    return result

@app.get("/health")
def health():
    return {"ok": True, "status": "online"}

@app.get("/consulta")
async def consulta(
    doc: str = Query(..., description="CPF ou CNPJ"),
    tipo: str = Query(..., description="Tipo do documento: cpf|cnpj")
):
    doc_digits = sanitize_doc(doc)
    doc_type = (tipo or "").strip().upper()
    
    if doc_type not in ("CPF", "CNPJ"):
        if doc_type.lower() in ("cpf", "cnpj"): doc_type = doc_type.upper()
        else: raise HTTPException(status_code=400, detail="Tipo inválido")

    if (doc_type == "CPF" and len(doc_digits) != 11) or (doc_type == "CNPJ" and len(doc_digits) != 14):
         raise HTTPException(status_code=400, detail="Documento com tamanho inválido")

    cache_key = f"{doc_digits}_{doc_type}"
    now = time.time()
    if cache_key in _cache and (now - _cache[cache_key]["ts"]) < CACHE_TTL:
        return _cache[cache_key]["data"]

    try:
        async def _run_scrape():
            async with SEMA:
                return await scrape_pje(doc_digits, doc_type)

        data = await asyncio.wait_for(_run_scrape(), timeout=180)
        _cache[cache_key] = {"ts": now, "data": data}
        return data

    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Tempo limite excedido")
    except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))
