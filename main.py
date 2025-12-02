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

async def select_radio_by_index(page, frame, index: int):
    """
    Clica no Radio Button baseado na posição visual (0-based).
    """
    try:
        # Busca todos os radios no frame
        radios = frame.locator("input[type='radio']")
        count = await radios.count()
        print(f"DEBUG: Encontrados {count} botões de rádio no frame.")
        
        if count > index:
            # Força o clique no índice específico
            # Em alguns casos do PJe, o input real está escondido e precisamos clicar no label ou span pai.
            # Mas o force=True do Playwright costuma resolver.
            await radios.nth(index).click(force=True)
            return True
        else:
            print(f"Erro: Tentou clicar no radio índice {index}, mas só achou {count} radios.")
            return False
    except Exception as e:
        print(f"Erro ao clicar no radio: {e}")
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
    start_time = time.time()
    while (time.time() - start_time) < 30: 
        frames = [page.main_frame] + page.frames
        for fr in frames:
            try:
                links = fr.locator("a").filter(has_text=CNJ_RE)
                if await links.count() > 0:
                    return fr, links
                
                rows = fr.locator("tr").filter(has_text=CNJ_RE)
                if await rows.count() > 0:
                    return fr, rows
                
                msg_el = fr.locator(".ui-messages-error, .ui-messages-info, .ui-messages-warn")
                if await msg_el.count() > 0:
                    txt = await msg_el.first.inner_text()
                    if "encontrado" in txt.lower() or "registro" in txt.lower():
                        return fr, None 
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
            # 1. Abre a URL
            page = await context.new_page()
            await page.goto(URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(2000)

            # Localiza o frame onde estão os inputs
            fr, doc_input = await find_input_any_frame(page)
            if not doc_input:
                raise Exception("Input de CPF/CNPJ não encontrado.")

            # 2. e 3. Verifica Tipo e Clica no Radio Button Correto (CORRIGIDO PARA 4 RADIOS)
            # Ordem visual: [0] Numeração, [1] Livre, [2] CPF, [3] CNPJ
            if doc_type == "CNPJ":
                radio_index = 3  # Quarto botão
            else:
                radio_index = 2  # Terceiro botão (CPF)

            print(f"Tipo: {doc_type}. Clicando no Radio Index: {radio_index}")
            
            success = await select_radio_by_index(page, fr, radio_index)
            if not success:
                # Tenta fallback na página principal se falhar no frame
                await select_radio_by_index(page, page, radio_index)

            # 4. Espera OBRIGATÓRIA de 5 segundos
            print("Aguardando 5 segundos para troca de máscara...")
            await page.wait_for_timeout(5000)
            
            # Recarrega input (o DOM pode ter mudado)
            fr, doc_input = await find_input_any_frame(page)
            if not doc_input:
                 raise Exception("Campo de texto perdido após troca de tipo.")

            # 5. Preenche o número
            print(f"Preenchendo {doc_digits}...")
            await doc_input.click()
            
            # Limpeza reforçada
            await doc_input.press("Control+A")
            await doc_input.press("Backspace")
            await page.wait_for_timeout(500)
            
            await doc_input.type(doc_digits, delay=100) 
            
            # Sai do campo para acionar validação
            await page.keyboard.press("Tab")
            await page.wait_for_timeout(1000)

            # 6. Confere os dígitos
            raw_val = await doc_input.input_value()
            clean_val = re.sub(r"\D+", "", raw_val)
            print(f"Valor lido no campo: {clean_val}")

            # Se for CNPJ e não tiver 14 dígitos, tenta corrigir na força bruta
            if doc_type == "CNPJ" and len(clean_val) != 14:
                print("ERRO: Valor incorreto/cortado. Tentando injeção JS...")
                await doc_input.evaluate(f"el => el.value = '{doc_digits}'")
                await doc_input.dispatch_event("input")
                await doc_input.dispatch_event("change")
                await page.wait_for_timeout(1000)
                
                # Re-confere
                raw_val = await doc_input.input_value()
                clean_val = re.sub(r"\D+", "", raw_val)
                if len(clean_val) != 14:
                     result["aviso_site"] = f"Falha crítica: O campo permaneceu com {len(clean_val)} dígitos após tentativas."

            # 7. Clica em Pesquisar
            print("Clicando em Pesquisar...")
            btn = fr.locator("button:has-text('PESQUISAR'), input[type='submit'][value*='PESQUISAR' i]").first
            if await btn.count() == 0:
                btn = page.locator("button:has-text('PESQUISAR')").first
            
            if await btn.count() > 0:
                await btn.click()
            else:
                await doc_input.press("Enter")
            
            # 8. Retorna Resultados
            print("Aguardando resultados...")
            res_frame, links = await wait_and_find_results(page)
            
            if not links or await links.count() == 0:
                msg = await page.locator(".ui-messages-error, .ui-messages-info").all_inner_texts()
                if not msg and res_frame:
                    msg = await res_frame.locator(".ui-messages-error, .ui-messages-info").all_inner_texts()
                
                if msg: 
                    result["aviso_site"] = msg
                
                return result

            count = await links.count()
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
