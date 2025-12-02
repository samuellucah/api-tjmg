import re
import time
import asyncio
import nest_asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query, HTTPException
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# Permite loops aninhados (essencial para FastAPI + Playwright)
nest_asyncio.apply()

URL = "https://pje-consulta-publica.tjmg.jus.br/"

# Regex CNJ: 0000000-00.0000.0.00.0000
CNJ_RE = re.compile(r"\b\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}\b")

# Filtra ruídos do texto extraído
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

# Limita a 1 requisição simultânea para não estourar a memória da VPS
SEMA = asyncio.Semaphore(1)          
CACHE_TTL = 300                      
_cache: Dict[str, Dict[str, Any]] = {} 

app = FastAPI(title="PJe TJMG - Scraper")

# --- FUNÇÕES AUXILIARES ---

async def find_input_any_frame(page):
    """Procura o campo de input em todos os frames/iframes."""
    frames = [page.main_frame] + [f for f in page.frames if f != page.main_frame]
    
    anchor_xpaths = [
        "xpath=//*[contains(.,'CPF') and contains(.,'CNPJ')][1]",
        "xpath=//label[contains(normalize-space(.),'CPF')][1]/parent::*",
        "xpath=//*[contains(normalize-space(.),'CNPJ')][1]/parent::*",
    ]
    # Input que vem logo após a label/radio
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

async def force_set_doc_type_radio(page, frame, doc_type: str):
    """
    Força a seleção do Radio Button (CPF/CNPJ).
    Retorna True se conseguiu mudar.
    """
    target = doc_type.upper().strip()
    
    # Lista de estratégias para achar a bolinha correta
    locators = [
        frame.get_by_label(target, exact=True),
        frame.locator(f"input[type='radio'][value='{target}']"),
        frame.locator(f"xpath=//label[contains(., '{target}')]//input[@type='radio']"),
        frame.get_by_text(target, exact=True)
    ]

    for loc in locators:
        try:
            if await loc.count() > 0:
                # Tenta clicar/marcar
                if await loc.first.is_visible():
                    await loc.first.check(force=True, timeout=1000)
                else:
                    await loc.first.evaluate("el => el.click()")
                
                # Espera o site processar a troca (AJAX do JSF)
                await page.wait_for_timeout(500)
                return True
        except:
            continue
            
    return False

async def open_process_popup(page, clickable):
    """Clica no link do processo e espera a nova aba/popup abrir."""
    try:
        async with page.expect_popup(timeout=15000) as pop:
            await clickable.click(timeout=10000)
        popup = await pop.value
        await popup.wait_for_load_state("domcontentloaded")
        return popup
    except:
        return None

async def extract_metadata(popup) -> Dict[str, Optional[str]]:
    """Extrai dados básicos do processo."""
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
    """Extrai as movimentações da tabela."""
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
            await page.goto(URL, wait_until="domcontentloaded", timeout=45000)
            await page.wait_for_timeout(2000)

            # 1. Busca Frame e Input inicial
            fr, doc_input = await find_input_any_frame(page)
            if not doc_input:
                raise Exception("Input de CPF/CNPJ não encontrado na página.")

            # 2. ORDEM CORRETA: SELECIONA TIPO -> DEPOIS DIGITA
            # Se for CNPJ, a gente clica primeiro, espera o campo atualizar a máscara, e só depois digita.
            if doc_type == "CNPJ":
                await force_set_doc_type_radio(page, fr, "CNPJ")
                # Importante: Como o PJe recarrega o campo (AJAX) ao mudar o tipo,
                # precisamos buscar o input novamente para não dar erro de elemento "stale" (velho)
                await page.wait_for_timeout(1000)
                fr, doc_input = await find_input_any_frame(page)

            # 3. Digita o número
            await doc_input.click()
            await doc_input.fill("")
            await doc_input.type(doc_digits, delay=80)
            
            # Pressiona TAB para sair do campo e forçar validação do PJe
            await page.keyboard.press("Tab")
            await page.wait_for_timeout(500)

            # 4. Pesquisa
            btn = fr.get_by_role("button", name="PESQUISAR")
            if await btn.count() == 0:
                btn = page.get_by_role("button", name="PESQUISAR")
            
            if await btn.count() > 0:
                await btn.first.click()
            else:
                await doc_input.press("Enter")
            
            # Espera carregamento
            try:
                await page.locator(".ui-progressbar").wait_for(state="visible", timeout=1500)
                await page.locator(".ui-progressbar").wait_for(state="hidden", timeout=25000)
            except:
                await page.wait_for_timeout(3000)

            # 5. Coleta Processos
            links = page.locator("a").filter(has_text=CNJ_RE)
            count = await links.count()
            
            if count == 0:
                msg = await page.locator(".ui-messages-error").all_inner_texts()
                if msg:
                    result["aviso_site"] = msg

            for i in range(count):
                link = links.nth(i)
                txt = await link.inner_text()
                m = CNJ_RE.search(txt)
                if not m: continue
                numero = m.group(0)

                popup = await open_process_popup(page, link)
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
            print(f"ERRO SCRAPING: {e}") 
            result["erro_interno"] = str(e)
        
        finally:
            await browser.close()

    return result

# --- API ENDPOINTS ---

@app.get("/health")
def health():
    return {"ok": True, "status": "online"}

@app.get("/consulta")
async def consulta(
    doc: str = Query(..., description="CPF ou CNPJ"),
    tipo: Optional[str] = Query(None)
):
    doc_digits = sanitize_doc(doc)
    
    # Detecção automática
    doc_type = "CPF"
    if tipo:
        doc_type = tipo.upper().strip()
    elif len(doc_digits) == 14:
        doc_type = "CNPJ"
    
    if len(doc_digits) not in [11, 14]:
         raise HTTPException(status_code=400, detail="Documento inválido (deve ter 11 ou 14 dígitos)")

    cache_key = f"{doc_digits}_{doc_type}"
    now = time.time()
    if cache_key in _cache:
        item = _cache[cache_key]
        if (now - item["ts"]) < CACHE_TTL:
            return item["data"]

    try:
        # Função wrapper para compatibilidade com versões antigas do Python (3.10)
        async def _run_scrape():
            async with SEMA:
                return await scrape_pje(doc_digits, doc_type)

        # FIX DO ERRO 500: Usamos wait_for em vez de timeout()
        data = await asyncio.wait_for(_run_scrape(), timeout=180)
        
        _cache[cache_key] = {"ts": now, "data": data}
        return data

    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Tempo limite excedido (Site do Tribunal lento)")
    except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))
