import re
import time
import asyncio
import nest_asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query, HTTPException
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# Permite rodar loops aninhados
nest_asyncio.apply()

URL = "https://pje-consulta-publica.tjmg.jus.br/"

# Regex para encontrar número de processo (CNJ)
CNJ_RE = re.compile(r"\b\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}\b")

# Regex para filtrar textos inúteis
UNWANTED_RE = re.compile(
    r"(documentos?\s+juntados|documento\b|certid[aã]o|visualizar|"
    r"pjeoffice|indispon[ií]vel|aplicativo\s+pjeoffice|"
    r"página\b|resultados?\s+encontrados|recibo)",
    re.IGNORECASE,
)

def _norm(txt: str) -> str:
    return re.sub(r"\s+", " ", (txt or "")).strip()

def sanitize_doc(doc: str) -> str:
    """Remove tudo que não for número."""
    return re.sub(r"\D+", "", doc or "")

# ===== Concurrency + Cache =====
SEMA = asyncio.Semaphore(1)          
CACHE_TTL = 300                      
_cache: Dict[str, Dict[str, Any]] = {} 

app = FastAPI(title="PJe TJMG - Consulta Pública (scraping)")

async def find_input_any_frame(page):
    """
    Encontra o input de texto onde digita o número.
    """
    frames = [page.main_frame] + [f for f in page.frames if f != page.main_frame]
    
    # Procura input próximo a label CPF ou CNPJ
    anchor_xpaths = [
        "xpath=//*[contains(.,'CPF') and contains(.,'CNPJ')][1]",
        "xpath=//label[contains(normalize-space(.),'CPF')][1]/parent::*",
        "xpath=//*[contains(normalize-space(.),'CNPJ')][1]/parent::*",
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

async def force_set_doc_type_radio(page, frame, doc_type: str):
    """
    Função BLINDADA: Tenta clicar e, se falhar, força via JavaScript.
    """
    target = doc_type.upper().strip() # CPF ou CNPJ
    
    # 1. Localizadores possíveis
    locators = [
        frame.get_by_label(target, exact=True),
        frame.locator(f"input[type='radio'][value='{target}']"),
        frame.locator(f"xpath=//label[contains(., '{target}')]/preceding-sibling::input[@type='radio']"),
        frame.locator(f"xpath=//label[contains(., '{target}')]//input[@type='radio']"),
        frame.get_by_text(target, exact=True)
    ]

    selected = False
    
    for loc in locators:
        try:
            if await loc.count() > 0:
                # Tenta check normal
                if await loc.first.is_visible():
                    await loc.first.check(force=True, timeout=1000)
                    selected = True
                else:
                    # Se tiver escondido, tenta via JS
                    await loc.first.evaluate("el => el.checked = true")
                    await loc.first.evaluate("el => el.click()") # Dispara evento
                    selected = True
                
                if selected:
                    break
        except:
            continue
            
    # Fallback Javascript Puro (caso os seletores do Playwright falhem)
    # Procura qualquer input radio que tenha o valor CNPJ ou esteja perto do texto CNPJ
    if not selected:
        try:
            await frame.evaluate(f"""() => {{
                const radios = Array.from(document.querySelectorAll('input[type="radio"]'));
                for (const r of radios) {{
                    // Tenta pelo valor
                    if (r.value === '{target}') {{
                        r.checked = true;
                        r.click();
                        return;
                    }}
                    // Tenta pelo label próximo
                    if (r.nextSibling && r.nextSibling.textContent && r.nextSibling.textContent.includes('{target}')) {{
                        r.checked = true;
                        r.click();
                        return;
                    }}
                    // Tenta pelo label pai
                    if (r.parentElement && r.parentElement.textContent.includes('{target}')) {{
                        r.checked = true;
                        r.click();
                        return;
                    }}
                }}
            }}""")
        except:
            pass

    await page.wait_for_timeout(500)
    return True

async def wait_spinner_or_delay(page):
    candidates = ".ui-widget-overlay, .ui-blockui, .ui-progressbar, [class*='loading' i], [class*='spinner' i]"
    loc = page.locator(candidates)
    try:
        await loc.first.wait_for(state="visible", timeout=2000)
        await loc.first.wait_for(state="hidden", timeout=25000)
    except PlaywrightTimeoutError:
        await page.wait_for_timeout(5000)

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
        "data_distribuicao": find_value(["data da distribuição", "distribuição"]),
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

async def scrape_pje(doc_digits: str, doc_type: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "documento": doc_digits,
        "tipo": doc_type,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "processos": [],
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            # Importante: Args extras para evitar detecção e melhorar performance
            args=[
                "--no-sandbox", 
                "--disable-setuid-sandbox", 
                "--disable-blink-features=AutomationControlled",
                "--ignore-certificate-errors"
            ]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            locale="pt-BR"
        )
        page = await context.new_page()

        try:
            await page.goto(URL, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000) # Espera inicial generosa

            # 1. Encontra o frame e o input
            fr, doc_input = await find_input_any_frame(page)
            if doc_input is None:
                raise HTTPException(status_code=500, detail="nao_encontrei_campo_input")

            # 2. LIMPA E PREENCHE PRIMEIRO O DOCUMENTO (Estratégia Anti-Reset)
            await doc_input.click(timeout=10000)
            await doc_input.fill("")
            # Digita mais devagar para parecer humano
            await doc_input.type(doc_digits, delay=100) 
            
            # 3. AGORA SIM, FORÇA A SELEÇÃO DO TIPO
            await force_set_doc_type_radio(page, fr, doc_type)

            # 4. Verifica se o texto ainda está lá (alguns sites limpam ao mudar o radio)
            typed = (await doc_input.input_value()).strip()
            if typed != doc_digits:
                await doc_input.fill("")
                await doc_input.type(doc_digits, delay=50)

            # 5. Clica pesquisar
            btn = fr.get_by_role("button", name="PESQUISAR")
            if await btn.count() == 0:
                btn = page.get_by_role("button", name="PESQUISAR")
            
            if await btn.count() > 0:
                await btn.first.click(timeout=30000)
            else:
                await doc_input.press("Enter")

            await wait_spinner_or_delay(page)

            # 6. Lista processos
            # Procura links que contenham o padrão CNJ
            proc_links = page.locator("a").filter(has_text=CNJ_RE)
            
            # Pequeno wait extra para garantir renderização da tabela
            try:
                await proc_links.first.wait_for(state="attached", timeout=4000)
            except:
                pass

            count = await proc_links.count()

            for i in range(count):
                link = proc_links.nth(i)
                txt = _norm(await link.inner_text())
                m = CNJ_RE.search(txt)
                if not m: continue
                numero = m.group(0)

                popup = await open_process_popup(page, link)
                if popup is None:
                    # Tenta ícone da lupa/pasta se houver
                    icon = link.locator("xpath=ancestor::*[self::tr or self::div][1]//a[contains(@title, 'Visualizar') or contains(@class, 'visualizar')]")
                    if await icon.count() > 0:
                        popup = await open_process_popup(page, icon.first)

                if popup is None:
                    result["processos"].append({"numero": numero, "erro": "nao_abriu_popup"})
                    continue

                await popup.wait_for_timeout(1500)
                meta = await extract_metadata(popup)
                movs = await extract_movements(popup)

                result["processos"].append({"numero": numero, **meta, "movimentacoes": movs})
                await popup.close()

        except Exception as e:
            await browser.close()
            # Retorna o erro detalhado para ajudar no debug
            raise HTTPException(status_code=500, detail=f"Erro no scraping: {str(e)}")

        await browser.close()

    return result

@app.get("/health")
def health():
    return {"ok": True, "status": "online"}

@app.get("/consulta")
async def consulta(
    doc: str = Query(..., description="Número do documento (CPF ou CNPJ)"),
    tipo: Optional[str] = Query(None, description="Tipo do documento: 'CPF' ou 'CNPJ'")
):
    doc_digits = sanitize_doc(doc)
    if not doc_digits:
        raise HTTPException(status_code=400, detail="documento_vazio")

    # Lógica de detecção automática do tipo
    doc_type = "CPF" # Default
    if tipo:
        doc_type = tipo.upper().strip()
    else:
        if len(doc_digits)
