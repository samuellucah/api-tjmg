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

# CNJ: 0000000-00.0000.0.00.0000
CNJ_RE = re.compile(r"\b\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}\b")

# Filtro para NÃO retornar ruídos
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
SEMA = asyncio.Semaphore(1)          # 1 request por vez (Playwright é pesado)
CACHE_TTL = 300                      # 5 minutos
# chave do cache: f"{type}:{doc}"
_cache: Dict[str, Dict[str, Any]] = {}

app = FastAPI(title="PJe TJMG - Consulta Pública (scraping CPF/CNPJ)")


# ========= Helpers de página =========

async def find_doc_input_any_frame(page):
    """
    Encontra o input correspondente ao bloco "CPF/CNPJ" (não o campo 'Processo').
    Procura em todas as frames.
    """
    frames = [page.main_frame] + [f for f in page.frames if f != page.main_frame]

    anchor_xpaths = [
        "xpath=//*[contains(.,'CPF') and contains(.,'CNPJ')][1]",
        "xpath=//label[contains(normalize-space(.),'CPF')][1]/parent::*",
        "xpath=//*[contains(normalize-space(.),'CPF')][1]",
    ]
    input_after = (
        "xpath=following::input[(not(@type) or @type='text' or @type='tel') "
        "and not(@disabled)][1]"
    )

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


async def selecionar_tipo_documento(page, tipo: str):
    """
    Se tipo == 'cnpj', clica explicitamente no rádio CNPJ:
    <input type="radio" name="tipoMascaraDocumento" onclick="mascaraDocumento('documentoParte', 'CNPJ')">
    """
    tipo = (tipo or "").strip().lower()
    if tipo != "cnpj":
        return

    # 1) Rádio com name + onclick contendo CNPJ
    try:
        # Precisamos buscar em todos os frames pois o formulário pode estar em um
        frames = [page.main_frame] + page.frames
        for fr in frames:
            cnpj_radio = fr.locator("input[name='tipoMascaraDocumento'][onclick*='CNPJ']")
            if await cnpj_radio.count() > 0:
                if await cnpj_radio.first.is_visible():
                    try:
                        await cnpj_radio.first.check(timeout=5000)
                    except:
                        await cnpj_radio.first.click(timeout=5000)
                else:
                    # Se estiver oculto, força clique via JS
                    await cnpj_radio.first.evaluate("el => el.click()")
                
                await page.wait_for_timeout(1000)
                return
    except:
        pass

    # 2) Fallback: tenta achar label e clicar
    try:
        await page.get_by_label("CNPJ", exact=True).click(force=True)
        await page.wait_for_timeout(1000)
    except:
        pass


async def wait_spinner_or_delay(page):
    """
    Aguarda o fim do 'spin' do PJe (quando existir).
    Caso não detecte, aguarda um pouco.
    """
    candidates = ".ui-widget-overlay, .ui-blockui, .ui-progressbar, [class*='loading' i], [class*='spinner' i]"
    try:
        await page.locator(candidates).first.wait_for(state="visible", timeout=2000)
        await page.locator(candidates).first.wait_for(state="hidden", timeout=25000)
    except:
        await page.wait_for_timeout(3000)


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
    """
    Tenta ir para a aba/área de Movimentações.
    Não falha se não achar.
    """
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
    """
    Extrai campos gerais do processo.
    """
    try:
        body = await popup.locator("body").inner_text()
    except:
        return {
            "assunto": None, "classe_judicial": None, "data_distribuicao": None,
            "orgao_julgador": None, "jurisdicao": None,
        }

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


async def extract_partes_from_row(link) -> Optional[str]:
    try:
        row = link.locator("xpath=ancestor::*[self::tr or self::div][1]")
        row_text = await row.inner_text()
        lines = [_norm(ln) for ln in row_text.splitlines() if ln.strip()]
        if not lines: return None
        for ln in reversed(lines):
            txt = ln.strip()
            if " x " in txt.lower() or " X " in txt: return txt
        return lines[-1]
    except:
        return None


# ========= Scraper principal =========

async def scrape_pje(doc_digits: str, tipo: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "documento": doc_digits,
        "tipo": tipo.upper(),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "processos": [],
    }

    async with async_playwright() as p:
        # --- AQUI ESTÁ A ÚNICA MUDANÇA EM RELAÇÃO AO COLAB ---
        # Adicionei os argumentos de segurança OBRIGATÓRIOS para rodar em VPS/Docker
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu"
            ]
        )
        
        # Ajuste de Viewport para garantir que elementos visuais apareçam
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768}
        )
        page = await context.new_page()

        try:
            await page.goto(URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(1500)

            # Seleciona CPF/CNPJ
            await selecionar_tipo_documento(page, tipo)

            fr, doc_input = await find_doc_input_any_frame(page)
            if doc_input is None:
                raise Exception("nao_encontrei_campo_documento")

            # Preenche documento
            await doc_input.click(timeout=60000)
            await doc_input.fill("")
            await page.wait_for_timeout(200) # Pequena pausa para garantir foco
            await doc_input.type(doc_digits, delay=50) # Digitação humana

            typed = (await doc_input.input_value()).strip()
            # Se for CNPJ e cortou o número, tenta injeção JS como fallback
            if tipo == "cnpj" and len(re.sub(r"\D", "", typed)) != 14:
                 await doc_input.evaluate(f"el => el.value = '{doc_digits}'")

            # Clica pesquisar
            btn = fr.get_by_role("button", name="PESQUISAR")
            if await btn.count() == 0:
                btn = page.get_by_role("button", name="PESQUISAR")
            
            if await btn.count() > 0:
                await btn.first.click(timeout=60000)
            else:
                await doc_input.press("Enter")

            await wait_spinner_or_delay(page)

            # Lista processos
            proc_links = page.locator("a").filter(has_text=CNJ_RE)
            if await proc_links.count() == 0:
                # Tenta buscar em linhas de tabela se não achar links diretos
                proc_links = page.locator("tr").filter(has_text=CNJ_RE)

            count = await proc_links.count()

            seen = set()
            for i in range(count):
                link = proc_links.nth(i)
                txt = _norm(await link.inner_text())
                m = CNJ_RE.search(txt)
                if not m: continue

                numero = m.group(0)
                if numero in seen: continue
                seen.add(numero)

                partes = await extract_partes_from_row(link)

                # Se for TR, busca o link clicável dentro
                clickable = link
                if await link.evaluate("el => el.tagName !== 'A'"):
                    clickable = link.locator("a").first

                if await clickable.count() > 0:
                    popup = await open_process_popup(page, clickable)
                    if popup is None:
                        # Tenta ícone lateral
                        icon = link.locator("xpath=ancestor::*[self::tr or self::div][1]//a[1]")
                        if await icon.count() > 0:
                            popup = await open_process_popup(page, icon.first)

                    if popup:
                        meta = await extract_metadata(popup)
                        movs = await extract_movements(popup)
                        result["processos"].append({
                            "numero": numero,
                            "partes": partes,
                            **meta,
                            "movimentacoes": movs,
                        })
                        await popup.close()
                    else:
                        result["processos"].append({"numero": numero, "erro": "nao_abriu_popup"})

        except Exception as e:
            result["erro_interno"] = str(e)
        finally:
            await browser.close()

    return result


# ========= Endpoints =========

@app.get("/health")
def health():
    return {"ok": True}


@app.get("/consulta")
async def consulta(
    doc: str = Query(..., description="CPF ou CNPJ (com ou sem pontuação)"),
    type: str = Query("cpf", description="Tipo do documento: 'cpf' ou 'cnpj'", alias="type"),
):
    tipo = (type or "").strip().lower()
    if tipo not in ("cpf", "cnpj"):
        raise HTTPException(status_code=400, detail="tipo_invalido (use 'cpf' ou 'cnpj')")

    doc_digits = sanitize_doc(doc)
    if not doc_digits:
        raise HTTPException(status_code=400, detail="documento_vazio")

    if tipo == "cpf" and len(doc_digits) != 11:
        raise HTTPException(status_code=400, detail="cpf_invalido")
    if tipo == "cnpj" and len(doc_digits) != 14:
        raise HTTPException(status_code=400, detail="cnpj_invalido")

    cache_key = f"{tipo}:{doc_digits}"
    now = time.time()
    cached = _cache.get(cache_key)
    if cached and (now - cached["ts"]) < CACHE_TTL:
        return cached["data"]

    async with SEMA:
        cached = _cache.get(cache_key)
        if cached and (time.time() - cached["ts"]) < CACHE_TTL:
            return cached["data"]

        # Adicionei tratamento de erro para timeout
        try:
            data = await asyncio.wait_for(scrape_pje(doc_digits, tipo), timeout=180)
            _cache[cache_key] = {"ts": time.time(), "data": data}
            return data
        except asyncio.TimeoutError:
             raise HTTPException(status_code=504, detail="timeout_no_tribunal")
        except Exception as e:
             raise HTTPException(status_code=500, detail=str(e))
