import re
import time
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query, HTTPException
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

URL = "https://pje-consulta-publica.tjmg.jus.br/"

# CNJ: 0000000-00.0000.0.00.0000
CNJ_RE = re.compile(r"\b\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}\b")

# Filtro para NÃO retornar ruídos (doc/certidão/visualizar/pjeoffice + paginação)
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

    <input type="radio" name="tipoMascaraDocumento"
           onclick="mascaraDocumento('documentoParte', 'CNPJ')">
    """
    tipo = (tipo or "").strip().lower()
    if tipo != "cnpj":
        return

    # 1) Rádio com name + onclick contendo CNPJ
    try:
        cnpj_radio = page.locator(
            "input[name='tipoMascaraDocumento'][onclick*='CNPJ']"
        )
        if await cnpj_radio.count() > 0 and await cnpj_radio.first.is_visible():
            try:
                await cnpj_radio.first.check(timeout=5000)
            except:
                await cnpj_radio.first.click(timeout=5000)
            await page.wait_for_timeout(1000)
            return
    except:
        pass

    # 2) Fallback: segundo input[name='tipoMascaraDocumento'] (CPF é o primeiro)
    try:
        radios = page.locator("input[name='tipoMascaraDocumento']")
        if await radios.count() >= 2:
            target = radios.nth(1)
            if await target.is_visible():
                try:
                    await target.check(timeout=5000)
                except:
                    await target.click(timeout=5000)
                await page.wait_for_timeout(1000)
                return
    except:
        pass
    # Se não achar, segue como CPF mesmo (não levantamos erro aqui).


async def wait_spinner_or_delay(page):
    """
    Aguarda o fim do 'spin' do PJe (quando existir).
    Caso não detecte, aguarda um pouco.
    """
    candidates = ".ui-widget-overlay, .ui-blockui, .ui-progressbar, [class*='loading' i], [class*='spinner' i]"
    loc = page.locator(candidates)
    try:
        await loc.first.wait_for(state="visible", timeout=2000)
        await loc.first.wait_for(state="hidden", timeout=25000)
    except TimeoutError:
        await page.wait_for_timeout(8000)
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
    Extrai campos gerais do processo: Assunto, Classe Judicial, Data Distribuição,
    Órgão Julgador, Jurisdição (às vezes aparece como Comarca).
    Implementação robusta por texto do body.
    """
    try:
        body = await popup.locator("body").inner_text()
    except:
        return {
            "assunto": None,
            "classe_judicial": None,
            "data_distribuicao": None,
            "orgao_julgador": None,
            "jurisdicao": None,
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
                    if not UNWANTED_RE.search(val):
                        return val
                if i + 1 < len(lines) and lines[i + 1]:
                    val = lines[i + 1]
                    if not UNWANTED_RE.search(val):
                        return val
        return None

    return {
        "assunto": find_value(["assunto", "assunto(s)"]),
        "classe_judicial": find_value(["classe judicial", "classe"]),
        "data_distribuicao": find_value(
            ["data da distribuição", "data de distribuição", "distribuição"]
        ),
        "orgao_julgador": find_value(["órgão julgador", "orgao julgador"]),
        "jurisdicao": find_value(["jurisdição", "jurisdicao", "comarca"]),
    }


async def extract_movements(popup) -> List[str]:
    """
    Extrai movimentações e filtra ruídos de documentos/certidões/visualizações.
    """
    await try_click_movements_tab(popup)

    texts: List[str] = []
    seen = set()

    selectors = [
        "css=[id*='moviment' i] tr",
        "css=[class*='moviment' i] tr",
        "css=[id*='moviment' i] li",
        "css=[class*='moviment' i] li",
        "xpath=//table[.//*[contains(translate(.,'MOVIMENTACOESÇÃ','movimentacoesca'),'moviment')]]//tr",
        "xpath=//ul[.//*[contains(translate(.,'MOVIMENTACOESÇÃ','movimentacoesca'),'moviment')]]//li",
    ]

    for sel in selectors:
        try:
            loc = popup.locator(sel)
            cnt = await loc.count()
            if cnt == 0:
                continue
            for i in range(min(cnt, 500)):
                t = _norm(await loc.nth(i).inner_text())
                if not t:
                    continue
                if UNWANTED_RE.search(t):
                    continue
                if t in seen:
                    continue
                seen.add(t)
                texts.append(t)
            if len(texts) >= 5:
                break
        except:
            pass

    if not texts:
        try:
            body = await popup.locator("body").inner_text()
            for ln in body.split("\n"):
                t = _norm(ln)
                if not t or UNWANTED_RE.search(t):
                    continue
                if t in seen:
                    continue
                seen.add(t)
                texts.append(t)
        except:
            pass

    return texts


async def extract_partes_from_row(link) -> Optional[str]:
    """
    Pega o texto da 'linha' da listagem e tenta extrair a linha com as partes
    (ex.: 'AUTOR X RÉU ...').
    """
    try:
        row = link.locator("xpath=ancestor::*[self::tr or self::div][1]")
        row_text = await row.inner_text()
        lines = [_norm(ln) for ln in row_text.splitlines()]
        lines = [ln for ln in lines if ln]
        if not lines:
            return None

        for ln in reversed(lines):
            txt = ln.strip()
            if not txt:
                continue
            if " x " in txt.lower() or " X " in txt:
                return txt

        return lines[-1]
    except:
        return None


# ========= Scraper principal =========

async def scrape_pje(doc_digits: str, tipo: str) -> Dict[str, Any]:
    """
    doc_digits: CPF ou CNPJ só com dígitos
    tipo: 'cpf' ou 'cnpj'
    """
    result: Dict[str, Any] = {
        "documento": doc_digits,
        "tipo": tipo.upper(),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "processos": [],
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1280, "height": 720})

        await page.goto(URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(1200)

        # Seleciona CPF/CNPJ antes de achar o campo
        await selecionar_tipo_documento(page, tipo)

        fr, doc_input = await find_doc_input_any_frame(page)
        if doc_input is None:
            await browser.close()
            raise HTTPException(status_code=500, detail="nao_encontrei_campo_documento")

        # Preenche documento
        await doc_input.click(timeout=60000)
        await doc_input.fill("")
        await doc_input.type(doc_digits, delay=40)

        typed = (await doc_input.input_value()).strip()
        if not typed:
            await browser.close()
            raise HTTPException(status_code=500, detail="documento_nao_preencheu")

        # Clica pesquisar
        btn = fr.get_by_role("button", name="PESQUISAR")
        if await btn.count() == 0:
            btn = page.get_by_role("button", name="PESQUISAR")
        await btn.first.click(timeout=60000)

        await wait_spinner_or_delay(page)

        # Lista processos (links com número CNJ)
        proc_links = page.locator("a").filter(has_text=CNJ_RE)
        count = await proc_links.count()

        for i in range(count):
            link = proc_links.nth(i)
            txt = _norm(await link.inner_text())
            m = CNJ_RE.search(txt)
            if not m:
                continue

            numero = m.group(0)

            # partes (linha vermelha)
            partes = await extract_partes_from_row(link)

            popup = await open_process_popup(page, link)
            if popup is None:
                icon = link.locator("xpath=ancestor::*[self::tr or self::div][1]//a[1]")
                if await icon.count() > 0:
                    popup = await open_process_popup(page, icon.first)

            if popup is None:
                result["processos"].append({
                    "numero": numero,
                    "partes": partes,
                    "assunto": None,
                    "classe_judicial": None,
                    "data_distribuicao": None,
                    "orgao_julgador": None,
                    "jurisdicao": None,
                    "movimentacoes": [],
                    "erro": "nao_abriu_popup",
                })
                continue

            await popup.wait_for_timeout(1200)

            meta = await extract_metadata(popup)
            movs = await extract_movements(popup)

            result["processos"].append({
                "numero": numero,
                "partes": partes,
                **meta,
                "movimentacoes": movs,
            })

            await popup.close()

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
    """
    Exemplo:
      /consulta?doc=068.871.486-25&type=cpf
      /consulta?doc=43.037.250/0001-09&type=cnpj
    """
    tipo = (type or "").strip().lower()
    if tipo not in ("cpf", "cnpj"):
        raise HTTPException(status_code=400, detail="tipo_invalido (use 'cpf' ou 'cnpj')")

    doc_digits = sanitize_doc(doc)
    if not doc_digits:
        raise HTTPException(status_code=400, detail="documento_vazio")

    # validação simples de tamanho
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

        data = await asyncio.wait_for(scrape_pje(doc_digits, tipo), timeout=180)
        _cache[cache_key] = {"ts": time.time(), "data": data}
        return data
