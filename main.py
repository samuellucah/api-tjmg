import re
import time
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Query, HTTPException
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

URL = "https://pje-consulta-publica.tjmg.jus.br/"

CNJ_RE = re.compile(r"\b\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}\b")

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


# -----------------------------
# FUNÇÕES AUXILIARES (DOM)
# -----------------------------
async def find_input_any_frame(page):
    """Procura o campo de input em todos os frames/iframes."""
    frames = [page.main_frame] + [f for f in page.frames if f != page.main_frame]

    # ✅ corrigido: xpaths válidos
    anchor_xpaths = [
        "xpath=//*[contains(normalize-space(.),'CPF') and contains(normalize-space(.),'CNPJ')][1]",
        "xpath=//label[contains(normalize-space(.),'CPF')][1]/parent::*",
        "xpath=//label[contains(normalize-space(.),'CNPJ')][1]/parent::*",
        "xpath=//*[contains(normalize-space(.),'CPF')][1]",
        "xpath=//*[contains(normalize-space(.),'CNPJ')][1]",
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
    """
    Força a seleção do Radio Button (CPF/CNPJ).
    Retorna True se conseguiu mudar.
    """
    target = (doc_type or "").upper().strip()
    if target not in ("CPF", "CNPJ"):
        return False

    # ✅ mais robusto: cobre value=cpf/cnpj e label -> input anterior
    locators = [
        frame.get_by_label(target, exact=True),
        frame.locator(f"input[type='radio'][value='{target}']"),
        frame.locator(f"input[type='radio'][value='{target.lower()}']"),
        frame.locator(f"xpath=//label[contains(normalize-space(.), '{target}')]/preceding::input[@type='radio'][1]"),
        frame.locator(f"xpath=//label[contains(normalize-space(.), '{target}')]/descendant-or-self::input[@type='radio'][1]"),
    ]

    for loc in locators:
        try:
            if await loc.count() > 0:
                el = loc.first
                try:
                    await el.check(force=True, timeout=1500)
                except:
                    await el.evaluate("e => e.click()")
                await page.wait_for_timeout(800)  # tempo p/ AJAX atualizar máscara
                return True
        except:
            continue

    return False


async def wait_results(page, timeout_ms: int = 25000):
    """Espera algum sinal de resultados (CNJ na tela ou texto 'resultados encontrados')."""
    start = time.time()
    while (time.time() - start) * 1000 < timeout_ms:
        try:
            if await page.get_by_text(CNJ_RE).count() > 0:
                return
        except:
            pass
        try:
            if await page.get_by_text(re.compile(r"resultados?\s+encontrados", re.I)).count() > 0:
                return
        except:
            pass
        await page.wait_for_timeout(500)


async def open_process_popup(page, clickable):
    """Clica no item que abre o processo e tenta capturar popup/aba."""
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
                    if not UNWANTED_RE.search(val):
                        return val
                if i + 1 < len(lines):
                    val = lines[i + 1]
                    if not UNWANTED_RE.search(val):
                        return val
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
            await popup.wait_for_timeout(600)
    except:
        pass

    rows = popup.locator("tr")
    count = await rows.count()

    for i in range(min(count, 120)):
        try:
            txt = _norm(await rows.nth(i).inner_text())
            if len(txt) > 10 and not UNWANTED_RE.search(txt) and txt not in seen:
                seen.add(txt)
                texts.append(txt)
        except:
            continue

    return texts[:10]


async def get_result_items(page):
    """
    ✅ Correção-chave p/ CNPJ:
    Em alguns resultados o CNJ NÃO está em <a>.
    Então pegamos o container do resultado (li/tr) que contém o CNJ.
    """
    items = page.locator("li").filter(has_text=CNJ_RE)
    if await items.count() > 0:
        return items

    items = page.locator("tr").filter(has_text=CNJ_RE)
    if await items.count() > 0:
        return items

    return None


def normalize_tipo(tipo: Optional[str], doc_digits: str) -> str:
    if tipo:
        t = tipo.strip().upper()
        if t in ("CPF", "CNPJ"):
            return t
        if t in ("DOC",):
            pass
    # fallback por tamanho
    return "CNPJ" if len(doc_digits) == 14 else "CPF"


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
            await page.wait_for_timeout(1800)

            # 1) acha input
            fr, doc_input = await find_input_any_frame(page)
            if not doc_input:
                raise Exception("Input de CPF/CNPJ não encontrado na página.")

            # 2) ✅ SEMPRE seleciona o tipo (CPF ou CNPJ) ANTES de digitar
            await force_set_doc_type_radio(page, fr, doc_type)

            # como o PJe pode recriar o input via AJAX, re-localiza
            await page.wait_for_timeout(900)
            fr, doc_input = await find_input_any_frame(page)
            if not doc_input:
                raise Exception("Input de CPF/CNPJ não encontrado após selecionar o tipo.")

            # 3) digita
            await doc_input.click()
            await doc_input.fill("")
            await doc_input.type(doc_digits, delay=60)

            await page.keyboard.press("Tab")
            await page.wait_for_timeout(400)

            # 4) pesquisar (botão pode ser button ou input submit)
            btn = fr.locator("button:has-text('PESQUISAR'), input[type='submit'][value*='PESQUISAR' i], a:has-text('PESQUISAR')").first
            if await btn.count() == 0:
                btn = page.locator("button:has-text('PESQUISAR'), input[type='submit'][value*='PESQUISAR' i], a:has-text('PESQUISAR')").first

            if await btn.count() > 0:
                await btn.click(timeout=10000)
            else:
                await doc_input.press("Enter")

            # espera progresso (se existir) + fallback por CNJ/texto
            try:
                await page.locator(".ui-progressbar").wait_for(state="visible", timeout=2500)
                await page.locator(".ui-progressbar").wait_for(state="hidden", timeout=25000)
            except:
                pass
            await wait_results(page, timeout_ms=25000)

            # 5) ✅ coleta processos
            # (A) tenta anchors (funciona em muitos casos)
            links = page.locator("a").filter(has_text=CNJ_RE)
            count_links = await links.count()

            seen = set()

            if count_links > 0:
                for i in range(min(count_links, 25)):
                    link = links.nth(i)
                    txt = await link.inner_text()
                    m = CNJ_RE.search(txt)
                    if not m:
                        # às vezes o CNJ está no container, não no texto do <a>
                        parent_txt = _norm(await link.locator("xpath=ancestor::*[1]").inner_text())
                        m = CNJ_RE.search(parent_txt)

                    if not m:
                        continue

                    numero = m.group(0)
                    if numero in seen:
                        continue
                    seen.add(numero)

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

            else:
                # (B) ✅ fallback p/ CNPJ: CNJ pode não estar em <a>
                items = await get_result_items(page)
                if not items or await items.count() == 0:
                    msg = await page.locator(".ui-messages-error").all_inner_texts()
                    if msg:
                        result["aviso_site"] = msg
                    return result

                for i in range(min(await items.count(), 25)):
                    item = items.nth(i)
                    txt = _norm(await item.inner_text())
                    m = CNJ_RE.search(txt)
                    if not m:
                        continue
                    numero = m.group(0)
                    if numero in seen:
                        continue
                    seen.add(numero)

                    # tenta clicar no ícone/link dentro do item (geralmente o quadradinho com seta)
                    clickable = item.locator("a:has(i), a, button").first
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
            result["erro_interno"] = str(e)

        finally:
            await browser.close()

    return result


# -----------------------------
# ENDPOINTS
# -----------------------------
@app.get("/health")
def health():
    return {"ok": True, "status": "online"}


@app.get("/consulta")
async def consulta(
    doc: str = Query(..., description="CPF ou CNPJ (em 'doc')"),
    tipo: str = Query(..., description="Tipo do documento: cpf|cnpj")
):
    doc_digits = sanitize_doc(doc)
    doc_type = (tipo or "").strip().upper()
    if doc_type not in ("CPF", "CNPJ"):
        # aceita cpf/cnpj em minúsculo também
        if doc_type.lower() in ("cpf", "cnpj"):
            doc_type = doc_type.upper()
        else:
            raise HTTPException(status_code=400, detail="Tipo inválido (use cpf ou cnpj)")

    # valida tamanho coerente
    if doc_type == "CPF" and len(doc_digits) != 11:
        raise HTTPException(status_code=400, detail="CPF inválido (deve ter 11 dígitos)")
    if doc_type == "CNPJ" and len(doc_digits) != 14:
        raise HTTPException(status_code=400, detail="CNPJ inválido (deve ter 14 dígitos)")

    cache_key = f"{doc_digits}_{doc_type}"
    now = time.time()
    if cache_key in _cache:
        item = _cache[cache_key]
        if (now - item["ts"]) < CACHE_TTL:
            return item["data"]

    try:
        async def _run_scrape():
            async with SEMA:
                return await scrape_pje(doc_digits, doc_type)

        data = await asyncio.wait_for(_run_scrape(), timeout=180)
        _cache[cache_key] = {"ts": now, "data": data}
        return data

    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Tempo limite excedido (Site do Tribunal lento)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
