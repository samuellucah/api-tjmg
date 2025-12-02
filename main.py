import re
import time
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Query, HTTPException
from playwright.async_api import async_playwright

URL = "https://pje-consulta-publica.tjmg.jus.br/"

# Regex CNJ: 0000000-00.0000.0.00.0000
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

def format_doc(doc_digits: str, doc_type: str) -> str:
    """Formata para a máscara do campo (ajuda MUITO no CNPJ)."""
    if doc_type == "CPF" and len(doc_digits) == 11:
        return f"{doc_digits[0:3]}.{doc_digits[3:6]}.{doc_digits[6:9]}-{doc_digits[9:11]}"
    if doc_type == "CNPJ" and len(doc_digits) == 14:
        return f"{doc_digits[0:2]}.{doc_digits[2:5]}.{doc_digits[5:8]}/{doc_digits[8:12]}-{doc_digits[12:14]}"
    return doc_digits

# 1 requisição simultânea
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
    """Força a seleção do radio CPF/CNPJ (cobrindo variações de value/label)."""
    target = (doc_type or "").upper().strip()
    if target not in ("CPF", "CNPJ"):
        return False

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
                await page.wait_for_timeout(900)  # AJAX/máscara
                return True
        except:
            continue

    return False


async def wait_results_any_frame(page, timeout_ms: int = 45000):
    """Espera o CNJ aparecer em algum frame (mais robusto p/ CNPJ)."""
    start = time.time()
    frames = [page.main_frame] + [f for f in page.frames if f != page.main_frame]

    while (time.time() - start) * 1000 < timeout_ms:
        for fr in frames:
            try:
                body_txt = await fr.evaluate("() => document.body ? document.body.innerText : ''")
                if CNJ_RE.search(body_txt or ""):
                    return
                # fallback: texto "resultados encontrados"
                if re.search(r"resultados?\s+encontrados", body_txt or "", re.I):
                    return
            except:
                pass
        await page.wait_for_timeout(600)


async def pick_results_frame(page) -> Optional[Any]:
    """Escolhe o frame onde existem mais CNJs (p/ extração via innerText)."""
    best = None
    best_count = 0
    frames = [page.main_frame] + [f for f in page.frames if f != page.main_frame]

    for fr in frames:
        try:
            txt = await fr.evaluate("() => document.body ? document.body.innerText : ''")
            found = CNJ_RE.findall(txt or "")
            if len(found) > best_count:
                best_count = len(found)
                best = fr
        except:
            continue

    return best


def unique_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    out = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


async def open_process_popup(page, clickable):
    """Clica no link do processo e tenta capturar popup/aba."""
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
            await tab.first.click(timeout=2500)
            await popup.wait_for_timeout(700)
    except:
        pass

    rows = popup.locator("tr")
    count = await rows.count()

    for i in range(min(count, 140)):
        try:
            txt = _norm(await rows.nth(i).inner_text())
            if len(txt) > 10 and not UNWANTED_RE.search(txt) and txt not in seen:
                seen.add(txt)
                texts.append(txt)
        except:
            continue

    return texts[:10]


async def find_clickable_for_cnj(results_frame, cnj: str):
    """
    Acha um elemento clicável relacionado ao CNJ.
    (No PJe geralmente existe um ícone/link ao lado do item do resultado.)
    """
    base = results_frame.get_by_text(cnj, exact=False).first

    # container típico (tr/li/div do item)
    container = base.locator(
        "xpath=ancestor::tr[1] | ancestor::li[1] | ancestor::div[contains(@class,'ui-datalist-item')][1] | ancestor::div[contains(@class,'ui-panel')][1] | ancestor::div[contains(@class,'ui-widget')][1]"
    ).first

    # tentativa de achar o “ícone de abrir” (a com i) e depois qualquer a/button
    candidates = [
        container.locator("a:has(i)").first,
        container.locator("a").first,
        container.locator("button").first,
    ]
    for c in candidates:
        try:
            if await c.count() > 0 and await c.is_visible():
                return c
        except:
            continue

    # fallback: tenta clicar no próprio texto
    try:
        if await base.count() > 0 and await base.is_visible():
            return base
    except:
        pass

    return None


# -----------------------------
# SCRAPER PRINCIPAL
# -----------------------------
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
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
            locale="pt-BR",
        )

        try:
            page = await context.new_page()
            await page.goto(URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(2000)

            # 1) acha input
            fr, doc_input = await find_input_any_frame(page)
            if not doc_input:
                raise Exception("Input de CPF/CNPJ não encontrado na página.")

            # 2) seleciona tipo SEMPRE (CPF/CNPJ)
            await force_set_doc_type_radio(page, fr, doc_type)

            # 3) re-localiza input (AJAX pode recriar o campo)
            await page.wait_for_timeout(1200)
            fr, doc_input = await find_input_any_frame(page)
            if not doc_input:
                raise Exception("Input de CPF/CNPJ não encontrado após selecionar o tipo.")

            # 4) digita usando a máscara (principal correção p/ CNPJ)
            typed = format_doc(doc_digits, doc_type)
            await doc_input.click()
            await doc_input.fill("")
            await doc_input.type(typed, delay=70)
            await page.keyboard.press("Tab")
            await page.wait_for_timeout(600)

            # 5) pesquisar
            btn = fr.locator(
                "button:has-text('PESQUISAR'), input[type='submit'][value*='PESQUISAR' i], a:has-text('PESQUISAR')"
            ).first
            if await btn.count() == 0:
                btn = page.locator(
                    "button:has-text('PESQUISAR'), input[type='submit'][value*='PESQUISAR' i], a:has-text('PESQUISAR')"
                ).first

            if await btn.count() > 0:
                await btn.click(timeout=15000)
            else:
                await doc_input.press("Enter")

            # 6) espera resultados em QUALQUER frame
            try:
                await page.locator(".ui-progressbar").wait_for(state="visible", timeout=3000)
                await page.locator(".ui-progressbar").wait_for(state="hidden", timeout=45000)
            except:
                pass

            await wait_results_any_frame(page, timeout_ms=45000)

            # 7) escolhe frame certo e extrai CNJs via innerText (fix definitivo p/ CNPJ)
            results_frame = await pick_results_frame(page)
            if not results_frame:
                msg = await page.locator(".ui-messages-error").all_inner_texts()
                if msg:
                    result["aviso_site"] = msg
                return result

            txt = await results_frame.evaluate("() => document.body ? document.body.innerText : ''")
            cnjs = unique_preserve_order(CNJ_RE.findall(txt or ""))

            if not cnjs:
                msg = await page.locator(".ui-messages-error").all_inner_texts()
                if msg:
                    result["aviso_site"] = msg
                return result

            # 8) para cada CNJ, tenta abrir popup e extrair meta/movs
            for cnj in cnjs[:25]:
                clickable = await find_clickable_for_cnj(results_frame, cnj)
                if not clickable:
                    result["processos"].append({"numero": cnj, "erro": "clickable_nao_encontrado"})
                    continue

                popup = await open_process_popup(page, clickable)
                if not popup:
                    result["processos"].append({"numero": cnj, "erro": "popup_bloqueado_ou_mesma_aba"})
                    continue

                meta = await extract_metadata(popup)
                movs = await extract_movements(popup)

                result["processos"].append({
                    "numero": cnj,
                    **meta,
                    "movimentacoes": movs
                })

                await popup.close()

            return result

        except Exception as e:
            result["erro_interno"] = str(e)
            return result

        finally:
            await browser.close()


# -----------------------------
# ENDPOINTS
# -----------------------------
@app.get("/health")
def health():
    return {"ok": True, "status": "online"}


@app.get("/consulta")
async def consulta(
    doc: str = Query(..., description="CPF ou CNPJ (em 'doc')"),
    tipo: str = Query(..., description="Tipo do documento: cpf|cnpj"),
):
    doc_digits = sanitize_doc(doc)
    doc_type = (tipo or "").strip().upper()
    if doc_type not in ("CPF", "CNPJ"):
        if (tipo or "").strip().lower() in ("cpf", "cnpj"):
            doc_type = (tipo or "").strip().upper()
        else:
            raise HTTPException(status_code=400, detail="Tipo inválido (use cpf ou cnpj)")

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
        async def _run():
            async with SEMA:
                return await scrape_pje(doc_digits, doc_type)

        data = await asyncio.wait_for(_run(), timeout=180)
        _cache[cache_key] = {"ts": now, "data": data}
        return data

    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Tempo limite excedido (Site do Tribunal lento)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
