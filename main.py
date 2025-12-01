import re
import time
import asyncio
import nest_asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query, HTTPException
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# --- URL ATUALIZADA PARA TRF6 ---
URL = "https://pje1g.trf6.jus.br/consultapublica/ConsultaPublica/listView.seam"

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

def sanitize_cpf(cpf: str) -> str:
    return re.sub(r"\D+", "", cpf or "")

# ===== Concurrency + Cache (para API pública) =====
SEMA = asyncio.Semaphore(1)          # 1 request por vez (Playwright é pesado)
CACHE_TTL = 300                      # 5 minutos
_cache: Dict[str, Dict[str, Any]] = {}  # cpf -> {"ts": epoch, "data": result}

app = FastAPI(title="PJe TRF6 - Consulta Pública (scraping)")

async def select_document_type_and_find_input(page, doc_digits: str):
    """
    1. Identifica se é CPF (11) ou CNPJ (14).
    2. Clica no Radio Button correspondente.
    3. Retorna o frame e o elemento Input para digitação.
    """
    frames = [page.main_frame] + [f for f in page.frames if f != page.main_frame]
    
    is_cnpj = len(doc_digits) > 11
    
    # Seletores para Radio Buttons baseados no texto do label
    # No PJe padrão, o label costuma ser "CPF" e "CNPJ" ao lado dos radios
    label_text = "CNPJ" if is_cnpj else "CPF"
    
    # XPath para achar o radio button associado ao label
    # Procura um label que contenha o texto, e pega o input radio anterior ou dentro dele
    radio_xpath = (
        f"xpath=//label[contains(normalize-space(.), '{label_text}')]/preceding-sibling::input[@type='radio'] | "
        f"xpath=//label[contains(normalize-space(.), '{label_text}')]//input[@type='radio'] | "
        f"xpath=//input[@type='radio' and following-sibling::label[contains(., '{label_text}')]]"
    )

    # XPath para achar o input de texto onde digita o número (geralmente próximo aos radios)
    input_xpath = "xpath=following::input[(not(@type) or @type='text') and not(@disabled)][1]"

    for fr in frames:
        try:
            # Tenta encontrar o Radio Button correto
            radio = fr.locator(radio_xpath).first
            if await radio.count() > 0 and await radio.is_visible():
                # Clica no tipo de documento correto
                await radio.click(timeout=5000)
                
                # Pequena pausa para garantir que o JS do PJe mudou a máscara se necessário
                await page.wait_for_timeout(500)
                
                # Agora procura o campo de input relativo a esse bloco
                # Geralmente o campo de texto está DEPOIS dos radios de CPF/CNPJ
                # Vamos tentar pegar o container pai dos radios e buscar o input dentro ou após
                
                # Estratégia A: Input logo após o radio clicado (ou grupo de radios)
                candidate = radio.locator(input_xpath)
                
                if await candidate.count() > 0 and await candidate.is_visible():
                    return fr, candidate
                
                # Estratégia B: Se falhar, busca input genérico próximo à label "CPF" 
                # (mesmo se selecionamos CNPJ, o campo físico é o mesmo na tela muitas vezes)
                anchor = fr.locator("xpath=//*[contains(normalize-space(.),'CPF')][1]")
                candidate_b = anchor.locator("xpath=following::input[(not(@type) or @type='text')][1]")
                if await candidate_b.count() > 0:
                     return fr, candidate_b

        except Exception as e:
            continue
            
    return None, None

async def wait_spinner_or_delay(page):
    """
    Aguarda o fim do 'spin' do PJe (quando existir).
    """
    candidates = ".ui-widget-overlay, .ui-blockui, .ui-progressbar, [class*='loading' i], [class*='spinner' i], #ajaxStatusModal"
    loc = page.locator(candidates)
    try:
        if await loc.count() > 0 and await loc.first.is_visible():
            await loc.first.wait_for(state="hidden", timeout=25000)
        else:
            await page.wait_for_timeout(2000) # delay padrão de segurança
    except PlaywrightTimeoutError:
        pass

async def open_process_popup(page, clickable):
    try:
        async with page.expect_popup(timeout=20000) as pop:
            # Força o clique via JS as vezes ajuda no PJe se o elemento estiver sob overlays
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
        return {k: None for k in ["assunto", "classe_judicial", "data_distribuicao", "orgao_julgador", "jurisdicao"]}

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
    
    # Seletores comuns de tabelas de movimentação no PJe
    selectors = [
        "css=[id*='moviment' i] tr",
        "css=[class*='moviment' i] tr",
        "xpath=//table[contains(@class, 'rich-table')]//tr", # PJe antigo usa richfaces
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
        except:
            pass

    return texts

async def scrape_pje(doc_digits: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "documento": doc_digits,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "processos": [],
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
            ]
        )
        
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768}
        )
        
        page = await context.new_page()

        try:
            await page.goto(URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(2000)

            # --- LÓGICA NOVA: Seleciona Radio (CPF ou CNPJ) e acha o input ---
            fr, doc_input = await select_document_type_and_find_input(page, doc_digits)
            
            if doc_input is None:
                raise HTTPException(status_code=500, detail="nao_encontrei_campo_documento")

            # Preenche Documento
            await doc_input.click(timeout=10000)
            await doc_input.fill("")
            # Digitação lenta ajuda a máscara do JSF a processar
            await doc_input.type(doc_digits, delay=100)
            
            # Clica fora para garantir trigger de validação
            await page.mouse.click(0, 0)

            # Clica pesquisar
            btn = fr.get_by_role("button", name="PESQUISAR")
            if await btn.count() == 0:
                # Tenta achar por ID parcial comum no PJe se o texto falhar
                btn = fr.locator("input[id$='searchButton'], button[id$='searchButton']")
                
            if await btn.count() > 0:
                await btn.first.click(timeout=30000)
            else:
                 raise HTTPException(status_code=500, detail="botao_pesquisar_nao_encontrado")

            await wait_spinner_or_delay(page)

            # Lista processos
            # O seletor foi ampliado para garantir captura de links com regex de CNJ
            proc_links = page.locator("a").filter(has_text=CNJ_RE)
            
            # Se não achou links diretos, tenta achar spans com o texto e pegar o link pai
            if await proc_links.count() == 0:
                 proc_links = page.locator("span").filter(has_text=CNJ_RE).locator("xpath=ancestor::a")

            count = await proc_links.count()

            for i in range(count):
                # Recarrega o elemento para evitar erro de elemento destacado do DOM
                link = proc_links.nth(i)
                txt = _norm(await link.inner_text())
                m = CNJ_RE.search(txt)
                if not m: continue
                
                numero = m.group(0)
                
                # Evitar duplicatas se a página tiver o mesmo link visualmente duplicado
                if any(p['numero'] == numero for p in result['processos']):
                    continue

                popup = await open_process_popup(page, link)
                
                if popup is None:
                    # Tentativa fallback: as vezes o ícone de 'autos' é que abre o popup
                    icon = link.locator("xpath=ancestor::tr//a[contains(@title, 'Autos') or contains(@id, 'alis')]")
                    if await icon.count() > 0:
                        popup = await open_process_popup(page, icon.first)

                if popup is None:
                    result["processos"].append({
                        "numero": numero,
                        "erro": "nao_abriu_popup",
                        "movimentacoes": []
                    })
                    continue

                await popup.wait_for_timeout(1500)
                meta = await extract_metadata(popup)
                movs = await extract_movements(popup)

                result["processos"].append({
                    "numero": numero,
                    **meta,
                    "movimentacoes": movs,
                })
                await popup.close()

        except Exception as e:
            await browser.close()
            # Log de erro para debug se necessário
            print(f"Erro no Playwright: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

        await browser.close()

    return result

@app.get("/health")
def health():
    return {"ok": True, "status": "online", "target": "TRF6"}

@app.get("/consulta")
async def consulta(doc: str = Query(..., description="CPF ou CNPJ (somente números)")):
    """
    Pesquisa por CPF ou CNPJ no PJe do TRF6.
    Detecta automaticamente se é CPF (11 dígitos) ou CNPJ (14 dígitos).
    """
    doc_digits = sanitize_cpf(doc) # Função limpa tudo que não é número
    
    if len(doc_digits) not in [11, 14]:
        raise HTTPException(status_code=400, detail="documento_invalido_deve_ter_11_ou_14_digitos")

    # cache simples
    now = time.time()
    cached = _cache.get(doc_digits)
    if cached and (now - cached["ts"]) < CACHE_TTL:
        return cached["data"]

    async with SEMA:
        # double check cache
        cached = _cache.get(doc_digits)
        if cached and (time.time() - cached["ts"]) < CACHE_TTL:
            return cached["data"]

        try:
            data = await asyncio.wait_for(scrape_pje(doc_digits), timeout=240) # Timeout aumentado
            _cache[doc_digits] = {"ts": time.time(), "data": data}
            return data
        except asyncio.TimeoutError:
            raise HTTPException(status_code=504, detail="timeout_no_tribunal")
