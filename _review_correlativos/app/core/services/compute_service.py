#app/core/services/compute_service.py

from __future__ import annotations
from typing import Dict, List, Optional, Tuple
from decimal import Decimal, ROUND_FLOOR
from ..repositories import events_repo, partners_repo, companies_repo
from ..enums import normalize_event_type

# ---------- utilidades de bloques ----------
def _split_block(block: dict, d:int, h:int) -> list[dict]:
    res = []
    a, b = block['rango_desde'], block['rango_hasta']
    if d is None or h is None:
        return [block]
    if h < a or d > b:
        return [block]
    if d > a:
        res.append({**block, 'rango_desde': a, 'rango_hasta': d-1})
    if h < b:
        res.append({**block, 'rango_desde': h+1, 'rango_hasta': b})
    return res

def _len_block(b: dict) -> int:
    return (b['rango_hasta'] - b['rango_desde'] + 1)

def _consolidate(blocks: list[dict]) -> list[dict]:
    clean = [b for b in blocks if b.get('rango_desde') is not None and b.get('rango_hasta') is not None]
    if not clean:
        return []
    clean = sorted(clean, key=lambda x: (x['socio_id'], x['right_type'], x['rango_desde'], x['rango_hasta']))
    merged = [clean[0].copy()]
    merged[0]["participaciones"] = _len_block(merged[0])
    for b in clean[1:]:
        last = merged[-1]
        if (
            b['socio_id']==last['socio_id']
            and b['right_type']==last['right_type']
            and b['rango_desde']==last['rango_hasta']+1
        ):
            last['rango_hasta'] = b['rango_hasta']
            last['participaciones'] = _len_block(last)
        else:
            nb = b.copy()
            nb["participaciones"] = _len_block(nb)
            merged.append(nb)
    return merged

# ---------- motor de aplicación (port v1, con tipos normalizados) ----------
def _apply_events(events: list[dict], valor_nominal_inicial: float = 5.0, part_tot_inicial: int = 0):
    from collections import defaultdict
    from datetime import date

    blocks: list[dict] = []
    valor_nominal = valor_nominal_inicial
    total_part = part_tot_inicial
    last_fecha = str(date.today())

    # agrupar por fecha
    by_date = defaultdict(list)
    for ev in events:
        ev = ev.copy()
        ev["tipo"] = normalize_event_type(ev.get("tipo"))
        by_date[str(ev["fecha"])] .append(ev)

    for f in sorted(by_date.keys()):
        day = by_date[f]
        last_fecha = f

        def _get_range(e):
            return (e.get('rango_desde') or 0, e.get('rango_hasta') or 0)

        # 1) BAJA / RED_AMORT (quitan)
        for ev in sorted([e for e in day if e.get('tipo') in ('BAJA','RED_AMORT')], key=_get_range):
            d, h = ev.get('rango_desde'), ev.get('rango_hasta')
            new_blocks = []
            for b in blocks:
                if b['right_type']=='plena' and b['socio_id']==ev.get('socio_transmite'):
                    new_blocks.extend(_split_block(b, d, h))
                else:
                    new_blocks.append(b)
            blocks = _consolidate(new_blocks)

        # 2) TRANSMISION / SUCESION (mueven)
        for ev in sorted([e for e in day if e.get('tipo') in ('TRANSMISION','SUCESION')], key=_get_range):
            d, h = ev.get('rango_desde'), ev.get('rango_hasta')
            new_blocks = []
            for b in blocks:
                if b['right_type']=='plena' and b['socio_id']==ev.get('socio_transmite'):
                    new_blocks.extend(_split_block(b, d, h))
                else:
                    new_blocks.append(b)
            blocks = _consolidate(new_blocks)
            blocks.append(dict(socio_id=ev.get('socio_adquiere'), right_type='plena', rango_desde=d, rango_hasta=h))
            blocks = _consolidate(blocks)

        # 3) ALTA / AMPL_EMISION (añaden)
        for ev in sorted([e for e in day if e.get('tipo') in ('ALTA','AMPL_EMISION')], key=_get_range):
            d, h = ev.get('rango_desde'), ev.get('rango_hasta')
            blocks.append(dict(socio_id=ev.get('socio_adquiere'), right_type='plena', rango_desde=d, rango_hasta=h))
            if h:
                total_part = max(total_part, h)
            blocks = _consolidate(blocks)

        # 4) USUFRUCTO / PIGNORACION / EMBARGO y AMPL_VALOR / RED_VALOR
        for ev in [e for e in day if e.get('tipo') in ('USUFRUCTO','PIGNORACION','EMBARGO')]:
            d, h = ev.get('rango_desde'), ev.get('rango_hasta')
            if ev.get('tipo') == 'USUFRUCTO':
                new_blocks = []
                for b in blocks:
                    if b['right_type']=='plena' and b['socio_id']==ev.get('socio_transmite'):
                        new_blocks.extend(_split_block(b, d, h))
                    else:
                        new_blocks.append(b)
                new_blocks.append(dict(socio_id=ev.get('socio_transmite'), right_type='nuda', rango_desde=d, rango_hasta=h))
                new_blocks.append(dict(socio_id=ev.get('socio_adquiere'),  right_type='usufructo', rango_desde=d, rango_hasta=h))
                blocks = _consolidate(new_blocks)
            else:
                holder = ev.get('socio_adquiere') or ev.get('socio_transmite')
                blocks.append(dict(socio_id=holder, right_type=('prenda' if ev.get('tipo')=='PIGNORACION' else 'embargo'),
                                   rango_desde=d, rango_hasta=h))
                blocks = _consolidate(blocks)

        for ev in [e for e in day if e.get('tipo') in ('AMPL_VALOR','RED_VALOR')]:
            nv = ev.get('nuevo_valor_nominal')
            if nv is not None:
                valor_nominal = float(nv)

        # 5) REDENOMINACION (al cierre del día)
        if any(e.get('tipo') == 'REDENOMINACION' for e in day):
            # suma por socio ('plena' vigente)
            current: Dict[int, int] = {}
            for b in blocks:
                if b['right_type'] != 'plena':
                    continue
                n = _len_block(b)
                current[b['socio_id']] = current.get(b['socio_id'], 0) + n

            old_total = sum(current.values())
            old_vn = Decimal(str(valor_nominal))
            old_capital = old_vn * Decimal(old_total)

            # VN nuevo (opcional, pero si viene debe ser único y > 0)
            vn_candidates = [e.get('nuevo_valor_nominal') for e in day if e.get('tipo') == 'REDENOMINACION' and e.get('nuevo_valor_nominal') not in (None, "")]
            new_vn = None
            if vn_candidates:
                vals = [float(v) for v in vn_candidates]
                if len({round(v, 6) for v in vals}) > 1:
                    raise ValueError(f"Valores nominales distintos en REDENOMINACION del día {f}: {vals}")
                new_vn = Decimal(str(vals[-1]))
                if new_vn <= 0:
                    raise ValueError(f"Nuevo valor nominal inválido en REDENOMINACION del día {f}: {new_vn}")

            if new_vn is None:
                new_total = old_total
            else:
                ratio = (old_capital / new_vn)
                if ratio != ratio.to_integral_value():
                    raise ValueError(f"El capital {old_capital} no es múltiplo del nuevo VN {new_vn} en REDENOMINACION del día {f}.")
                new_total = int(ratio)
                valor_nominal = float(new_vn)

            # Reasignación proporcional por restos (enteros, suma exacta)
            if old_total == 0:
                blocks = _consolidate(blocks)
                total_part = 0
            else:
                socios = sorted(current.keys())
                exact = {sid: (Decimal(current[sid]) * Decimal(str(new_total)) / Decimal(old_total)) for sid in socios}
                base  = {sid: int(exact[sid].to_integral_value(rounding=ROUND_FLOOR)) for sid in socios}
                asignadas = sum(base.values())
                resto = new_total - asignadas
                fracs = sorted([(sid, (exact[sid] - Decimal(base[sid]))) for sid in socios], key=lambda x: (x[1], -x[0]), reverse=True)
                for i in range(resto):
                    base[fracs[i][0]] += 1

                cursor = 1
                new_blocks = []
                for sid in socios:
                    n = base[sid]
                    if n <= 0:
                        continue
                    new_blocks.append(dict(socio_id=sid, right_type='plena', rango_desde=cursor, rango_hasta=cursor+n-1))
                    cursor += n
                blocks = _consolidate(new_blocks)
                total_part = new_total

        # ajuste fin día: recalcula total por bloques 'plena'
        total_part = sum(_len_block(b) for b in blocks if b['right_type'] == 'plena')

    return blocks, valor_nominal, total_part, last_fecha

# ---------- interfaz alto nivel ----------
def compute_snapshot(company_id: int, hasta_fecha: Optional[str] = None) -> dict:
    partners = {p["id"]: p for p in partners_repo.list_by_company(company_id)}
    events = events_repo.list_events_upto(company_id, hasta_fecha)
    company = companies_repo.get_company(company_id) or {}

    vn_ini = company.get("valor_nominal") or 5.0
    part_tot_ini = company.get("participaciones_totales") or 0

    blocks, valor_nominal, total_part, _ = _apply_events(events, float(vn_ini), int(part_tot_ini))

    # holdings (vigentes)
    holdings_rows = []
    for b in blocks:
        if b["right_type"] != "plena":
            continue
        pid = b["socio_id"]
        p = partners.get(pid) or {}
        holdings_rows.append({
            "partner_id": pid,
            "nombre": p.get("nombre") or f"Socio {pid}",
            "right_type": "plena",
            "rango_desde": b["rango_desde"],
            "rango_hasta": b["rango_hasta"],
            "participaciones": _len_block(b),
        })

    # agregados por socio vigente
    agreg: Dict[int, int] = {}
    for r in holdings_rows:
        agreg[r["partner_id"]] = agreg.get(r["partner_id"], 0) + int(r["participaciones"])

    socios_vigentes = []
    for pid, qty in sorted(agreg.items(), key=lambda t: (-t[1], partners.get(t[0], {}).get("nombre",""))):
        if qty <= 0: 
            continue
        p = partners.get(pid) or {}
        pct = (qty / total_part * 100.0) if total_part else 0.0
        capital_socio = float(valor_nominal) * float(qty) if valor_nominal is not None else None
        socios_vigentes.append({
            "partner_id": pid,
            "nombre": p.get("nombre"),
            "nif": p.get("nif"),
            "participaciones": qty,
            "porcentaje": round(pct, 6),
            "capital_socio": capital_socio,
        })

    # todos los socios (presentes y pasados, sin histórico de movimientos)
    todos_socios = []
    for p in partners.values():
        todos_socios.append({
            "partner_id": p["id"],
            "nombre": p.get("nombre"),
            "nif": p.get("nif"),
            "domicilio": p.get("domicilio"),
            "nacionalidad": p.get("nacionalidad"),
        })

    return {
        "socios_vigentes": socios_vigentes,
        "holdings_vigentes": holdings_rows,
        "todos_socios": sorted(todos_socios, key=lambda x: x["nombre"] or ""),
        "meta": {
            "a_fecha": hasta_fecha,
            "total_participaciones": total_part,
            "valor_nominal": valor_nominal,
            "capital_social": (float(valor_nominal) * float(total_part)) if valor_nominal is not None else None,
        },
    }