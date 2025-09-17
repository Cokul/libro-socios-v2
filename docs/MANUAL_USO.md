# 📘 Libro de Socios – Manual de Uso (V2)

Este documento explica de forma breve cómo utilizar la aplicación.  
El foco principal está en la gestión de **Eventos**, que constituyen el núcleo del libro de socios.

---

## 1. Pantalla y flujo de trabajo

Al arrancar la aplicación:

- En la **barra lateral** selecciona la **sociedad** sobre la que trabajar.  
- El menú principal ofrece varias secciones:

  1. **Overview** – resumen general de la sociedad.  
  2. **Sociedades** – alta, edición y eliminación de sociedades.  
  3. **Gobernanza** – gestión del consejo y órganos sociales.  
  4. **Socios** – alta, edición y eliminación de socios.  
  5. **Eventos** – registro de actos societarios.  
  6. **Reports** – exportaciones legales y operativas en PDF/Excel.  
  7. **Utilidades** – copias de seguridad, mantenimiento de BD, logs y chequeos de integridad.

---

## 2. Alta de datos iniciales

1. **Crear la sociedad** en la pestaña *Sociedades*.  
2. **Dar de alta a los socios** en la pestaña *Socios* (nombre, NIF, domicilio, nacionalidad, fecha).  
3. Una vez creados, ya se pueden registrar **eventos**.

---

## 3. Eventos

La pestaña **Eventos** es el núcleo de la aplicación.  
Permite filtrar, listar, dar de alta, editar y eliminar actos jurídicos que afectan al capital y a la titularidad.

### 3.1 Pantalla y flujo

- **Filtros de fecha** (desde/hasta): solo afectan al listado superior.  
- **Listado**: muestra ID, correlativo, fecha, tipo, socios, rangos, etc.  
- **➕ Alta de evento**: formulario dinámico según tipo.  
- **✏️ Editar / 🗑️ Eliminar**: selector por ID y formulario de edición.

> **Formato de fecha:** YYYY-MM-DD  
> **Rangos:** los campos `rango_desde` y `rango_hasta` son enteros e incluyen ambos extremos.

### 3.2 Alta de evento

1. En “➕ Alta de evento” elige el **tipo de evento**.  
2. Indica **fecha** y completa los campos que muestre el formulario.  
3. Opcionales: **documento** y **observaciones**.  
4. Pulsa **Guardar**.

El formulario solo pide lo necesario para ese tipo.

### 3.3 Edición y borrado

- Selecciona el evento por **ID** (se muestra un resumen).  
- Modifica lo necesario y pulsa **💾 Guardar cambios**.  
- Para borrar: **🗑️ Eliminar evento**.

---

## 4. Tipos de evento

**Leyenda de campos**:  
- **F** = Fecha  
- **ST** = Socio transmite (origen)  
- **SA** = Socio adquiere (destino/beneficiario)  
- **RD** = Rango desde  
- **RH** = Rango hasta  
- **VN** = Nuevo valor nominal (€)  
- **DOC** = Documento (opcional)  
- **OBS** = Observaciones (opcional)

### Principales tipos:

- **ALTA** – Añadir participaciones nuevas a un socio (RD–RH).  
  Campos: F, SA, RD, RH, (DOC/OBS)

- **AMPL_EMISION** – Ampliación de capital por emisión de participaciones.  
  Campos: F, SA, RD, RH, (DOC/OBS)

- **TRANSMISION** – Transferir un rango de un socio a otro (venta, donación…).  
  Campos: F, ST, SA, RD, RH, (DOC/OBS)

- **SUCESION** – Transmisión mortis causa (igual que TRANSMISION).  
  Campos: F, ST (causante), SA (heredero), RD, RH, (DOC/OBS)

- **BAJA** – Baja de participaciones (se extinguen, no pasan a otro).  
  Campos: F, ST, RD, RH, (DOC/OBS)

- **RED_AMORT** – Reducción de capital por amortización de participaciones.  
  Campos: F, ST, RD, RH, (DOC/OBS)

- **USUFRUCTO** – Divide nuda propiedad y usufructo.  
  Campos: F, ST (nuda), SA (usufructo), RD, RH, (DOC/OBS)

- **PIGNORACION** – Grava un rango a favor de acreedor (SA).  
  Campos: F, SA, RD, RH, (DOC/OBS)

- **EMBARGO** – Anota embargo sobre un rango (SA = beneficiario).  
  Campos: F, SA, RD, RH, (DOC/OBS)

- **AMPL_VALOR** – Aumenta el valor nominal global.  
  Campos: F, VN, (DOC/OBS)

- **RED_VALOR** – Reduce el valor nominal global.  
  Campos: F, VN, (DOC/OBS)

- **REDENOMINACION** – Reexpresa u homogeneiza el nominal.  
  Campos:  
  • F  
  • VN (opcional en constancia, obligatorio en recálculo)  
  • ST + RD/RH (solo en modo por bloque)  
  • (DOC/OBS)

  **Modalidades:**  
  1. **Global – constancia**: sin rangos ni socios, VN opcional (>0).  
     Compacta bloques por socio, nº de participaciones se mantiene.  
  2. **Global – recálculo**: sin rangos ni socios, VN obligatorio >0.  
     Mantiene capital y recalcula nº total = Capital / VN.  
     Rechaza si el capital no es múltiplo exacto de VN.  
  3. **Por bloque (RD–RH)**: con socio y rango, afecta solo a ese bloque.  
     VN opcional (>0), no recalcula nº total.

- **OTRO** – Caso libre no encajable.  
  Campos: F; opcionalmente ST, SA, RD, RH, nº participaciones, (DOC/OBS)

---

## 5. Campos comunes

- **Fecha**: día del acto o del documento.  
- **Documento**: referencia breve (ej. “Escritura 1234/2025”).  
- **Observaciones**: notas internas (precio, condiciones, acuerdos).  
- **Socios (ST/SA)**: selección por ID, etiquetas “ID – Nombre (NIF)”.  
- **Nº de participaciones**: solo en “OTRO”; la lógica principal siempre va por rangos.

---

## 6. Buenas prácticas

- Cada evento debe reflejar **un acto jurídico claro** (no mezclar operaciones).  
- Revisar posibles **solapes de rangos** en la misma fecha.  
- Documentar siempre (DOC/OBS) para trazabilidad.  
- Usar **filtros de fecha** para auditar la secuencia temporal.  
- En Redenominaciones: verificar que el capital sea múltiplo exacto del nuevo VN antes de guardar.

---

## 7. Reports

- **Libro Registro de Socios** en PDF (legalizable).  
- **Cap table y movimientos** en Excel.  
- **Certificados** históricos por socio.  

---

## 8. Utilidades

- **Backups**: copias de seguridad de la base de datos.  
- **Mantenimiento**: `ANALYZE`, `REINDEX`, `VACUUM`.  
- **Salud BD**: PRAGMA integrity_check y foreign_key_check.  
- **Logs**: consulta de incidencias en `logs/app.log`.

---

## 9. Notas finales

- **Capital = nº de participaciones × valor nominal.**  
- Los eventos garantizan consistencia con la Ley de Sociedades de Capital.  
- Los triggers de base de datos bloquean operaciones incoherentes (ej. VN=0, rangos incompletos, capital no múltiplo en redenominación).