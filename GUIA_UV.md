# Guía de uv (para quien viene de pip)

uv reemplaza de una vez a `pip`, `venv`, `pip-tools` y `pyenv`. Hace lo mismo
que ya conocés, pero con dos diferencias que importan para el disco:

1. **Guarda cada librería una sola vez** en un caché global y los ambientes
   virtuales apuntan a esa copia con enlaces. Diez proyectos con pandas no
   ocupan diez pandas.
2. **Anota las versiones exactas** en un archivo `uv.lock`, así el ambiente
   se puede recrear idéntico en otra máquina (o borrarlo sin miedo y volver a
   crearlo en segundos).

## 1. Instalación en Windows

En PowerShell:

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Cerrá y volvé a abrir la terminal, y verificá:

```powershell
uv --version
```

No hace falta instalar Python aparte: uv descarga la versión que pida cada
proyecto (acá, 3.12) la primera vez.

## 2. De pip a uv: equivalencias

| Lo que hacías con pip | Con uv |
|---|---|
| `python -m venv .venv` | no hace falta, `uv sync` lo crea |
| `.venv\Scripts\activate` | no hace falta, usás `uv run ...` (activar igual funciona) |
| `pip install pandas` | `uv add pandas` |
| `pip install pytest` (sólo para desarrollo) | `uv add --dev pytest` |
| `pip uninstall pandas` | `uv remove pandas` |
| `pip install -r requirements.txt` | `uv sync` (lee `pyproject.toml` y `uv.lock`) |
| `pip freeze > requirements.txt` | no hace falta, `uv.lock` se actualiza solo |
| `python script.py` | `uv run python script.py` |
| `pytest` | `uv run pytest` |
| `pip list` | `uv pip list` |
| `pip install --upgrade pandas` | `uv lock --upgrade-package pandas` y después `uv sync` |

La diferencia de fondo: con pip instalabas cosas en el ambiente y después
anotabas (o te olvidabas de anotar) qué instalaste. Con uv es al revés:
declarás lo que necesitás en `pyproject.toml` (con `uv add`) y uv hace que el
ambiente coincida exactamente con eso.

## 3. Qué hace cada archivo

| Archivo | Qué es | ¿Se sube a git? |
|---|---|---|
| `pyproject.toml` | la lista de dependencias que pediste (como un `requirements.txt` mejorado) | sí |
| `uv.lock` | las versiones exactas que se resolvieron, de todo y de las dependencias de las dependencias | sí |
| `.venv/` | el ambiente virtual en sí | no (está en `.gitignore`) |

## 4. Este repositorio: un workspace

Este repo es un **workspace**: varias carpetas con su propio `pyproject.toml`
(`tarea3/` y `trabajo-final/`) comparten **un solo `.venv` y un solo
`uv.lock` en la raíz**. Por eso no se duplica nada entre entregas.

### Primera vez

```powershell
cd <carpeta del repo>
uv sync --all-packages
```

Eso crea `.venv` en la raíz con todo lo que necesitan las cuatro entregas.

### Uso diario

```powershell
# correr las pruebas de una entrega (desde la raíz)
uv run --directory tarea3 pytest
uv run --directory trabajo-final pytest

# o entrando a la carpeta
cd trabajo-final
uv run pytest
uv run marimo edit producer_notebook.py
uv run python scripts/run_local.py --max-readings 3000
```

`uv run` siempre usa el `.venv` de la raíz, estés en la carpeta que estés.

### La única trampa

**No corras `uv sync` a secas dentro de una subcarpeta.** Si estás en
`tarea3/` y hacés `uv sync`, uv deja el ambiente con *sólo* lo de la Tarea 3
y desinstala lo del trabajo final. No se rompe nada grave (se arregla con un
comando), pero confunde. Siempre:

```powershell
uv sync --all-packages
```

`uv run` no tiene ese problema, porque agrega lo que falta pero no borra nada.

### Agregar una librería

Pensá primero a qué entrega pertenece y agregala ahí:

```powershell
cd trabajo-final
uv add scikit-learn          # queda en trabajo-final/pyproject.toml
cd ..
uv sync --all-packages
```

Para algo que usan las Tareas 1 o 2 (que no son paquetes), agregalo en la
raíz con `uv add` desde la raíz.

## 5. PyCharm

PyCharm tiene que usar el `.venv` de la raíz:

1. `File > Settings > Project > Python Interpreter`.
2. `Add Interpreter > Add Local Interpreter`.
3. Elegí **Existing** (o *Select existing*) y apuntá a
   `<carpeta del repo>\.venv\Scripts\python.exe`.

Las versiones recientes de PyCharm (2024.3 en adelante) también reconocen uv
directamente: en *Add Interpreter* aparece el tipo **uv**. Cualquiera de las
dos formas sirve. Si PyCharm te ofrece "instalar requirements", decile que no:
eso lo hace `uv sync`.

## 6. Pasar un proyecto viejo de pip a uv

Para un proyecto que tiene `requirements.txt`:

```powershell
cd C:\ruta\al\proyecto-viejo
uv init --bare                         # crea un pyproject.toml mínimo
uv add -r requirements.txt             # pasa todo al pyproject y genera uv.lock
uv sync                                # crea el .venv nuevo
uv run python main.py                  # probá que funciona
```

Si funciona, borrá la carpeta del ambiente viejo (`venv`, `env`, `.venv`
anterior). Eso es lo que libera espacio de verdad.

Si un proyecto viejo no tiene `requirements.txt`, primero sacalo del ambiente
viejo con `ruta\al\venv\Scripts\pip freeze > requirements.txt` y seguí igual.

Y si en algún caso preferís seguir trabajando "al estilo pip", uv también
tiene una interfaz compatible, con el mismo ahorro de espacio:

```powershell
uv venv
uv pip install -r requirements.txt
```

## 7. Mantener el disco bajo control

```powershell
uv cache dir          # dónde está el caché (normalmente %LOCALAPPDATA%\uv\cache)
uv cache prune        # borra lo que ya no usa ningún proyecto
uv python list        # versiones de Python que uv descargó
```

Dos cosas para que los enlaces funcionen y no se copie todo:

- El caché de uv y tus proyectos tienen que estar **en el mismo disco**
  (normalmente `C:`). Si están en discos distintos, uv copia en lugar de
  enlazar.
- **No pongas `.venv` dentro de una carpeta sincronizada** (OneDrive, Dropbox
  o Google Drive). Esos servicios intentan subir miles de archivos pequeños, y
  además los enlaces no funcionan bien ahí. Conviene clonar el repo en una
  carpeta local común.

Un último detalle: el Explorador de Windows cuenta dos veces los archivos
enlazados, así que el "tamaño" que muestra cada `.venv` es mayor que lo que
realmente ocupa en el disco.

## 8. Resumen para pegar al lado del monitor

```text
uv sync --all-packages        preparar o actualizar el ambiente (desde la raíz)
uv run <comando>              ejecutar cualquier cosa dentro del ambiente
uv add <paquete>              agregar una dependencia
uv remove <paquete>           quitarla
uv lock --upgrade             actualizar todas las versiones
uv cache prune                limpiar el caché
```
