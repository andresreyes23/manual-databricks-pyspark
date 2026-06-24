# Manual: Python en Databricks (PySpark)

Repositorio preparado para conectar con Databricks. Contenido mínimo y útil para importar y ejecutar en Databricks:

- Notebooks: `notebooks/` (contiene el notebook básico en formato .py exportable/inmportable a Databricks)
- Dependencias (opcional): `requirements.txt`

Quickstart (importar en Databricks)

1. En la interfaz de Databricks, crea un nuevo notebook e importa el archivo `notebooks/01-basic_notebook.py` o copia/pega su contenido.
2. Adjunta un clúster y ejecuta las celdas en orden.

Quickstart (ejecución local con PySpark)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Luego puedes ejecutar scripts PySpark localmente si lo deseas.

He dejado sólo la carpeta `notebooks/` con un notebook .py básico listo para Databricks.