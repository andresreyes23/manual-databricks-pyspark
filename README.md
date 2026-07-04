# Manual práctico: Python en Databricks con PySpark

Este repositorio está pensado como base para un caso práctico de la asignatura Herramientas del Científico de Datos. La idea es mostrar una propuesta sólida y organizada para trabajar con Python en Databricks, utilizando PySpark como motor principal para el análisis de datos.

El documento académico completo del caso se encuentra en [docs/manual.md](docs/manual.md). Este repositorio complementa esa documentación con una estructura lista para desarrollar el ejemplo práctico y la parte de código.

## Objetivo del caso práctico

Desarrollar un manual de usuario sobre una herramienta de Big Data, en este caso Python + Databricks, siguiendo los cuatro puntos requeridos:

1. Descripción de la herramienta
2. Funciones más destacadas
3. Aplicaciones
4. Ejemplo de aplicación

## Estructura del repositorio

- [docs/](docs/): documentación del caso práctico y manual de usuario.
- [notebooks/](notebooks/): ejemplo de notebook básico exportable a Databricks.
- [src/](src/): scripts y módulos de Python para continuar el desarrollo del ejemplo.
- [data/](data/): carpeta para almacenar datasets o archivos de prueba.
- [results/](results/): salida de procesos, tablas o resultados generados.
- [requirements.txt](requirements.txt): dependencias necesarias para ejecutar el ejemplo.

## Qué incluye este proyecto

- Un notebook inicial con un flujo básico de PySpark.
- Un ejemplo de script Python reutilizable para ejecutar el mismo proceso localmente.
- Una estructura preparada para ampliar el proyecto con datos reales, limpiezas, agregaciones y análisis más complejos.

## Inicio rápido

### Opción 1: importar en Databricks

1. Crear un nuevo notebook en Databricks.
2. Importar o copiar el contenido de [notebooks/01-basic_notebook.py](notebooks/01-basic_notebook.py).
3. Adjuntar un clúster y ejecutar las celdas en orden.

### Opción 2: ejecutar localmente

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python src/pyspark_example.py
```

## Siguiente paso recomendado

Para hacer el caso más completo, puedes continuar con una de estas ideas:

- cargar un archivo CSV real en [data/](data/)
- aplicar limpieza de datos con PySpark
- realizar agregaciones o filtros más avanzados
- guardar resultados en [results/](results/)
- documentar el flujo en el notebook y en el manual

## Nota

Este repositorio ya está preparado como punto de partida para seguir desarrollando el ejemplo del caso práctico de forma ordenada, clara y escalable.