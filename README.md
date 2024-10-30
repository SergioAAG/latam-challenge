# Challenge Ingeniero de Datos
Este proyecto contiene la resolución del challenge de ingeniero de datos de LATAM Airlines.

# Acerca del Proyecto
El análisis completo del challenge se encuentra en [challenge.ipynb](src/challenge.ipynb)

El flujo de desarrollo utilizado fue GitFlow, pero como el encabezado del challenge lo solicitaba, no se eliminaron las ramas de desarrollo (features).

## Requisitos Previos
- Python 3.10
- Tener las dependencias instaladas de ambos requirements
    - requirements.txt
    - requirements_dev.txt
- Tener el archivo JSONL en la raíz del proyecto

## Supuestos
- Se asume que la data JSONL es data RAW, por lo tanto, se procede a transformarla y adaptarla a un formato más analítico

## Navegación
La estructura del proyecto es la siguiente:

```
latam-challenge/
├── src/
│   ├── utils/
│   │   ├── data_conversion.py
│   │   ├── plotting.py
│   │   └── profiling.py
│   ├── challenge.ipynb
│   ├── main.py
│   ├── q1_time.py
│   ├── q1_memory.py
│   ├── q2_time.py
│   ├── q2_memory.py
│   ├── q3_time.py
│   └── q3_memory.py
├── tests/
│   ├── test_q1_time.py
│   ├── test_q1_memory.py
│   └── ...
├── Dockerfile
├── requirements.txt
└── requirements_dev.txt
```

Archivos principales:
- `src/`: Contiene todo el código fuente para el desafío y el notebook con el análisis
    - `utils/`: Módulos de utilidades
        - `data_conversion.py`: Funciones para la transformación y procesamiento de datos
        - `plotting.py`: Funciones para la visualización de resultados
        - `profiling.py`: Utilidades para el análisis de rendimiento
    - `challenge.ipynb`: Notebook principal con el análisis detallado y resultados
    - `main.py`: API simplificada implementada con FastAPI para demostración
    - `q1_time.py`: Implementación optimizada en tiempo para el primer ejercicio (top 10 fechas con más tweets)
    - `q1_memory.py`: Implementación optimizada en memoria para el primer ejercicio
    - `q2_time.py`: Implementación optimizada en tiempo para el segundo ejercicio (top 10 emojis)
    - `q2_memory.py`: Implementación optimizada en memoria para el segundo ejercicio
    - `q3_time.py`: Implementación optimizada en tiempo para el tercer ejercicio (usuarios más influyentes)
    - `q3_memory.py`: Implementación optimizada en memoria para el tercer ejercicio
- `tests/`: Pruebas unitarias para cada implementación
- `Dockerfile`: Configuración para la containerización de la aplicación
- `requirements.txt`: Dependencias del proyecto
- `requirements_dev.txt`: Dependencias de desarrollo del proyecto

## Tecnologías Utilizadas
- **Python**: Lenguaje de programación principal
- **DuckDB**: Manipulación y análisis de datos
- **Pandas**: Visualización preliminar de los datos
- **FastAPI**: Framework para desarrollo de APIs
- **memory-profiler**: Análisis de uso de memoria
- **pytest**: Framework de pruebas
- **Jupyter Notebook**: Análisis interactivo del challenge
- **Docker**: Containerización de la aplicación

## Flujo de CI/CD
El proyecto implementa un workflow de CI/CD el cual está compuesto de dos pipelines usando GitHub Actions:

1. **Lint and Test (ci.yml)**:
   - Se ejecuta en las ramas: `main`, `develop` y `feature/**`
   - Verificación de calidad de código con Ruff
   - Ejecución de pruebas automatizadas con pytest

2. **Deploy (deploy.yml)**:
   - Se ejecuta con push a las ramas `develop` y `main`
   - Despliegue diferenciado para ambientes de desarrollo y producción
   - Pipeline de despliegue a Google Cloud Platform:
     - Construcción y etiquetado de imagen Docker
     - Configuración de Artifact Registry
     - Despliegue a Cloud Run
    
    Obs: En el pipeline de despliegue se dejaron comentados los comandos para hacer el despliegue en cloud, en su lugar se dejaron echo que simulan el deploy a modo de ejemplo.

## Información complementaria
Se implementó un archivo `main.py` que contiene una API simplificada utilizando FastAPI a modo de ejemplo, junto con un Dockerfile para su eventual despliegue en cloud. Esta implementación demuestra cómo se podrían exponer los resultados del análisis a través de endpoints REST.

## Próximos Pasos
Como posibles mejoras se sugieren las siguientes ideas:

- Implementación de versionamiento compatible con el flujo de desarrollo utilizado (GitFlow), podría ser versionamiento semántico
- Después de diseñar arquitectura, implementarla en cloud utilizando IaC, podría ser Terraform
- Profundizar en la compatibilidad de DuckDB con la librería de emoji para una mejora en los resultados de las funciones q2
