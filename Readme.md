# Proceso ETL Inversion Clientes

## Descripción

Este proyecto implementa un proceso ETL (Extract, Transform, Load) para extraer datos de diversas fuentes, transformarlos según las necesidades del negocio y cargarlos en un almacén de datos.

En este sentido se requiere desarrollar una herramienta analítica que les permita visualizar:

1.El portafolio de cada cliente y que porcentaje representa cada macroactivo y activo en el total del portafolio teniendo en cuenta la última fecha disponible.

2.El portafolio por banca y que porcentaje representa cada macroactivo teniendo en cuenta la última fecha disponible.

3.El portafolio por perfil de riesgo y que porcentaje representa cada macroactivo teniendo en cuenta la última fecha disponible.

4.La evolución mes a mes del ABA (Activos Bajo Administración) promedio del total del portafolio. Es deseable poder seleccionar fechas de inicio y fin para determinar el periodo de tiempo a analizar. 


## Tabla de Contenidos

1. [Requisitos](#requisitos)
2. [Arquitectura](#arquitectura)
3. [Extracción](#extracción)
4. [Transformación](#transformación)
5. [Carga](#carga)
6. [Visualización]

## Requisitos

- Python 3.x
- Bibliotecas Python: `pandas`, `sqlalchemy`, `requests`, `dash`, `dash-bootstrap-components`,`psycopg2`
- PostgreSQL
- Acceso a las fuentes de datos (APIs, bases de datos, etc.)

## Arquitectura

El proceso ETL se divide en tres fases principales:

![Diagrama del Proceso ETL](https://github.com/JhonaChinome/Tal-B-Inv/blob/main/img/imagen.PNG)

1. **Extracción (Extract)**: Obtención de datos desde diversas fuentes.
2. **Transformación (Transform)**: Limpieza y manipulación de los datos para adecuarlos a las necesidades del análisis.
3. **Carga (Load)**: Inserción de los datos transformados en el almacén de datos.
4. **Visualización**: Creación de dashboards interactivos utilizando Dash.

## Extracción

En esta fase, los datos se extraen de múltiples fuentes como bases de datos, APIs, archivos CSV, etc.

### Código de Extracción

```python
df_cat_perfil_riesgo=pd.read_csv("./data/cat_perfil_riesgo.csv")
df_catalogo_activos=pd.read_csv("./data/catalogo_activos.csv")
df_catalogo_banca=pd.read_csv("./data/catalogo_banca.csv")
df_historico_aba_macroactivos=pd.read_csv("./data/historico_aba_macroactivos.csv")

try:
    engine=create_engine("postgresql+psycopg2://postgres:phy04@localhost:5433/InversionClientes")
    print("Conexión exitosa.")
    df_cat_perfil_riesgo.to_sql(name="cat_perfil_riesgo",con=engine,if_exists="append",index=False)
    df_catalogo_activos.to_sql(name="catalogo_activos",con=engine,if_exists="append",index=False)
    df_catalogo_banca.to_sql(name="catalogo_banca",con=engine,if_exists="append",index=False)
    df_historico_aba_macroactivos.to_sql(name="historico_aba_macroactivos",con=engine,if_exists="replace",index=False)
    
except Exception as ex:
    print("Error durante el importe de Datos: {}".format(ex))

finally:
    connection.close()  # Se cerró la conexión a la BD.
    print("La conexión ha finalizado.")
```

## Transformación
Durante la transformación, los datos se limpian, se realizan cálculos necesarios y se estructuran según los requerimientos del análisis usando query SQL.

### Código de Transformación

Se eliminan muestras o de caracateristicas que contengas valores nulos , y puedan producir inconsistencias.

```python

try:
  engine = create_engine("postgresql+psycopg2://postgres:phy04@localhost:5433/InversionClientes")
  conn = engine.connect()
  consulta_etl = pd.read_sql_query("""
        WITH ETLNOT AS (
        SELECT *
          FROM historico_aba_macroactivos                            
          WHERE 
            ingestion_year IS NOT NULL
            and ingestion_month IS NOT NULL
            and ingestion_day IS NOT NULL
            and id_sistema_cliente IS NOT NULL
            and macroactivo IS NOT NULL
            and cod_activo IS NOT NULL
            and aba IS NOT NULL
            and cod_perfil_riesgo IS NOT NULL
            and cod_banca IS NOT NULL
            and year IS NOT NULL
            and month IS NOT NULL
            and LENGTH(id_sistema_cliente)>3
                                        
        ),
```
Se toman muestras no duplicadas.

```python

        """ETLDIST AS (
          SELECT DISTINCT *
          FROM ETLNOT 
        ),
```
Se realiza un cambio de tipo a cada una de las caracteristica.

```python
        """ETLCAST AS (
            SELECT
            ingestion_year::integer as año,
            ingestion_month ::integer as mes ,
            ingestion_day :: integer as dia,
            id_sistema_cliente::float AS id_sistema_cliente"""
```
De acuerdo con los requerimientos se realiza cruce de 
tablas y obtener informacion relevante.

```python
        """ETLJOINONE AS(
                SELECT *
                FROM ETLCAST
                JOIN catalogo_activos ON ETLCAST cod_activo = catalogo_activos cod_activo   
                ),
```

## Carga
Durante la Carga de Datos, los datos optenidos en la 
tranformacion se importaran a postgresql.

### Código de Carga

```python
        try:
    engine=create_engine("postgresql+psycopg2://postgres:phy04@localhost:5433/InversionClientes")
    print("Conexión exitosa.")
    consulta_etl.to_sql(name="consulta_etl",con=engine,if_exists="append",index=False)
    
except Exception as ex:
    print("Error durante el importe de Datos: {}".format(ex))

finally:
    connection.close()  # Se cerró la conexión a la BD.
    print("La conexión ha finalizado.")
```

## Visualización
Utilizando Dash, se crea un dashboard interactivo para visualizar los datos cargados en PostgreSQL.

![Diagrama uno](https://github.com/JhonaChinome/Tal-B-Inv/blob/main/img/PortafolioBanca.PNG)

![Diagrama dos](https://github.com/JhonaChinome/Tal-B-Inv/blob/main/img/PortafolioClientes.PNG)

![Diagrama tres](https://github.com/JhonaChinome/Tal-B-Inv/blob/main/img/PortafolioClientes2.PNG)

![Diagrama cuatro](https://github.com/JhonaChinome/Tal-B-Inv/blob/main/img/PortafolioRiesgo.PNG)


