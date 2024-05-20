# %%
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dash import Dash, dcc, html, Input, Output, callback
import plotly.express as px


# %% [markdown]
# ### Conexion base de datos
# 
# 

# %%
try:
    connection= psycopg2.connect(database="InversionClientes", user='postgres', 
    password='phy04', host='localhost', port='5433')
    print("Conexión exitosa.")
    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    row = cursor.fetchone()
    print("Versión del servidor de SQL Server: {}".format(row))
    
except Exception as ex:
    print("Error durante la conexión: {}".format(ex))

# %%
connection.close()


# %% [markdown]
# ### Crear Tablas

# %%
try:
    connection= psycopg2.connect(database="InversionClientes", user='postgres', 
    password='phy04', host='localhost', port='5433')
    print("Conexión exitosa.")
    cursor = connection.cursor()


    cursor.execute(
        """CREATE TABLE cat_perfil_riesgo(
            cod_perfil_riesgo integer,
            perfil_riesgo character varying(20)
         
        ) ;""")
    connection.commit()


except Exception as ex:
    print("Error durante la creacion de Tabla: {}".format(ex))

finally:
    connection.close()  # Se cerró la conexión a la BD.
    print("La conexión ha finalizado.")
        

# %%
try:
    connection= psycopg2.connect(database="InversionClientes", user='postgres', 
    password='phy04', host='localhost', port='5433')
    print("Conexión exitosa.")
    cursor = connection.cursor()


    cursor.execute(
        """CREATE TABLE catalogo_activos(
            cod_activo integer,
            activo character varying(100)


        );""")
    connection.commit()


except Exception as ex:
    print("Error durante la creacion de Tabla: {}".format(ex))

finally:
    connection.close()  # Se cerró la conexión a la BD.
    print("La conexión ha finalizado.")

# %%
try:
    connection= psycopg2.connect(database="InversionClientes", user='postgres', 
    password='phy04', host='localhost', port='5433')
    print("Conexión exitosa.")
    cursor = connection.cursor()


    cursor.execute(
        """CREATE TABLE catalogo_banca(
            cod_banca character(2),
            banca character varying(20)

        );""")
    connection.commit()


except Exception as ex:
    print("Error durante la creacion de Tabla: {}".format(ex))

finally:
    connection.close()  # Se cerró la conexión a la BD.
    print("La conexión ha finalizado.")

# %%
try:
    connection= psycopg2.connect(database="InversionClientes", user='postgres', 
    password='phy04', host='localhost', port='5433')
    print("Conexión exitosa.")
    cursor = connection.cursor()


    cursor.execute(
        """CREATE TABLE historico_aba_macroactivos(
            ingestion_year integer,
            ingestion_month integer,
            ingestion_day integer,
            id_sistema_cliente double precision,
            macroactivo character varying(100),
            cod_activo integer,
            aba double precision,
            cod_perfil_riesgo integer,
            cod_banca character(2),
            year integer,
            month integer

        );""")
    connection.commit()


except Exception as ex:
    print("Error durante la creacion de Tabla: {}".format(ex))

finally:
    connection.close()  # Se cerró la conexión a la BD.
    print("La conexión ha finalizado.")

# %% [markdown]
# ### POBLAR DATOS A TABLAS

# %%
df_cat_perfil_riesgo=pd.read_csv("C:/Users/javie/Documentos/Bancolombia/Nueva carpeta/data/cat_perfil_riesgo.csv")
df_catalogo_activos=pd.read_csv("C:/Users/javie/Documentos/Bancolombia/Nueva carpeta/data/catalogo_activos.csv")
df_catalogo_banca=pd.read_csv("C:/Users/javie/Documentos/Bancolombia/Nueva carpeta/data/catalogo_banca.csv")
df_historico_aba_macroactivos=pd.read_csv("C:/Users/javie/Documentos/Bancolombia/Nueva carpeta/data/historico_aba_macroactivos.csv")



# %%
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

# %% [markdown]
# ### ETL
# 

# %%
engine = create_engine("postgresql+psycopg2://postgres:phy04@localhost:5433/InversionClientes")
conn = engine.connect()
consulta = pd.read_sql_query("""SELECT * FROM historico_aba_macroactivos order by ingestion_year,ingestion_month,ingestion_day,id_sistema_cliente""", engine)
#print(consulta.to_markdown())
conn.close()

# %%
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
      ETLDIST AS (
        SELECT DISTINCT *
        FROM ETLNOT 
      ),
      ETLCAST AS (
          SELECT
          ingestion_year::integer as año,
          ingestion_month ::integer as mes ,
          ingestion_day :: integer as dia,
          id_sistema_cliente::float AS id_sistema_cliente,
          macroactivo::character varying(100) as macroactivo,
          case 
            when cod_activo::integer = 10007 then 1007
            when cod_activo::integer = 1015 then 1115
            when cod_activo::integer = 1022 then 1020
            else cod_activo::integer
          end as cod_activo,
          aba::float as aba,
          cod_perfil_riesgo::integer as cod_perfil_riesgo,
          cod_banca::character(2) as cod_banca

        FROM ETLDIST
      ),
      
      ETLJOINONE AS(
        SELECT *
        FROM ETLCAST
        JOIN catalogo_activos ON ETLCAST.cod_activo = catalogo_activos.cod_activo                               
      ),
      
      ETLJOINTWO AS(
        SELECT *
        FROM ETLJOINONE
        JOIN cat_perfil_riesgo ON ETLJOINONE.cod_perfil_riesgo = cat_perfil_riesgo.cod_perfil_riesgo                            
      ),
      
      ETLJOINTRE AS(
        SELECT *
        FROM ETLJOINTWO
        JOIN catalogo_banca ON ETLJOINTWO.cod_banca = catalogo_banca.cod_banca                           
      )
                                      
      SELECT 
          año,
          mes,
          dia,
          id_sistema_cliente,
          macroactivo,
          activo,
          aba,                     
          perfil_riesgo,
          banca 
      FROM ETLJOINTRE """, engine)

conn.close()


# %%

df = consulta_etl

s=consulta_etl.sort_values(by=['año','mes','dia'],ascending=False).reset_index()
año=s['año'][0]
mes=s['mes'][0]
dia=s['dia'][0]

app = Dash(__name__)

app.layout = html.Div(children=[html.Div([
    dcc.Graph(id='graph-with-slider'),
    dcc.Slider(
        df['año'].min(),
        df['año'].max(),
        step=None,
        value=df['año'].min(),
        marks={str(year): str(year) for year in df['año'].unique()},
        id='year-slider'
    ),]),
    html.Div([
    dcc.Graph(id='indicator-graphic'),

    dcc.Dropdown(
        id='xaxis-column',
        options=s['id_sistema_cliente'].unique(),
        value=10038643094

    ),

]),
html.Div([
    dcc.Graph(id='indicator-graphic-ac'),

    dcc.Dropdown(
        id='xaxis-column-ac',
        options=s['id_sistema_cliente'].unique(),
        value=10038643094

    ),

]),
html.Div([
    dcc.Graph(id='indicator-graphic-ev'),

    dcc.Dropdown(
        id='xaxis-column-ev',
        options=s['banca'].unique(),
        value='Personal'

    ),

]),
html.Div([
    dcc.Graph(id='indicator-graphic-ri'),

    dcc.Dropdown(
        id='xaxis-column-ri',
        options=s['perfil_riesgo'].unique(),
        value='MODERADO'

    ),

])

    
])
@callback(
    Output('graph-with-slider', 'figure'),
    Input('year-slider', 'value'))#,

def update_figure(selected_year):
    filtered_df = df[(df.año == selected_year)]

    mes_aba=filtered_df.groupby("mes").mean("aba")

    fig = px.line(mes_aba, x=list(mes_aba.index), y="aba",labels={"x":"MES","aba":"Promedio ABA"} ,title= "Evolucion ABA",markers=True)
    return fig

@callback(
    Output('indicator-graphic', 'figure'),
    Input('xaxis-column', 'value')
    )
def update_graph_macr(select_cliente):
    
    filtered_df = consulta_etl[(consulta_etl.año == año)&(consulta_etl.mes == mes)&(consulta_etl.dia == dia)]
    macro_aba=filtered_df.groupby(["id_sistema_cliente","macroactivo",'activo'],as_index=False).count()
    macro_aba=macro_aba[macro_aba.id_sistema_cliente==select_cliente]
    macro_aba['porcent']=(macro_aba['aba']*100)/(macro_aba['id_sistema_cliente'].count())
    
    labels1 = macro_aba['macroactivo']
    labels2 = macro_aba['activo']

    fig=px.pie(macro_aba,values="porcent", names=labels1)

    fig.update_layout(
        title_text="Portafolio Clientes")
        

    return fig


@callback(
    Output('indicator-graphic-ac', 'figure'),
    Input('xaxis-column-ac', 'value')
    )
def update_graph_act(select_cliente):
    
    filtered_df = consulta_etl[(consulta_etl.año == año)&(consulta_etl.mes == mes)&(consulta_etl.dia == dia)]
    macro_aba=filtered_df.groupby(["id_sistema_cliente","macroactivo",'activo'],as_index=False).count()
    macro_aba=macro_aba[macro_aba.id_sistema_cliente==select_cliente]
    macro_aba['porcent']=(macro_aba['aba']*100)/(filtered_df['id_sistema_cliente'].count())
    
    labels1 = macro_aba['macroactivo']
    labels2 = macro_aba['activo']


    fig=px.pie(macro_aba,values="porcent", names=labels2)

    fig.update_layout(
        title_text="Portafolio Clientes")
        

    return fig

@callback(
    Output('indicator-graphic-ev', 'figure'),
    Input('xaxis-column-ev', 'value')
    )
def update_graph(select_cliente):
    
    filtered_df = consulta_etl[(consulta_etl.año == año)&(consulta_etl.mes == mes)&(consulta_etl.dia == dia)]
    macro_aba=filtered_df.groupby(["banca","macroactivo"],as_index=False).count()
    macro_aba=macro_aba[macro_aba.banca==select_cliente]
    macro_aba['porcent']=(macro_aba['aba']*100)/(macro_aba['banca'].count())
    
    labels2 = macro_aba['macroactivo']


    fig=px.pie(macro_aba,values="aba", names=labels2)

    fig.update_layout(
        title_text="Portafolio banca")
        

    return fig

@callback(
    Output('indicator-graphic-ri', 'figure'),
    Input('xaxis-column-ri', 'value')
    )
def update_graph_ri(select_cliente):
    
    filtered_df = consulta_etl[(consulta_etl.año == año)&(consulta_etl.mes == mes)&(consulta_etl.dia == dia)]
    macro_aba=filtered_df.groupby(["perfil_riesgo","macroactivo"],as_index=False).count()
    macro_aba=macro_aba[macro_aba.perfil_riesgo==select_cliente]
    macro_aba['porcent']=(macro_aba['aba']*100)/(macro_aba['perfil_riesgo'].count())
    
    labels2 = macro_aba['macroactivo']


    fig=px.pie(macro_aba,values="aba", names=labels2)

    fig.update_layout(
        title_text="Portafolio Riesgo")
        

    return fig


if __name__ == '__main__':
    app.run(debug=True)