import mysql.connector
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    mydb = mysql.connector.connect(
        host=os.getenv('GLPI_HOST'),
        user=os.getenv('GLPI_USER'),
        password=os.getenv('GLPI_PASSWORD'),
        database=os.getenv('GLPI_DATABASE')
    )

    return mydb

def get_db_connection_dev():
    mydb = mysql.connector.connect(
        host=os.getenv('GLPI_HOST_DEV'),
        user=os.getenv('GLPI_USER_DEV'),
        password=os.getenv('GLPI_PASSWORD_DEV'),
        database=os.getenv('GLPI_DATABASE_DEV')
    )

    return mydb

def consultar_glpi():
    try:
        conn  = get_db_connection()
        sql = """
                SELECT
                    CURDATE() AS 'data_dados',
                    gc.name AS'hostname',
                    gs.completename AS 'status',
                    gl.completename AS 'localizacao',
                    gm.name AS 'fabricante',
                    gc.serial AS 'numero_de_serie',
                    gcm.name AS 'modelo',
                    gc.contact AS 'usuario',
                    gct.name AS 'tipo',
                    (
                        select
                            gosv.name
                        from
                            glpi_items_operatingsystems gios
                            INNER JOIN glpi_operatingsystemversions gosv ON gios.operatingsystemversions_id = gosv.id
                        where
                            gios.itemtype = 'Computer' AND	
                            gios.items_id = gc.id
                            
                        ORDER by gios.id DESC LIMIT 1
                    ) AS 'sistema_operacional',
                    (
                        select
                            gpfa.last_contact
                        from
                            glpi_plugin_fusioninventory_agents gpfa
                        WHERE gpfa.computers_id = gc.id
                    ) AS 'fusion_ultimo_contato',
                    (
                        select
                            gpfic.last_fusioninventory_update
                        from
                            glpi_plugin_fusioninventory_inventorycomputercomputers gpfic
                        where
                            gpfic.computers_id = gc.id
                    ) AS 'fusion_ultimo_inventario',
                    (
                        select
                            gdp.designation
                        from
                            glpi_items_deviceprocessors gidp
                            INNER JOIN glpi_deviceprocessors gdp ON gidp.deviceprocessors_id = gdp.id 
                        where
                            gidp.itemtype = 'Computer' AND
                            gidp.items_id = gc.id
                        ORDER BY gidp.id DESC LIMIT 1
                    ) AS 'processador',
                    (
                        select
                            CONCAT(CEILING(sum(gidm.size) / 1024), ' GB')
                        from
                            glpi_items_devicememories gidm
                        where

                            gidm.items_id = gc.id
                        ORDER BY gidm.id LIMIT 3
                    ) AS 'memoria',
                    (
                        select
                            gic.warranty_date
                        from
                            glpi_infocoms gic
                        where
                            gic.itemtype = 'Computer' and
                            gic.items_id = gc.id
                    ) AS 'garantia_inicio',
                    (
                        select
                            DATE_ADD(gic.warranty_date, INTERVAL gic.warranty_duration MONTH)
                        from
                            glpi_infocoms gic
                        where
                            gic.itemtype = 'Computer' and
                            gic.items_id = gc.id
                    ) AS 'garantia_fim',
                    (
                        select
                            gs.name
                        from
                            glpi_infocoms gic
                            INNER JOIN glpi_suppliers gs ON gic.suppliers_id = gs.id
                        where
                            gic.itemtype = 'Computer' and
                            gic.items_id = gc.id
                    ) AS 'fornecedor',
                    (
                        select
                            gic.immo_number
                        from
                            glpi_infocoms gic
                        where
                            gic.itemtype LIKE 'Computer' and
                            gic.items_id = gc.id
                    ) AS 'patrimonio',
                    gc.id AS 'id_glpi'
                    
                    
                FROM 
                    glpi_computers gc
                    INNER JOIN glpi_states gs ON gc.states_id = gs.id
                    INNER JOIN glpi_locations gl ON gc.locations_id = gl.id
                    INNER JOIN glpi_manufacturers gm ON gc.manufacturers_id = gm.id
                    INNER JOIN glpi_computertypes gct ON gct.id = gc.computertypes_id
                    INNER JOIN glpi_computermodels gcm ON gc.computermodels_id = gcm.id
                    
                WHERE
                    gc.is_deleted = 0
            """
        mycursor = conn.cursor()
        mycursor.execute(sql)

        df = pd.DataFrame(mycursor.fetchall())
        df = df.rename(columns={0: 'data_dados', 1:'hostname', 2: 'status', 3: 'localizacao', 4: 'fabricante', 5: 'numero_de_serie', 6: 'modelo', 7: 'usuario', 8:'tipo', 9:'sistema_operacional', 10:'fusion_ultimo_contato', 11:'fusion_ultimo_inventario', 12:'processador', 13:'memoria', 14:'garantia_inicio', 15:'garantia_fim', 16:'fornecedor', 17: 'patrimonio', 18:'id_glpi'})
        
        conn.close()
    except Exception as e:
        print(e)
    
    return df

def delete_data_curdate_dev():
    mydb = get_db_connection_dev()

    mycursor = mydb.cursor()

    sql = """
            DELETE
            FROM
                computadores_ga
            WHERE 
                computadores_ga.data_dados = CURDATE()
        """
    
    mycursor.execute(sql)

    mydb.commit()

    mycursor.close()

    mydb.close()

def carga_db_dev(df):
    mydb = get_db_connection_dev()
    mycursor = mydb.cursor()

    sql = """
        INSERT INTO computadores_ga (
            data_dados, 
            hostname, 
            status, 
            localicacao, 
            fabricante, 
            numero_de_serie, 
            modelo, 
            usuario, 
            tipo, 
            sistema_operacional,
            fusion_ultimo_contato,
            fusion_ultimo_inventario, 
            processador, 
            memoria, 
            garantia_inicio, 
            garantia_fim, 
            fornecedor,
            patrimonio,
            id_glpi) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    for x in df.index:
        data_dados = df.at[x, 'data_dados']
        hostname = df.at[x, 'hostname']
        status = df.at[x, 'status']
        localizacao = df.at[x, 'localizacao']
        fabricante = df.at[x, 'fabricante']
        numero_de_serie = df.at[x, 'numero_de_serie']
        modelo = df.at[x, 'modelo']
        usuario = df.at[x, 'usuario']
        tipo = df.at[x, 'tipo']
        sistema_operacional = df.at[x, 'sistema_operacional']
        fusion_ultimo_contato = df.at[x, 'fusion_ultimo_contato']
        fusion_ultimo_contato = fusion_ultimo_contato if not pd.isnull(fusion_ultimo_contato) and str(fusion_ultimo_contato) != 'NaT' else None
        fusion_ultimo_inventario = df.at[x, 'fusion_ultimo_inventario']
        fusion_ultimo_inventario = fusion_ultimo_inventario if not pd.isnull(fusion_ultimo_inventario) and str(fusion_ultimo_inventario) != 'NaT' else None
        processador = df.at[x, 'processador']
        memoria = df.at[x, 'memoria']
        garantia_inicio = df.at[x, 'garantia_inicio']
        garantia_fim = df.at[x, 'garantia_fim']
        id_glpi = df.at[x, 'id_glpi']
        fornecedor = df.at[x, 'fornecedor']
        patrimonio = df.at[x, 'patrimonio']

        val = (data_dados,
            hostname,
            status,
            localizacao,
            fabricante,
            numero_de_serie,
            modelo,
            usuario,
            tipo,
            sistema_operacional,
            fusion_ultimo_contato,
            fusion_ultimo_inventario,
            processador,
            memoria,
            garantia_inicio,
            garantia_fim,
            fornecedor,
            patrimonio,
            id_glpi
            )

        mycursor.execute(sql, val)

    mydb.commit()
    mydb.close()

print('inicio')
glpi = consultar_glpi()
print('sucesso - coleta')
delete_data_curdate_prod()
print('Sucesso - remoção dados antigos')
carga_db_dev(glpi)
print('Sucesso - producao')

print('Done')
