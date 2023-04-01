import mysql.connector
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection_prod():
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

def consult_computers_prod():
    try:
        conn  = get_db_connection_prod()
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

def delete_computers_dev():
    # get db connection
    mydb = get_db_connection_dev()

    # get cursor
    mycursor = mydb.cursor()

    try:
        sql = """
                    DELETE
                    FROM
                        computadores_ga
                    WHERE 
                        month(computadores_ga.data_dados) = month(CURDATE()) AND
                        year(computadores_ga.data_dados) = year(CURDATE())
                """
        
        # execute
        mycursor.execute(sql)
        mydb.commit()
        mycursor.close()
        mydb.close()
    except Exception as e:
        print(e)

def delete_printers_dev():
    mydb = get_db_connection_dev()

    mycursor = mydb.cursor()

    try:
        sql = """
                    DELETE
                    FROM
                        impressoras_ga
                    WHERE 
                        month(impressoras_ga.data_dados) = month(CURDATE()) AND
                        year(impressoras_ga.data_dados) = year(CURDATE())
                """
        
        # execute
        mycursor.execute(sql)
        mydb.commit()
        mycursor.close()
        mydb.close()
    except Exception as e:
        print(e)

def delete_phones_dev():
    mydb = get_db_connection_dev()

    mycursor = mydb.cursor()

    try:
        sql = """
                    DELETE
                    FROM
                        telefones_ga
                    WHERE 
                        month(telefones_ga.data_dados) = month(CURDATE()) AND
                        year(telefones_ga.data_dados) = year(CURDATE())
                """
        
        # execute
        mycursor.execute(sql)
        mydb.commit()
        mycursor.close()
        mydb.close()
    except Exception as e:
        print(e)

def update_computers_dev(df):
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
        data_dados = str(df['data_dados'][x]) if df['data_dados'][x] != None else None
        hostname = str(df['hostname'][x]) if df['hostname'][x] != None else None
        status = str(df['status'][x]) if df['status'][x] != None else None
        localizacao = str(df['localizacao'][x]) if df['localizacao'][x] != None else None
        fabricante = str(df['fabricante'][x]) if df['fabricante'][x] != None else None
        numero_de_serie = str(df['numero_de_serie'][x]) if df['numero_de_serie'][x] != None else None
        modelo = str(df['modelo'][x]) if df['modelo'][x] != None else None
        usuario = str(df['usuario'][x]) if df['usuario'][x] != None else None
        tipo = str(df['tipo'][x]) if df['tipo'][x] != None else None
        sistema_operacional = str(df['sistema_operacional'][x]) if df['sistema_operacional'][x] != None else None
        fusion_ultimo_contato = str(df['fusion_ultimo_contato'][x]) if df['fusion_ultimo_contato'][x] != None else None
        fusion_ultimo_contato = fusion_ultimo_contato if str(fusion_ultimo_contato) != 'NaT' else None
        fusion_ultimo_inventario = str(df['fusion_ultimo_inventario'][x]) if df['fusion_ultimo_inventario'][x] != None else None
        fusion_ultimo_inventario = fusion_ultimo_inventario if str(fusion_ultimo_inventario) != 'NaT' else None
        processador = str(df['processador'][x]) if df['processador'][x] != None else None
        memoria = str(df['memoria'][x]) if df['memoria'][x] != None else None
        garantia_inicio = str(df['garantia_inicio'][x]) if df['garantia_inicio'][x] != None else None
        garantia_fim = str(df['garantia_fim'][x]) if df['garantia_fim'][x] != None else None
        id_glpi = str(df['id_glpi'][x]) if df['id_glpi'][x] != None else None
        fornecedor = str(df['fornecedor'][x]) if df['fornecedor'][x] != None else None
        patrimonio = str(df['patrimonio'][x]) if df['patrimonio'][x] != None else None

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

def transfer_printers_data():
    # Conectar no banco de origem
    from_conn = get_db_connection_prod()

    from_cursor = from_conn.cursor()

    # Consulta SQL para recuperar os dados das impressoras do banco de origem
    sql = """
        SELECT
            curdate() AS data_dados,
            gp.name AS nome,
            (
                SELECT gpfp.fornecedordaimpressorafield 
                FROM glpi_plugin_fields_printerprinters gpfp 
                WHERE gpfp.items_id = gp.id
            ) AS fornecedor, 
            (
                SELECT gpfp.funodaimpressorafield 
                FROM glpi_plugin_fields_printerprinters gpfp 
                WHERE gpfp.items_id = gp.id
            ) AS funcao_impressora, 
            gs.completename AS status, 
            gm.name AS fabricante, 
            gpm.name AS modelo, 
            gl.completename AS localizacao, 
            gpt.name AS tipo, 
            gp.serial AS serie, 
            gn.name AS rede,
            (
                SELECT gpfp.localdeinstalaofield 
                FROM glpi_plugin_fields_printerprinters gpfp 
                WHERE gpfp.items_id = gp.id
            ) AS local_instalacao
        FROM 
            glpi_printers gp 
            INNER JOIN glpi_printertypes gpt ON gpt.id = gp.printertypes_id 
            INNER JOIN glpi_states gs ON gs.id = gp.states_id 
            INNER JOIN glpi_manufacturers gm ON gm.id = gp.manufacturers_id 
            INNER JOIN glpi_locations gl ON gl.id = gp.locations_id 
            INNER JOIN glpi_printermodels gpm ON gpm.id = gp.printermodels_id 
            INNER JOIN glpi_networks gn ON gn.id = gp.networks_id 
        WHERE 
            gp.is_deleted = 0
    """

    from_cursor.execute(sql)

    # Conectar no banco de destino
    to_conn = get_db_connection_dev()

    to_cursor = to_conn.cursor()

    # Inserir os dados no banco de destino
    insert_sql = """
        INSERT INTO impressoras_ga (
            data_dados, 
            nome, 
            fornecedor, 
            funcao_impressora, 
            status, 
            fabricante, 
            modelo, 
            localizacao, 
            tipo, 
            serie, 
            rede,
            local_instalacao
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for row in from_cursor:
        to_cursor.execute(insert_sql, row)

    to_conn.commit()

    # Fechar as conexões
    from_cursor.close()
    from_conn.close()
    to_cursor.close()
    to_conn.close()

def transfer_phones_data():
    # Conectar no banco de origem
    from_conn = get_db_connection_prod()

    from_cursor = from_conn.cursor()

    # Consulta SQL para recuperar os dados dos telefones do banco de origem
    sql = """
        SELECT
            curdate() AS data_dados,
            gp.name AS hostname,
            gs.completename AS status,
            gm.name AS fabricante,
            gl.completename AS localizacao,
            gpm.name AS modelo,
            gpfp.ndalinhacomdddfield AS linha,
            gu.name AS usuario,
            gp.serial AS numero_de_serie,
            gpt.name AS tipo,
            gpfp.imeizeroonefield AS imei
        FROM 
            glpi_phones gp 
            INNER JOIN glpi_plugin_fields_phonetelefones gpfp ON gpfp.items_id = gp.id
            INNER JOIN glpi_users gu ON gu.id = gp.users_id
            INNER JOIN glpi_phonetypes gpt ON gpt.id = gp.phonetypes_id
            INNER JOIN glpi_states gs ON gs.id = gp.states_id
            INNER JOIN glpi_manufacturers gm ON gm.id = gp.manufacturers_id
            INNER JOIN glpi_locations gl ON gl.id = gp.locations_id
            INNER JOIN glpi_phonemodels gpm ON gpm.id = gp.phonemodels_id
        WHERE 
            gp.is_deleted = 0
    """

    from_cursor.execute(sql)

    # Conectar no banco de destino
    to_conn = get_db_connection_dev()

    to_cursor = to_conn.cursor()

    # Inserir os dados no banco de destino
    insert_sql = """
        INSERT INTO telefones_ga (
            data_dados,
            hostname,
            status,
            fabricante,
            localizacao,
            modelo,
            linha,
            usuario,
            numero_de_serie,
            tipo,
            imei
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for row in from_cursor:
        to_cursor.execute(insert_sql, row)

    to_conn.commit()

    # Fechar as conexões
    from_cursor.close()
    from_conn.close()
    to_cursor.close()
    to_conn.close()


print('Inicio - coleta')
computersConsult = consult_computers_prod()
print('sucesso - coleta')
print('Inicio - remoção dados antigos')
delete_computers_dev()
delete_printers_dev()
delete_phones_dev()
print('Sucesso - remoção dados antigos')
print('Inicio - transferencia de dados')
update_computers_dev(computersConsult)
transfer_phones_data()
transfer_printers_data()
print('Sucesso - transferencia de dados')

print('Done')
