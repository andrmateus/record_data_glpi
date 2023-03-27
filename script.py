import mysql.connector
import pandas as pd

try:

    def consultar_glpi():

        mydb = mysql.connector.connect(
        host="192.168.31.94",
        user="consulta",
        password="C0n5ult0NC0CL1N1C45@2021",
        database="glpi"
        )

        mycursor = mydb.cursor()

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

        mycursor.execute(sql)

        df = pd.DataFrame(mycursor.fetchall())
        df = df.rename(columns={0: 'data_dados', 1:'hostname', 2: 'status', 3: 'localizacao', 4: 'fabricante', 5: 'numero_de_serie', 6: 'modelo', 7: 'usuario', 8:'tipo', 9:'sistema_operacional', 10:'fusion_ultimo_contato', 11:'fusion_ultimo_inventario', 12:'processador', 13:'memoria', 14:'garantia_inicio', 15:'garantia_fim', 16:'fornecedor', 17: 'patrimonio', 18:'id_glpi'})
        
        mydb.close()
        
        return df

    def delete_data_curdate_prod():
        mydb = mysql.connector.connect(
                host="192.168.31.83",
                user="root",
                password="oZK34C099O6G@#ay",
                database="glpi_resumo"
            )

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

    def carga_db_prod(df):
        for x in df.index:
            mydb = mysql.connector.connect(
                host="192.168.31.83",
                user="root",
                password="oZK34C099O6G@#ay",
                database="glpi_resumo"
            )

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

    print('inicio')
    glpi = consultar_glpi()
    print('sucesso - coleta')
    delete_data_curdate_prod()
    print('Sucesso - remoção dados antigos')
    carga_db_prod(glpi)
    print('Sucesso - producao')

    print('Done')

except mysql.connector.Error as err:
    print("Something went wrong: {}".format(err))