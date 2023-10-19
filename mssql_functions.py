cnx = None
mssql_params = {}

def mssql_connect(sql_creds):
    import pymssql
    cnx = pymssql.connect(
        server=sql_creds['DB_HOST'],
        user=sql_creds['DB_USER'],
        password=sql_creds['DB_PASSWORD'],
        database=sql_creds['DB_NAME'])
    return cnx

def sql_log_in(username, usertype):
    global cnx, mssql_params
    read = ""
    if(usertype == "Admin"):
        read = "SELECT * FROM Administradores WHERE usuario = %s"
    else:
        read = "SELECT * FROM Recolectores WHERE usuario = %s"
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (username,))
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (username,))
        
        userdata = cursor.fetchall()
        cursor.close()
        return userdata
    except Exception as err:
        raise TypeError("sql_read_where:%s" % err)
    
def sql_recolecciones(id, estado):
    import pymssql
    global cnx, mssql_params
    read = "SELECT * FROM Recibos WHERE _id_recolector = %s AND estado_recogido = %s"
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (id, estado,))
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (id, estado,))
        recolecciones = cursor.fetchall()
        cursor.close()
        return recolecciones
    except Exception as err:
        raise TypeError("sql_read_where:%s" % err)
    
def sql_recoleccion_detalles(id):
    import pymssql
    global cnx, mssql_params
    read = "SELECT * FROM Recibos R JOIN Donadores D ON R._id_donador = D._id_donador WHERE _id_recibo = %s"
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (id,))
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (id,))
        recoleccion = cursor.fetchall()
        cursor.close()
        return recoleccion
    except Exception as err:
        raise TypeError("sql_read_where:%s" % err)
    
def sql_recoleccion_estado(id, estado, comentarios):
    import pymssql
    global cnx, mssql_params
    update = "UPDATE Recibos SET estado_recogido = %s, comentarios = %s WHERE _id_recibo = %s"
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            a = cursor.execute(update, (estado, comentarios, id,))
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            a = cursor.execute(update, (estado, comentarios, id,))
        cnx.commit()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("sql_update_where:%s" % e)

def sql_recolectores(id):
    import pymssql
    global cnx, mssql_params
    read = "SELECT * FROM Recolectores WHERE _id_adminisrador = %s"
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (id,))
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (id,))
        recolecciones = cursor.fetchall()
        cursor.close()
        return recolecciones
    except Exception as err:
        raise TypeError("sql_read_where:%s" % err)

import pymssql
import bcrypt

def sql_insert_recolector(table_name, d):
    global cnx, mssql_params
    keys = ""
    values = ""
    data = []
    for k in d:
        keys += k + ','
        values += "%s," 
        if isinstance(d[k], bool):
            data.append(int(d[k] == True))
        else:
            data.append(d[k])

    keys += 'contrasena' + ','
    values += "%s," 
    password = "1234".encode('utf-8')  # Encode the password as bytes
    hashed_password = bcrypt.hashpw(password, bcrypt.gensalt())
    data.append(hashed_password)

    keys = keys[:-1]
    values = values[:-1]
    insert = 'INSERT INTO %s (%s) VALUES (%s)'
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            query = insert % (table_name, keys, values)
            cursor.execute(query, data)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            query = insert % (table_name, keys, values)
            cursor.execute(query, data)
        cnx.commit()
        id_new = cursor.lastrowid
        cursor.close()
        return id_new
    except Exception as e:
        raise TypeError("sql_insert_row_into:%s" % e)


def read_user_data(table_name, username):
    import pymssql
    global cnx, mssql_params
    read = "SELECT * FROM {} WHERE username = %s".format(table_name)
    
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (username,))
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, (username,))
        a = cursor.fetchall()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("read_user_data: %s" % e)


import pymssql

def sql_read_all(table_name):
    global cnx, mssql_params
    # Check if the table_name is a valid table name
    # You should implement your own validation logic here
    if not is_valid_table_name(table_name):
        raise ValueError("Invalid table name")

    read = 'SELECT * FROM {}'.format(table_name)
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read)
        a = cursor.fetchall()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("sql_read_where:%s" % e)

# Implement your own validation logic for table names
def is_valid_table_name(table_name):
    # You should define your own validation rules here
    # For example, check if the table_name only contains allowed characters
    # and does not contain SQL keywords
    # This is a simplified example; you may need more stringent checks.
    allowed_characters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_'
    if all(c in allowed_characters for c in table_name):
        return True
    return False


import pymssql

def sql_read_where(table_name, d_where):
    global cnx, mssql_params
    placeholders = []
    values = []

    for k, v in d_where.items():
        if v is not None:
            if isinstance(v, bool):
                placeholders.append("{} = %s".format(k))
                values.append(int(v))
            else:
                placeholders.append("{} = %s".format(k))
                values.append(v)
        else:
            placeholders.append("{} IS NULL".format(k))

    where_clause = " AND ".join(placeholders)
    read = "SELECT * FROM {} WHERE {}".format(table_name, where_clause)

    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, tuple(values))
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read, tuple(values))
        a = cursor.fetchall()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("sql_read_where:%s" % e)


import pymssql

def sql_update_where(table_name, d_field, d_where):
    global cnx, mssql_params
    set_clauses = []
    where_clauses = []
    values = []

    for k, v in d_field.items():
        if v is None:
            set_clauses.append("{} = NULL".format(k))
        else:
            set_clauses.append("{} = %s".format(k))
            values.append(v)

    for k, v in d_where.items():
        if v is not None:
            where_clauses.append("{} = %s".format(k))
            values.append(v)
        else:
            where_clauses.append("{} IS NULL".format(k))

    set_clause = ", ".join(set_clauses)
    where_clause = " AND ".join(where_clauses)

    update = "UPDATE {} SET {} WHERE ({})".format(table_name, set_clause, where_clause)

    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(update, tuple(values))
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(update, tuple(values))
        cnx.commit()
        cursor.close()
        return cursor.rowcount
    except Exception as e:
        raise TypeError("sql_update_where:%s" % e)


import pymssql

def sql_delete_where(table_name, d_where):
    global cnx, mssql_params
    where_clauses = []
    values = []

    for k, v in d_where.items():
        if v is not None:
            if isinstance(v, bool):
                where_clauses.append("{} = %s".format(k))
                values.append(int(v))
            else:
                where_clauses.append("{} = %s".format(k))
                values.append(v)
        else:
            where_clauses.append("{} IS NULL".format(k))

    where_clause = " AND ".join(where_clauses)
    delete = "DELETE FROM {} WHERE ({})".format(table_name, where_clause)

    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(delete, tuple(values))
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(delete, tuple(values))
        cnx.commit()
        cursor.close()
        return cursor.rowcount
    except Exception as e:
        raise TypeError("sql_delete_where:%s" % e)


if _name_ == '_main_':
    import json
    mssql_params = {}
    mssql_params['DB_HOST'] = '10.14.255.69'
    mssql_params['DB_NAME'] = 'alumno01'
    mssql_params['DB_USER'] = 'SA'
    mssql_params['DB_PASSWORD'] = 'Shakira123.'
    cnx = mssql_connect(mssql_params)

    # Do your thing
    try:
        rx = sql_read_all('users')
        print(json.dumps(rx, indent=4))
        input("press Enter to continue...")
        rx = read_user_data('users', 'hugo')
        print(rx)
        input("press Enter to continue...")
        print("Querying for user 'paco'...")
        d_where = {'username': 'paco'}
        rx = sql_read_where('users', d_where)
        print(rx)
        input("press Enter to continue...")
        print("Inserting user 'otro'...")
        rx = sql_insert_recolector('users',{'username': 'otro', 'password': 'otro123'})
        print("Inserted record", rx)
        rx = sql_read_all('users')
        print(json.dumps(rx, indent=4))
        input("press Enter to continue...")
        print("Modifying password for user 'otro'...")
        d_field = {'password': 'otro456'}
        d_where = {'username': 'otro'}
        sql_update_where('users', d_field, d_where)
        print("Record updated")
        rx = sql_read_all('users')
        print(json.dumps(rx, indent=4))
        input("press Enter to continue...")
        print("Deleting user 'otro'...")
        d_where = {'username': 'otro'}
        sql_delete_where('users', d_where)
        print("Record deleted")
        rx = sql_read_all('users')
        print(json.dumps(rx, indent=4))
    except Exception as e:
        print(e)
    cnx.close()
