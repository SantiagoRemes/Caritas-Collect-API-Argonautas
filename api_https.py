from flask import Flask, jsonify, make_response, request, send_file
import json
import sys
import mssql_functions as MSSql

# Connect to mssql dB from start
mssql_params = {}
mssql_params['DB_HOST'] = '10.14.255.69'
mssql_params['DB_NAME'] = 'Argonautasuwutest1'
mssql_params['DB_USER'] = 'SA'
mssql_params['DB_PASSWORD'] = 'Argentina2-'

try:
    MSSql.cnx = MSSql.mssql_connect(mssql_params)
except Exception as e:
    print("Cannot connect to mssql server!: {}".format(e))
    sys.exit()

app = Flask(__name__)

@app.route("/login", methods=['GET'])
def log_in():
    import bcrypt
    username = request.args.get('usuario', None)
    password = request.args.get('contrasena', None)
    d_user = MSSql.sql_log_in(username)
    if(len(d_user) == 0):
        return make_response(jsonify({"mensaje": "El usuario no existe", "success": False, "_id_recolector": -1}))
    bpassword = password.encode('utf-8')
    stored_hash = d_user[0]['contrasena'].encode('utf-8')
    if bcrypt.checkpw(bpassword, stored_hash):
        return make_response(jsonify({"mensaje": "La contraseña es correcta", "success": True, "_id_recolector": int(d_user[0]['_id_recolector'])}))
    else:
        return make_response(jsonify({"mensaje": "La contraseña es incorrecta", "success": False, "_id_recolector": -1}))

@app.route("/recolecciones", methods=['GET'])
def recolecciones():
    id = request.args.get('id', None)
    estado = request.args.get('estado', None)
    recolecciones = MSSql.sql_recolecciones(id, estado)
    return make_response(jsonify({"recolecciones": recolecciones, "success": True}))

@app.route("/crud/create", methods=['POST'])
def crud_create():
    data = request.json
    idUser = MSSql.sql_insert_recolector('Recolectores', data)
    return make_response(jsonify(idUser))
    
@app.route("/hello")
def hello():
    return "Shakira rocks!\n"

@app.route("/user")
def user():
    username = request.args.get('username', None)
    #print(username)
    d_user = MSSql.read_user_data('users', username)
    return make_response(jsonify(d_user))

@app.route("/crud/read", methods=['GET'])
def crud_read():
    username = request.args.get('username', None)
    d_user = MSSql.sql_read_where('users', {'username': username})
    return make_response(jsonify(d_user))

@app.route("/crud/update", methods=['PUT'])
def crud_update():
    d = request.json
    d_field = {'password': d['password']}
    d_where = {'username': d['username']}
    MSSql.sql_update_where('users', d_field, d_where)
    return make_response(jsonify('ok'))


@app.route("/crud/delete", methods=['DELETE'])
def crud_delete():
    d = request.json
    d_where = {'username': d['username']}
    MSSql.sql_delete_where('users', d_where)
    return make_response(jsonify('ok'))

API_CERT = '/home/user01/Reto/SSL/equipo05.tc2007b.tec.mx.cer'
API_KEY = '/home/user01/Reto/SSL/equipo05.tc2007b.tec.mx.key'

if __name__ == '__main__':
    print ("Running API...")
    app.run(host='0.0.0.0', port=10206, debug=True)
