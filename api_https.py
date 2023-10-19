from flask import Flask, jsonify, make_response, request, send_file
import json
import sys
import mssql_functions as MSSql
import os

# Connect to mssql dB from start
mssql_params = {}
mssql_params['DB_HOST'] = os.environ["DB_HT"]
mssql_params['DB_NAME'] = os.environ["DB_NM"]
mssql_params['DB_USER'] = os.environ["DB_US"]
mssql_params['DB_PASSWORD'] = os.environ["DB_PW"]

try:
    MSSql.cnx = MSSql.mssql_connect(mssql_params)
except Exception as e:
    print("Cannot connect to mssql server!: {}".format(e))
    sys.exit()

try:
    import logging
    import logging.handlers
    # Logging (remember the 5Ws: “What”, “When”, “Who”, “Where”, “Why”)
    LOG_PATH = '/var/log/api_https'
    LOGFILE = LOG_PATH  + '/api_https.log'
    logformat = '%(asctime)s.%(msecs)03d %(levelname)s: %(message)s'
    formatter = logging.Formatter(logformat, datefmt='%d-%b-%y %H:%M:%S')
    loggingRotativo = False
    DEV = True
    if loggingRotativo:
        # Logging rotativo
        LOG_HISTORY_DAYS = 3
        handler = logging.handlers.TimedRotatingFileHandler(
                LOGFILE,
                when='midnight',
                backupCount=LOG_HISTORY_DAYS)
    else:
        handler = logging.FileHandler(filename=LOGFILE)
    handler.setFormatter(formatter)
    my_logger = logging.getLogger("api_https")
    my_logger.addHandler(handler)
    if DEV:
        my_logger.setLevel(logging.DEBUG)
    else:
        my_logger.setLevel(logging.INFO)
except:
    pass

# Remove 'Server' from header
from gunicorn.http import wsgi
class Response(wsgi.Response):
    def default_headers(self, *args, **kwargs):
        headers = super(Response, self).default_headers(*args, **kwargs)
        return [h for h in headers if not h.startswith('Server:')]
wsgi.Response = Response

app = Flask(_name_)

@app.after_request
def add_header(r):
    import secure
    secure_headers = secure.Secure()
    secure_headers.framework.flask(r)
    #r.headers['X-Frame-Options'] = 'SAMEORIGIN' # ya lo llena 'secure'
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Content-Security-Policy"] = "default-src 'none'"
    r.headers["Content-type"] = "application/json"
    r.headers["Server"] = "None"
    #r.headers["Expires"] = "0"
    return r

@app.route("/login", methods=['GET'])
def log_in():
    import bcrypt
    username = request.args.get('usuario', None)
    password = request.args.get('contrasena', None)
    usertype = request.args.get('usertype', None)
    d_user = MSSql.sql_log_in(username, usertype)
    if(len(d_user) == 0):
        return make_response(jsonify({"mensaje": "El usuario no existe", "success": False, "id": -1}))
    bpassword = password.encode('utf-8')
    stored_hash = d_user[0]['contrasena'].encode('utf-8')
    id = 0
    if(usertype == "Admin"):
        id = d_user[0]['_id_adminisrador']
    else:
        id = d_user[0]['_id_recolector']
    if bcrypt.checkpw(bpassword, stored_hash):
        return make_response(jsonify({"mensaje": "La contraseña es correcta", "success": True, "id": int(id)}))
    else:
        return make_response(jsonify({"mensaje": "La contraseña es incorrecta", "success": False, "id": -1}))

@app.route("/recolecciones", methods=['GET'])
def recolecciones():
    id = request.args.get('id', None)
    estado = request.args.get('estado', None)
    recolecciones = MSSql.sql_recolecciones(id, estado)
    return make_response(jsonify({"recolecciones": recolecciones, "success": True}))

@app.route("/detalles", methods=['GET'])
def recoleccion_detalles():
    import requests
    id = request.args.get('id', None)
    recoleccion = MSSql.sql_recoleccion_detalles(id)
    print(recoleccion)
    response = requests.get("https://geocode.maps.co/search?q={}}")
    recoleccion["lat"] = response.lat
    recoleccion["long"] = response.long
    return make_response(jsonify({"recoleccion": recoleccion, "success": True}))

@app.route("/estado", methods=['PUT'])
def recoleccion_estado():
    d = request.json
    id = d['id']
    estado = d['estado']
    comentarios = d['comentarios']
    MSSql.sql_recoleccion_estado(id, estado, comentarios)
    return make_response(jsonify({"mensaje": 'Estado Actualizado Exitosamente', "id": id, "success": True}))

@app.route("/recolectores", methods=['GET'])
def recolectores():
    id = request.args.get('id', None)
    recolectores = MSSql.sql_recolectores(id)
    return make_response(jsonify({"recolectores": recolectores, "success": True}))

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

if _name_ == '_main_':
    import ssl
    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    context.load_cert_chain(API_CERT, API_KEY)
    app.run(host='0.0.0.0', port=10206, ssl_context=context, debug=False)
