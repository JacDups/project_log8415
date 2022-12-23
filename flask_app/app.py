from flask import Flask, request
from proxy import execute

app = Flask('project_log8415')


@app.after_request
def log_the_request(response):
    client_ip = request.headers.get('cf-connecting-ip')
    if client_ip is None:
        client_ip = request.remote_addr
    app.logger.info(
        f'{client_ip.center(15)} - {response.status_code} - {request.path}')
    return response


@app.route("/direct", methods=["POST"])
def direct_proxy():
    data = request.get_json()
    query: str = data['query']
    try:
        output, rowcount, dbname = execute(query, 'direct')
    except Exception as e:
        return str(e), 500
    return {
        'output': output,
        'rowcount': rowcount,
        'node': dbname
    }, 200


@app.route("/random", methods=["POST"])
def random_proxy():
    data = request.get_json()
    query: str = data['query']
    try:
        output, rowcount, dbname = execute(query, 'random')
    except Exception as e:
        return str(e), 500
    return {
        'output': output,
        'rowcount': rowcount,
        'node': dbname
    }, 200


@app.route("/custom", methods=["POST"])
def custom_proxy():
    data = request.get_json()
    query: str = data['query']
    try:
        output, rowcount, dbname = execute(query, 'custom')
    except Exception as e:
        return str(e), 500
    return {
        'output': output,
        'rowcount': rowcount,
        'node': dbname
    }, 200


if __name__ == "__main__":
    from waitress import serve
    serve(app, host="0.0.0.0", port=80)
