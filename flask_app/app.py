from flask import Flask

app = Flask('project_log8415')

if __name__ == "__main__":
    from waitress import serve
    serve(app, host="0.0.0.0", port=80)