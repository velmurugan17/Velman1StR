from flask import Flask, request, Response
from lib import table_name, push_data_to_redshift
import json

app = Flask(__name__)


@app.route('/shopify', methods=['POST'])
def api_echo():
    if request.headers['Content-Type'] == 'application/json':
        wh_data = [json.dumps(request.json)]
        push_data_to_redshift(wh_data, table_name)
        resp = Response(status=200)
        return resp


if __name__ == '__main__':
    app.run()
