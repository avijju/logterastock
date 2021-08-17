import os
from flask import Flask, render_template, request, jsonify
import json
import requests


app = Flask(__name__)


@app.route("/")
def main():
    response = requests.get("https://stock.avinashbhatt.com/api/StockData/agreegateAsync")
    print(response.json())
    return  json.dumps(response.json(), indent=4, sort_keys=True, default=json)#json.loads(response.text)    