from flask import render_template, request, send_file, jsonify, redirect, url_for
from app import app
import pandas as pd
from app.sendInMQ import AsyncProcess
from flask_pymongo import PyMongo
from app.MapReduce import Map, Reduce
import glob,os

mongo = PyMongo(app)

@app.route("/top", methods=["POST", "GET"])
def top():
    args = {"method": "GET"}
    if (request.args.get('field') is None and request.args.get('count') is None):
        if request.method == "POST":
            algorithm = request.form['options']
            field = request.form['field']
            count = request.form['count']
            file = request.files["file"]
            if (algorithm == 'simple'):
                table = pd.read_excel(file)
                result = table.sort_values(by=[field]).head(int(count))
                result.to_excel('./files/result_simple.xlsx')
            elif (algorithm == 'distr'):
                n_parts = 10
                file = request.files["file"]
                field = request.form['field']
                count = request.form['count']
                Map(file, field, count, n_parts)
                result = Reduce(n_parts, field, count)
                result.to_excel('./files/result_distr.xlsx')
            args["method"] = "POST"
            return render_template("downloadFiles.html", args=args)
    else:
        field = request.args.get('field')
        count = request.args.get('count')
        answer = mongo.db.stringsOfFile.find().sort(field).limit(int(count))
        return jsonify([str(v) for v in answer])

    return render_template("top.html", args=args)


@app.route("/downloadFile")
def downloadFile():
    list_of_files = glob.glob(os.getcwd() + '\\files/*')
    latest_file = max(list_of_files, key=os.path.getmtime)
    with open(latest_file) as fp:
        name = fp.name
    return send_file(name,
                     mimetype='text/xlsx',
                     attachment_filename='output.xlsx',
                     as_attachment=True)

@app.route("/back")
def back():
    return redirect(url_for('top'))


@app.route("/upload", methods=["POST", "GET"])
def upload():
    args = {"method": "GET"}
    if request.method == "POST":
        file = request.files["file"]
        mongo.db.stringsOfFile.delete_many({})
        AsyncProcess(file)
        args["method"] = "POST"
    return render_template("upload.html", args=args)
