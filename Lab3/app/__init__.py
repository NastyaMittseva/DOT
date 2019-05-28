from flask import Flask

app = Flask(__name__)
app.config['MONGO_DBNAME'] = 'processingFile'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/processingFile'
