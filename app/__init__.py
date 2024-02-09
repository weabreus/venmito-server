from flask import Flask
from dotenv import load_dotenv
import os



def create_app():
    load_dotenv()
    app = Flask(__name__)
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploaded_files')
    app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
    app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
    app.config['MONGO_CONNECTION_URI'] = os.getenv('MONGO_CONNECTION_URI')

    # Import routes
    from . import routes
    app.register_blueprint(routes.bp)

    return app
