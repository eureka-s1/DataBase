from app import create_app
from app.db import init_db

app = create_app()

if __name__ == '__main__':
    init_db()
    app.run(host='127.0.0.1', port=5000, debug=True)
