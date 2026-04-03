from __future__ import annotations

from flask import Flask, jsonify, request

from .db import db_session, init_db
from .services.customers import create_customer, list_customers, resolve_customer_id, upsert_alias


def create_app() -> Flask:
    app = Flask(__name__)

    @app.get('/health')
    def health():
        return {'status': 'ok'}

    @app.post('/init-db')
    def init_db_api():
        init_db()
        return {'message': 'database initialized'}

    @app.get('/customers')
    def customers_list():
        with db_session() as conn:
            return jsonify(list_customers(conn))

    @app.post('/customers')
    def customers_create():
        payload = request.get_json(force=True)
        required = ('customer_code', 'name')
        missing = [k for k in required if not payload.get(k)]
        if missing:
            return {'error': f'missing fields: {", ".join(missing)}'}, 400

        with db_session() as conn:
            customer_id = create_customer(
                conn,
                customer_code=payload['customer_code'],
                name=payload['name'],
                phone=payload.get('phone'),
                country=payload.get('country'),
                email=payload.get('email'),
                default_price_per_m3=float(payload.get('default_price_per_m3', 89.71)),
            )
        return {'id': customer_id}, 201

    @app.post('/customer-aliases')
    def alias_upsert():
        payload = request.get_json(force=True)
        customer_id = payload.get('customer_id')
        alias_name = payload.get('alias_name')
        if not customer_id or not alias_name:
            return {'error': 'customer_id and alias_name are required'}, 400

        with db_session() as conn:
            upsert_alias(
                conn,
                customer_id=int(customer_id),
                alias_name=alias_name,
                source=payload.get('source', 'MANUAL'),
                is_primary=int(payload.get('is_primary', 0)),
                is_active=int(payload.get('is_active', 1)),
                remark=payload.get('remark'),
            )
        return {'message': 'ok'}

    @app.get('/customer-resolve')
    def customer_resolve():
        name = request.args.get('name', '').strip()
        if not name:
            return {'error': 'name is required'}, 400

        with db_session() as conn:
            customer_id = resolve_customer_id(conn, name)
        if customer_id is None:
            return {'matched': False}
        return {'matched': True, 'customer_id': customer_id}

    return app
