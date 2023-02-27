import pyodbc
from flask import Flask, jsonify, request

app = Flask(__name__)

api_key_list = ['Tango', 'Hulk', 'Joker', 'Shiushin', 'paciencialavidaesasi',
                'Wiseasyouwillhavebecome,sofullofexperience,you’llhaveunderstoodbythenwhattheseIthakasmean.']

try:
    connection = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost;"
        "Database=products;"
        "Trusted_Connection=yes;"
        "", timeout=1
    )

    cursor = connection.cursor()


    @app.route('/api/products', methods=['GET', 'POST'])
    def products():
        if request.method == 'GET':
            get_query: str = "SELECT * FROM products_tb FOR JSON PATH"
            cursor.execute(get_query)
            return cursor.fetchone()[0], 200
        if request.method == 'POST':
            if request.headers.get('key') in api_key_list:
                if request.form.get('name') is None:
                    return jsonify({"Error": "Name field is mandatory."}), 400
                elif not isinstance(request.form.get('name'), str):
                    return jsonify({"Error": "Name field should be a string."}), 400
                if request.form.get('price') is None:
                    return jsonify({"Error": "Price field is mandatory."}), 400
                elif not request.form.get('price').isnumeric():
                    return jsonify({"Error": "Price field should be a number."}), 400
                if request.form.get('types') is None:
                    return jsonify({"Error": "Type field is mandatory."}), 400
                elif not isinstance(request.form.get('types'), str):
                    return jsonify({"Error": "Type field should be a string."}), 400
                if request.form.get('featured') is None:
                    return jsonify({"Error": "Featured field is mandatory."}), 400
                elif not isinstance(request.form.get('featured'), str):
                    return jsonify({"Error": "Featured field should be a string."}), 400
                if request.form.get('visibility') is None:
                    return jsonify({"Error": "Visibility field is mandatory."}), 400
                elif not isinstance(request.form.get('visibility'), str):
                    return jsonify({"Error": "Visibility field should be a string."}), 400
                if request.form.get('description') is None:
                    return jsonify({"Error": "Description field is mandatory."}), 400
                elif not isinstance(request.form.get('description'), str):
                    return jsonify({"Error": "Description field should be a string."}), 400
                insert_query = "INSERT INTO products_tb(name, price, types, featured, visibility, description) VALUES(?, ?, ?, ?, ?, ?)"
                name = request.form.get('name')
                price = request.form.get('price')
                type = request.form.get('types')
                featured = request.form.get('featured')
                visibility = request.form.get('visibility')
                description = request.form.get('description')
                cursor.execute(insert_query, (name, price, type, featured, visibility, description))
                connection.commit()
                get_query = "SELECT * FROM products_tb"
                cursor.execute(get_query)
                rows = cursor.fetchall()
                for row in rows:
                    if name == row.name:
                        element = {"id": row.id,
                                   "name": row.name,
                                   "price": row.price,
                                   "types": row.type,
                                   "featured": row.featured,
                                   "visibility": row.visibility,
                                   "description": row.description}
                        return element
            return jsonify({"Error": "Invalid  API_KEY."}), 403


    @app.route('/api/products/products')
    def query_product_type():
        args = request.args
        types = args.get('type')
        get_query = "SELECT * FROM products_tb WHERE types=?"
        cursor.execute(get_query, types)
        rows = cursor.fetchall()
        value = []
        for row in rows:
            value.append({"id": row.id,
                       "name": row.name,
                       "price": row.price,
                       "types": row.types,
                       "featured": row.featured,
                       "visibility": row.visibility,
                       "description": row.description})
        connection.commit()
        return jsonify(value), 200



    @app.route('/api/products/<int:product_id>', methods=['GET', 'POST', 'PUT', 'DELETE'])
    def product(product_id):
        if request.method == 'GET':
            get_query = "SELECT * FROM products_tb"
            cursor.execute(get_query)
            rows = cursor.fetchall()
            for row in rows:
                if product_id == row.id:
                    element = {"id": row.id,
                               "name": row.name,
                               "price": row.price,
                               "types": row.types,
                               "featured": row.featured,
                               "visibility": row.visibility,
                               "description": row.description}
                    return element
            return jsonify({"Error": "Product not found!"}), 400
        if request.method == 'PUT':
            if request.headers.get('key') in api_key_list:
                if request.form.get('name') is None:
                    return jsonify({"Error": "Name field is mandatory."}), 400
                elif not isinstance(request.form.get('name'), str):
                    return jsonify({"Error": "Name field should be a string."}), 400
                if request.form.get('price') is None:
                    return jsonify({"Error": "Price field is mandatory."}), 400
                elif not request.form.get('price').isnumeric():
                    return jsonify({"Error": "Price field should be a number."}), 400
                if request.form.get('types') is None:
                    return jsonify({"Error": "Type field is mandatory."}), 400
                elif not isinstance(request.form.get('types'), str):
                    return jsonify({"Error": "Type field should be a string."}), 400
                if request.form.get('featured') is None:
                    return jsonify({"Error": "Featured field is mandatory."}), 400
                elif not isinstance(request.form.get('featured'), str):
                    return jsonify({"Error": "Featured field should be a string."}), 400
                if request.form.get('visibility') is None:
                    return jsonify({"Error": "Visibility field is mandatory."}), 400
                elif not isinstance(request.form.get('visibility'), str):
                    return jsonify({"Error": "Visibility field should be a string."}), 400
                if request.form.get('description') is None:
                    return jsonify({"Error": "Description field is mandatory."}), 400
                elif not isinstance(request.form.get('description'), str):
                    return jsonify({"Error": "Description field should be a string."}), 400
                insert_query = f"UPDATE products_tb SET name = ?, price = ?, types = ?, featured = ?, visibility = ?, description = ? WHERE id = {product_id}"
                name = request.form.get('name')
                price = request.form.get('price')
                type = request.form.get('types')
                featured = request.form.get('featured')
                visibility = request.form.get('visibility')
                description = request.form.get('description')
                cursor.execute(insert_query, (name, price, type, featured, visibility, description))
                connection.commit()
                get_query = "SELECT * FROM products_tb"
                cursor.execute(get_query)
                rows = cursor.fetchall()
                ids = []
                for row in rows:
                    ids.append(row.id)
                if product_id in ids:
                    for row in rows:
                        if product_id == row.id:
                            element = {"id": row.id,
                                       "name": row.name,
                                       "price": row.price,
                                       "types": row.type,
                                       "featured": row.featured,
                                       "visibility": row.visibility,
                                       "description": row.description}
                            return element
                return jsonify({"Error": f"No product found with {product_id} ID!"}), 404
            return jsonify({"Error": "Invalid  API_KEY."}), 403
        if request.method == 'DELETE':
            if request.headers.get('key') in api_key_list:
                get_query = "SELECT * FROM products_tb"
                cursor.execute(get_query)
                rows = cursor.fetchall()
                ids = []
                for row in rows:
                    ids.append(row.id)
                if product_id in ids:
                    element = {}
                    for row in rows:
                        if product_id == row.id:
                            element = {"id": row.id,
                                       "name": row.name,
                                       "price": row.price,
                                       "types": row.type,
                                       "featured": row.featured,
                                       "visibility": row.visibility,
                                       "description": row.description}
                            insert_query = f"DELETE FROM products_tb WHERE id = {row.id}"
                            cursor.execute(insert_query)
                            connection.commit()
                    return element
                return jsonify({"Error": f"No product found with {product_id} ID!"}), 404
            return jsonify({"Error": "Invalid  API_KEY."}), 403
        return jsonify({"Error": "Coding error."}), 403

except pyodbc.OperationalError as e:
    print("SQL Server connection error.")
except pyodbc.ProgrammingError as e:
    print("SQL statement error.")
finally:
    # cursor.close()
    # connection.close()
    print('Connection closed')


if __name__ == '__main__':
    app.run(debug=True)
