import json
import pyodbc
from flask import Flask, jsonify, request, render_template, url_for, redirect

app = Flask(__name__)

api_key_list = ['Tango', 'Hulk', 'Joker', 'Shiushin', 'paciencialavidaesasi',
            'Andifyoufindherpoor,Ithakawon’thavefooled you.Wiseasyouwillhavebecome,sofullofexperience,you’llhaveunderstoodbythenwhattheseIthakasmean.']


@app.route('/')
def home():
    return render_template('index.html')


try:
    connection = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost;"
        "Database=movies;"
        "Trusted_Connection=yes;"
        "", timeout=1
    )

    cursor = connection.cursor()


    @app.route('/movies')
    def movies_home():
        cursor.execute("SELECT * FROM movies")
        rows = cursor.fetchall()
        movies = []
        for row in rows:
            element = {"id": row.id,
                       "title": row.movie_title,
                       "year": row.year_of_release,
                       "genre": row.movie_genre,
                       "description": row.short_description}
            movies.append(element)
        return render_template('movies.html', movies_list=movies)


    @app.route('/add')
    def add():
        return render_template('add.html')


    @app.route('/edit_movie/<int:movie_id>')
    def edit_movie(movie_id):
        return render_template('edit.html', id_movie=movie_id)


    @app.route('/api/movies', methods=['GET', 'POST'])
    def movies1():
        if request.method == 'GET':
            get_query = "SELECT * FROM movies FOR JSON PATH"
            cursor.execute(get_query)
            return cursor.fetchone()[0], 200
        if request.method == 'POST':
            if request.form.get('key') in api_key_list:
                if request.form.get('movie_title') is None:
                    return jsonify({"Error": "Title field is mandatory."}), 400
                elif not isinstance(request.form.get('movie_title'), str):
                    return jsonify({"Error": "Title field should be a string."}), 400
                if request.form.get('year_of_release') is None:
                    return jsonify({"Error": "Year field is mandatory."}), 400
                elif not request.form.get('year_of_release').isnumeric():
                    return jsonify({"Error": "Year field should be a number."}), 400
                if request.form.get('movie_genre') is None:
                    return jsonify({"Error": "Genre field is mandatory."}), 400
                elif not isinstance(request.form.get('movie_genre'), str):
                    return jsonify({"Error": "Genre field should be a string."}), 400
                if request.form.get('short_description') is None:
                    return jsonify({"Error": "Description field is mandatory."}), 400
                elif not isinstance(request.form.get('short_description'), str):
                    return jsonify({"Error": "Description field should be a string."}), 400
                insert_query = "INSERT INTO movies(movie_title, year_of_release, movie_genre, short_description) VALUES(?, ?, ?, ?)"
                title = request.form.get('movie_title')
                year = request.form.get('year_of_release')
                genre = request.form.get('movie_genre')
                description = request.form.get('short_description')
                cursor.execute(insert_query, (title, year, genre, description))
                connection.commit()
                get_query = "SELECT * FROM movies"
                cursor.execute(get_query)
                rows = cursor.fetchall()
                for row in rows:
                    if title == row.movie_title:
                        element = {"id": row.id,
                                   "title": row.movie_title,
                                   "year": row.year_of_release,
                                   "genre": row.movie_genre,
                                   "description": row.short_description}
                        return redirect(url_for('movies_home')), 201
            return jsonify({"Error": "Invalid  API_KEY."}), 403

    @app.route('/api/movies/<int:movie_id>', methods=['GET', 'POST', 'PUT', 'DELETE'])
    def movie(movie_id):
        print("--------here----------------")
        print("-------method------", request.form.get("_method"))
        # if request.method == 'GET':
        if request.form.get("_method") == "GET":
            print("--------get----------------")
            get_query = "SELECT * FROM movies"
            cursor.execute(get_query)
            rows = cursor.fetchall()
            for row in rows:
                if movie_id == row.id:
                    element = {"id": row.id,
                               "title": row.movie_title,
                               "year": row.year_of_release,
                               "genre": row.movie_genre,
                               "description": row.short_description}
                    return json.dumps(element), 201
            return jsonify({"Error": "Movie not found!"}), 400
        # if request.method == 'PUT':
        if request.form.get("_method") == "PUT":
            print("-----------put------------")
            if request.form.get('key') in api_key_list:
                print("-----------key------------", request.form.get('key'))
                if request.form.get('movie_title') is None:
                    return jsonify({"Error": "Title field is mandatory."}), 400
                elif not isinstance(request.form.get('movie_title'), str):
                    return jsonify({"Error": "Title field should be a string."}), 400
                if request.form.get('year_of_release') is None:
                    return jsonify({"Error": "Year field is mandatory."}), 400
                elif not request.form.get('year_of_release').isnumeric():
                    return jsonify({"Error": "Year field should be a number."}), 400
                if request.form.get('movie_genre') is None:
                    return jsonify({"Error": "Genre field is mandatory."}), 400
                elif not isinstance(request.form.get('movie_genre'), str):
                    return jsonify({"Error": "Genre field should be a string."}), 400
                if request.form.get('short_description') is None:
                    return jsonify({"Error": "Description field is mandatory."}), 400
                elif not isinstance(request.form.get('short_description'), str):
                    return jsonify({"Error": "Description field should be a string."}), 400
                insert_query = f"UPDATE  movies SET movie_title = ?, year_of_release = ?, movie_genre = ?, short_description = ? WHERE id = {movie_id}"
                title = request.form.get('movie_title')
                year = request.form.get('year_of_release')
                genre = request.form.get('movie_genre')
                description = request.form.get('short_description')
                cursor.execute(insert_query, (title, year, genre, description))
                connection.commit()
                get_query = "SELECT * FROM movies"
                cursor.execute(get_query)
                rows = cursor.fetchall()
                ids = []
                for row in rows:
                    ids.append(row.id)
                if movie_id in ids:
                    for row in rows:
                        if movie_id == row.id:
                            element = {"id": row.id,
                                       "title": row.movie_title,
                                       "year": row.year_of_release,
                                       "genre": row.movie_genre,
                                       "description": row.short_description}
                            return redirect(url_for('movies_home')), 201
                return jsonify({"Error": f"No movie found with {movie_id} ID!"}), 404
            return jsonify({"Error": "Invalid  API_KEY."}), 403
        # if request.method == 'DELETE':
        if request.form.get("_method") == "DELETE":
            print("--------delete----------------")
            if request.form.get('key') in api_key_list:
                get_query = "SELECT * FROM movies"
                cursor.execute(get_query)
                rows = cursor.fetchall()
                ids = []
                for row in rows:
                    ids.append(row.id)
                if movie_id in ids:
                    element = {}
                    for row in rows:
                        if movie_id == row.id:
                            element = {"id": row.id,
                                       "title": row.movie_title,
                                       "year": row.year_of_release,
                                       "genre": row.movie_genre,
                                       "description": row.short_description}
                            insert_query = f"DELETE FROM movies WHERE id = {row.id}"
                            cursor.execute(insert_query)
                            connection.commit()
                    return json.dumps(element), 202
                return jsonify({"Error": f"No movie found with {movie_id} ID!"}), 404
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
