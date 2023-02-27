from flask import Flask, render_template, url_for, request, redirect
import pyodbc
import pyautogui as pag

app = Flask(__name__)

try:
    connection = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost;"
        "Database=users;"
        "Trusted_Connection=yes;"
        "", timeout=1
    )

    cursor = connection.cursor()
    # insert_query = "INSERT INTO users VALUES(?, ?);"
    # username = 'mike'
    # password = '11111111'
    # cursor.execute(insert_query,(username, password))
    # insert_query = "INSERT INTO users VALUES(?, ?);"
    # username = 'moraki'
    # password = '22222222'
    # cursor.execute(insert_query,(username, password))
    # insert_query = "INSERT INTO users VALUES(?, ?);"
    # username = 'sachito'
    # password = '33333333'
    # cursor.execute(insert_query,(username, password))
    # insert_query = "INSERT INTO users VALUES(?, ?);"
    # username = 'lolito'
    # password = '44444444'
    # cursor.execute(insert_query,(username, password))

    connection.commit()
    cursor.close()
except pyodbc.OperationalError as e:
    print("SQL Server connection error.")
except pyodbc.ProgrammingError as e:
    print("SQL statement error.")
finally:
    connection.close()
    print('Connection closed')


@app.route('/')
def home():
    print(f"Method used: {request.method}")
    pag.alert(text=f"Method used: {request.method}", title="Home Page")
    return render_template('index.html')


@app.route('/log-in', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        print(f"Method used: {request.method}")
        pag.alert(text=f"Method used: {request.method}", title="Login Page")
        return render_template('login.html')
    elif request.method == 'POST':
        print(f"Method used: {request.method}")
        pag.alert(text=f"Method used: {request.method}", title="Login Page")
        username = request.form['username']
        password = request.form['pwd']
        try:
            with open("users.txt", 'r') as file:
                for line in file:
                    elements = line.strip().split(',')
                    u = elements[0]
                    p = elements[1]
                    if (username == u) and (password == p):
                        return render_template('welcome.html', name=username)
            return redirect(url_for('login'))
        except FileNotFoundError:
            return redirect(url_for('home'))


@app.route('/sign-up', methods=['GET', 'POST'])
def signup():
    if request.method == 'GET':
        print(f"Method used: {request.method}")
        pag.alert(text=f"Method used: {request.method}", title="SignUp Page")
        return render_template('signup.html')
    elif request.method == 'POST':
        print(f"Method used: {request.method}")
        pag.alert(text=f"Method used: {request.method}", title="SignUp Page")
        username = request.form['username']
        password = request.form['pwd']
        email = request.form['email']
        try:
            # First method, inserting the new registration on a pre-chosen line.
            # Here I insert the new registration in the first line of the file.
            with open('users.txt', 'r') as file:
                lines = file.readlines()
            lines.insert(0,",".join([username, password, email+"\n"]))
            with open('users.txt', 'w') as file:
                file.writelines(lines)

            # Second method, appending the new registration on the last line each time
            # with open("users.txt", 'a') as file:
            #     file.write("\n")
            #     file.write(username)
            #     file.write(",")
            #     file.write(password)
            #     file.write(",")
            #     file.write(email)
            return redirect(url_for('login'))
        except FileNotFoundError:
            return redirect(url_for('home'))


if __name__ == '__main__':
    app.run(debug=True)
