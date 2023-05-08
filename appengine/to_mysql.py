from flask import Flask, request, render_template
import mysql.connector

app = Flask(__name__)

@app.route('/')
def form():
    return render_template('form.html')

@app.route('/submit', methods=['POST'])
def submit():
    # Retrieve the form data
    name = request.form['name']
    email = request.form['email']
    message = request.form['message']

    cnx = mysql.connector.connect(user='user', password='password',
                                  host='localhost', database='mydatabase')
    cursor = cnx.cursor()

    query = "INSERT INTO messages (name, email, message) VALUES (%s, %s, %s)"
    values = (name, email, message)
    cursor.execute(query, values)
    cnx.commit()

    cursor.close()
    cnx.close()

    # Return a success message
    return 'Form submitted successfully!'

if __name__ == '__main__':
    app.run()
