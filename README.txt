# Set Up the MySQL Database:
   Import earthquake data into the MySQL database using the provided Python code:
	* Open a Python environment and execute the following code:

		!pip install pandas
		!pip install pymysql
		!pip install sqlalchemy
		import pandas as pd
		from sqlalchemy import create_engine

		data = pd.read_csv('path/to/your/database.csv')
		engine = create_engine("mysql+pymysql://rohan:password@localhost:3306/taskdb")
		data.to_sql('neic_earthquakes', con=engine, if_exists='replace', index=False)

# Adjust the database connection details (username, password, and host) in the create_engine function.

	Run the PySpark Application:
	Clone the Repository:

		git clone https://github.com/Rohan-11-11/Aidetic-Data-Engineer-Assignment.git
		cd your-repo

#  Open the aidetic_task.py file:

	Modify the script to match your MySQL connection properties if needed.
	username - rohan
	password - password

# Run the PySpark Application:

	Open a terminal in the repository directory.
	Execute the PySpark script:

	spark-submit --master local[*] aidetic_task.py

The script performs various analyses on the earthquake data and displays the results in the terminal.
