# Set Up the MySQL Database:
1. Database Creation:
	create database taskdb;

	use taskdb;        # to use the taskdb as working database

2. Creating new table named (neic_earthquakes)

CREATE TABLE neic_earthquakes (
    Date DATE,
    Time TIME,
    Latitude FLOAT,
    Longitude FLOAT,
    Type VARCHAR(50),
    Depth FLOAT,
    Depth_Error FLOAT,
    Depth_Seismic_Stations INTEGER,
    Magnitude FLOAT,
    Magnitude_Type VARCHAR(50),
    Magnitude_Error FLOAT,
    Magnitude_Seismic_Stations INTEGER,
    Azimuthal_Gap FLOAT,
    Horizontal_Distance FLOAT,
    Horizontal_Error FLOAT,
    Root_Mean_Square FLOAT,
    ID INTEGER,
    Source VARCHAR(50),
    Location_Source VARCHAR(50),
    Magnitude_Source VARCHAR(50),
    Status VARCHAR(50)
);

3. Grant Table Access Permissions from Databse

	FLUSH PRIVILEGES;


4. Import earthquake data into the MySQL database using the provided Python code:
	* Open a Python environment and execute the following code:


		import pandas as pd
		from sqlalchemy import create_engine

		data = pd.read_csv('path/to/your/database.csv')
		engine = create_engine("mysql+pymysql://username:password@localhost:3306/taskdb")
		data.to_sql('neic_earthquakes', con=engine, if_exists='replace', index=False)

5. Adjust the database connection details (username, password, and host) in the create_engine function.

	Run the PySpark Application:
	Clone the Repository:

		git clone https://github.com/Rohan-11-11/Aidetic-Data-Engineer-Assignment.git
		cd your-repo

6.  Open the aidetic_task.py file:

	Modify the script to match your MySQL connection properties if needed.
	username - rohan
	password - password

7. Run the PySpark Application:

	Open a terminal in the repository directory.
	Execute the PySpark script:

	spark-submit --master local[*] aidetic_task.py

The script performs various analyses on the earthquake data and displays the results in the terminal.