Instructions to activate Website:
1. have Python, Django, Scikit, and pandas installed
2. have a local copy of the project into a single project directory downloaded
3. open command line terminal
4. change directory into the project directory
5. run: python manage.py runserver

Instructions to operate website
1. go to link: http://127.0.0.1:8000/
2. select building from cards
3. select room, day of week, and day of month from drop down list
4. select "predict" butoon
5. enjoy results

If you run into an error where the model is not found
1. deactivate server from command line
2. run: python room_predictor.py --train
3. reactivate server and open website
