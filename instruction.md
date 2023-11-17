# SC3020
## Project 2 for NTU's database system principles

This is a tutorial on how to execute the project. 

The environment information is incorporated in ```environment.yaml``` for Conda environments, and ```requirements.txt``` for pip venv.

The program also assumes that you have a default web browser and an internet connection, which would be utilized by the file ```graphviz.py```

After loading and activating the environment, the project can be ran by running the file ```project.py``` in your preferred IDE, or in CLI: 
  
  - ```python3 project.py```
  - ```python project.py```

The instruction and overview about the algorithm and the GUI can be found in the project.

The database configuration can be modified in ```database.ini```, which would allow connection to different database schema, and carry out exploration on them.

The software should work well for non-nested queries and non-view queries, which are currently still limitations as creating a temporary view and aliasing them would be a difficulty for our algorithm implementation.



