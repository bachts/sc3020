import tkinter as tk
from PIL import ImageTk, Image

import psycopg2
from psycopg2 import sql

import explore

def starting_menu():
  global main_menu
  main_menu = tk.Tk()
  main_menu.title('App shit')
  main_menu.geometry('500x500')
  
  text_frame = tk.Frame()
  text_frame.pack(side='top', fill=None)
  
  # logo_frame = tk.Frame()
  # logo_frame.pack(side='top', pady=10)
  # img = ImageTk.PhotoImage(Image.open('png-transparent-postgresql-database-logo-database-symbol-blue-text-logo-thumbnail.png'), height=10, width=10)
  # tk.Label(logo_frame, image=img).pack()
  
  tk.Label(text_frame, text='Very good query explainer v1.0').pack(pady=20)
  
  selection_frame = tk.Frame()
  selection_frame.pack(side='top')
  tk.Button(selection_frame, text='Start Exploring', width=15, height=1, command=start()).pack(pady=10)
  tk.Button(selection_frame, text='About', width=10, height=1).pack(pady=10)
  
  main_menu.mainloop()
  
def start():
  pass



def display_blocks(locations,query,crsr):
    print("Displaying blocks")
    relation_details={}
    
    relations = explore.extract_original_tables(query)
    for relation in relations:        
        #OrderedDict(relation:[block][tuple_details])
        #[ [block1],[block2],...]
        
        query = sql.SQL(f'SELECT ctid,* FROM {relation} ORDER BY ctid')
        crsr.execute(query)
        rows = crsr.fetchall()
        
        block_content={}
        
        for row in rows:
            block,offset = list(map(int,row[0].strip('"()"').split(',')))
            tuple = [offset] + list(row[1:]) 
            if block not in block_content.keys():
                block_content[block]=[tuple]
            else:
                block_content[block].append(tuple)
        
            relation_details[relation]=block_content
    for relation,content in relation_details.items():
        for block,tuples in content.items():
            print(f'BLOCK {block}')
            for tuple in tuples: 
                print (tuple)
            print("#####################################################################################################################################################################")