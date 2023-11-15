import tkinter as tk
from tkinter import ttk
from functools import partial

from PIL import ImageTk, Image
import explore as e

import psycopg2
from psycopg2 import sql

ws = '1000x800'
title = 'Query Explainer'
def starting_menu():
  global root
  root = tk.Tk()
  root.title(title)
  root.geometry(ws)
  
  text_frame = tk.Frame()
  text_frame.pack(side='top', fill=None)
  
  # logo_frame = tk.Frame()
  # logo_frame.pack(side='top', pady=10)
  # img = ImageTk.PhotoImage(Image.open('png-transparent-postgresql-database-logo-database-symbol-blue-text-logo-thumbnail.png'), height=10, width=10)
  # tk.Label(logo_frame, image=img).pack()
  
  ttk.Title(text_frame, text='Very good query explainer v1.0').pack(pady=20)
  
  main_frame = ttk.Frame()
  main_frame.pack(side='top')
  ttk.Button(main_frame, text='Start Exploring', command=start).pack(pady=10)
  ttk.Button(main_frame, text='About', command=show_info).pack(pady=10)
  ttk.Button()
  ttk.Button(main_frame, text='Exit Program', command=partial(delete, root)).pack()
  
  root.mainloop()
  
def show_info():
  global info
  info = tk.Toplevel(root)
  info.geometry('200x200')
  
  ttk.Label(info, text='XD').pack()
  ttk.Button(info, text='Ok', command=partial(delete, info)).pack()
  
def delete(obj):
  obj.destroy()
  
def start():
  
  conn = e.connect()
  cursor = conn.cursor()
  
  start_window = tk.Tk()
  start_window.title(title)
  start_window.geometry(ws)
  start_window.pack()
  start_window.mainloop()



def display_blocks(locations,query,crsr):
    print("Displaying blocks")
    relation_details={}
    
    relations = e.extract_original_tables(query)
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