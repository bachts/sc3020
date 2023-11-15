import tkinter as tk
from tkinter import ttk
from functools import partial
import json
from PIL import ImageTk, Image
import explore as e

import psycopg2
from psycopg2 import sql

ws = '1000x800'
title = 'Query Explainer'

def starting_menu():
  
  '''Starting menu with button options'''
  
  global root
  
  root = tk.Tk()
  root.title(title)
  root.geometry(ws)
  
  text_frame = tk.LabelFrame()
  text_frame.pack(side='top', fill=None)
  
  tk.Label(text_frame, text='Very good query explainer v1.0').pack(pady=20)
  
  main_frame = ttk.Frame()
  main_frame.pack(side='top')
  ttk.Button(main_frame, text='Start Exploring', command=start).pack(pady=10)
  ttk.Button(main_frame, text='About', command=show_info).pack(pady=10)
  ttk.Button()
  ttk.Button(main_frame, text='Exit Program', command=exit_program).pack()
  
  root.mainloop()
  
def show_info():
  
  '''Information about the '''
  info = tk.Toplevel(root)
  info.geometry('200x200')
  
  ttk.Label(info, text='Information about the app here').pack()
  ttk.Button(info, text='Ok', command=partial(delete, info)).pack()
  
def delete(obj):
  obj.destroy()
  
def exit_program():
  
  exit_message = tk.Toplevel(root)
  # exit_message.geometry('200x200')
  l = ttk.LabelFrame(exit_message, text='Are you sure you want to quit?')
  l.pack()
  b1 = ttk.Button(l, text='Ok', command=partial(delete, root))
  b1.grid(row=0, column=0)
  b2 = ttk.Button(l, text='No Go Back', command=partial(delete, exit_message))
  b2.grid(row=0, column=1)
def start():
  
  delete(root)
  
  global conn
  global cursor
  
  conn = e.connect()
  cursor = conn.cursor()
  
  global start_window
  start_window = tk.Tk()
  start_window.title(title)
  start_window.geometry(ws)
  
  tables = e.get_database_tables(cursor)
  relations = tables.keys()
  # print(relations)
  
  ### Info of database relations columns
  
  db_info = ttk.Frame(start_window, relief='raised', width=200, height=1000)
  db_info.pack(expand=True, fill='both', side='left')
  
  # Create a treeview to display all relations
  tree = ttk.Treeview(db_info)
  tree.heading('#0', text='List of relations', anchor=tk.W)
  
  # Inserting relations into treeview
  for id, relation in enumerate(relations):
    tree.insert('', tk.END, text=relation, iid=id, open=False)
    for column, dtype in tables[relation]:
      tree.insert(id, tk.END, text=column + '-' + dtype, open=False)
  tree.pack()
  ttk.Button(db_info, text='Go Back', command=partial(start_to_root, start_window)).pack()
  
  
  ### Place to input SQL query
  
  query_menu = ttk.Frame(start_window, relief='groove')
  query_menu.pack(expand=True, fill='both', side='right')
  
  global tuple_output
  global query_viz  
  
  input_query = ttk.Frame(query_menu, relief='groove', width=800, height=300)
  tuple_output = ttk.Frame(query_menu, relief='groove', width=800, height=200)
  query_viz = ttk.Frame(query_menu, relief='groove', width=800, height=500)
  
  
  query_viz.grid(row=0, column=0)
  input_query.grid(row=1, column=0)
  tuple_output.grid(row=2, column=0)
  
  
  
  global entry 
  
  entry_text = ttk.Label(input_query, text='Your SQL query here:')
  entry = ttk.Entry(input_query)
  submit = ttk.Button(input_query, text='SUBMIT \n QUERY',command=partial(process_query))

  entry_text.grid(row=0, column=0)
  entry.grid(row=1, column=0)
  submit.grid(row=1, column=1)
  
  
  start_window.mainloop()


  
def process_query():
   
  query = entry.get()
  relations = e.extract_original_tables(query)
  if not relations:
    query_error()
    return
  tuples, tree = e.process(cursor, query)
  relation_details = e.display_blocks(relations, cursor)
  if not tuples or not tree:
    query_error()
    return
  
  slaves = query_viz.pack_slaves()
  for slave in slaves:
    slave.pack_forget()
  
  global diagram
  diagram = ttk.Treeview(query_viz)
  diagram.heading('#0', text='Visualization of the query', anchor=tk.W)
  diagram.pack()
  build_tree(tree['Plan'])
  
  print(tuples, tree)
  
def build_tree(tree):
  
  node_type = tree['Node Type']
  
  
  pass
def start_to_root(start_window):
  '''Go back to main menu and end the connection'''
  delete(start_window)
  e.disconnect(conn)
  starting_menu()
 
def query_error():
  error_message = tk.Toplevel(start_window)
  # start_window.wait_window(error_message)
  l = ttk.LabelFrame(error_message, text="Please check your \n query's syntax")
  l.pack()
  
  button = ttk.Button(l, text='Ok', command=partial(delete, error_message))
  button.pack()