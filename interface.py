import tkinter as tk
from tkinter import ttk
from functools import partial
import json
from PIL import ImageTk, Image
import explore as e
from tkinter import font as tkFont  
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
  ttk.Style().configure('Treeview',rowheight=30)
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
  db_info.place(x=0, y=0, width=200, height=800)
  
  # Create a treeview to display all relations
  tree = ttk.Treeview(db_info)
  tree.heading('#0', text='List of relations', anchor=tk.W)
  tree.place(x=0, y=0, width=200, height=700)

  # Inserting relations into treeview
  for id, relation in enumerate(relations):
    tree.insert('', tk.END, text=relation, iid=id, open=False)
    for column, dtype in tables[relation]:
      tree.insert(id, tk.END, text=column + '-' + dtype, open=False)
  button = ttk.Button(db_info, text='Go Back', command=partial(start_to_root, start_window))
  button.place(x=0, y=700, height=100, width=200)
  
  
  ### Place to input SQL query
  
  query_menu = ttk.Frame(start_window, relief='groove')
  query_menu.place(x=200, y=0, width=800, height=800)
  
  global tuple_output
  global query_viz  
  
  input_query = ttk.Frame(query_menu, relief='groove', width=800, height=200)
  tuple_output = ttk.Frame(query_menu, relief='groove', width=800, height=200)
  query_viz = ttk.Frame(query_menu, relief='groove', width=800, height=600)
  
  
  query_viz.place(x=0, y=0)
  input_query.place(x=0, y=600)
  tuple_output.place(x=0, y=800)
  
  
  global diagram
  diagram = ttk.Treeview(query_viz)
  diagram.heading('#0', text='Visualization of the query')
  diagram.place(x=0, y=0, width=800, height=600)
  
  
  global entry 
  entry_text = ttk.Label(input_query, text='Your SQL query here:')
  entry = ttk.Entry(input_query, width=70)
  submit = ttk.Button(input_query, text='SUBMIT \n QUERY',command=partial(process_query))

  entry_text.grid(row=0, column=0)
  entry.grid(row=1, column=0)
  submit.grid(row=1, column=1)
  
  
  start_window.mainloop()


  
def process_query():
   
  query = entry.get()
  print(query)
  relations = e.extract_original_tables(query)
  print(relations)
  if not relations:
    print('wtf?')
    query_error()
    return
  tuples, tree_dict = e.process(cursor, query)
  relation_details = e.display_blocks(relations, cursor)
  if not tuples or not tree_dict:
    print(tuples, tree_dict)
    query_error()
    return
  
  slaves = query_viz.place_slaves()
  for slave in slaves:
    slave.place_forget()
  
  global diagram
  style = ttk.Style()
  style.configure('diagram.Treeview', rowheight=100)
  style.map('diagram.Treeview')
  
  diagram = ttk.Treeview(query_viz, style='diagram.Treeview')
  diagram.heading('#0', text='Visualization of the query')
  details = tree_dict.items()
  plans = tree_dict['Plan']
  desc = generate_description(plans)
  diagram.insert('', tk.END, text=desc, iid=0, open=False)
  build_tree(plans, 0)
  diagram.place(x=0, y=0, width=800, height=600)
  
  # print(tuples, tree)
  
def build_tree(tree, parent_id):
  
  if 'Plans' not in tree.keys():
    return
  
  for plan in tree['Plans']:
    desc = generate_description(plan)
    
    current_id = len(get_all_children(diagram))
    diagram.insert(parent_id, tk.END, text=desc, iid=len(get_all_children(diagram)), open=False)
    if 'Plans' in plan.keys():
      build_tree(plan, current_id)
  
def start_to_root(start_window):
  '''Go back to main menu and end the connection'''
  delete(start_window)
  e.disconnect(conn)
  starting_menu()
 
def generate_description(plans):
  desc = f"{plans['Node Type']}\nTotal Cost: {plans['Total Cost']}\n {plans['Plan Rows']} rows and {plans['Plan Width']} width\nTime taken: {plans['Actual Total Time']}\nBlocks hit: {plans['Shared Hit Blocks']} Blocks Read: {plans['Shared Read Blocks']}"
  if 'Join' in plans['Node Type']:
    desc = desc + f"\nJoin Type: {plans['Join Type']}"
  if 'Aggregate' in plans['Node Type']:
    desc = desc + f"\nStrategy: {plans['Strategy']}"
    pass
  if 'Sort' in plans['Node Type']:
    desc = desc + f"\nSort Method: {plans['Sort Method']}\n\tSort Space Used: {plans['Sort Space Used']}\n\tSort Space Type: {plans['Sort Space Type']}"
    desc = desc + f"\nSort Keys:"
    for key in plans['Sort Key']:
      print(key)
      desc = desc + f"\n\t{key}"
      
  if 'Scan' in plans['Node Type']:
    desc = desc + f"\nRelation Name: {plans['Relation Name']}"
    desc = desc + f"\nFilter: {plans['Filter']}"
  
  return desc

def get_all_children(tree, item=""):
    children = tree.get_children(item)
    for child in children:
        children += get_all_children(tree, child)
    return children 
  
def query_error():
  error_message = tk.Toplevel(start_window)
  # start_window.wait_window(error_message)
  l = ttk.LabelFrame(error_message, text="Please check your \n query's syntax")
  l.pack()
  
  button = ttk.Button(l, text='Ok', command=partial(delete, error_message))
  button.pack()