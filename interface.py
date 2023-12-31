import tkinter as tk
from tkinter import ttk
from functools import partial
import json
from PIL import ImageTk, Image
import explore as e
from tkinter import font as tkFont  
import psycopg2
from psycopg2 import sql
from graphviz import open_web
import random
from ttkthemes import ThemedTk

ws = '1000x1000'
title = 'Query Explainer'

def starting_menu():
  
  '''Starting menu with button options'''
  
  global root
  
  root = tk.Tk()
  root.title(title)
  root.geometry('500x500')
  
  root.tk.call('source', 'Azure-ttk-theme-main/azure.tcl')
  root.tk.call('set_theme', 'dark')
  
  text_frame = ttk.LabelFrame()
  text_frame.pack(side='top', fill=None)
  
  ttk.Label(text_frame, text='Very good query explainer v1.0').pack(pady=20)
  ttk.Style().configure('Treeview',rowheight=30)
  main_frame = ttk.Frame()
  main_frame.pack(side='top')
  ttk.Button(main_frame, text='Start Exploring', command=start).pack(pady=10)
  # ttk.Button(main_frame, text='About', command=show_info).pack(pady=10)
  ttk.Button(main_frame, text='Exit Program', command=exit_program).pack()
  
  root.mainloop()

  
def delete(obj):
  # delete a tkinter object
  obj.destroy()
  
def exit_program():
  '''Exit from the program'''
  exit_message = tk.Toplevel(root)
  exit_message.tk.call('set_theme', 'dark')

  # exit_message.geometry('200x200')
  l = ttk.LabelFrame(exit_message, text='Are you sure you want to quit?')
  l.pack()
  b1 = ttk.Button(l, text='Ok', command=partial(delete, root))
  b1.grid(row=0, column=0)
  b2 = ttk.Button(l, text='No Go Back', command=partial(delete, exit_message))
  b2.grid(row=0, column=1)
def start():
  '''Start Exploration'''
  delete(root)
  
  global conn
  global cursor
  
  conn = e.connect()
  cursor = conn.cursor()
  
  global start_window
  start_window = tk.Tk()
  start_window.tk.call('source', 'Azure-ttk-theme-main/azure.tcl')
  start_window.tk.call('set_theme', 'dark')
  start_window.title(title)
  start_window.geometry(ws)
  
  global tables
  tables = e.get_database_tables(cursor)
  global relations
  relations = tables.keys()
  # print(relations)
  
  ### Info of database relations columns
  
  db_info = ttk.Frame(start_window, relief='raised', width=200, height=1000)
  db_info.place(x=0, y=0, width=200, height=1000)
  
  # Create a treeview to display all relations
  tree = ttk.Treeview(db_info)
  tree.heading('#0', text='List of relations', anchor=tk.W)
  tree.place(x=0, y=0, width=200, height=900)

  # Inserting relations into treeview
  for id, relation in enumerate(relations):
    tree.insert('', tk.END, text=relation, iid=id, open=False)
    for column, dtype in tables[relation]:
      tree.insert(id, tk.END, text=column + '-' + dtype, open=False)
  button = ttk.Button(db_info, text='Go Back', command=partial(start_to_root, start_window))
  button.place(x=0, y=900, height=100, width=200)
  scrollbar = ttk.Scrollbar(db_info, command=tree.yview)
  scrollbar.place(x=180, y=0, height=900, width=20)

  
  ### Place to input SQL query
  
  query_menu = ttk.Frame(start_window, relief='groove')
  query_menu.place(x=200, y=0, width=800, height=1000)
  
  global tuple_output
  global query_viz  
  
  input_query = ttk.Frame(query_menu, relief='groove', width=800, height=200)
  tuple_output = ttk.Frame(query_menu, relief='groove', width=800, height=300)
  query_viz = ttk.Frame(query_menu, relief='groove', width=800, height=500)
  
  
  query_viz.place(x=0, y=0)
  input_query.place(x=0, y=500)
  tuple_output.place(x=0, y=700)
  
  global diagram
  diagram = ttk.Treeview(query_viz)
  diagram.heading('#0', text='Visualization of the query')
  diagram.place(x=0, y=0, width=800, height=500)
  
  
  global entry 
  entry_text = ttk.Label(input_query, text='Your SQL query here:')
  entry = tk.Text(input_query)
  submit = ttk.Button(input_query, text='SUBMIT \n QUERY', command=partial(process_query))

  entry_text.place(x=0, y=0, height=20)
  entry.place(x=0, y=20, height=180, width=600)
  submit.place(x=600, y=20, height=180, width=200)
  
  
  start_window.mainloop()


  
def process_query():
  '''Process a query and update the UI accordingly'''
  query = entry.get('1.0', tk.END)
  # print(query)
  # relations = e.extract_original_tables(query)
  # # print(relations)
  # if not relations:
  #   query_error()
  #   return
  tuples, tree_dict, relations = e.process(cursor, query)
  
  if tuples=='wrong':
    print(tuples, tree_dict)
    query_error()
    return
  tuple_locations = e.get_unique_tuples(tuples, relations)
  # print(tuple_locations)
  relation_details = e.display_blocks(relations, cursor)
  
  
  populate_query_viz(tree_dict)
  populate_tuples(relation_details, tuple_locations)
  # print(relation_details)
  # print(tuples, tree)

def populate_tuples(relation_details, tuple_locations):
  '''Populate the frame tuple_output with data on the tuples
     Input: relation_details: output from display_blocks
            tuple_locations: output from get_unique_tuples'''
  
  print('Populating Tuples')
  import tksheet
  tabs = ttk.Notebook(tuple_output)
  for relation, content in relation_details.items():
    
    # Create a tab for each relations
    sub_tabs = ttk.Notebook(tabs)
    tabs.add(sub_tabs, text=relation)
    
    # Get the blocks accessed for that relation
    blocks_accessed = {}
    print(tuple_locations[relation])
    for row in tuple_locations[relation]:
      # print(row)
      if row[0] not in blocks_accessed.keys():
        blocks_accessed[row[0]] = []
      blocks_accessed[row[0]].append(row[1])
    # print(blocks_accessed[3698])
    if len(blocks_accessed.keys()) > 5:
      for block in random.sample(list(blocks_accessed.keys()), 5):
        tuples = content[block]
        tuples_number = blocks_accessed[block]
        min_num = tuples[0][0]
        for i in range(len(tuples_number)):
          tuples_number[i] -= min_num
        block_tab = ttk.Notebook(sub_tabs)
        sub_tabs.add(block_tab, text=f'ACC{block}')       
        headers = ['tuple_number']
        for i in tables[relation]:
          headers.append(i[0])
        sheet = tksheet.Sheet(block_tab, data=tuples, headers=headers, width=200, height=200)
        sheet.highlight_rows(rows = tuples_number, bg='yellow')  

        sheet.pack(fill='both')

    else:
      for block in blocks_accessed.keys(): 
        tuples = content[block]
        tuples_number = blocks_accessed[block]
        min_num = tuples[0][0]

        for i in range(len(tuples_number)):
          tuples_number[i] -= min_num
        print(tuples_number)
        block_tab = ttk.Notebook(sub_tabs)
        sub_tabs.add(block_tab, text=f'ACC{block}')
        headers = ['tuple_number']
        for i in tables[relation]:
          headers.append(i[0])
        sheet = tksheet.Sheet(block_tab, data=tuples, headers=headers, width=200, height=200)
        sheet.pack(fill='both')
        sheet.highlight_rows(rows = tuples_number, bg='yellow')  
    
    if len(content.items()) > 5 : # Sample 5 blocks only
      for block, tuples in random.sample(content.items(), 5):
        if block not in blocks_accessed.keys():
          block_tab = ttk.Notebook(sub_tabs)
          sub_tabs.add(block_tab, text=f'UNAC{block}')       
          headers = ['tuple_number']
          for i in tables[relation]:
            headers.append(i[0])
          sheet = tksheet.Sheet(block_tab, data=tuples, headers=headers, width=200, height=200)
          sheet.pack(fill='both')
    else:
      for block, tuples in zip(content.keys(), content.values()): 
        if block not in list(blocks_accessed.keys()): 
          block_tab = ttk.Notebook(sub_tabs)
          sub_tabs.add(block_tab, text=f'UNAC{block}')
          headers = ['tuple_number']
          for i in tables[relation]:
            headers.append(i[0])
          sheet = tksheet.Sheet(block_tab, data=tuples, headers=headers, width=200, height=200)
          sheet.pack(fill='both')
        
  print('Done')
  tabs.place(x=0, y=0, width=800)

def populate_query_viz(tree_dict):  
  '''Populate the frame query_viz
     Input: tree_dict: Query Plan in a JSON format, generated by Postgres EXPLAIN command'''
  slaves = query_viz.place_slaves()
  for slave in slaves:
    slave.place_forget()
  
  global diagram
  style = ttk.Style()
  style.configure('diagram.Treeview', rowheight=180)
  style.map('diagram.Treeview')
  
  
  diagram = ttk.Treeview(query_viz, style='diagram.Treeview')
  diagram.heading('#0', text='Visualization of the query')
  plans = tree_dict['Plan']
  desc = generate_description(plans)
  diagram.insert('', tk.END, text=desc, iid=0, open=False)
  build_tree(plans, 0)
  diagram.place(x=0, y=0, width=800, height=600)
  scrollbar = ttk.Scrollbar(query_viz, command=diagram.yview)
  diagram.configure(yscrollcommand=scrollbar.set)
  scrollbar.place(x=780, y=0, height=600, width=20)
  web_view = ttk.Button(query_viz, text='Online Version',command=open_web)
  web_view.place(x=600, y=100, height=50, width=130)
def build_tree(tree, parent_id):
  '''Recursively build Tkinter treeview based on the parent'''
  
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
  '''Generate Description for Treeview nodes based on the query plan'''
  desc = f"{plans['Node Type']}"
  if 'Join' in plans['Node Type'] or 'Nested Loop' in plans['Node Type']:
    desc = desc + f"\nJoin Type: {plans['Join Type']}"
    if plans['Node Type'] == 'Nested Loop':
      desc = desc + f"\nNumber of Loops: {plans['Actual Loops']}"
    if plans['Node Type'] == 'Hash Join':
      desc = desc + f"\nHash Cond: {plans['Hash Cond']}"
  if 'Aggregate' in plans['Node Type']:
    desc = desc + f"\nStrategy: {plans['Strategy']}"
    desc = desc + f"\nPartial Mode: {plans['Partial Mode']}"
    if 'Group Key' in plans.keys():
      desc = desc + f"\nGroup Key:"
      for key in plans['Group Key']:
        desc = desc + f"\n\t{key}"
    pass
  if 'Sort' in plans['Node Type']:
    desc = desc + f"\nSort Method: {plans['Sort Method']}\n\tSort Space Used: {plans['Sort Space Used']}\n\tSort Space Type: {plans['Sort Space Type']}"
    desc = desc + f"\nSort Keys:"
    for key in plans['Sort Key']:
      desc = desc + f"\n\t{key}"
  if 'Scan' in plans['Node Type']:
    desc = desc + f"\nRelation Name: {plans['Relation Name']}"
    if plans['Node Type']=='Index Scan':
      desc = desc + f"\n\tScan Direction: {plans['Scan Direction']}"
      desc = desc + f"\n\tIndex Name: {plans['Index Name']}"
      desc = desc + f"\n\tIndex Cond: {plans['Index Cond']}"
 
    if 'Filter' in plans.keys():
      desc = desc + f"\nFilter: {plans['Filter']}"
  if plans['Node Type']=='Hash':
    desc = desc + '\nHash Information'
    desc = desc + f"\n\tHash Buckets: {plans['Hash Buckets']}"
    desc = desc + f"\n\tHash Batches: {plans['Hash Batches']}"
    desc = desc + f"\n\tPeak Memory: {plans['Peak Memory Usage']}"


  desc = desc + f"\nTotal Cost: {plans['Total Cost']}\n{plans['Plan Rows']} rows and {plans['Plan Width']} width\nTime taken: {plans['Actual Total Time']}\nBlocks hit: {plans['Shared Hit Blocks']} Blocks Read: {plans['Shared Read Blocks']}"
  
  return desc

def get_all_children(tree, item=""):
  '''Get all of the children of a treeview item'''
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