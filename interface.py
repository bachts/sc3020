import tkinter as tk
from PIL import ImageTk, Image

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