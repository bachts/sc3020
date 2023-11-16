import networkx as nx
import explore as e
import json
import webbrowser
import requests


def open_web():
  data = e.loadjson()
  data = data['Plan']
  
  
  key = '$2a$10$xfz3Fvi9U71ZpHDC1fBZqe8AueMwan9A5NhKIg0L7SpgIX6ojqZ0W'
  url = 'https://api.jsonbin.io/v3/b'
  headers = {
    'Content-Type': 'application/json',
    'X-Master-Key': f'{key}',
    'X-Bin-Private': 'false',
  }
  
  read_url = f'https://api.jsonbin.io/v3/b'
  req = requests.post(url, json=data, headers=headers)
  
  json_string = json.dumps(json.dumps(req.text, indent=2))
  print(json_string)
  query = json.loads(req.text)
  print(type(query))
  print(query['metadata'])
  read_url = f"https://api.jsonbin.io/v3/b/{query['metadata']['id']}"
  
  url = f'https://jsoncrack.com/editor?json={read_url}'
  webbrowser.open(url)