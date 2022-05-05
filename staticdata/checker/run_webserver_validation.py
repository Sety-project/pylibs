from flask import Flask, request
# from pylibs.staticdata.checker.log_changes import LogTypes, LogChanges
from pylibs.staticdata.checker.update_static_in_db import UpdateStaticInDB
import json
import copy
import ast

HOST = 'http://127.0.0.1:5000'
app = Flask(__name__)
um = UpdateStaticInDB()

#def format_suggested_change(change):
#    if change["change_report"] == 'bookfield_amended':
#        return f'{change["change_report"]} bookid={change["bookid"]}  {change["bookfield"]}: {change["current"]} -> {change["suggested"]}'
#    else:
#        return change

def format_change(change):
    formated_change = copy.copy(change)
    formated_change.pop('_id')
    formated_change.pop('approved')
    formated_change.pop('refused')
    formated_change['change_report'] = str(formated_change['change_report']).upper()
    formated_change['ts'] = str(formated_change['ts']).split('T')[0]
    return formated_change

def push_approve_or_refuse(change, _id):
   return f'{format_change(change)}<br><a href="{HOST}/approve?id={_id}">approve</a> | <a href="{HOST}/refuse?id={_id}">refuse</a><p>'

def push_approve_or_refuse_all():
   return f'<br><a href="{HOST}/approve_all?">approve_all</a> | <a href="{HOST}/refuse_all?">refuse_all</a><p>'

#def push_approv_or_refuse(change, _id):
 #   if change["change_report"] == 'bookfield_amended':
  #      return f'<tr><td><a href="{HOST}/approve?id={_id}">approve</a></td><td><a href="{HOST}/refuse?id={_id}">refuse</a></td><td>{change["change_report"]}</td><td>{change["bookid"]}</td><td>{change["bookfield"]}</td><td>{change["current"]}</td><td>{change["suggested"]}</td><td></td></tr>'
   # else:
    #    return change
 # return f'{format_suggested_change(change)}<br><a href="{HOST}/approve?id={_id}">approve</a> | <a href="{HOST}/refuse?id={_id}">refuse</a><p>'

@app.route("/approve")
def approve():
    um.approve_single_change_in_db(request.args.get("id"))
    return get_html_headers() + 'The following change was approved: id=' + request.args.get("id") + '<p>' + get_proposed_changes() + get_html_footers()

@app.route("/refuse")
def refuse():
    um.refuse_single_change_in_db(request.args.get("id"))
    return get_html_headers() + 'The following change was refused: id=' + request.args.get("id") + '<p>' + get_proposed_changes() + get_html_footers()


@app.route("/approve_all")
def approve_all():
    um.pull_all_unprocessed_changes()
    um.approve_all_changes_in_db()
    return get_html_headers() + 'All changes approved' + get_html_footers()

@app.route("/refuse_all")
def refuse_all():
    um.pull_all_unprocessed_changes()
    um.refuse_all_changes_in_db()
    return get_html_headers() + 'All changes refused' + get_html_footers()


@app.route("/send_msg")
def change_return_msg():
    return 'The change was ' + request.args.get("content")

def get_proposed_changes():
    um.pull_all_unprocessed_changes()
    html = ''
    if len(um.get_all_unprocessed_changes())>0 :
        for id, change in um.get_all_unprocessed_changes().items():
            html += push_approve_or_refuse(change, id)
    return html

def get_approve_or_refuse_all():
    html = ''
    html += push_approve_or_refuse_all()
    return html

def get_table_header():
    return '<table>'
    #return '<table><tr><th>approve</th><th>refuse</th><th>change_type</th><th>bookid</th><th>field</th><th>current</th><th>suggested</th><th>other</th></tr>'

def get_html_headers():
    html = ''
    html += '<html><style>body { background-color:black; color:white } a:hover { color: orange; } a { color: cyan } table { border-collapse: collapse; border: 1 px solid grey; } tr:hover { background-color: orange; }</style><body>' + get_table_header()
    return html

def get_html_footers():
    html = ''
    html += '</table></body></html>'
    return html

@app.route('/mainpage')
def main_page():
    html = get_html_headers()
    html += get_approve_or_refuse_all()
    html += get_proposed_changes()
    html += get_html_footers()
    return html


