from prefect import Flow, Task, unmapped
from prefect.triggers import all_successful
from prefect.tasks.secrets import EnvVarSecret
from prefect.engine.results import LocalResult
from requests import session
from bs4 import BeautifulSoup
from cryptography.fernet import Fernet
import json
import prefect
import os

class MagicTask(Task):
    def run(self, magic_kw):
        f = Fernet(magic_kw)
        with open("data/header.csv", "rb") as file:
            encrypted_data = file.read()
        decrypted_data = f.decrypt(encrypted_data)
        data = json.loads(decrypted_data.decode())
        local_result.write(data, task_name=prefect.context.task_name)

class InitSessionTask(Task):
    def run(self):
        return session()

class GetLoginPageHtmlTask(Task):
    def run(self, session):
        return session.get("https://www.myprimobox.net/Login")

class FetchTokenFromLoginPage(Task):
    def run(self, html, name):
        soup = BeautifulSoup(html.text, 'html.parser')
        tokens = soup.find('input', attrs={"name": str(name)}).get("value")
        return tokens

class FetchCookiesSession(Task):
    def run(self, session):
        return session.cookies.get_dict()

class LoginToMyPrimobox(Task):
    def run(self, session, cookies_login, tokens, login, pwd):
        data_request = local_result.read(location="MagicTask").value
        data = data_request["login"]["data"]
        data['formLogin'] = str(tokens[0])
        data['login'] = str(login)
        data['password'] = str(pwd)
        data['_csrf'] = str(tokens[1])
        return session.post('https://www.myprimobox.net/callback', headers=data_request["login"]["header"], cookies=cookies_login, data=data)

class ListDossierTask(Task):
    def run(self, session, header, data, cookies, endpoint):
        return session.post('https://www.myprimobox.net/{}'.format(endpoint), headers=header, cookies=cookies, data=data)

class CloseSessionTask(Task):
    def run(self, session):
        return session()

# Result
local_result = LocalResult(dir=str(os.path.abspath(os.getcwd())), location='{task_name}')

# Flow
flow = Flow("Transfer payslip from Myprimobox to Dropbox", result=local_result)

# Secrets tasks
magic_kw = EnvVarSecret("MAGIC_KW_VAR")
login = EnvVarSecret("MYPRIMOBOX_USERNAME_VAR")
pwd = EnvVarSecret("MYPRIMOBOX_PWD_VAR")

# Tasks
magic_task = MagicTask()
init_session = InitSessionTask(trigger=all_successful)
get_login_html = GetLoginPageHtmlTask(trigger=all_successful)
fetch_tokens = FetchTokenFromLoginPage(trigger=all_successful)
cookies_session = FetchCookiesSession(trigger=all_successful)
login_primobox = LoginToMyPrimobox(trigger=all_successful)
# list_dossier = ListDossierTask(trigger=all_successful)

# Edges
flow.add_edge(upstream_task=magic_task, downstream_task=init_session)
flow.add_edge(upstream_task=init_session, downstream_task=get_login_html, key='session')
flow.add_edge(upstream_task=init_session, downstream_task=cookies_session, key='session')
flow.add_edge(upstream_task=init_session, downstream_task=login_primobox, key='session')
# flow.add_edge(upstream_task=login_primobox, downstream_task=list_dossier, key='session')

# Maps
result_tokens = fetch_tokens.map(name=["formLogin", "_csrf"], html=unmapped(get_login_html), upstream_tasks=[unmapped(init_session)], flow=flow)
task_ref = flow.get_tasks()[0]
# result_list = list_dossier.map(header=unmapped())


# Tasks dependencies
magic_task.set_upstream(task=magic_kw, flow=flow, key='magic_kw')
cookies_session.set_upstream(task=get_login_html, flow=flow)
login_primobox.set_upstream(task=result_tokens, flow=flow, key='tokens')
login_primobox.set_upstream(task=cookies_session, flow=flow, key='cookies_login')
login_primobox.set_upstream(task=login, flow=flow, key='login')
login_primobox.set_upstream(task=pwd, flow=flow, key='pwd')
login_primobox.set_upstream(task=magic_task, flow=flow)

# Run
flow.run()
# low.visualize()
