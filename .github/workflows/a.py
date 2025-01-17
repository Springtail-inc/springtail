import sys
import os
import json

VAR_NAME = 'reviewers_mentions'
MEMBER_ID_MAP = json.loads(os.environ['MEMBER_ID_MAP'])

reviewers = sys.argv[1].split(',')
mentions = ' '.join([f'<@{MEMBER_ID_MAP[n.strip()]}> ' for n in reviewers if n.strip() in MEMBER_ID_MAP])
print(f'{VAR_NAME}={mentions}', end="")