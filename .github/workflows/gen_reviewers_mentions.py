import sys

VAR_NAME = "reviewers_mentions"

# Map from github username to Slack member id
MEMBER_ID_MAP = {
    'arunreturns': 'U086U81BPKR',
    'clifffrey': 'U085LGXFERJ',
    'craigsoules': 'U057KPPE0KB',
    'curiosthoth': 'U06P66F7B51',
    'egladysh': 'U07S4HBD553',
    'ella-springtail': 'U07ULFFS4GL',
    'garthgoodson': 'U05743KQR9B',
    'georgeszundi': 'U0579GF6XNY',
}


def main():
    if len(sys.argv) < 2:
        print(f"{VAR_NAME}=''", end="")
    else:
        names = sys.argv[1].split(",")
        ret = []
        for name in names:
            n = name.strip()
            if n in MEMBER_ID_MAP:
                ret.append(f'<@{MEMBER_ID_MAP[n]}>')
        print(f"{VAR_NAME}={' '.join(ret)}", end="")


if __name__ == '__main__':
    main()
