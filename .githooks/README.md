This directory contains git hooks that are tracked with the repository, making sure that we have visibility 
on what hooks are being used. 
To enable them, run the following command:

```bash
$ git config core.hooksPath .githooks
```

or, if you do not want to change the hooksPath, simply make symbolic links to the hooks in this directory.

```bash
$ ln -s .githooks/pre-commit .git/hooks/pre-commit
```

If you'd like to skip the hook check, use the `--no-verify` flag with your git command.

```shell
$ git commit --no-verify -m "Your commit message"
```