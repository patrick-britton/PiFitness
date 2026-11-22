---
description: Diagnose the deployment error
---

# /deployment-error

There was an error in the most recent bootstrapping attempt.

# Context
- User will define the environment as part of the call -- either `/deployment-error test` or `/deployment-error prod`
- User ran the bootstrap via `/deploy/run-bootstrap.ps1` which calls a shell script on the remote Pi, which, in turn, calls `/deploy/scripts/autodeploy.sh`
- Upon completion, that script will regenerate `/deploy/bootstrap.log`

# Instructions
1. Read `/deploy/bootstrap.log`, identifying the error.
2. Diagnose the root cause of the error.
3. If the error is straightforward to fix, apply the fix, otherwise stop and describe the next steps for the user.
