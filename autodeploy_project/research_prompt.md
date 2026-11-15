I'm building out an automatic bootstrapper piece-by-piece and trying to stay as true to the `autodeploy_objectives.md` as possible.

I only ever test by running the pi shell script and relying on the reported output to fix. 

This is a personal-use, and single-developer application, so don't over engineer.

Your task is to build undo_test_db.py -- or whatever else you think is the next appropriate step to take given the latest results from the shell output at the bottom of this file.


# Ground rules:
1. autodeploy_objectives.md is the specification. Stay as true to it as possible. Change course only when something critical is discovered, and say so explicitly when you do.
2. Do not assume anything. If you need a file or a fact, ask for it. Say clearly what is verified and what is untested.
3. TEST and PRD must run the same code in the same kind of environment. The only intended differences are the branch pulled (handled by the launcher), the env file used, and TEST restoring previously active services. step_dependencies.py must contain no 4. TEST/PRD branches. Anything that differs must come from values in the Pi env file (VENV_DIR, APP_DIR, ...). If you find yourself writing if environment == ..., stop and raise it with the user.
5. Minimal configuration. The user dislikes proliferating config keys and "self-referential paths". Values that never vary (for example where the app reads its env file) are constants in the helper. Things the script can work out at run time should be worked out at run time. Add a config key only when a value truly varies.
6. Guard against over-engineering. Prefer the canonical tools (uv, npm ci, systemctl, ...) over reimplementing them. If something in the objectives or in your plan looks like more machinery than it is worth, say so.
Be honest about what could not be tested.
7. If you have built new files, always inform the user which directory they go in. Do not produce files that do not belong in the repo (except updated objectives file)
8. If the user must execute manual steps, you must lay them out in clear, step-by-step, idiot-proof instructions, including which machine, file, and values to change or execute.

# Latest Shell output:
