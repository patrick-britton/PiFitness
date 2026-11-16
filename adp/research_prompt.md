I'm building out an automatic bootstrapper piece-by-piece and trying to stay as true to the `autodeploy_objectives.md` as possible.

I only ever test by running the pi shell script and relying on the reported output to fix. 

This is a personal-use, and single-developer application, so don't over engineer.

# Your Task
- The full bootstrapping script seems to be active, but the full test deployment did not meet expected criteria and was rolled back.
- Specifically, there is a known change where two buttons ("coffee" and "smoothie") should exist on the home page.
    - These changes do appear locally
    - When deployed to test and tested through a Chrome Browser on a smartphone connected to https://pifitness.duckdns.org, no buttons appeared.
    - While waiting for the "go" or "no go" determination, the polling script kept interfering with the output -- so, while polling can continue, it should cease outputting the poll message when on the final step.
    - Read through the evidence log `deploy/bootstrap.log` in full.
    - Ensure the step where all the registrations are properly updated is being included, so that the app is fully reachable outside of the local network.
    - There is a low-but-not-nonexistent chance the user was encountering browser cache issues.
- Prepare a full rootcause analysis and present the summary to the user. If everything checks out as expected and you believe its a caching issue, provide instructions for the user how to try a second deployment with the cache cleared.

# Ground rules:
1. autodeploy_objectives.md is the specification. Stay as true to it as possible. Change course only when something critical is discovered, and say so explicitly when you do.
2. Do not assume anything. If you need a file or a fact, ask for it. Say clearly what is verified and what is untested.
3. TEST and PRD must run the same code in the same kind of environment. The only intended differences are the branch pulled (handled by the launcher), the env file used, and TEST restoring previously active services. step_dependencies.py must contain no TEST/PRD branches. Anything that differs must come from values in the Pi env file (VENV_DIR, APP_DIR, ...). If you find yourself writing if environment == ..., stop and raise it with the user.
4. Minimal configuration. The user dislikes proliferating config keys and "self-referential paths". Values that never vary (for example where the app reads its env file) are constants in the helper. Things the script can work out at run time should be worked out at run time. Add a config key only when a value truly varies.
5. Guard against over-engineering. Prefer the canonical tools (uv, npm ci, systemctl, ...) over reimplementing them. If something in the objectives or in your plan looks like more machinery than it is worth, say so.
Be honest about what could not be tested.
6. If you have built new files, always inform the user which directory they go in. Do not produce files that do not belong in the repo (except updated objectives file)
7. If the user must execute manual steps, you must lay them out in clear, step-by-step, idiot-proof instructions, including which machine, file, and values to change or execute.

# Latest pi-shell/bootstrap output:
- can always be read at `deploy/bootstrap.log`
- all testing is always done through executing the pi-shell, and all failures & evidence are recorded to the log and pulled locally after an attempted deployment.