# Install checklist

- [ ] Back up the current repository `.clinerules` directory outside the repo.
- [ ] Replace the old active rule Markdown files with the six rules in this kit. Do not leave duplicate/legacy rules active.
- [ ] Copy `tools/data_dictionary/` into the repo and inspect any filename collisions first.
- [ ] Copy `prompts/data-dictionary-evergreen.md` to a convenient repository location.
- [ ] Verify that the configured local database is exactly `localhost:15432 / pifitness_dev / dev` before using DB tools.
- [ ] Run offline inventory, repository-reference, and audit commands first.
- [ ] Run the included unit tests.
- [ ] Start Cline CLI interactively (not `--yolo`) and paste the evergreen prompt.
- [ ] Review the first database connection and output paths before continuing.
