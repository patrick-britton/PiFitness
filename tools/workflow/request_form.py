#!/usr/bin/env python3
"""PiFitness Feature Request desktop form (tkinter, stdlib-only).

A friendly, single-file desktop GUI that captures a non-technical feature
request and writes `.features/descriptions/<id>.md` in one step.

No external dependencies. Runs with the project Python interpreter:

    python tools/workflow/request_form.py
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

SLUG_RE = re.compile(r"^[a-z][a-z0-9-]*$")
DEFAULT_DIR = Path(".features/descriptions").resolve()

FIELDS = (
    ("featureId", "Feature ID"),
    ("title", "Title"),
    ("problems", "Problem Statements (P1, P2…)"),
    ("desired", "Desired Behavior"),
    ("surfaces", "Affected Surfaces & Known Constraints"),
    ("nongoals", "Out of Scope / Non-Goals"),
)

CATEGORIES = ("New Capability", "Enhancement", "Bug Fix")


def validate(values: dict[str, str]) -> str | None:
    if not values.get("featureId"):
        return "Feature ID is required."
    if not SLUG_RE.match(values["featureId"]):
        return ("Feature ID must use lowercase letters, digits, hyphens only "
                "and must start with a letter (e.g. feat-music-search).")
    if not values.get("title"):
        return "Title is required."
    if not values.get("problems"):
        return "At least one problem statement is required."
    if not values.get("desired"):
        return "Desired behavior is required."
    return None


def build_markdown(values: dict[str, str]) -> str:
    lines = [
        "---",
        f"feature-id: {values['featureId']}",
        f"title: {values['title']}",
        f"category: {values['category']}",
        f"created: {date.today().isoformat()}",
        "---",
        "",
        f"# Feature: {values['title']}",
        "",
        "## 1. Summary / Users & Use Cases",
        f"- As a human developer, I want {values['title'].lower()}, so that the described problem is resolved.",
        "",
        "## 2. Problem / Motivation",
        values["problems"],
        "",
        "## 3. Desired Behavior",
        values["desired"],
        "",
        "## 4. Affected Surfaces & Known Constraints",
        values.get("surfaces") or "-",
        "",
        "## 5. Out of Scope / Non-Goals",
        values.get("nongoals") or "-",
        "",
    ]
    return "\n".join(lines)


class FeatureRequestForm(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("PiFitness — Feature Request")
        self.minsize(640, 560)
        self.category = tk.StringVar(value=CATEGORIES[0])
        self.entries: dict[str, tk.Widget] = {}
        self._build_ui()

    def _build_ui(self):
        container = ttk.Frame(self, padding=16)
        container.pack(fill=tk.BOTH, expand=True)

        ttk.Label(container, text="PiFitness — Feature Request", font=("Segoe UI", 12, "bold")).pack(anchor="w", pady=(0, 4))
        ttk.Label(
            container,
            text="Fill in the non-technical problem statement. Export writes `.features/descriptions/<id>.md`.",
            foreground="#666",
        ).pack(anchor="w", pady=(0, 12))

        form = ttk.Frame(container)
        form.pack(fill=tk.BOTH, expand=True)

        for i, (key, label) in enumerate(FIELDS):
            ttk.Label(form, text=label).grid(row=i, column=0, sticky="nw", padx=(0, 8), pady=4)
            if key in ("problems", "desired", "surfaces", "nongoals"):
                txt = tk.Text(form, width=56, height=4)
                txt.grid(row=i, column=1, sticky="nsew", pady=4)
                self.entries[key] = txt
            else:
                ent = ttk.Entry(form, width=60)
                ent.grid(row=i, column=1, sticky="ew", pady=4)
                self.entries[key] = ent

        ttk.Label(
            form,
            text="Lowercase letters, digits, hyphens only; must start with a letter.",
            foreground="#888",
            font=("Segoe UI", 8),
        ).grid(row=len(FIELDS), column=1, sticky="w", pady=(0, 8))

        ttk.Label(form, text="Category").grid(row=len(FIELDS) + 1, column=0, sticky="nw", padx=(0, 8), pady=4)
        cat_frame = ttk.Frame(form)
        cat_frame.grid(row=len(FIELDS) + 1, column=1, sticky="w", pady=4)
        for j, cat in enumerate(CATEGORIES):
            ttk.Radiobutton(cat_frame, text=cat, variable=self.category, value=cat).grid(row=0, column=j, padx=(0, 12))

        form.columnconfigure(1, weight=1)

        btns = ttk.Frame(container)
        btns.pack(fill=tk.X, pady=(16, 0))
        ttk.Button(btns, text="Export Request Markdown", command=self.export).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(btns, text="Copy Markdown to Clipboard", command=self.copy).pack(side=tk.LEFT)
        ttk.Button(btns, text="Clear", command=self.clear).pack(side=tk.RIGHT)

        self.status = ttk.Label(container, text="", foreground="#2e7d32")
        self.status.pack(anchor="w", pady=(12, 0))

    def get_values(self) -> dict[str, str]:
        values: dict[str, str] = {}
        for key, _ in FIELDS:
            widget = self.entries[key]
            if isinstance(widget, tk.Text):
                values[key] = widget.get("1.0", "end-1c").strip()
            else:
                values[key] = widget.get().strip()
        values["category"] = self.category.get()
        return values

    def export(self):
        values = self.get_values()
        err = validate(values)
        if err:
            messagebox.showerror("Validation error", err)
            return
        DEFAULT_DIR.mkdir(parents=True, exist_ok=True)
        path = filedialog.asksaveasfilename(
            title="Save feature request",
            initialdir=str(DEFAULT_DIR),
            initialfile=f"{values['featureId']}.md",
            defaultextension=".md",
            filetypes=[("Markdown", "*.md")],
        )
        if not path:
            return
        Path(path).write_text(build_markdown(values), encoding="utf-8")
        self.status.config(text=f"Saved: {path}", foreground="#2e7d32")

    def copy(self):
        values = self.get_values()
        err = validate(values)
        if err:
            messagebox.showerror("Validation error", err)
            return
        self.clipboard_clear()
        self.clipboard_append(build_markdown(values))
        self.status.config(text="Markdown copied to clipboard", foreground="#2e7d32")

    def clear(self):
        for key, _ in FIELDS:
            widget = self.entries[key]
            if isinstance(widget, tk.Text):
                widget.delete("1.0", "end")
            else:
                widget.delete(0, tk.END)
        self.category.set(CATEGORIES[0])
        self.status.config(text="")


if __name__ == "__main__":
    app = FeatureRequestForm()
    app.mainloop()
