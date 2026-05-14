import mkdocs_gen_files
from pathlib import Path

DOCUMENT_PATHS = ["needle/config", "needle/lib", "needle/modules"]
nav = mkdocs_gen_files.Nav()

for path in sorted(Path("needle").rglob("*.py")):
    if path.name == "__init__.py":
        continue
    if not any(str(path).startswith(p) for p in DOCUMENT_PATHS):
        continue

    module_path = path.with_suffix("")
    doc_path = path.with_suffix(".md")
    full_doc_path = Path("api_reference") / doc_path  # files go in api/

    parts = list(module_path.parts)
    module_name = ".".join(parts)

    nav_path = ["API Reference"] + parts[1:]  # e.g.  API Reference/modules
    nav[nav_path] = str(full_doc_path)

    with mkdocs_gen_files.open(full_doc_path, "w") as f:
        f.write(f"::: {module_name}\n")

    mkdocs_gen_files.set_edit_path(full_doc_path, path)

with mkdocs_gen_files.open("SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
