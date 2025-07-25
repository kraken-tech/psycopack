# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Psycopack"
copyright = "Kraken Technologies Limited"
author = "Kraken Tech"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx_rtd_theme",
    "sphinx_design",
]
html_theme = "sphinx_rtd_theme"

html_context = {
    "display_github": True,
    "github_user": "kraken-tech",
    "github_repo": "psycopack",
    "github_version": "main/docs/",
}
